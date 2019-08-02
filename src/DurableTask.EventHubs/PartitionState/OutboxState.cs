//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class OutboxState : TrackedObject, Backend.ISendConfirmationListener
    {
        [DataMember]
        public SortedList<long, List<TaskMessage>> Outbox { get; private set; } = new SortedList<long, List<TaskMessage>>();

        [DataMember]
        public long LastPersistedAck { get; set; } = -1;

        [IgnoreDataMember]
        public long LastNonPersistedAck { get; set; } = -1;

        [IgnoreDataMember]
        public override string Key => "Outbox";

        public long GetLastAckedQueuePosition() { return LastPersistedAck; }

        protected override void Restore()
        {
            // re-send all messages as they could have been lost after the failure
            foreach (var kvp in Outbox)
            {
                this.Send(kvp.Key, kvp.Value);
            }
        }

        private void Send(long queuePosition, List<TaskMessage> messages)
        {
            var toSend = new Dictionary<uint, TaskMessageReceived>();

            foreach (var message in messages)
            {
                var instanceId = message.OrchestrationInstance.InstanceId;
                var partitionId = this.Partition.PartitionFunction(instanceId);

                if (!toSend.TryGetValue(partitionId, out var outmessage))
                {
                    toSend[partitionId] = outmessage = new TaskMessageReceived()
                    {
                        PartitionId = partitionId,
                        OriginPartition = this.Partition.PartitionId,
                        OriginPosition = queuePosition,
                        TaskMessages = new List<TaskMessage>(),
                    };
                }
                outmessage.TaskMessages.Add(message);
            }

            foreach (var outmessage in toSend.Values)
            {
                Partition.Submit(outmessage, this);
            }
        }

        public void ConfirmDurablySent(Event evt)
        {
            if (evt is TaskMessageReceived taskMessageReceived)
            {
                bool loopbackAlreadyUnderway = this.LastPersistedAck < this.LastNonPersistedAck;

                System.Diagnostics.Debug.Assert(this.LastNonPersistedAck < taskMessageReceived.OriginPosition);

                this.LastNonPersistedAck = taskMessageReceived.OriginPosition;
                 
                if (!loopbackAlreadyUnderway)
                {
                    this.Partition.TraceContext.Value = "SWorker";
                    this.Partition.Submit(new SentMessagesAcked()
                    {
                        PartitionId = this.Partition.PartitionId,
                        LastAckedQueuePosition = LastNonPersistedAck,
                    });
                }
            }
        }

        public void ReportSenderException(Event evt, Exception e)
        {
            // this should never be called because all events sent by partitions are at-least-once
            throw new NotImplementedException();
        }

        // BatchProcessed

        public void Apply(BatchProcessed evt)
        {
            Outbox.Add(evt.QueuePosition, evt.OrchestratorMessages);
            this.Send(evt.QueuePosition, evt.OrchestratorMessages);
        }

        // OutgoingMessagesAcked

        public void Process(SentMessagesAcked evt, EffectTracker effect)
        {
            if (Outbox.Count > 0 && Outbox.First().Key < evt.LastAckedQueuePosition)
            {
                effect.ApplyTo(this);
            }
        }

        public void Apply(SentMessagesAcked evt)
        {
            while (Outbox.Count > 0)
            {
                var first = Outbox.First();

                if (first.Key < evt.LastAckedQueuePosition)
                {
                    Outbox.Remove(first.Key);
                }
                else
                {
                    break;
                }
            }

            LastPersistedAck = evt.LastAckedQueuePosition;

            if (LastPersistedAck < LastNonPersistedAck)
            {
                // more messages were acked and we still have to persist that fact 
                this.Partition.Submit(new SentMessagesAcked()
                {
                    PartitionId = this.Partition.PartitionId,
                    LastAckedQueuePosition = LastNonPersistedAck,
                });
            }
        }

    }
}
