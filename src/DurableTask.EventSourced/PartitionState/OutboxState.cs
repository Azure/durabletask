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

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class OutboxState : TrackedObject, Backend.ISendConfirmationListener
    {
        [DataMember]
        public SortedList<long, Dictionary<uint, TaskMessageReceived>> Outbox { get; private set; } = new SortedList<long, Dictionary<uint, TaskMessageReceived>>();

        [DataMember]
        public long LastPersistedAck { get; set; } = -1;

        [IgnoreDataMember]
        public List<(long, uint)> CurrentAckBatch { get; set; } = new List<(long, uint)>();

        [IgnoreDataMember]
        public bool AckBatchInProgress { get; set; } = false;

        [IgnoreDataMember]
        public override string Key => "Outbox";

        protected override void Restore()
        {
            // re-send all messages as they could have been lost after the failure
            foreach (var kvp in Outbox)
            {
                foreach (var outmessage in kvp.Value.Values)
                {
                    Partition.Submit(outmessage, this);
                }
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
                this.CurrentAckBatch.Add((taskMessageReceived.OriginPosition, taskMessageReceived.OriginPartition));

                if (!this.AckBatchInProgress)
                {
                    this.Partition.TraceContext.Value = "SWorker";
                    this.Partition.Submit(new SentMessagesAcked()
                    {
                        PartitionId = this.Partition.PartitionId,
                        DurablySent = this.CurrentAckBatch.ToArray(),
                    });
                    this.CurrentAckBatch.Clear();
                    this.AckBatchInProgress = true;
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
            var toSend = new Dictionary<uint, TaskMessageReceived>();

            foreach (var message in evt.OrchestratorMessages)
            {
                var instanceId = message.OrchestrationInstance.InstanceId;
                var partitionId = this.Partition.PartitionFunction(instanceId);

                if (!toSend.TryGetValue(partitionId, out var outmessage))
                {
                    toSend[partitionId] = outmessage = new TaskMessageReceived()
                    {
                        PartitionId = partitionId,
                        OriginPartition = this.Partition.PartitionId,
                        OriginPosition = evt.QueuePosition,
                        TaskMessages = new List<TaskMessage>(),
                    };
                }
                outmessage.TaskMessages.Add(message);
            }

            if (!this.Partition.Settings.PartitionCommunicationIsExactlyOnce)
            {
                Outbox.Add(evt.QueuePosition, toSend);
            }

            foreach (var outmessage in toSend.Values)
            {
                Partition.Submit(outmessage, this.Partition.Settings.PartitionCommunicationIsExactlyOnce ? null : this);
            }
        }

        // OutgoingMessagesAcked

        public void Process(SentMessagesAcked evt, EffectTracker effect)
        {
            effect.ApplyTo(this);
        }

        public void Apply(SentMessagesAcked evt)
        {
            foreach(var (queuePosition, destination) in evt.DurablySent)
            {
                if (this.Outbox.TryGetValue(queuePosition, out var dictionary))
                {
                    dictionary.Remove(destination);
                }
            }

            if (this.CurrentAckBatch.Count == 0)
            {
                this.AckBatchInProgress = false;
            }
            else
            {
                this.Partition.Submit(new SentMessagesAcked()
                {
                    PartitionId = this.Partition.PartitionId,
                    DurablySent = this.CurrentAckBatch.ToArray(),
                });
                this.CurrentAckBatch.Clear();
            }
        }
    }
}
