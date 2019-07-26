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
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class SessionsState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, Session> Sessions { get; private set; } = new Dictionary<string, Session>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataContract]
        internal class Session
        {
            [DataMember]
            public long SessionId { get; set; }

            [DataMember]
            public long BatchStartPosition { get; set; }

            [DataMember]
            public List<TaskMessage> Batch { get; set; }
        }

        [IgnoreDataMember]
        public override string Key => "Sessions";

        protected override void Restore()
        {
            // create work items for all sessions
            foreach(var kvp in Sessions)
            {
                OrchestrationWorkItem.EnqueueWorkItem(Partition, kvp.Key, kvp.Value);
            }
        }

        private void AddMessageToSession(TaskMessage message)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (this.Sessions.TryGetValue(instanceId, out var session))
            {
                session.Batch.Add(message);
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>() { message },
                    BatchStartPosition = 0
                };

                OrchestrationWorkItem.EnqueueWorkItem(Partition, instanceId, session);
            }
        }

        private void AddMessagesToSession(TaskMessage[] messages)
        {
            var instanceId = messages[0].OrchestrationInstance.InstanceId;

            if (this.Sessions.TryGetValue(instanceId, out var session))
            {
                session.Batch.AddRange(messages);
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = messages.ToList(),
                    BatchStartPosition = 0
                };

                OrchestrationWorkItem.EnqueueWorkItem(Partition, instanceId, session);
            }
        }

        // TaskMessageReceived

        public void Apply(TaskMessageReceived taskMessageReceived)
        {
            this.AddMessageToSession(taskMessageReceived.TaskMessage);
        }

        // ClientTaskMessagesReceived

        public void Apply(ClientTaskMessagesReceived evt)
        {
            this.AddMessagesToSession(evt.TaskMessages);
        }

        // CreationMessageReceived

        public void Apply(CreationRequestReceived creationRequestReceived)
        {
            this.AddMessageToSession(creationRequestReceived.TaskMessage);
        }

        // TimerFired

        public void Apply(TimerFired timerFired)
        {
            this.AddMessageToSession(timerFired.TimerFiredMessage);
        }

        // ActivityCompleted

        public void Apply(ActivityCompleted activityCompleted)
        {
            this.AddMessageToSession(activityCompleted.Response);
        }

        // BatchProcessed

        public void Scope(BatchProcessed evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (evt.State != null
                && this.Sessions.TryGetValue(evt.InstanceId, out var session)
                && session.SessionId == evt.SessionId
                && session.BatchStartPosition == evt.BatchStartPosition)
            {
                apply.Add(State.GetInstance(evt.InstanceId));

                apply.Add(State.GetHistory(evt.InstanceId));

                if (evt.OrchestratorMessages?.Count > 0)
                {
                    apply.Add(State.Outbox);
                }

                if (evt.ActivityMessages?.Count > 0)
                {
                    apply.Add(State.Activities);
                }

                if (evt.TimerMessages?.Count > 0)
                {
                    apply.Add(State.Timers);
                }

                apply.Add(this);
            }
        }

        public void Apply(BatchProcessed evt)
        {
            var session = this.Sessions[evt.InstanceId];

            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStartPosition += evt.BatchLength;

            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(evt.InstanceId);
            }
            else
            {
                // there are more messages. Prepare another work item.
                OrchestrationWorkItem.EnqueueWorkItem(Partition, evt.InstanceId, session);
            }
        }
    }
}
