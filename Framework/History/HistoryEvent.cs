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

namespace DurableTask.History
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    [KnownType(typeof (ExecutionStartedEvent))]
    [KnownType(typeof (ExecutionCompletedEvent))]
    [KnownType(typeof (ExecutionTerminatedEvent))]
    [KnownType(typeof (TaskCompletedEvent))]
    [KnownType(typeof (TaskFailedEvent))]
    [KnownType(typeof (TaskScheduledEvent))]
    [KnownType(typeof (SubOrchestrationInstanceCreatedEvent))]
    [KnownType(typeof (SubOrchestrationInstanceCompletedEvent))]
    [KnownType(typeof (SubOrchestrationInstanceFailedEvent))]
    [KnownType(typeof (TimerCreatedEvent))]
    [KnownType(typeof (TimerFiredEvent))]
    [KnownType(typeof (OrchestratorStartedEvent))]
    [KnownType(typeof (OrchestratorCompletedEvent))]
    [KnownType(typeof (EventRaisedEvent))]
    [KnownType(typeof (ContinueAsNewEvent))]
    [KnownType(typeof (HistoryStateEvent))]
    public abstract class HistoryEvent
    {
        public HistoryEvent(int eventId)
        {
            EventId = eventId;
            IsPlayed = false;
            Timestamp = DateTime.UtcNow;
        }

        [DataMember]
        public int EventId { get; private set; }

        [DataMember]
        public bool IsPlayed { get; internal set; }

        [DataMember]
        public DateTime Timestamp { get; internal set; }

        [DataMember]
        public virtual EventType EventType { get; private set; }
    }
}