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

namespace DurableTask.Core.History
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class for history events
    /// </summary>
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
    public abstract class HistoryEvent : IExtensibleDataObject
    {
        /// <summary>
        /// Creates a new history event with the supplied eventid
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        public HistoryEvent(int eventId)
        {
            EventId = eventId;
            IsPlayed = false;
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the event id
        /// </summary>
        [DataMember]
        public int EventId { get; private set; }

        /// <summary>
        /// Gets the isplayed status
        /// </summary>
        [DataMember]
        public bool IsPlayed { get; internal set; }

        /// <summary>
        /// Gets the event timestamp
        /// </summary>
        [DataMember]
        public DateTime Timestamp { get; internal set; }

        /// <summary>
        /// Gets the event type
        /// </summary>
        [DataMember]
        public virtual EventType EventType { get; private set; }

        /// <summary>
        /// Implementation for <see cref="IExtensibleDataObject.ExtensionData"/>.
        /// </summary>
        public ExtensionDataObject ExtensionData { get; set; }
    }
}