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
    using System.Collections;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class for history events
    /// </summary>
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    public abstract class HistoryEvent : IExtensibleDataObject
    {
        /// <summary>
        /// List of all event classes, for use by the DataContractSerializer
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<Type> KnownTypes()
        {
            yield return typeof(ExecutionStartedEvent);
            yield return typeof(ExecutionCompletedEvent);
            yield return typeof(ExecutionTerminatedEvent);
            yield return typeof(TaskCompletedEvent);
            yield return typeof(TaskFailedEvent);
            yield return typeof(TaskScheduledEvent);
            yield return typeof(SubOrchestrationInstanceCreatedEvent);
            yield return typeof(SubOrchestrationInstanceCompletedEvent);
            yield return typeof(SubOrchestrationInstanceFailedEvent);
            yield return typeof(TimerCreatedEvent);
            yield return typeof(TimerFiredEvent);
            yield return typeof(OrchestratorStartedEvent);
            yield return typeof(OrchestratorCompletedEvent);
            yield return typeof(EventSentEvent);
            yield return typeof(EventRaisedEvent);
            yield return typeof(ContinueAsNewEvent);
            yield return typeof(HistoryStateEvent);
        }

        /// <summary>
        /// Creates a new history event
        /// </summary>
        internal HistoryEvent()
        {
            IsPlayed = false;
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates a new history event with the supplied event id
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        protected HistoryEvent(int eventId)
        {
            EventId = eventId;
            IsPlayed = false;
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the event id
        /// </summary>
        [DataMember]
        public int EventId { get; internal set; }

        /// <summary>
        /// Gets the IsPlayed status
        /// </summary>
        [DataMember]
        public bool IsPlayed { get; set; }

        /// <summary>
        /// Gets the event timestamp
        /// </summary>
        [DataMember]
        public DateTime Timestamp { get; set; }

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