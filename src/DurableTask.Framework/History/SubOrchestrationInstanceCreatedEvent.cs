﻿//  ----------------------------------------------------------------------------------
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
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for sub orchestration instance creation
    /// </summary>
    [DataContract]
    public class SubOrchestrationInstanceCreatedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new SubOrchestrationInstanceCreatedEvent with the supplied eventid
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        public SubOrchestrationInstanceCreatedEvent(int eventId)
            : base(eventId)
        {
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.SubOrchestrationInstanceCreated; }
        }

        /// <summary>
        /// Gets or sets the sub orchestration Name
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the sub orchestration Version
        /// </summary>
        [DataMember]
        public string Version { get; set; }

        /// <summary>
        /// Gets or sets the instance Id
        /// </summary>
        [DataMember]
        public string InstanceId { get; set; }

        /// <summary>
        /// Gets or sets the sub orchestration's serialized input
        /// </summary>
        [DataMember]
        public string Input { get; set; }
    }
}