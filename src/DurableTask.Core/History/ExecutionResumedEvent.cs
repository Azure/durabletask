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

namespace DurableTask.Core.History
{
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for orchestration resuming
    /// </summary>
    [DataContract]
    public class ExecutionResumedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new ExecutionResumedEvent with the supplied params
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        /// <param name="reason">The serialized reason of the resuming event</param>
        public ExecutionResumedEvent(int eventId, string reason)
            : base(eventId)
        {
            Reason = reason;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.ExecutionResumed;

        /// <summary>
        /// Gets or sets the reason for the resuming event
        /// </summary>
        [DataMember]
        public string Reason { get; set; }
    }
}