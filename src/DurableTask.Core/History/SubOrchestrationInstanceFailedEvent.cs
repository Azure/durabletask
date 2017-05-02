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
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for a sub orchestration instance failure
    /// </summary>
    [DataContract]
    public class SubOrchestrationInstanceFailedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new SubOrchestrationInstanceFailedEvent with the supplied params
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        /// <param name="taskScheduledId">The scheduled parent instance event id</param>
        /// <param name="reason">The sub orchestration failure reason</param>
        /// <param name="details">Details of the sub orchestration failure</param>
        public SubOrchestrationInstanceFailedEvent(int eventId, int taskScheduledId, string reason, string details)
            : base(eventId)
        {
            TaskScheduledId = taskScheduledId;
            Reason = reason;
            Details = details;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.SubOrchestrationInstanceFailed; }
        }

        /// <summary>
        /// Gets the scheduled parent instance event id
        /// </summary>
        [DataMember]
        public int TaskScheduledId { get; private set; }

        /// <summary>
        /// Gets the sub orchestration failure reason
        /// </summary>
        [DataMember]
        public string Reason { get; private set; }

        /// <summary>
        /// Gets the details of the sub orchestration failure
        /// </summary>
        [DataMember]
        public string Details { get; private set; }
    }
}