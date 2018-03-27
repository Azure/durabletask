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
    /// A history event for sub orchestration instance completion
    /// </summary>
    [DataContract]
    public class SubOrchestrationInstanceCompletedEvent : HistoryEvent
    {
        /// <summary>
        /// Create a new SubOrchestrationInstanceCompletedEvent with the supplied params
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        /// <param name="taskScheduledId">The scheduled parent instance event id</param>
        /// <param name="result">The serialized result</param>
        public SubOrchestrationInstanceCompletedEvent(int eventId, int taskScheduledId, string result)
            : base(eventId)
        {
            TaskScheduledId = taskScheduledId;
            Result = result;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.SubOrchestrationInstanceCompleted; }
        }

        /// <summary>
        /// Gets the scheduled parent instance event id
        /// </summary>
        [DataMember]
        public int TaskScheduledId { get; private set; }

        /// <summary>
        /// Get the serialized result
        /// </summary>
        [DataMember]
        public string Result { get; private set; }
    }
}