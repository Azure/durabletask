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
    /// History state event
    /// </summary>
    [DataContract]
    public class HistoryStateEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new HistoryStateEvent with the supplied eventid and state
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        /// <param name="state">The event state</param>
        public HistoryStateEvent(int eventId, OrchestrationState state)
            : base(eventId)
        {
            State = state;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.HistoryState;

        /// <summary>
        /// Gets the orchestration state
        /// </summary>
        [DataMember]
        public OrchestrationState State { get; set; }
    }
}