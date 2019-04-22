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
    /// A history event for continue-as-new
    /// </summary>
    [DataContract]
    public class ContinueAsNewEvent : ExecutionCompletedEvent
    {
        /// <summary>
        /// Creates a new ExecutionStartedEvent with the supplied parameters
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        /// <param name="input">The serialized orchestration input</param>
        public ContinueAsNewEvent(int eventId, string input)
            : base(eventId, input, OrchestrationStatus.ContinuedAsNew)
        {
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.ContinueAsNew;
    }
}