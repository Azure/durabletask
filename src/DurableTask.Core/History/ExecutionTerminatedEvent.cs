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
    /// A history event for orchestration abrupt termination
    /// </summary>
    [DataContract]
    public class ExecutionTerminatedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new ExecutionTerminatedEvent with the supplied params
        /// </summary>
        /// <param name="eventId">The eventid of the history event</param>
        /// <param name="input">The serialized input of the termination event</param>
        public ExecutionTerminatedEvent(int eventId, string input)
            : base(eventId)
        {
            Input = input;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.ExecutionTerminated; }
        }

        /// <summary>
        /// Gets or sets the serialized input for the the termination event
        /// </summary>
        [DataMember]
        public string Input { get; set; }
    }
}