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
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for orchestration execution starting
    /// </summary>
    [DataContract]
    public class ExecutionStartedEvent : HistoryEvent
    {
        /// <summary>
        /// The orchestration instance for this event
        /// </summary>
        [DataMember] public OrchestrationInstance OrchestrationInstance;

        /// <summary>
        /// Creates a new ExecutionStartedEvent with the supplied parameters
        /// </summary>
        /// <param name="eventId">The evnet id of the history event</param>
        /// <param name="input">The serialized orchestration input </param>
        public ExecutionStartedEvent(int eventId, string input)
            : base(eventId)
        {
            Input = input;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.ExecutionStarted; }
        }

        /// <summary>
        /// Gets or sets the parent instance of the event 
        /// </summary>
        [DataMember]
        public ParentInstance ParentInstance { get; set; }

        /// <summary>
        /// Gets or sets the orchestration name
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the orchestration version
        /// </summary>
        [DataMember]
        public string Version { get; set; }

        /// <summary>
        /// Gets or sets the serialized input to the orchestration
        /// </summary>
        [DataMember]
        public string Input { get; set; }

        /// <summary>
        /// Gets or sets a dictionary of tags of string, string
        /// </summary>
        [DataMember]
        public IDictionary<string, string> Tags { get; set; }
    }
}