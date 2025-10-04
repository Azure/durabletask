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
    /// Generic History event
    /// </summary>
    [DataContract]
    public class ExecutionRewoundEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new ExecutionRewoundEvent with the supplied event id and reason.
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        /// <param name="reason">The reason for the rewind event</param>
        public ExecutionRewoundEvent(int eventId, string reason)
            : base(eventId)
        {
            this.Reason = reason;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.ExecutionRewound;

        /// <summary>
        /// Gets or sets the reason for the rewind event.
        /// </summary>
        [DataMember]
        public string Reason { get; set; }


        /// <summary>
        /// Gets or sets the input of the orchestration being rewound
        /// </summary>
        [DataMember]
        public string Input { get; set; }

        /// <summary>
        /// Gets or sets the dictionary of tags of the orchestration being rewound
        /// </summary>
        [DataMember]
        public IDictionary<string, string> Tags { get; set; }

        /// <summary>
        /// Gets or sets the parent instance of the suborchestration being rewound
        /// </summary>
        [DataMember]
        public ParentInstance ParentInstance { get; set; }

        /// <summary>
        /// Gets or sets the name of the orchestration being rewound
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the version of the orchestration being rewound
        /// </summary>
        [DataMember]
        public string Version { get; set; }

        /// <summary>
        /// Gets or sets the instance ID of the orchestration being rewound
        /// </summary>
        [DataMember]
        public string InstanceId { get; set; }
    }
}