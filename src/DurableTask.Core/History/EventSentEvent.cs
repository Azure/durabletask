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
    /// A history event for event sent
    /// </summary>
    [DataContract]
    public class EventSentEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new <see cref="EventSentEvent"/> with the supplied event id.
        /// </summary>
        /// <param name="eventId">The ID of the event.</param>
        public EventSentEvent(int eventId)
            : base(eventId)
        {
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.EventSent;

        /// <summary>
        /// The orchestration instance ID for this event
        /// </summary>
        [DataMember]
        public string InstanceId { get; set; }

        /// <summary>
        /// Gets or sets the orchestration name
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the serialized payload of the event
        /// </summary>
        [DataMember]
        public string Input { get; set; }
    }
}