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
    /// Generic History event
    /// </summary>
    [DataContract]
    public class GenericEvent : HistoryEvent
    {
        /// <summary>
        /// String data for this event
        /// </summary>
        [DataMember] public string Data;

        /// <summary>
        /// Creates a new GenericEvent with the supplied eventid and data
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        /// <param name="data">The data for the event</param>
        public GenericEvent(int eventId, string data)
            : base(eventId)
        {
            Data = data;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType
        {
            get { return EventType.GenericEvent; }
        }
    }
}