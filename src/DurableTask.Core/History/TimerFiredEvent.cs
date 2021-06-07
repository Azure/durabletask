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
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for a timer firing
    /// </summary>
    [DataContract]
    public class TimerFiredEvent : HistoryEvent
    {
        // Private ctor for deserialization
        TimerFiredEvent()
            : base(-1)
        {
        }

        /// <summary>
        /// Creates a new TimerFiredEvent with the supplied event id
        /// </summary>
        /// <param name="eventId"></param>
        [Obsolete]
        public TimerFiredEvent(int eventId)
            : base(eventId)
        {
        }

        /// <summary>
        /// Creates a new <see cref="TimerFiredEvent"/> with the specified parameters.
        /// </summary>
        /// <param name="timerId">The ID of the corresponding <see cref="TimerCreatedEvent"/>.</param>
        /// <param name="fireAt">The time at which the timer fired.</param>
        public TimerFiredEvent(int timerId, DateTime fireAt)
            : base(-1)
        {
            if (timerId < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(timerId));
            }

            this.TimerId = timerId;
            this.FireAt = fireAt;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.TimerFired;

        /// <summary>
        /// Gets or sets the timer id
        /// </summary>
        [DataMember]
        public int TimerId { get; set; }

        /// <summary>
        /// Gets or sets datetime to fire
        /// </summary>
        [DataMember]
        public DateTime FireAt { get; set; }
    }
}