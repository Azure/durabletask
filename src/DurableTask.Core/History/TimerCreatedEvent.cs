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
    using DurableTask.Core.Tracing;
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for a new timer creation
    /// </summary>
    [DataContract]
    public class TimerCreatedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new TimerCreatedEvent with the supplied event id
        /// </summary>
        /// <param name="eventId"></param>
        public TimerCreatedEvent(int eventId)
            : base(eventId)
        {
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.TimerCreated;

        /// <summary>
        /// Gets or sets the desired datetime to fire
        /// </summary>
        [DataMember]
        public DateTime FireAt { get; set; }

        /// <summary>
        /// The W3C trace context associated with this event.
        /// </summary>
        [DataMember]
        public DistributedTraceContext ParentTraceContext { get; set; }

        /// <summary>
        /// Gets the W3C distributed trace parent context from this event, if any.
        /// </summary>
        public bool TryGetParentTraceContext(out ActivityContext parentTraceContext)
        {
            if (this.ParentTraceContext?.TraceParent == null)
            {
                parentTraceContext = default;
                return false;
            }

            return ActivityContext.TryParse(
                this.ParentTraceContext.TraceParent,
                this.ParentTraceContext.TraceState,
                out parentTraceContext);
        }

        // Used for new orchestration and sub-orchestration
        internal void SetParentTraceContext(Activity traceActivity)
        {
            if (traceActivity != null)
            {
                this.ParentTraceContext = new DistributedTraceContext(
                    traceActivity.Id,
                    traceActivity.TraceStateString);
            }
        }
    }
}