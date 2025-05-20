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
#nullable enable
namespace DurableTask.Core.History
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// A history event for a new task scheduled
    /// </summary>
    [DataContract]
    public class TaskScheduledEvent : HistoryEvent, ISupportsDurableTraceContext
    {
        // Private ctor for JSON deserialization (required by some storage providers and out-of-proc executors)
        TaskScheduledEvent()
            : base(-1)
        {
        }

        /// <summary>
        /// Creates a new <see cref="TaskScheduledEvent"/> with the supplied event ID.
        /// </summary>
        /// <param name="eventId">The ID of the history event.</param>
        /// <param name="name">The name of the scheduled task activity.</param>
        /// <param name="version">The version of the scheduled task activity.</param>
        /// <param name="input">The input of the activity task.</param>
        public TaskScheduledEvent(
            int eventId,
            string name,
            string? version = null,
            string? input = null)
            : base(eventId)
        {
            this.Name = name;
            this.Version = version;
            this.Input = input;
        }

        /// <summary>
        /// Creates a new TaskScheduledEvent with the supplied event id
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        public TaskScheduledEvent(int eventId)
            : base(eventId)
        {
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.TaskScheduled;

        /// <summary>
        /// Gets or sets the orchestration Name
        /// </summary>
        [DataMember]
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the orchestration Version
        /// </summary>
        [DataMember]
        public string? Version { get; set; }

        /// <summary>
        /// Gets or sets the task's serialized input
        /// </summary>
        [DataMember]
        public string? Input { get; set; }

        /// <summary>
        /// The W3C trace context associated with this event.
        /// </summary>
        [DataMember]
        public DistributedTraceContext? ParentTraceContext { get; set; }
        
        /// <summary>
        /// Gets or sets a dictionary of tags of string, string
        /// </summary>
        [DataMember]
        public IDictionary<string, string>? Tags { get; set; }
    }
}