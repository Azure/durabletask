﻿//  ----------------------------------------------------------------------------------
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
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for a task failure
    /// </summary>
    [DataContract]
    public class TaskFailedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new TaskFailedEvent with the supplied parameters
        /// </summary>
        /// <param name="eventId">The event id of the history event</param>
        /// <param name="taskScheduledId">The scheduled parent instance event id</param>
        /// <param name="reason">The task failure reason</param>
        /// <param name="details">Serialized details of the task failure</param>
        /// <param name="failureDetails">Structured details of the task failure.</param>
        public TaskFailedEvent(int eventId, int taskScheduledId, string? reason, string? details, FailureDetails? failureDetails)
            : base(eventId)
        {
            TaskScheduledId = taskScheduledId;
            Reason = reason;
            Details = details;
            FailureDetails = failureDetails;
        }

        /// <inheritdoc cref="TaskFailedEvent(int, int, string?, string?, FailureDetails?)"/>
        public TaskFailedEvent(int eventId, int taskScheduledId, string? reason, string? details)
            : this(eventId, taskScheduledId, reason, details, failureDetails: null)
        {
        }

        // Needed for deserialization
        private TaskFailedEvent()
            : base(-1)
        { }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.TaskFailed;

        /// <summary>
        /// Gets the scheduled parent instance event id
        /// </summary>
        [DataMember]
        public int TaskScheduledId { get; private set; }

        /// <summary>
        /// Gets the task failure reason
        /// </summary>
        [DataMember]
        public string? Reason { get; private set; }

        /// <summary>
        /// Gets details of the task failure
        /// </summary>
        [DataMember]
        public string? Details { get; private set; }

        /// <summary>
        /// Gets the structured details of the task failure.
        /// </summary>
        [DataMember]
        public FailureDetails? FailureDetails { get; private set; }
    }
}