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

namespace DurableTask.Core
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// An active instance / work item of a task activity
    /// </summary>
    public class TaskActivityWorkItem
    {
        /// <summary>
        /// The Id of the work work item, likely related to the task message
        /// </summary>
        public string Id;

        /// <summary>
        /// The datetime this work item is locked until
        /// </summary>
        public DateTime LockedUntilUtc;

        /// <summary>
        /// The task message associated with this work item
        /// </summary>
        public TaskMessage TaskMessage;

        /// <summary>
        /// Gets or sets the one-based delivery attempt number for the current activity work item.
        /// </summary>
        /// <remarks>
        /// This value does not represent the Activity attempt number of a Durable Task retry policy, and
        /// does not necessarily reflect the amount of times user code has been executed.
        /// A value of <c>null</c> indicates that the delivery attempt number is not available.
        /// </remarks>
        public long? DeliveryAttempt { get; set; }

        /// <summary>
        /// The TraceContext which is included on the queue.
        /// </summary>
        public TraceContextBase TraceContextBase;
    }
}