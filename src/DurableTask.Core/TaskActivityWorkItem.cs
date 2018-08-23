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
    }
}