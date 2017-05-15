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
    using System.Collections.Generic;

    /// <summary>
    /// An active tracking work item
    /// </summary>
    public class TrackingWorkItem
    {
        /// <summary>
        /// The instance id of this tracking work item
        /// </summary>
        public string InstanceId;

        /// <summary>
        /// The datetime this work item is locked until
        /// </summary>
        public DateTime LockedUntilUtc;

        /// <summary>
        /// The list of new messages to process tracking for
        /// </summary>
        public IList<TaskMessage> NewMessages;

        /// <summary>
        /// The session instance this tracking item is associated with
        /// </summary>
        public object SessionInstance;
    }
}