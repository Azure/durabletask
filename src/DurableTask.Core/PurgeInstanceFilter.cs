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
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Criteria for selecting orchestration instances to purge.
    /// </summary>
    public class PurgeInstanceFilter
    {
        /// <summary>
        /// Constructor for purge instance conditions
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges state of orchestrations created after this time.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges state of orchestrations created before this time.</param>
        /// <param name="runtimeStatus">The runtime status of the orchestrations to purge.</param>
        public PurgeInstanceFilter(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus>? runtimeStatus)
        {
            this.CreatedTimeFrom = createdTimeFrom;
            this.CreatedTimeTo = createdTimeTo;
            this.RuntimeStatus = runtimeStatus;
        }

        /// <summary>
        /// CreatedTime of orchestrations. Purges state of orchestrations created after this time.
        /// </summary>
        public DateTime CreatedTimeFrom { get; }

        /// <summary>
        /// CreatedTime of orchestrations. Purges state of orchestrations created before this time.
        /// </summary>
        public DateTime? CreatedTimeTo { get; }

        /// <summary>
        /// The runtime status of the orchestrations to purge.
        /// </summary>
        public IEnumerable<OrchestrationStatus>? RuntimeStatus { get; }
    }
}