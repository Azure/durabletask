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
    /// Class to hold conditions that should match to purge orchestration history
    /// </summary>
    public class PurgeInstanceCondition
    {
        /// <summary>
        /// Constructor for purge instance conditions
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges history grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges history less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        public PurgeInstanceCondition(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            this.CreatedTimeFrom = createdTimeFrom;
            this.CreatedTimeTo = createdTimeTo;
            this.RuntimeStatus = runtimeStatus;
        }

        /// <summary>
        /// CreatedTime of orchestrations. Purges history grater than this value.
        /// </summary>
        public DateTime CreatedTimeFrom { get; set; }

        /// <summary>
        /// CreatedTime of orchestrations. Purges history less than this value.
        /// </summary>
        public DateTime? CreatedTimeTo { get; set; }

        /// <summary>
        /// RuntimeStatus of orchestrations
        /// </summary>
        public IEnumerable<OrchestrationStatus> RuntimeStatus { get; set; }

    }
}