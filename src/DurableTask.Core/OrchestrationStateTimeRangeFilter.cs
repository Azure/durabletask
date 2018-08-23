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
    /// Filter for Orchestration State time range on a time range type
    /// </summary>
    public class OrchestrationStateTimeRangeFilter : OrchestrationStateQueryFilter
    {
        /// <summary>
        /// Gets or sets the StartTime for the filter
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the EndTime for the filter
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets the time range filter type for the filter
        /// </summary>
        public OrchestrationStateTimeRangeFilterType FilterType { get; set; }
    }
}