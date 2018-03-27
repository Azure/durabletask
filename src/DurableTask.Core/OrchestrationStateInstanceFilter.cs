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
    /// <summary>
    ///     Filter for Orchestration instance filter
    /// </summary>
    public class OrchestrationStateInstanceFilter : OrchestrationStateQueryFilter
    {
        /// <summary>
        ///     Creates a new instance of the OrchestrationStateInstanceFilter with default settings
        /// </summary>
        public OrchestrationStateInstanceFilter()
        {
            // default is exact match
            StartsWith = false;
        }

        /// <summary>
        ///     Gets or sets the InstanceId for the filter
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        ///     Gets or sets the ExecutionId for the filter
        /// </summary>
        public string ExecutionId { get; set; }

        /// <summary>
        ///     Gets or sets the match type of either starts with or exact match for the filter
        /// </summary>
        public bool StartsWith { get; set; }
    }
}