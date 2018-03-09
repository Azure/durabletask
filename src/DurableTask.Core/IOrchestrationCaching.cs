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
    using System.Threading.Tasks;
    using DurableTask.Core.Settings;

    /// <summary>
    /// Optional interface for <see cref="IOrchestrationService"/> implementations which can be used to enable orchestration caching.
    /// </summary>
    public interface IOrchestrationCaching
    {
        /// <summary>
        /// Fetch the runtime state for a particular orchestration work item.
        /// </summary>
        Task<OrchestrationRuntimeState> FetchOrchestrationRuntimeStateAsync(TaskOrchestrationWorkItem workItem);

        /// <summary>
        /// The cache settings for the orchestration dispatcher. Returning null will disable caching.
        /// </summary>
        OrchestrationInstanceCacheSettings OrchestrationCacheSettings { get; }
    }

}
