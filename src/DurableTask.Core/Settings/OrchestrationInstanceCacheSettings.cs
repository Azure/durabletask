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

namespace DurableTask.Core.Settings
{
    /// <summary>
    /// Settings to configure orchestration instance caching in the task orchestration dispatcher.
    /// </summary>
    public class OrchestrationInstanceCacheSettings
    {
        /// <summary>
        /// The maximum number of orchestration instances to cache.
        /// Setting to zero or less disables orchestration instance caching.
        /// </summary>
        public int MaxCachedInstances { get; set; }
    }
}
