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

namespace DurableTask.AzureServiceFabric
{
    using DurableTask.Core;
    using DurableTask.Core.Settings;

    /// <summary>
    /// Provides settings for service fabric based custom provider implementations
    /// for <see cref="IOrchestrationService"/> and <see cref="IOrchestrationServiceClient"/>
    /// to be used in constructing <see cref="TaskHubWorker"/> and <see cref="TaskHubClient"/>.
    /// </summary>
    public sealed class FabricOrchestrationProviderSettings
    {
        /// <summary>
        /// Constructor. Initializes all settings to their default values.
        /// </summary>
        public FabricOrchestrationProviderSettings()
        {
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings()
            {
                MaxConcurrentOrchestrations = 1000,
                DispatcherCount = 10
            };

            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings()
            {
                MaxConcurrentActivities = 1000,
                DispatcherCount = 10
            };
        }

        /// <summary>
        ///     Settings to configure the Task Orchestration Dispatcher
        /// </summary>
        public TaskOrchestrationDispatcherSettings TaskOrchestrationDispatcherSettings { get; set; }

        /// <summary>
        ///     Settings to configure the Task Activity Dispatcher
        /// </summary>
        public TaskActivityDispatcherSettings TaskActivityDispatcherSettings { get; set; }
    }
}
