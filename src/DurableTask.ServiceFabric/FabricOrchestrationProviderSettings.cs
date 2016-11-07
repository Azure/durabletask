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

namespace DurableTask.ServiceFabric
{
    using DurableTask.Settings;

    public sealed class FabricOrchestrationProviderSettings
    {
        public FabricOrchestrationProviderSettings()
        {
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings()
            {
                MaxConcurrentOrchestrations = 1000,
                DispatcherCount = 1
            };

            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings()
            {
                MaxConcurrentActivities = 10000,
                DispatcherCount = 1
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
