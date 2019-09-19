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

namespace TestApplication.StatefulService
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric;
    using DurableTask.Test.Orchestrations.Performance;

    using TestApplication.Common.Orchestrations;
    using TestApplication.Common.OrchestrationTasks;


    /// <inheritdoc/>
    public class TestOrchestrationsProvider
    {
        /// <inheritdoc/>
        public FabricOrchestrationProviderSettings GetFabricOrchestrationProviderSettings()
        {
            var settings = new FabricOrchestrationProviderSettings();
            settings.TaskOrchestrationDispatcherSettings.DispatcherCount = 5;
            settings.TaskActivityDispatcherSettings.DispatcherCount = 5;
            return settings;
        }

        /// <inheritdoc/>
        public void RegisterOrchestrations(TaskHubWorker taskHubWorker)
        {
            taskHubWorker
                .AddTaskOrchestrations(this.GetOrchestrationTypes().ToArray())
                .AddTaskOrchestrations(this.GetTaskOrchestrations().Select(instance => new DefaultObjectCreator<TaskOrchestration>(instance.Value)).ToArray())
                .AddTaskActivitiesFromInterface<IUserTasks>(new UserTasks())
                .AddTaskActivitiesFromInterface<ITestTasks>(new TestTasks())
                .AddTaskActivities(GetActivityTypes().ToArray());
        }

        /// <inheritdoc/>
        private IEnumerable<Type> GetActivityTypes()
        {
            return new Type[]
            {
                typeof(RandomTimeWaitingTask),
                typeof(ExecutionCountingActivity)
            };
        }

        /// <inheritdoc/>
        private IEnumerable<Type> GetOrchestrationTypes()
        {
            return new Type[]
            {
                typeof(SimpleOrchestrationWithTasks),
                typeof(SimpleOrchestrationWithTimer),
                typeof(SimpleOrchestrationWithSubOrchestration),
                typeof(DriverOrchestration),
                typeof(TestOrchestration),
                typeof(ExecutionCountingOrchestration)
            };
        }

        /// <inheritdoc/>
        private IEnumerable<KeyValuePair<string, TaskOrchestration>> GetTaskOrchestrations()
        {
            yield return new KeyValuePair<string, TaskOrchestration>(typeof(OrchestrationRunningIntoRetry).Name, new OrchestrationRunningIntoRetry());
        }
    }
}
