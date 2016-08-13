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

namespace DurableTask.Samples.Replat
{
    using System;
    using System.Threading.Tasks;

    public interface IManagementSqlOrchestrationTasks
    {
        Task<Application[]> GetApplicationNames(string subscriptionId);

        Task<bool> UpsertSubscriptionLock(string subscriptionId, bool isLocked);
    }

    public class ManagementSqlOrchestrationTasks : IManagementSqlOrchestrationTasks
    {
        public Task<Application[]> GetApplicationNames(string subscriptionId)
        {
            Application[] applications = new Application[20]
            {
                new Application() { Name = "App1", SiteName = "App1", Platform = RuntimePlatform.Node, Region = "West US" }, 
                new Application() { Name = "App2", SiteName = "App2", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App3", SiteName = "App3", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App4", SiteName = "App4", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App5", SiteName = "App5", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App6", SiteName = "App6", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App7", SiteName = "App7", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App8", SiteName = "App8", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App9", SiteName = "App9", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App10", SiteName = "App10", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App11", SiteName = "App11", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App12", SiteName = "App12", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App13", SiteName = "App13", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App14", SiteName = "App14", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App15", SiteName = "App15", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App16", SiteName = "App16", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App17", SiteName = "App17", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App18", SiteName = "App18", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App19", SiteName = "App19", Platform = RuntimePlatform.Node, Region = "West US" },
                new Application() { Name = "App20", SiteName = "App20", Platform = RuntimePlatform.Node, Region = "West US" },
            };

            return Task.FromResult(applications);
        }

        public Task<bool> UpsertSubscriptionLock(string subscriptionId, bool isLocked)
        {
            Console.WriteLine(string.Format(
                "Upsert Subscription Lock called for SubId '{0}' with value '{1}'", subscriptionId, isLocked));

            return Task.FromResult(true);
        }
    }
}
