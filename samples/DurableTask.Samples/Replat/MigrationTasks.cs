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

    public interface IMigrationTasks
    {
        Task<bool> ExportSite(string id, string subscriptionId, Application application);

        Task<bool> ImportSite(string id, string subscriptionId, Application application);

        Task<bool> MigrateServerFarmConfig(string id, string subscriptionId, Application application);

        Task<bool> UpdateWebSiteHostName(string subscriptionId, Application application);

        Task<bool> WhitelistSubscription(string subscriptionId);

        Task<bool> CleanupPrivateStamp(string subscriptionId);

        Task<bool> EnableSubscription(string subscriptionId);

        Task<bool> DisableSubscription(string subscriptionId);

        Task<bool> UpdateTtl(string subscriptionId);
    }

    public class MigrationTasks : IMigrationTasks
    {
        public Task<bool> ExportSite(string id, string subscriptionId, Application application)
        {
            Console.WriteLine($"Export Site Called for SubId: '{subscriptionId}' with Id: '{id}'.  App Name: '{application.Name}'");

            return Task.FromResult(true);
        }

        public Task<bool> ImportSite(string id, string subscriptionId, Application application)
        {
            Console.WriteLine($"Import Site Called for SubId: '{subscriptionId}' with Id: '{id}'.  App Name: '{application.Name}'");

            return Task.FromResult(true);
        }

        public Task<bool> MigrateServerFarmConfig(string id, string subscriptionId, Application application)
        {
            Console.WriteLine($"Migrate ServerFarm Called for SubId: '{subscriptionId}' with Id: '{id}'.  App Name: '{application.Name}'");

            return Task.FromResult(true);
        }

        public Task<bool> UpdateWebSiteHostName(string subscriptionId, Application application)
        {
            Console.WriteLine($"Update Website Hostname Called for SubId: '{subscriptionId}'.  App Name: '{application.Name}'");

            return Task.FromResult(true);
        }

        public Task<bool> WhitelistSubscription(string subscriptionId)
        {
            Console.WriteLine($"WhitelistSubscription Called for SubId: '{subscriptionId}'.");

            return Task.FromResult(true);
        }

        public Task<bool> CleanupPrivateStamp(string subscriptionId)
        {
            Console.WriteLine($"CleanupPrivateStamp Called for SubId: '{subscriptionId}'.");

            return Task.FromResult(true);
        }

        public Task<bool> EnableSubscription(string subscriptionId)
        {
            throw new NotSupportedException();
        }

        public Task<bool> DisableSubscription(string subscriptionId)
        {
            throw new NotSupportedException();
        }

        public Task<bool> UpdateTtl(string subscriptionId)
        {
            Console.WriteLine($"UpdateTtl Called for SubId: '{subscriptionId}'.");

            return Task.FromResult(true);
        }
    }
}
