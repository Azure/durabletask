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

namespace DurableTask.EventHubs.Tests
{
    using DurableTask.Core;
    using System;
    using System.Threading.Tasks;
    using Xunit;

    public class OrchestrationTests
    {
        [Fact]
        public async Task RunNoOpOrchestration()
        {
            var settings = new EventHubsOrchestrationServiceSettings();
            settings.TaskHubName = "EventHubsTesting";
            settings.EventHubConnectionString = "Endpoint=sb://cgillum-eventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;EntityPath=test-hub;SharedAccessKey=[redacted]";
            settings.StorageConnectionString = "UseDevelopmentStorage=true";

            var service = new EventHubsOrchestrationService(settings);
            await service.CreateIfNotExistsAsync();

            var worker = new TaskHubWorker(service);
            worker.AddTaskOrchestrations(typeof(NoOpOrchestration));
            
            try
            {
                var client = new TaskHubClient(service);
                var instance = await client.CreateOrchestrationInstanceAsync(
                    typeof(NoOpOrchestration),
                    input: null);
                await worker.StartAsync();
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(30));                
            }
            finally
            {
                await worker.StopAsync(true);
                await service.DeleteAsync();
            }
        }

        class NoOpOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input) 
                => Task.FromResult("nop");
        }
    }
}
