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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ActivityWorkItemTests
    {
        [TestMethod]
        public async Task DeliveryAttemptComesFromDequeuedQueueMessage()
        {
            AzureStorageOrchestrationServiceSettings settings =
                TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = $"deliveryattempt{Guid.NewGuid():N}";

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateIfNotExistsAsync();
            await service.StartAsync();

            try
            {
                var instance = new OrchestrationInstance
                {
                    InstanceId = Guid.NewGuid().ToString("N"),
                    ExecutionId = Guid.NewGuid().ToString("N"),
                };
                var message = new TaskMessage
                {
                    OrchestrationInstance = instance,
                    Event = new TaskScheduledEvent(0, "TestActivity"),
                };

                await service.WorkItemQueue.AddMessageAsync(message, instance);

                using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                TaskActivityWorkItem firstDelivery =
                    await service.LockNextTaskActivityWorkItem(TimeSpan.FromSeconds(10), cancellation.Token);

                Assert.AreEqual(1L, firstDelivery.DeliveryAttempt);

                await service.AbandonTaskActivityWorkItemAsync(firstDelivery);

                TaskActivityWorkItem secondDelivery =
                    await service.LockNextTaskActivityWorkItem(TimeSpan.FromSeconds(10), cancellation.Token);

                Assert.AreEqual(2L, secondDelivery.DeliveryAttempt);
            }
            finally
            {
                await service.StopAsync();
                await service.DeleteAsync();
            }
        }
    }
}
