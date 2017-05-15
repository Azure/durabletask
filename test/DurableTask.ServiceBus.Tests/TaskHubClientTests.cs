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

namespace DurableTask.ServiceBus.Tests
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.ServiceBus.Settings;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TaskHubClientTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        [TestMethod]
        public async Task TestCreateIfNew()
        {
            taskHub = TestHelpers.CreateTaskHub();
            var service = taskHub.orchestrationService as ServiceBusOrchestrationService;
            Assert.IsNotNull(service);

            await service.CreateAsync();
            await service.CreateIfNotExistsAsync();
            Assert.IsTrue(await service.HubExistsAsync());
            await service.DeleteAsync();
            Assert.IsFalse(await service.HubExistsAsync());
        }

        [TestMethod]
        public async Task TestOrchestrationCount()
        {
            taskHub = TestHelpers.CreateTaskHub();
            client = TestHelpers.CreateTaskHubClient();
            var service = taskHub.orchestrationService as ServiceBusOrchestrationService;
            Assert.IsNotNull(service);
            await service.CreateAsync();
            await client.CreateOrchestrationInstanceAsync("foo", "1.0", null);
            await client.CreateOrchestrationInstanceAsync("foo1", "1.0", null);
            await client.CreateOrchestrationInstanceAsync("foo2", "1.0", null);
            Assert.IsTrue(service.GetPendingOrchestrationsCount() == 3);
            await service.DeleteAsync();
        }

        [TestMethod]
        public async Task TestMaxDeliveryCount()
        {
            var settings = new ServiceBusOrchestrationServiceSettings
            {
                MaxTaskActivityDeliveryCount = 100,
                MaxTaskOrchestrationDeliveryCount = 100,
                MaxTrackingDeliveryCount = 100
            };

            taskHub = TestHelpers.CreateTaskHub(settings);
            var service = taskHub.orchestrationService as ServiceBusOrchestrationService;
            Assert.IsNotNull(service);
            await service.CreateAsync();

            Dictionary<string, int> retQueues = await service.GetHubQueueMaxDeliveryCountsAsync();

            Assert.AreEqual(settings.MaxTaskActivityDeliveryCount, retQueues["TaskOrchestration"]);
            Assert.AreEqual(settings.MaxTaskOrchestrationDeliveryCount, retQueues["TaskActivity"]);
            Assert.AreEqual(settings.MaxTrackingDeliveryCount, retQueues["Tracking"]);

            await service.DeleteAsync();
        }

        [TestMethod]
        public async Task TestMaxDeliveryCountIfNew()
        {
            var settings = new ServiceBusOrchestrationServiceSettings
            {
                MaxTaskActivityDeliveryCount = 100,
                MaxTaskOrchestrationDeliveryCount = 100,
                MaxTrackingDeliveryCount = 100
            };

            taskHub = TestHelpers.CreateTaskHub(settings);
            var service = taskHub.orchestrationService as ServiceBusOrchestrationService;
            Assert.IsNotNull(service);
            await service.CreateIfNotExistsAsync();

            Dictionary<string, int> retQueues = await service.GetHubQueueMaxDeliveryCountsAsync();

            Assert.AreEqual(settings.MaxTaskActivityDeliveryCount, retQueues["TaskOrchestration"]);
            Assert.AreEqual(settings.MaxTaskOrchestrationDeliveryCount, retQueues["TaskActivity"]);
            Assert.AreEqual(settings.MaxTrackingDeliveryCount, retQueues["Tracking"]);

            await service.DeleteAsync();
        }

//        [TestMethod]
//        public void TestWorkItemCount()
//        {
//            this.client.Create();
//            this.client.CreateOrchestrationInstance("foo", "1.0", null);
//            this.client.CreateOrchestrationInstance("foo1", "1.0", null);
//            this.client.CreateOrchestrationInstance("foo2", "1.0", null);
//            Assert.IsTrue(this.client.GetOrchestrationCount() == 1);
//            this.client.Delete();
//        }
    }
}