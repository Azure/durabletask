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

namespace FrameworkUnitTests
{
    using DurableTask;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TaskHubClientTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            client = TestHelpers.CreateTaskHubClient();
            taskHub = TestHelpers.CreateTaskHub();
        }

        [TestMethod]
        public void TestCreateIfNew()
        {
            taskHub.CreateHub();
            taskHub.CreateHubIfNotExists();
            Assert.IsTrue(taskHub.HubExists());
            taskHub.DeleteHub();
            Assert.IsFalse(taskHub.HubExists());
        }

        [TestMethod]
        public void TestOrchestrationCount()
        {
            taskHub.CreateHub();
            client.CreateOrchestrationInstance("foo", "1.0", null);
            client.CreateOrchestrationInstance("foo1", "1.0", null);
            client.CreateOrchestrationInstance("foo2", "1.0", null);
            Assert.IsTrue(client.GetPendingOrchestrationsCount() == 3);
            taskHub.DeleteHub();
        }

        [TestMethod]
        public void TestMaxDeliveryCount()
        {
            var desc = new TaskHubDescription
            {
                MaxTaskActivityDeliveryCount = 100,
                MaxTaskOrchestrationDeliveryCount = 100,
                MaxTrackingDeliveryCount = 100,
            };

            taskHub.CreateHub(desc);
            TaskHubDescription retDesc = taskHub.GetTaskHubDescriptionAsync().Result;

            Assert.AreEqual(desc.MaxTaskActivityDeliveryCount, retDesc.MaxTaskActivityDeliveryCount);
            Assert.AreEqual(desc.MaxTaskOrchestrationDeliveryCount, retDesc.MaxTaskOrchestrationDeliveryCount);
            Assert.AreEqual(desc.MaxTrackingDeliveryCount, retDesc.MaxTrackingDeliveryCount);

            taskHub.DeleteHub();
        }

        [TestMethod]
        public void TestMaxDeliveryCountIfNew()
        {
            var desc = new TaskHubDescription
            {
                MaxTaskActivityDeliveryCount = 100,
                MaxTaskOrchestrationDeliveryCount = 100,
                MaxTrackingDeliveryCount = 100,
            };

            taskHub.CreateHubIfNotExists(desc);
            TaskHubDescription retDesc = taskHub.GetTaskHubDescriptionAsync().Result;

            Assert.AreEqual(desc.MaxTaskActivityDeliveryCount, retDesc.MaxTaskActivityDeliveryCount);
            Assert.AreEqual(desc.MaxTaskOrchestrationDeliveryCount, retDesc.MaxTaskOrchestrationDeliveryCount);
            Assert.AreEqual(desc.MaxTrackingDeliveryCount, retDesc.MaxTrackingDeliveryCount);

            taskHub.DeleteHub();
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

        [TestCleanup]
        public void TestCleanup()
        {
        }
    }
}