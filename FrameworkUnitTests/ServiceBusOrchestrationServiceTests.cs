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
    using System;
    using System.Threading.Tasks;
    using DurableTask;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ServiceBusOrchestrationServiceTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                client = TestHelpers.CreateTaskHubClient();

                taskHub = TestHelpers.CreateTaskHub(TimeSpan.FromSeconds(30));
                taskHub.orchestrationService.CreateAsync(true).Wait();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                taskHub.StopAsync(true).Wait();
                taskHub.orchestrationService.DeleteAsync(true).Wait();
            }
        }

        [TestMethod]
        public async Task SimplestGreetingsJumpStartTest()
        {
            await taskHub.AddTaskOrchestrations(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(DurableTaskCoreFxTest.SimplestGetUserTask), typeof(DurableTaskCoreFxTest.SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateJumpStartOnlyOrchestrationInstanceAsync(
            (ServiceBusOrchestrationService)taskHub.orchestrationService,
            NameVersionHelper.GetDefaultName(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration)),
            NameVersionHelper.GetDefaultVersion(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration)),
            null
            );

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", DurableTaskCoreFxTest.SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task SimplestGreetingsJumpStartDelayTest()
        {
            await taskHub.AddTaskOrchestrations(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(DurableTaskCoreFxTest.SimplestGetUserTask), typeof(DurableTaskCoreFxTest.SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateJumpStartOnlyOrchestrationInstanceAsync(
            (ServiceBusOrchestrationService)taskHub.orchestrationService,
            NameVersionHelper.GetDefaultName(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration)),
            NameVersionHelper.GetDefaultVersion(typeof(DurableTaskCoreFxTest.SimplestGreetingsOrchestration)),
            null
            );

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 20);
            Assert.IsFalse(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 20));
        }
    }
}