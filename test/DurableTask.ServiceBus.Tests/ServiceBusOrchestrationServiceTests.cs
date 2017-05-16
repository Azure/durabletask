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
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Test.Orchestrations;
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
            var sbService = (ServiceBusOrchestrationService)taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task SimplestGreetingsJumpStartDelayTest()
        {
            var sbService = (ServiceBusOrchestrationService)taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            // Wait only 8 seconds, the jumptstartinterval is set to 10
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 8);
            Assert.IsFalse(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 8));
        }

        [TestMethod]
        public async Task DupeDetectionByInstanceStoreTest()
        {
            var sbService = (ServiceBusOrchestrationService)taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            // Write to jumpstart table only
            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            // Try to create orchestraton with same instanceId
            var exception = await TestHelpers.ThrowsAsync<InvalidOperationException>(() => TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, id.InstanceId, null, false, false));
            Assert.IsTrue(exception.Message.Contains("already exists"));
        }

        [TestMethod]
        public async Task DupeDetectionByServiceBusQueueTest()
        {
            var sbService = (ServiceBusOrchestrationService)taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await taskHub.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            GenerationBasicTask.GenerationCount = 0;
            int generationCount = 0;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), generationCount);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            // We use instanceId and execution for SB level d-dup
            // Write to ServiceBus only. SB drops the message and the orchestraion does not start
            id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, id.InstanceId, id.ExecutionId, false, true);

            await Task.Delay(TimeSpan.FromSeconds(5));

            long count = TestHelpers.GetOrchestratorQueueMessageCount();

            IList<OrchestrationState> executions = await client.GetOrchestrationStateAsync(id.InstanceId, true);
            Assert.AreEqual(1, executions.Count, "Duplicate detection failed and orchestration ran for second time");

            // Make sure the second orchestraton never started by checking the output from first orchestration
            Assert.AreNotEqual("Greeting send to Gabbar", executions[0].Output);
            Assert.AreEqual(generationCount + 1, int.Parse(executions[0].Output));
        }
    }
}