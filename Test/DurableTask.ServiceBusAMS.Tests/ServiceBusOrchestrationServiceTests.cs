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
    using DurableTask.Core.Exceptions;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

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
                this.client = TestHelpers.CreateTaskHubClient();

                this.taskHub = TestHelpers.CreateTaskHub(TimeSpan.FromSeconds(30));
                this.taskHub.orchestrationService.CreateAsync(true).Wait();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                this.taskHub.StopAsync(true).Wait();
                this.taskHub.orchestrationService.DeleteAsync(true).Wait();
            }
        }

        [TestMethod]
        public async Task SimplestGreetingsJumpStartTest()
        {
            var sbService = (ServiceBusOrchestrationService)this.taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await this.taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task ActorOrchestrationTest()
        {
            var sbService = (ServiceBusOrchestrationService)this.taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(CounterOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(CounterOrchestration));

            await this.taskHub.AddTaskOrchestrations(typeof(CounterOrchestration))
                .StartAsync();

            int initialValue = 0;
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(CounterOrchestration), initialValue);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await TestHelpers.WaitForInstanceAsync(this.client, id, 10, waitForCompletion: false);

            OrchestrationInstance temp = new OrchestrationInstance() { InstanceId = id.InstanceId };
            // Perform some operations
            await this.client.RaiseEventAsync(temp, "operation", "incr");
            await this.client.RaiseEventAsync(temp, "operation", "incr");
            await this.client.RaiseEventAsync(temp, "operation", "decr");
            await this.client.RaiseEventAsync(temp, "operation", "incr");
            await this.client.RaiseEventAsync(temp, "operation", "incr");
            await this.client.RaiseEventAsync(temp, "operation", "end");
            await Task.Delay(4000);

            // Make sure it's still running and didn't complete early (or fail).
            var status = await client.GetOrchestrationStateAsync(id);
            Assert.IsTrue(
                status?.OrchestrationStatus == OrchestrationStatus.Running ||
                status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

            // The end message will cause the actor to complete itself.
            await this.client.RaiseEventAsync(temp, "operation", "end");

            status = await client.WaitForOrchestrationAsync(temp, TimeSpan.FromSeconds(10));

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.AreEqual(3, JToken.Parse(status?.Output));

            // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
            Assert.AreNotEqual(initialValue, JToken.Parse(status?.Input));
        }

        [TestMethod]
        public async Task SimplestGreetingsJumpStartDelayTest()
        {
            var sbService = (ServiceBusOrchestrationService)this.taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await this.taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            // Wait only 8 seconds, the jumpStart interval is set to 10
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 8);
            Assert.IsFalse(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 8));
        }

        [TestMethod]
        public async Task DupeDetectionByInstanceStoreTest()
        {
            var sbService = (ServiceBusOrchestrationService)this.taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await this.taskHub.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            // Write to jumpStart table only
            OrchestrationInstance id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, null, null, true, false);

            // Try to create orchestration with same instanceId
            OrchestrationAlreadyExistsException exception = await TestHelpers.ThrowsAsync<OrchestrationAlreadyExistsException>(() => TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, id.InstanceId, null, false, false));
            Assert.IsTrue(exception.Message.Contains("already exists"));
        }

        [TestMethod]
        public async Task DupeDetectionByServiceBusQueueTest()
        {
            var sbService = (ServiceBusOrchestrationService)this.taskHub.orchestrationService;
            string name = NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration));
            string version = NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration));

            await this.taskHub.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            GenerationBasicTask.GenerationCount = 0;
            var generationCount = 0;
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), generationCount);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            // We use instanceId and execution for SB level d-dup
            // Write to ServiceBus only. SB drops the message and the orchestration does not start
            id = await TestHelpers.CreateOrchestrationInstanceAsync(sbService, name, version, id.InstanceId, id.ExecutionId, false, true);

            await Task.Delay(TimeSpan.FromSeconds(5));

            // ReSharper disable once UnusedVariable
            long count = await TestHelpers.GetOrchestratorQueueMessageCount();

            IList<OrchestrationState> executions = await this.client.GetOrchestrationStateAsync(id.InstanceId, true);
            Assert.AreEqual(1, executions.Count, "Duplicate detection failed and orchestration ran for second time");

            // Make sure the second orchestration never started by checking the output from first orchestration
            Assert.AreNotEqual("Greeting send to Gabbar", executions[0].Output);
            Assert.AreEqual(generationCount + 1, int.Parse(executions[0].Output));
        }
    }
}