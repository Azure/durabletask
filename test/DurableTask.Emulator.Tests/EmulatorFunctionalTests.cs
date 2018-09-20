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

namespace DurableTask.Emulator.Tests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the core dtfx via the emulator local orchestration service and client provider
    /// </summary>
    [TestClass]
    public class EmulatorFunctionalTests
    {
        public TestContext TestContext { get; set; }

        [TestMethod]
        public async Task MockOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result, "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockRecreateOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            await Assert.ThrowsExceptionAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null));

            await Assert.ThrowsExceptionAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new[] { OrchestrationStatus.Completed }));

            SimplestGreetingsOrchestration.Result = String.Empty;

            OrchestrationInstance id2 = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new OrchestrationStatus[] { });
            result = await client.WaitForOrchestrationAsync(id2, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result on Re-Create is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockTimerTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "6");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.IsTrue(sw.Elapsed.Seconds > 6);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockRepeatTimerTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GreetingsRepeatWaitOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GreetingsRepeatWaitOrchestration), "1");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.IsTrue(sw.Elapsed.Seconds > 3);

            Assert.AreEqual("Greeting send to Gabbar", GreetingsRepeatWaitOrchestration.Result,
                "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockGenerationTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            // strip out the eid so we wait for the latest one always
            var masterId = new OrchestrationInstance { InstanceId = id.InstanceId };

            OrchestrationState result1 = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), CancellationToken.None);

            OrchestrationState result2 = await client.WaitForOrchestrationAsync(masterId, TimeSpan.FromSeconds(20), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, result1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, result2.OrchestrationStatus);

            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task MockSubOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(ParentWorkflow), typeof(ChildWorkflow))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), true);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id,
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            ParentWorkflow.Result = string.Empty;

            id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), false);

            result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockRaiseEventTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(
                typeof(GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance { InstanceId = id.InstanceId };

            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "3"); // will be received by next generation
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "5"); // will be received by next generation
            await client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.Signal.Set();

            OrchestrationState result = await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = id.InstanceId },
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("5", GenerationSignalOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        ////[TestMethod]
        ////public async Task TerminateOrchestrationTest()
        ////{
        ////    var orchestrationService = new LocalOrchestrationService();

        ////    await orchestrationService.StartAsync();

        ////    TaskHubWorker worker = new TaskHubWorker(orchestrationService, "test", new TaskHubWorkerSettings());

        ////    worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
        ////        .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
        ////        .Start();

        ////    TaskHubClient client = new TaskHubClient(orchestrationService, "test", new TaskHubClientSettings());

        ////    OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "60");

        ////    OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), new CancellationToken());
        ////    Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
        ////        "Orchestration Result is wrong!!!");

        ////    await orchestrationService.StopAsync();
        ////}
    }
}