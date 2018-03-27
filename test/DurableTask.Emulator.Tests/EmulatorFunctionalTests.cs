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
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            TaskHubClient client = new TaskHubClient(orchService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockTimerTest()
        {
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            TaskHubClient client = new TaskHubClient(orchService);

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
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);

            await worker.AddTaskOrchestrations(typeof(GreetingsRepeatWaitOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            TaskHubClient client = new TaskHubClient(orchService);

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

            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);
            TaskHubClient client = new TaskHubClient(orchService);

            await worker.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            // strip out the eid so we wait for the latest one always
            OrchestrationInstance masterid = new OrchestrationInstance { InstanceId = id.InstanceId };

            OrchestrationState result1 = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), CancellationToken.None);

            OrchestrationState result2 = await client.WaitForOrchestrationAsync(masterid, TimeSpan.FromSeconds(20), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, result1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, result2.OrchestrationStatus);

            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task MockSuborchestrationTest()
        {
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);
            TaskHubClient client = new TaskHubClient(orchService);

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
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker worker = new TaskHubWorker(orchService);
            TaskHubClient client = new TaskHubClient(orchService);

            await worker.AddTaskOrchestrations(typeof(GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(
                typeof(GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance { InstanceId = id.InstanceId };

            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "3"); // will be recieved by next generation
            GenerationSignalOrchestration.signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "5"); // will be recieved by next generation
            await client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.signal.Set();

            OrchestrationState result = await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = id.InstanceId },
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("5", GenerationSignalOrchestration.Result, "Orchestration Result is wrong!!!");
        }



        //[TestMethod]
        //public async Task TerminateOrchestrationTest()
        //{
        //    LocalOrchestrationService orchService = new LocalOrchestrationService();

        //    await orchService.StartAsync();

        //    TaskHubWorker worker = new TaskHubWorker(orchService, "test", new TaskHubWorkerSettings());

        //    worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
        //        .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
        //        .Start();

        //    TaskHubClient client = new TaskHubClient(orchService, "test", new TaskHubClientSettings());

        //    OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "60");

        //    OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), new CancellationToken());
        //    Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
        //        "Orchestration Result is wrong!!!");

        //    await orchService.StopAsync();
        //}
    }
}