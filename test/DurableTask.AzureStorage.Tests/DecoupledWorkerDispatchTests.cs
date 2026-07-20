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

namespace DurableTask.AzureStorage.Tests
{
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class DecoupledWorkerDispatchTests
    {
        static readonly string TestConnectionString = TestHelpers.GetTestStorageAccountConnectionString();

        // ---------------------------------------------------------------------------------------
        // Unit tests: the WorkerDispatchMode setting maps onto the IOrchestrationService dispatcher
        // counts, which is the signal TaskHubWorker uses to gate each dispatcher. No storage needed.
        // ---------------------------------------------------------------------------------------

        [TestMethod]
        public void DefaultMode_IsBoth_AndRunsBothDispatchers()
        {
            var service = CreateService("DispatchModeDefault", mode: null);

            Assert.AreEqual(WorkerDispatchMode.Both, new AzureStorageOrchestrationServiceSettings().WorkerDispatchMode);
            Assert.AreEqual(1, service.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(1, service.TaskActivityDispatcherCount);
        }

        [TestMethod]
        public void BothMode_RunsBothDispatchers()
        {
            var service = CreateService("DispatchModeBoth", WorkerDispatchMode.Both);

            Assert.AreEqual(1, service.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(1, service.TaskActivityDispatcherCount);
        }

        [TestMethod]
        public void OrchestratorMode_DisablesActivityDispatcher()
        {
            var service = CreateService("DispatchModeOrch", WorkerDispatchMode.Orchestrator);

            Assert.AreEqual(1, service.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(0, service.TaskActivityDispatcherCount);
        }

        [TestMethod]
        public void ActivityMode_DisablesOrchestrationDispatcher()
        {
            var service = CreateService("DispatchModeAct", WorkerDispatchMode.Activity);

            Assert.AreEqual(0, service.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(1, service.TaskActivityDispatcherCount);
        }

        static AzureStorageOrchestrationService CreateService(string taskHub, WorkerDispatchMode? mode)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = TestHelpers.GetTestTaskHubName() + taskHub,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
            };

            if (mode.HasValue)
            {
                settings.WorkerDispatchMode = mode.Value;
            }

            return new AzureStorageOrchestrationService(settings);
        }

        // ---------------------------------------------------------------------------------------
        // Integration test (Azurite): two workers on the SAME task hub, one Orchestrator-only and
        // one Activity-only. The orchestration (which calls an activity) must run to completion, and
        // the roles must be decoupled: the orchestrator worker never dequeues the work-item queue and
        // the activity worker never leases a control-queue partition.
        // ---------------------------------------------------------------------------------------

        [TestMethod]
        public async Task DecoupledWorkers_RunOrchestrationToCompletion()
        {
            string taskHub = TestHelpers.GetTestTaskHubName() + "Decoupled";
            const string input = "world";

            var orchestratorSettings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = taskHub,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                WorkerDispatchMode = WorkerDispatchMode.Orchestrator,
                WorkerId = "orchestrator-worker",
            };

            var activitySettings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = taskHub,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                WorkerDispatchMode = WorkerDispatchMode.Activity,
                WorkerId = "activity-worker",
            };

            var orchestratorService = new AzureStorageOrchestrationService(orchestratorSettings);
            var activityService = new AzureStorageOrchestrationService(activitySettings);

            // Sanity check on the mode gating before starting anything.
            Assert.AreEqual(1, orchestratorService.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(0, orchestratorService.TaskActivityDispatcherCount);
            Assert.AreEqual(0, activityService.TaskOrchestrationDispatcherCount);
            Assert.AreEqual(1, activityService.TaskActivityDispatcherCount);

            // Both deployments ship the same code, so both workers register the same orchestration and
            // activity. Each worker only dispatches the kind of work allowed by its mode.
            var orchestratorWorker = new TaskHubWorker(orchestratorService);
            orchestratorWorker.AddTaskOrchestrations(typeof(DecoupledHelloOrchestrator));
            orchestratorWorker.AddTaskActivities(typeof(DecoupledHello));

            var activityWorker = new TaskHubWorker(activityService);
            activityWorker.AddTaskOrchestrations(typeof(DecoupledHelloOrchestrator));
            activityWorker.AddTaskActivities(typeof(DecoupledHello));

            var client = new TaskHubClient(orchestratorService);

            await orchestratorWorker.StartAsync();
            await activityWorker.StartAsync();

            try
            {
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    typeof(DecoupledHelloOrchestrator), input);

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(90));

                // The orchestrator-only worker cannot dequeue activities and the activity-only worker
                // cannot dequeue orchestrations, so completion proves the two roles cooperated.
                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.AreEqual($"\"Hello, {input}!\"", state.Output);

                // The activity worker must never have leased a control-queue partition...
                Assert.AreEqual(0, activityService.OwnedControlQueues.Count(), "Activity-only worker should not lease any control-queue partition.");
                // ...while the orchestrator worker owns at least one partition.
                await TestHelpers.WaitFor(() => orchestratorService.OwnedControlQueues.Any(), TimeSpan.FromSeconds(30));
                Assert.IsTrue(orchestratorService.OwnedControlQueues.Any(), "Orchestrator-only worker should lease control-queue partitions.");
            }
            finally
            {
                await orchestratorWorker.StopAsync(isForced: true);
                await activityWorker.StopAsync(isForced: true);
                await orchestratorService.DeleteAsync();
            }
        }

        internal class DecoupledHelloOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(DecoupledHello), input);
            }
        }

        internal class DecoupledHello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    throw new ArgumentNullException(nameof(input));
                }

                return $"Hello, {input}!";
            }
        }
    }
}
