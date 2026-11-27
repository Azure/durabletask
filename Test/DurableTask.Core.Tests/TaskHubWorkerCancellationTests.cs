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
#if !NET48 // same reason as DispatcherMiddlewareTests: tests that use the emulator are not discoverable on 1ES for net48
namespace DurableTask.Core.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TaskHubWorkerCancellationTests
    {
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        static TaskHubWorker CreateWorker(LocalOrchestrationService service)
        {
            var worker = new TaskHubWorker(service);
            worker
                .AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask));
            return worker;
        }

        [TestMethod]
        public async Task StartAsyncWithToken_RunsOrchestration_AndStopAsyncWithTokenStopsWorker()
        {
            var service = new LocalOrchestrationService();
            TaskHubWorker worker = CreateWorker(service);
            var client = new TaskHubClient(service);

            using var cts = new CancellationTokenSource();
            TaskHubWorker started = await worker.StartAsync(cts.Token);
            Assert.AreSame(worker, started);

            OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);
            OrchestrationState state = await client.WaitForOrchestrationAsync(instance, Timeout);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);

            await worker.StopAsync(cts.Token);

            // A stopped worker can be started again, which shows the stop took effect.
            await worker.StartAsync();
            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task StartAsyncWithCanceledToken_ThrowsAndLeavesWorkerNotStarted()
        {
            var service = new LocalOrchestrationService();
            TaskHubWorker worker = CreateWorker(service);

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            try
            {
                await worker.StartAsync(cts.Token);
                Assert.Fail("StartAsync should not succeed with a canceled token.");
            }
            catch (OperationCanceledException)
            {
                // Expected. The wait on the start lock may surface it as a TaskCanceledException.
            }

            // Nothing was started, so a plain start succeeds instead of reporting "already started".
            await worker.StartAsync();
            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task StopAsyncWithCanceledToken_StillStopsTheWorker()
        {
            var service = new LocalOrchestrationService();
            TaskHubWorker worker = CreateWorker(service);
            await worker.StartAsync();

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await worker.StopAsync(cts.Token);

            // Starting again would throw "Worker is already started" if the stop had been skipped.
            await worker.StartAsync();
            await worker.StopAsync(true);
        }
    }
}
#endif
