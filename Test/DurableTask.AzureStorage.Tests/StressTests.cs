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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class StressTests
    {
        /// <summary>
        /// Starts a large'ish number of orchestrations concurrently and verifies correct behavior
        /// both in the case where they all share the same instance ID and when they have unique
        /// instance IDs.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ConcurrentOrchestrationStarts(bool useSameInstanceId)
        {
            // Set the minimum thread count to 64+ to make this test extra concurrent.
            ThreadPool.GetMinThreads(out int minWorkerThreads, out int minIoThreads);
            ThreadPool.SetMinThreads(Math.Max(64, minWorkerThreads), Math.Max(64, minIoThreads));
            try
            {
                using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                    enableExtendedSessions: false,
                    modifySettingsAction: settings => settings.ThrowExceptionOnInvalidDedupeStatus = false))
                {
                    await host.StartAsync();

                    var results = new ConcurrentBag<string>();

                    // We want a number sufficiently high that it results in multiple message batches
                    const int MaxConcurrency = 40;

                    TaskActivity activity = TestOrchestrationHost.MakeActivity(
                        delegate(TaskContext ctx, string input)
                        {
                            string result = $"Hello, {input}!";
                            results.Add(result);
                            return result;
                        });

                    // Use the same instance name for all instances
                    Func<int, string> instanceIdGenerator;
                    Func<int, string> inputGenerator;
                    if (useSameInstanceId)
                    {
                        instanceIdGenerator = _ => $"ConcurrentInstance_SINGLETON";
                        inputGenerator = _ => "World";
                    }
                    else
                    {
                        instanceIdGenerator = i => $"ConcurrentInstance_{i:00}";
                        inputGenerator = i => $"{i:00}";
                    }

                    List<TestInstance<string>> instances = await host.StartInlineOrchestrations(
                        MaxConcurrency,
                        instanceIdGenerator,
                        inputGenerator,
                        orchestrationName: "SayHelloOrchestration",
                        version: string.Empty,
                        implementation: (ctx, input) => ctx.ScheduleTask<string>("SayHello", "", input),
                        activities: ("SayHello", activity));

                    Assert.AreEqual(MaxConcurrency, instances.Count);

                    // All returned objects point to the same orchestration instance
                    OrchestrationState[] finalStates = await Task.WhenAll(instances.Select(
                        i => i.WaitForCompletion(timeout: TimeSpan.FromMinutes(2), expectedOutputRegex: @"Hello, \w+!")));

                    if (useSameInstanceId)
                    {
                        // Make sure each instance is exactly the same
                        string firstInstanceJson = JsonConvert.SerializeObject(finalStates[0]);
                        foreach (OrchestrationState state in finalStates.Skip(1))
                        {
                            string json = JsonConvert.SerializeObject(state);
                            Assert.AreEqual(firstInstanceJson, json, "Expected that all instances have the same data.");
                        }
                    }
                    else
                    {
                        // Make sure each instance is different
                        Assert.AreEqual(MaxConcurrency, finalStates.Select(s => s.OrchestrationInstance.InstanceId).Distinct().Count());
                        Assert.AreEqual(MaxConcurrency, finalStates.Select(s => s.OrchestrationInstance.ExecutionId).Distinct().Count());
                        Assert.AreEqual(MaxConcurrency, finalStates.Select(s => s.Input).Distinct().Count());
                        Assert.AreEqual(MaxConcurrency, finalStates.Select(s => s.Output).Distinct().Count());
                    }

                    await host.StopAsync();
                }
            }
            finally
            {
                // Reset the thread pool configuration
                ThreadPool.SetMinThreads(minWorkerThreads, minIoThreads);
            }
        }

        internal class Approval : TaskOrchestration<string, TimeSpan, bool, string>
        {
            TaskCompletionSource<bool> waitForApprovalHandle;

            public override async Task<string> RunTask(OrchestrationContext context, TimeSpan timeout)
            {
                Task<bool> approvalTask = this.GetWaitForApprovalTask();


                await Task.WhenAny(approvalTask);
                // The timer must be cancelled or fired in order for the orchestration to complete.

                bool approved = approvalTask.Result;
                return approved ? "Approved" : "Rejected";
            }

            async Task<bool> GetWaitForApprovalTask()
            {
                this.waitForApprovalHandle = new TaskCompletionSource<bool>();
                bool approvalResult = await this.waitForApprovalHandle.Task;
                this.waitForApprovalHandle = null;
                return approvalResult;
            }

            public override void OnEvent(OrchestrationContext context, string name, bool approvalResult)
            {
                Assert.AreEqual("approval", name, true, "Unknown signal recieved...");
                if (this.waitForApprovalHandle != null)
                {
                    this.waitForApprovalHandle.SetResult(approvalResult);
                }
            }
        }

        [DataTestMethod]
        public async Task SingletonsLoadCorrectHistoryWithRaiseEvent()
        {

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();
                int numExternalEvents = 50;
                int maxIterations = 20;
                int currentIteration = 0;
                do
                {
                    // (1) Start a singleton orchestration that waits for a single event (wait for it to finish starting)
                    TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Approval), input: null, instanceId: "mySingleton");
                    DateTime startTime = DateTime.UtcNow;
                    OrchestrationState existingInstance = null;
                    while (existingInstance == null || !existingInstance.OrchestrationStatus.Equals(OrchestrationStatus.Running))
                    {
                        existingInstance = await client.GetStatusAsync();
                        if (DateTime.UtcNow - startTime > TimeSpan.FromMinutes(2))
                        {
                            Assert.Fail($"Singleton did not complete within 2 minutes with status {existingInstance.OrchestrationStatus}  at iteration {currentIteration}");
                        }
                    }

                    // (2) Send a large number of external events AND start a new instance of the same orchestration concurrently.
                    List<Task> externalEvents = new List<Task>();
                    for (var i = 0; i < numExternalEvents; i++)
                    {
                        externalEvents.Add(client.RaiseEventAsync(eventName: "approval", eventData: true));
                    }

                    async Task getStartNewSingletonTask()
                    {
                        existingInstance = null;
                        while (existingInstance == null || !existingInstance.OrchestrationStatus.Equals(OrchestrationStatus.Completed))
                        {
                            await client.RaiseEventAsync(eventName: "approval", eventData: true);
                            existingInstance = await client.GetStatusAsync();
                        }
                        await host.StartOrchestrationAsync(typeof(Approval), input: null, instanceId: "mySingleton");
                    };

                    Task startNewSingletonTask = getStartNewSingletonTask();
                    externalEvents.Add(startNewSingletonTask);

                    await Task.WhenAll(externalEvents);

                    // (3) check orchestration is not pending
                    startTime = DateTime.UtcNow;
                    existingInstance = null;
                    while (existingInstance == null || !existingInstance.OrchestrationStatus.Equals(OrchestrationStatus.Completed))
                    {
                        await client.RaiseEventAsync(eventName: "approval", eventData: true);
                        existingInstance = await client.GetStatusAsync();
                        if (DateTime.UtcNow - startTime > TimeSpan.FromMinutes(2))
                        {
                            Assert.Fail($"Singleton did not complete within 2 minutes with status {existingInstance.OrchestrationStatus} at iteration {currentIteration}");
                        }
                    }


                }
                while (currentIteration++ < maxIterations);
                Assert.IsTrue(true);
                await host.StopAsync();
            }
        }
    }
}
