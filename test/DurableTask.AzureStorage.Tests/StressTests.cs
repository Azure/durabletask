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
        int originalMinWorkerThreads;
        int originalMinIoThreads;

        [TestInitialize()]
        public void Startup()
        {
            // Set the minimum thread count to 64+ to make these tests extra concurrent.
            ThreadPool.GetMinThreads(out this.originalMinWorkerThreads, out this.originalMinIoThreads);
            ThreadPool.SetMinThreads(Math.Max(64, this.originalMinWorkerThreads), Math.Max(64, this.originalMinIoThreads));
        }

        [TestCleanup()]
        public void Cleanup()
        {
            // Reset the thread pool configuration
            ThreadPool.SetMinThreads(this.originalMinWorkerThreads, this.originalMinIoThreads);
        }

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
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.ThrowExceptionOnInvalidDedupeStatus = false);

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

        [TestMethod]
        public async Task RestartOrchestrationWithExternalEvents()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false);
            await host.StartAsync();

            // This is the local method we'll use to continuously recreate the same instance.
            // It doesn't matter that the orchestration name is different as long as the instance ID is the same.
            Task<TestInstance<string>> CreateInstance(int i) => host.StartInlineOrchestration(
                input: "Hello, world!",
                instanceId: "Singleton",
                orchestrationName: $"EchoOrchestration{i}",
                implementation: (ctx, input) => Task.FromResult(input));

            // Start the first iteration and wait for it to complete.
            TestInstance<string> instance = await CreateInstance(0);
            await instance.WaitForCompletion();

            int restartIterations = 10;
            int externalEventCount = 10;

            // We'll keep track of the execution IDs to make 100% sure we get new instances every iteration.
            var executionIds = new HashSet<string> { instance.ExecutionId };

            // Simultaneously send a bunch of external events as well as a "recreate" message. Repeat this
            // several times. Need to ensure that the orchestration can always be restarted reliably. It
            // doesn't matter whether the orchestration handles the external events or not.
            for (int i = 1; i <= restartIterations; i++)
            {
                List<Task> concurrentTasks = Enumerable.Range(0, externalEventCount).Select(j => instance.RaiseEventAsync("DummyEvent", j)).ToList();
                Task<TestInstance<string>> recreateInstanceTask = CreateInstance(i);
                concurrentTasks.Add(recreateInstanceTask);
                await Task.WhenAll(concurrentTasks);

                // Wait for the newly created instance to complete.
                instance = await recreateInstanceTask;
                await instance.WaitForCompletion();

                // Ensure that this is a new execution ID.
                Assert.IsTrue(executionIds.Add(instance.ExecutionId));
            }
        }
    }
}
