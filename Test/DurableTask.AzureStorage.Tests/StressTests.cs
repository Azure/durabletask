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
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
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
                        DateTime now = DateTime.Now;
                        instanceIdGenerator = _ => $"ConcurrentInstance_{now:yyyyMMdd.hhmmss.fff}";
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
    }
}
