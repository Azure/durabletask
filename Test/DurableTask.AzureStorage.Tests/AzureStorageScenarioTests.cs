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
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class AzureStorageScenarioTests
    {
        /// <summary>
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
        /// </summary>
        [TestMethod]
        public async Task HelloWorldOrchestration_Inline()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("World", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, World!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which runs a simple orchestrator function that calls a single activity function.
        /// </summary>
        [TestMethod]
        public async Task HelloWorldOrchestration_Activity()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithActivity), "World");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("World", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, World!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [TestMethod]
        public async Task SequentialOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 10);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(10, JToken.Parse(status?.Input));
                Assert.AreEqual(3628800, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [TestMethod]
        public async Task ParallelOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DiskUsage), Environment.CurrentDirectory);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(90));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(Environment.CurrentDirectory, JToken.Parse(status?.Input));
                Assert.IsTrue(long.Parse(status?.Output) > 0L);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing a counter actor pattern.
        /// </summary>
        [TestMethod]
        public async Task ActorOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                int initialValue = 0;
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), initialValue);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Perform some operations
                await client.RaiseEventAsync("operation", "incr");

                // TODO: Sleeping to avoid a race condition where multiple ContinueAsNew messages
                //       are processed by the same instance at the same time, resulting in a corrupt
                //       storage failure in DTFx.
                await Task.Delay(2000);
                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(2000);
                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(2000);
                await client.RaiseEventAsync("operation", "decr");
                await Task.Delay(2000);
                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(2000);

                // Make sure it's still running and didn't complete early (or fail).
                var status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(3, JToken.Parse(status?.Output));

                // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
                Assert.AreNotEqual(initialValue, JToken.Parse(status?.Input));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the Terminate functionality.
        /// </summary>
        [TestMethod]
        public async Task TerminateOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

                // Need to wait for the instance to start before we can terminate it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.TerminateAsync("sayōnara");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("sayōnara", status?.Output);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the cancellation of durable timers.
        /// </summary>
        [TestMethod]
        public async Task TimerCancellation()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var timeout = TimeSpan.FromSeconds(10);
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                await client.RaiseEventAsync("approval", eventData: true);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Approved", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of durable timer expiration.
        /// </summary>
        [TestMethod]
        public async Task TimerExpiration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                var timeout = TimeSpan.FromSeconds(10);
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Don't send any notification - let the internal timeout expire

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(20));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Expired", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations run concurrently of each other (up to 100 by default).
        /// </summary>
        [TestMethod]
        public async Task OrchestrationConcurrency()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                Func<Task> orchestrationStarter = async delegate ()
                {
                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    // Don't send any notification - let the internal timeout expire
                };

                int iterations = 50;
                var tasks = new Task[iterations];
                for (int i = 0; i < iterations; i++)
                {
                    tasks[i] = orchestrationStarter();
                }

                // The 50 orchestrations above (which each delay for 10 seconds) should all complete in less than 60 seconds.
                Task parallelOrchestrations = Task.WhenAll(tasks);
                Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(60));

                Task winner = await Task.WhenAny(parallelOrchestrations, timeoutTask);
                Assert.AreEqual(parallelOrchestrations, winner);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the orchestrator's exception handling behavior.
        /// </summary>
        [TestMethod]
        public async Task HandledActivityException()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                // Empty string input should result in ArgumentNullException in the orchestration code.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.TryCatchLoop), 5);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(5, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from orchestrator code.
        /// </summary>
        [TestMethod]
        public async Task UnhandledOrchestrationException()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                // Empty string input should result in ArgumentNullException in the orchestration code.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), "");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains("null") == true);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from activity code.
        /// </summary>
        [TestMethod]
        public async Task UnhandledActivityException()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost())
            {
                await host.StartAsync();

                string message = "Kah-BOOOOM!!!";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains(message) == true);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Test which validates the ETW event source.
        /// </summary>
        [TestMethod]
        public void ValidateEventSource()
        {
            EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
        }

        static class Orchestrations
        {
            internal class SayHelloInline : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult($"Hello, {input}!");
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class SayHelloWithActivity : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Hello), input);
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class Factorial : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i });
                    }

                    return result;
                }
            }

            [KnownType(typeof(Activities.GetFileList))]
            [KnownType(typeof(Activities.GetFileSize))]
            internal class DiskUsage : TaskOrchestration<long, string>
            {
                public override async Task<long> RunTask(OrchestrationContext context, string directory)
                {
                    string[] files = await context.ScheduleTask<string[]>(typeof(Activities.GetFileList), directory);

                    var tasks = new Task<long>[files.Length];
                    for (int i = 0; i < files.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<long>(typeof(Activities.GetFileSize), files[i]);
                    }

                    await Task.WhenAll(tasks);

                    long totalBytes = tasks.Sum(t => t.Result);
                    return totalBytes;
                }
            }

            internal class Counter : TaskOrchestration<int, int>
            {
                TaskCompletionSource<string> waitForOperationHandle;

                public override async Task<int> RunTask(OrchestrationContext context, int currentValue)
                {
                    string operation = await this.WaitForOperation();

                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case "incr":
                            currentValue++;
                            break;
                        case "decr":
                            currentValue--;
                            break;
                        case "end":
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(currentValue);
                    }

                    return currentValue;
                }

                async Task<string> WaitForOperation()
                {
                    this.waitForOperationHandle = new TaskCompletionSource<string>();
                    string operation = await this.waitForOperationHandle.Task;
                    this.waitForOperationHandle = null;
                    return operation;
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    Assert.AreEqual("operation", name, true, "Unknown signal recieved...");
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class Approval : TaskOrchestration<string, TimeSpan, bool, string>
            {
                TaskCompletionSource<bool> waitForApprovalHandle;

                public override async Task<string> RunTask(OrchestrationContext context, TimeSpan timeout)
                {
                    DateTime deadline = context.CurrentUtcDateTime.Add(timeout);

                    using (var cts = new CancellationTokenSource())
                    {
                        Task<bool> approvalTask = this.GetWaitForApprovalTask();
                        Task timeoutTask = context.CreateTimer(deadline, cts.Token);

                        if (approvalTask == await Task.WhenAny(approvalTask, timeoutTask))
                        {
                            // The timer must be cancelled or fired in order for the orchestration to complete.
                            cts.Cancel();

                            bool approved = approvalTask.Result;
                            return approved ? "Approved" : "Rejected";
                        }
                        else
                        {
                            return "Expired";
                        }
                    }
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

            [KnownType(typeof(Activities.Throw))]
            internal class Throw : TaskOrchestration<string, string>
            {
                public override async Task<string> RunTask(OrchestrationContext context, string message)
                {
                    if (string.IsNullOrEmpty(message))
                    {
                        // This throw happens directly in the orchestration.
                        throw new ArgumentNullException(nameof(message));
                    }

                    // This throw happens in the implementation of an activity.
                    await context.ScheduleTask<string>(typeof(Activities.Throw), message);
                    return null;
                }
            }

            [KnownType(typeof(Activities.Throw))]
            internal class TryCatchLoop : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    int catchCount = 0;

                    for (int i = 0; i < iterations; i++)
                    {
                        try
                        {
                            await context.ScheduleTask<string>(typeof(Activities.Throw), "Kah-BOOOOOM!!!");
                        }
                        catch
                        {
                            catchCount++;
                        }
                    }

                    return catchCount;
                }
            }
        }

        static class Activities
        {
            internal class Hello : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    return $"Hello, {input}!";
                }
            }

            internal class Multiply : TaskActivity<long[], long>
            {
                protected override long Execute(TaskContext context, long[] values)
                {
                    return values[0] * values[1];
                }
            }

            internal class GetFileList : TaskActivity<string, string[]>
            {
                protected override string[] Execute(TaskContext context, string directory)
                {
                    return Directory.GetFiles(directory, "*", SearchOption.TopDirectoryOnly);
                }
            }

            internal class GetFileSize : TaskActivity<string, long>
            {
                protected override long Execute(TaskContext context, string fileName)
                {
                    var info = new FileInfo(fileName);
                    return info.Length;
                }
            }

            internal class Throw : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string message)
                {
                    throw new Exception(message);
                }
            }
        }
    }
}
