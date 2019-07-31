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

namespace DurableTask.EventHubs.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    //using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Xunit;
    using Xunit.Abstractions;


    // These tests are copied from AzureStorageScenarioTests
    [Collection("EventHubsTests")]
    public partial class PortedAzureScenarioTests : IClassFixture<TestFixture>, IDisposable
    {
        private readonly TestFixture fixture;
        private readonly TestOrchestrationHost host;
        private readonly TestTraceListener traceListener;

        public PortedAzureScenarioTests(TestFixture fixture, ITestOutputHelper outputHelper)
        {
            this.fixture = fixture;
            this.host = fixture.Host;
            this.traceListener = new TestTraceListener(outputHelper);
            System.Diagnostics.Trace.Listeners.Add(this.traceListener);
        }

        public void Dispose()
        {
            System.Diagnostics.Trace.Listeners.Remove(this.traceListener);
        }

        private class TestTraceListener : TraceListener
        {
            ITestOutputHelper _output;
            public TestTraceListener(ITestOutputHelper output) { _output = output; }
            public override void Write(string message) { }
            public override void WriteLine(string message) { _output.WriteLine(message); }
        }

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
        /// </summary>
        [Fact]
        public async Task HelloWorldOrchestration_Inline()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which runs a simple orchestrator function that calls a single activity function.
        /// </summary>
        [Fact]
        public async Task HelloWorldOrchestration_Activity()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithActivity), "World");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("World", JToken.Parse(status?.Input));
            Assert.Equal("Hello, World!", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [Fact]
        public async Task SequentialOrchestration()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 10);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task ParentOfSequentialOrchestration()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(10, JToken.Parse(status?.Input));
            Assert.Equal(3628800, JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task EventConversation()
        {
            var client = await host.StartOrchestrationAsync(typeof(DurableTask.Test.Orchestrations.EventConversationOrchestration), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }


        [Fact]
        public async Task AutoStart()
        {
            host.AddAutoStartOrchestrator(typeof(Orchestrations.AutoStartOrchestration.Responder));

            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AutoStartOrchestration), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }

        [Fact]
        public async Task ContinueAsNewThenTimer()
        {
            var client = await host.StartOrchestrationAsync(typeof(DurableTask.Test.Orchestrations.ContinueAsNewThenTimerOrchestration), 0);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("OK", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [Fact]
        public async Task ParallelOrchestration()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DiskUsage), Environment.CurrentDirectory);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(Environment.CurrentDirectory, JToken.Parse(status?.Input));
            Assert.True(long.Parse(status?.Output) > 0L);
        }

        [Fact]
        public async Task LargeFanOutOrchestration()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 1000);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        }

        [Fact]
        public async Task FanOutOrchestration_LargeHistoryBatches()
        {
            // This test creates history payloads that exceed the 4 MB limit imposed by Azure Storage
            // when 100 entities are uploaded at a time.
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SemiLargePayloadFanOutFanIn), 90);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing a counter actor pattern.
        /// </summary>
        [Fact]
        public async Task ActorOrchestration()
        {
            int initialValue = 0;
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), initialValue);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            // Perform some operations
            await client.RaiseEventAsync("operation", "incr");
            await client.RaiseEventAsync("operation", "incr");
            await client.RaiseEventAsync("operation", "incr");
            await client.RaiseEventAsync("operation", "decr");
            await client.RaiseEventAsync("operation", "incr");
            await Task.Delay(2000);

            // Make sure it's still running and didn't complete early (or fail).
            var status = await client.GetStatusAsync();
            Assert.True(
                status?.OrchestrationStatus == OrchestrationStatus.Running ||
                status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

            // The end message will cause the actor to complete itself.
            await client.RaiseEventAsync("operation", "end");

            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(3, JToken.Parse(status?.Output));

            // When using ContinueAsNew, the original input is discarded and replaced with the most recent state.
            Assert.NotEqual(initialValue, JToken.Parse(status?.Input));
        }

        /// <summary>
        /// End-to-end test which validates the Terminate functionality.
        /// </summary>
        [Fact]
        public async Task TerminateOrchestration()
        {
            // Using the counter orchestration because it will wait indefinitely for input.
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

            // Need to wait for the instance to start before we can terminate it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            await client.TerminateAsync("sayōnara");

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
            Assert.Equal("sayōnara", status?.Output);
        }

        [Fact]
        public async Task TimerCancellation()
        {
            var timeout = TimeSpan.FromSeconds(10);
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
            await client.RaiseEventAsync("approval", eventData: true);

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Approved", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates the handling of durable timer expiration.
        /// </summary>
        [Fact]
        public async Task TimerExpiration()
        {
            var timeout = TimeSpan.FromSeconds(10);
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            // Don't send any notification - let the internal timeout expire

            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(20));
            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal("Expired", JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations run concurrently of each other (up to 100 by default).
        /// </summary>
        [Fact]
        public async Task OrchestrationConcurrency()
        {
            Func<Task> orchestrationStarter = async () =>
            {
                System.Diagnostics.Trace.TraceInformation($"Starting orchestration");
                try
                {
                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    System.Diagnostics.Trace.TraceInformation($"Starting wait for {client.InstanceId}");
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    // Don't send any notification - let the internal timeout expire
                }
                catch(Exception e)
                {
                    System.Diagnostics.Trace.TraceInformation($"Error in orchestration: {e}");
                }
                finally
                {
                    System.Diagnostics.Trace.TraceInformation($"Completed orchestration");
                }
            };

            int iterations = 10;
            var tasks = new Task[iterations];
            for (int i = 0; i < iterations; i++)
            {
                tasks[i] = orchestrationStarter();
            }

            // The 10 orchestrations above (which each delay for 10 seconds) should all complete in less than 60 seconds.
            Task parallelOrchestrations = Task.WhenAll(tasks);
            Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(60));

            Task winner = await Task.WhenAny(parallelOrchestrations, timeoutTask);
            foreach(var t in tasks)
            {
                await t;
            }
            Assert.Equal(parallelOrchestrations, winner);
        }

        /// <summary>
        /// End-to-end test which validates the orchestrator's exception handling behavior.
        /// </summary>
        [Fact]
        public async Task HandledActivityException()
        {
            // Empty string input should result in ArgumentNullException in the orchestration code.
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.TryCatchLoop), 5);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(5, JToken.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from orchestrator code.
        /// </summary>
        [Fact]
        public async Task UnhandledOrchestrationException()
        {
            // Empty string input should result in ArgumentNullException in the orchestration code.
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), "");
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);
            Assert.True(status?.Output.Contains("null") == true);
        }

        /// <summary>
        /// End-to-end test which validates the handling of unhandled exceptions generated from activity code.
        /// </summary>
        [Fact]
        public async Task UnhandledActivityException()
        {
            string message = "Kah-BOOOOM!!!";
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Throw), message);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Failed, status?.OrchestrationStatus);
            Assert.True(status?.Output.Contains(message) == true);
        }

        /// <summary>
        /// Fan-out/fan-in test which ensures each operation is run only once.
        /// </summary>
        [Fact]
        public async Task FanOutToTableStorage()
        {
            int iterations = 100;

            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.MapReduceTableStorage), iterations);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
            Assert.Equal(iterations, int.Parse(status?.Output));
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with <=60KB text message sizes can run successfully.
        /// </summary>
        [Fact]
        public async Task SmallTextMessagePayloads()
        {
                // Generate a small random string payload
                const int TargetPayloadSize = 1 * 1024; // 1 KB
                const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.-";
                var sb = new StringBuilder();
                var random = new Random();
                while (Encoding.Unicode.GetByteCount(sb.ToString()) < TargetPayloadSize)
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        sb.Append(Chars[random.Next(Chars.Length)]);
                    }
                }

                string message = sb.ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.Equal(message, JToken.Parse(status?.Output));
        }

        private StringBuilder GenerateMediumRandomStringPayload()
        {
            // Generate a medium random string payload
            const int TargetPayloadSize = 128 * 1024; // 128 KB
            const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.-";
            var sb = new StringBuilder();
            var random = new Random();
            while (Encoding.Unicode.GetByteCount(sb.ToString()) < TargetPayloadSize)
            {
                for (int i = 0; i < 1000; i++)
                {
                    sb.Append(Chars[random.Next(Chars.Length)]);
                }
            }

            return sb;
        }

        /// <summary>
        /// Tests an orchestration that does two consecutive fan-out, fan-ins.
        /// This is a regression test for https://github.com/Azure/durabletask/issues/241.
        /// </summary>
        [Fact]
        public async Task DoubleFanOut()
        {
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DoubleFanOut), null);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(OrchestrationStatus.Completed, status?.OrchestrationStatus);
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

            [KnownType(typeof(Activities.HelloFailActivity))]
            internal class SayHelloWithActivityFail : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.HelloFailActivity), input);
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
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialOrchestratorFail : TaskOrchestration<long, int>
            {
                public static bool ShouldFail = true;
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.Multiply), new[] { result, i }));
                    }
                    if (ShouldFail)
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }
                    return result;
                }
            }

            [KnownType(typeof(Activities.MultiplyMultipleActivityFail))]
            internal class FactorialMultipleActivityFail : TaskOrchestration<long, int>
            {
                public override async Task<long> RunTask(OrchestrationContext context, int n)
                {
                    long result = 1;
                    for (int i = 1; i <= n; i++)
                    {
                        result = await (context.ScheduleTask<long>(typeof(Activities.MultiplyMultipleActivityFail), new[] { result, i }));
                    }

                    return result;
                }
            }

            [KnownType(typeof(Activities.Multiply))]
            internal class FactorialNoReplay : Factorial
            {
                public override Task<long> RunTask(OrchestrationContext context, int n)
                {
                    if (context.IsReplaying)
                    {
                        throw new Exception("Replaying is forbidden in this test.");
                    }

                    return base.RunTask(context, n);
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

            [KnownType(typeof(Activities.Hello))]
            internal class FanOutFanIn : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.HelloFailFanOut))]
            internal class FanOutFanInRewind : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.HelloFailFanOut), i.ToString("000"));
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class SemiLargePayloadFanOutFanIn : TaskOrchestration<string, int>
            {
                static readonly string Some50KBPayload = new string('x', 25 * 1024); // Assumes UTF-16 encoding
                static readonly string Some16KBPayload = new string('x', 8 * 1024); // Assumes UTF-16 encoding

                public override async Task<string> RunTask(OrchestrationContext context, int parallelTasks)
                {
                    var tasks = new Task[parallelTasks];
                    for (int i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Echo), Some50KBPayload);
                    }

                    await Task.WhenAll(tasks);

                    return "Done";
                }

                public override string GetStatus()
                {
                    return Some16KBPayload;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ChildWorkflowSubOrchestrationFail : TaskOrchestration<string, int>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating sub-orchestration failure...");
                    }
                    var result = await context.ScheduleTask<string>(typeof(Activities.Hello), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ChildWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailSubOrchestrationActivity), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ChildWorkflowNestedActivityFail : TaskOrchestration<string, int>
            {
                public override async Task<string> RunTask(OrchestrationContext context, int input)
                {
                    var result = await context.ScheduleTask<string>(typeof(Activities.HelloFailNestedSuborchestration), input);
                    return result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail))]
            [KnownType(typeof(Activities.Hello))]
            public class ParentWorkflowSubOrchestrationFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }


            [KnownType(typeof(Orchestrations.GrandparentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class ParentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail))]
            [KnownType(typeof(Activities.HelloFailSubOrchestrationActivity))]
            public class ParentWorkflowSubOrchestrationActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ChildWorkflowSubOrchestrationActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
                }
            }

            [KnownType(typeof(Orchestrations.ParentWorkflowNestedActivityFail))]
            [KnownType(typeof(Orchestrations.ChildWorkflowNestedActivityFail))]
            [KnownType(typeof(Activities.HelloFailNestedSuborchestration))]
            public class GrandparentWorkflowNestedActivityFail : TaskOrchestration<string, bool>
            {
                public static string Result;
                public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
                {
                    var results = new Task<string>[2];
                    for (int i = 0; i < 2; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(Orchestrations.ParentWorkflowNestedActivityFail), i);
                        if (waitForCompletion)
                        {
                            await r;
                        }
                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                    return Result;
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
                    Assert.Equal("operation", name);
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class CharacterCounter : TaskOrchestration<Tuple<string, int>, Tuple<string, int>>
            {
                TaskCompletionSource<string> waitForOperationHandle;

                public override async Task<Tuple<string, int>> RunTask(OrchestrationContext context, Tuple<string, int> inputData)
                {
                    string operation = await this.WaitForOperation();
                    bool done = false;
                    switch (operation?.ToLowerInvariant())
                    {
                        case "double":
                            inputData = new Tuple<string, int>(
                                $"{inputData.Item1}{inputData.Item1.Reverse()}",
                                inputData.Item2 * 2);
                            break;
                        case "end":
                            done = true;
                            break;
                    }

                    if (!done)
                    {
                        context.ContinueAsNew(inputData);
                    }

                    return inputData;
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
                    Assert.Equal("operation", name);
                    if (this.waitForOperationHandle != null)
                    {
                        this.waitForOperationHandle.SetResult(input);
                    }
                }
            }

            internal class Approval : TaskOrchestration<string, TimeSpan, bool, string>
            {
                TaskCompletionSource<bool> waitForApprovalHandle;
                public static bool shouldFail = false;

                public override async Task<string> RunTask(OrchestrationContext context, TimeSpan timeout)
                {
                    DateTime deadline = context.CurrentUtcDateTime.Add(timeout);

                    using (var cts = new CancellationTokenSource())
                    {
                        Task<bool> approvalTask = this.GetWaitForApprovalTask();
                        Task timeoutTask = context.CreateTimer(deadline, cts.Token);

                        if (shouldFail)
                        {
                            throw new Exception("Simulating unhanded error exception");
                        }

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
                    Assert.Equal("approval", name);
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
                        catch (TaskFailedException)
                        {
                            catchCount++;
                        }
                    }

                    return catchCount;
                }
            }

            [KnownType(typeof(Activities.Echo))]
            internal class Echo : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<string>(typeof(Activities.Echo), input);
                }
            }

            [KnownType(typeof(Activities.EchoBytes))]
            internal class EchoBytes : TaskOrchestration<byte[], byte[]>
            {
                public override Task<byte[]> RunTask(OrchestrationContext context, byte[] input)
                {
                    return context.ScheduleTask<byte[]>(typeof(Activities.EchoBytes), input);
                }
            }

            [KnownType(typeof(Activities.WriteTableRow))]
            [KnownType(typeof(Activities.CountTableRows))]
            internal class MapReduceTableStorage : TaskOrchestration<int, int>
            {
                public override async Task<int> RunTask(OrchestrationContext context, int iterations)
                {
                    string instanceId = context.OrchestrationInstance.InstanceId;

                    var tasks = new List<Task>(iterations);
                    for (int i = 1; i <= iterations; i++)
                    {
                        tasks.Add(context.ScheduleTask<string>(
                            typeof(Activities.WriteTableRow),
                            new Tuple<string, string>(instanceId, i.ToString("000"))));
                    }

                    await Task.WhenAll(tasks);

                    return await context.ScheduleTask<int>(typeof(Activities.CountTableRows), instanceId);
                }
            }

            [KnownType(typeof(Factorial))]
            [KnownType(typeof(Activities.Multiply))]
            internal class ParentOfFactorial : TaskOrchestration<int, int>
            {
                public override Task<int> RunTask(OrchestrationContext context, int input)
                {
                    return context.CreateSubOrchestrationInstance<int>(typeof(Factorial), input);
                }
            }

            [KnownType(typeof(Activities.Hello))]
            internal class DoubleFanOut : TaskOrchestration<string, string>
            {
                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    Random r = new Random();
                    var tasks = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks[i] = context.ScheduleTask<string>(typeof(Activities.Hello), i.ToString());
                    }

                    await Task.WhenAll(tasks);

                    var tasks2 = new Task<string>[5];
                    for (int i = 0; i < 5; i++)
                    {
                        int x = r.Next(10000);
                        tasks2[i] = context.ScheduleTask<string>(typeof(Activities.Hello), (i + 10).ToString());
                    }

                    await Task.WhenAll(tasks2);

                    return "OK";
                }
            }

            [KnownType(typeof(AutoStartOrchestration.Responder))]
            internal class AutoStartOrchestration : TaskOrchestration<string, string>
            {
                private readonly TaskCompletionSource<string> tcs
                    = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

                // HACK: This is just a hack to communicate result of orchestration back to test
                public static bool OkResult;

                private const string ChannelName = "conversation";

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var responderId = $"@{typeof(Responder).FullName}";
                    var responderInstance = new OrchestrationInstance() { InstanceId = responderId };

                    // send the id of this orchestration to a not-yet-started orchestration
                    context.SendEvent(responderInstance, ChannelName, context.OrchestrationInstance.InstanceId);

                    // wait for a response event 
                    var message = await tcs.Task;
                    if (message != "hello from autostarted orchestration")
                        throw new Exception("test failed");

                    OkResult = true;

                    return "OK";
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    if (name == ChannelName)
                    {
                        tcs.TrySetResult(input);
                    }
                }

                public class Responder : TaskOrchestration<string, string>
                {
                    private readonly TaskCompletionSource<string> tcs
                        = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

                    public async override Task<string> RunTask(OrchestrationContext context, string input)
                    {
                        var message = await tcs.Task;
                        string responseString;

                        // send a message back to the sender
                        if (input != null)
                        {
                            responseString = "expected null input for autostarted orchestration";
                        }
                        else
                        {
                            responseString = "hello from autostarted orchestration";
                        }
                        var senderInstance = new OrchestrationInstance() { InstanceId = message };
                        context.SendEvent(senderInstance, ChannelName, responseString);

                        return "this return value is not observed by anyone";
                    }

                    public override void OnEvent(OrchestrationContext context, string name, string input)
                    {
                        if (name == ChannelName)
                        {
                            tcs.TrySetResult(input);
                        }
                    }
                }
            }
        }

        static class Activities
        {
            internal class HelloFailActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailFanOut : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2) //&& (input == "0" || input == "2"))
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailMultipleActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailNestedSuborchestration : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class HelloFailSubOrchestrationActivity : TaskActivity<string, string>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override string Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    if (ShouldFail1 || ShouldFail2)
                    {
                        throw new Exception("Simulating unhandled activty function failure...");
                    }

                    return $"Hello, {input}!";
                }
            }

            internal class Hello : TaskActivity<string, string>
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

            internal class Multiply : TaskActivity<long[], long>
            {
                protected override long Execute(TaskContext context, long[] values)
                {
                    return values[0] * values[1];
                }
            }

            internal class MultiplyMultipleActivityFail : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

                    return values[0] * values[1];
                }
            }
            internal class MultiplyFailOrchestration : TaskActivity<long[], long>
            {
                public static bool ShouldFail1 = true;
                public static bool ShouldFail2 = true;
                protected override long Execute(TaskContext context, long[] values)
                {
                    if ((ShouldFail1 && (values[1] == 1)) || (ShouldFail2 && values[1] == 2))
                    {
                        throw new Exception("Simulating a transient, unhandled exception");
                    }

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

            internal class WriteTableRow : TaskActivity<Tuple<string, string>, string>
            {
                static CloudTable cachedTable;

                internal static CloudTable TestCloudTable
                {
                    get
                    {
                        if (cachedTable == null)
                        {
                            string connectionString = TestHelpers.GetStorageConnectionString();
                            CloudTable table = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient().GetTableReference("TestTable");
                            table.CreateIfNotExistsAsync().Wait();
                            cachedTable = table;
                        }

                        return cachedTable;
                    }
                }

                protected override string Execute(TaskContext context, Tuple<string, string> rowData)
                {
                    var entity = new DynamicTableEntity(
                        partitionKey: rowData.Item1,
                        rowKey: $"{rowData.Item2}.{Guid.NewGuid():N}");
                    TestCloudTable.ExecuteAsync(TableOperation.Insert(entity)).Wait();
                    return null;
                }
            }

            internal class CountTableRows : TaskActivity<string, int>
            {
                protected override int Execute(TaskContext context, string partitionKey)
                {
                    var query = new TableQuery<DynamicTableEntity>().Where(
                        TableQuery.GenerateFilterCondition(
                            "PartitionKey",
                            QueryComparisons.Equal,
                            partitionKey));

                    return WriteTableRow.TestCloudTable.ExecuteQuerySegmentedAsync(query, null).GetAwaiter().GetResult().Count();
                }
            }

            internal class Echo : TaskActivity<string, string>
            {
                protected override string Execute(TaskContext context, string input)
                {
                    return input;
                }
            }

            internal class EchoBytes : TaskActivity<byte[], byte[]>
            {
                protected override byte[] Execute(TaskContext context, byte[] input)
                {
                    return input;
                }
            }
        }
    }
}
