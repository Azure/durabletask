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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class AzureStorageScenarioTests
    {
        /// <summary>
        /// End-to-end test which validates a simple orchestrator function which doesn't call any activity functions.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HelloWorldOrchestration_Inline(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HelloWorldOrchestration_Activity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
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
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        [TestMethod]
        public async Task SequentialOrchestrationNoReplay()
        {
            // Enable extended sesisons to ensure that the orchestration never gets replayed
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FactorialNoReplay), 10);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(10, JToken.Parse(status?.Input));
                Assert.AreEqual(3628800, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task GetAllOrchestrationStatuses()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "wolrd one");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "wolrd two");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                // Create a client for testing
                var serviceClient = host.GetServiceClient();
                // TODO Currently we can't use TaskHub. It requires review of Core team. 
                // Until then, we test it, not using TaskHub. Call diretly the method with some configuration. 
                var results = await serviceClient.GetOrchestrationStateAsync();
                Assert.AreEqual(2, results.Count);
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, wolrd one!\""));
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, wolrd two!\""));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        //[DataRow(false)] // TODO: Re-enable when fixed: https://github.com/Azure/azure-functions-durable-extension/issues/344
        public async Task ParallelOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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

        [DataTestMethod]
        [DataRow(true)]
        //[DataRow(false)]
        public async Task LargeFanOutOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 1000);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task FanOutOrchestration_LargeHistoryBatches()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                // This test creates history payloads that exceed the 4 MB limit imposed by Azure Storage
                // when 100 entities are uploaded at a time.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SemiLargePayloadFanOutFanIn), 90);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing a counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TerminateOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TimerCancellation(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TimerExpiration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        //[DataRow(false)] // TODO: Re-enable when fixed: https://github.com/Azure/azure-functions-durable-extension/issues/344
        public async Task OrchestrationConcurrency(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task HandledActivityException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task UnhandledOrchestrationException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task UnhandledActivityException(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
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
        /// Fan-out/fan-in test which ensures each operation is run only once.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        //[DataRow(false)] // TODO: Re-enable when fixed: https://github.com/Azure/azure-functions-durable-extension/issues/344
        public async Task FanOutToTableStorage(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                int iterations = 100;

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.MapReduceTableStorage), iterations);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(iterations, int.Parse(status?.Output));

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

        /// <summary>
        /// End-to-end test which validates that orchestrations with <=60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task SmallTextMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

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

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

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

                string message = sb.ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary bytes message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeBinaryByteMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Construct byte array from large binary file of size 826KB
                string originalFileName = "large.jpeg";
                string currentDirectory = Directory.GetCurrentDirectory();
                string originalFilePath = Path.Combine(currentDirectory, originalFileName);
                byte[] readBytes = File.ReadAllBytes(originalFilePath);
                
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.EchoBytes), readBytes);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                byte[] outputBytes = JToken.Parse(status?.Output).ToObject<byte[]>();
                Assert.IsTrue(readBytes.SequenceEqual(outputBytes), "Original message byte array and returned messages byte array are not equal");

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary string message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeBinaryStringMessagePayloads(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Construct string message from large binary file of size 826KB
                string originalFileName = "large.jpeg";
                string currentDirectory = Directory.GetCurrentDirectory();
                string originalFilePath = Path.Combine(currentDirectory, originalFileName);
                byte[] readBytes = File.ReadAllBytes(originalFilePath);
                string message = Convert.ToBase64String(readBytes);

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a completed singleton instance can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateCompletedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "One",
                    instanceId: singletonInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("One", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, One!", JToken.Parse(status?.Output));

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "Two",
                    instanceId: singletonInstanceId);
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Two", JToken.Parse(status?.Input));
                Assert.AreEqual("Hello, Two!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a failed singleton instance can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateFailedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: null, // this will cause the orchestration to fail
                    instanceId: singletonInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivity),
                    input: "NotNull",
                    instanceId: singletonInstanceId);
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("Hello, NotNull!", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a terminated orchestration can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateTerminatedInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{Guid.NewGuid():N}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: -1,
                    instanceId: singletonInstanceId);

                // Need to wait for the instance to start before we can terminate it.
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.TerminateAsync("sayōnara");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("-1", status?.Input);
                Assert.AreEqual("sayōnara", status?.Output);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);
                status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that a running orchestration can be recreated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task RecreateRunningInstance(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions,
                extendedSessionTimeoutInSeconds: 15))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);

                var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);
                Assert.AreEqual(null, status?.Output);

                client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 99,
                    instanceId: singletonInstanceId);

                // Note that with extended sessions, the startup time may take longer because the dispatcher
                // will wait for the current extended session to expire before the new create message is accepted.
                status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(20));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("99", status?.Input);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that an orchestration can continue processing
        /// even after its extended session has expired.
        /// </summary>
        [TestMethod]
        public async Task ExtendedSessions_SessionTimeout()
        {
            const int SessionTimeoutInseconds = 5;
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: true,
                extendedSessionTimeoutInSeconds: SessionTimeoutInseconds))
            {
                await host.StartAsync();

                string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Counter),
                    input: 0,
                    instanceId: singletonInstanceId);

                var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
                Assert.AreEqual("0", status?.Input);
                Assert.AreEqual(null, status?.Output);

                // Delay long enough for the session to expire
                await Task.Delay(TimeSpan.FromSeconds(SessionTimeoutInseconds + 1));

                await client.RaiseEventAsync("operation", "incr");
                await Task.Delay(TimeSpan.FromSeconds(2));

                // Make sure it's still running and didn't complete early (or fail).
                status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(1, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
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
        }

        static class Activities
        {
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
                            string connectionString = TestHelpers.GetTestStorageAccountConnectionString();
                            CloudTable table = CloudStorageAccount.Parse(connectionString).CreateCloudTableClient().GetTableReference("TestTable");
                            table.CreateIfNotExists();
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
                    TestCloudTable.Execute(TableOperation.Insert(entity));
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

                    return WriteTableRow.TestCloudTable.ExecuteQuery(query).Count();
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
