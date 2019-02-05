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
    using System.Configuration;
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
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
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
        public async Task ParentOfSequentialOrchestration()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.ParentOfFactorial), 10);
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
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world one");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world two");
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                var results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(2, results.Count);
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world one!\""));
                Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world two!\""));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task EventConversation(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.EventConversationOrchestration), "");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }


        [TestMethod]
        public async Task PurgeInstanceHistoryForSingleInstanceWithoutLargeMessageBlobs()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                string instanceId = Guid.NewGuid().ToString();
                await host.StartAsync();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 110, instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 0);

                IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

                await client.PurgeInstanceHistory();

                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

                orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.IsNull(orchestrationStateList.First());

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForSingleInstanceWithLargeMessageBlobs()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                string instanceId = Guid.NewGuid().ToString();
                string message = this.GenerateMediumRandomStringPayload().ToString();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, instanceId);
                OrchestrationState status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 0);

                IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0);

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(1, results.Count);

                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    instanceId,
                    results.First(x => x.OrchestrationInstance.InstanceId == instanceId).Output,
                    Encoding.UTF8.GetByteCount(message));

                await client.PurgeInstanceHistory();


                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

                orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.IsNull(orchestrationStateList.First());

                blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.AreEqual(0, blobCount);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForTimePeriodDeleteAll()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.Now;
                string firstInstanceId = "instance1";
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string secondInstanceId = "instance2";
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string thirdInstanceId = "instance3";
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                string fourthInstanceId = "instance4";
                string message = this.GenerateMediumRandomStringPayload().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, fourthInstanceId);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(4, results.Count);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == firstInstanceId).Output);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == secondInstanceId).Output);
                Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == thirdInstanceId).Output);

                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    fourthInstanceId,
                    results.First(x => x.OrchestrationInstance.InstanceId == fourthInstanceId).Output,
                    Encoding.UTF8.GetByteCount(message));

                List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.IsTrue(firstHistoryEvents.Count > 0);

                List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(thirdHistoryEvents.Count > 0);

                List<HistoryStateEvent> fourthHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(fourthHistoryEvents.Count > 0);

                IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
                Assert.AreEqual(1, fourthOrchestrationStateList.Count);
                Assert.AreEqual(fourthInstanceId, fourthOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                int blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
                Assert.AreEqual(6, blobCount);

                await client.PurgeInstanceHistoryByTimePeriod(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus>
                    {
                        OrchestrationStatus.Completed,
                        OrchestrationStatus.Terminated,
                        OrchestrationStatus.Failed,
                        OrchestrationStatus.Running
                    });

                List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.AreEqual(0, secondHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.AreEqual(0, thirdHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent>fourthHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(fourthInstanceId);
                Assert.AreEqual(0, fourthHistoryEventsAfterPurging.Count);

                firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.IsNull(firstOrchestrationStateList.First());

                secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.IsNull(secondOrchestrationStateList.First());

                thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.IsNull(thirdOrchestrationStateList.First());

                fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
                Assert.AreEqual(1, fourthOrchestrationStateList.Count);
                Assert.IsNull(fourthOrchestrationStateList.First());

                blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
                Assert.AreEqual(0, blobCount);

                await host.StopAsync();
            }
        }

        private async Task<int> GetBlobCount(string containerName, string directoryName)
        {
            string storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];
            CloudStorageAccount storageAccount;
            if (!CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
            {
                return 0;
            }

            CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            await cloudBlobContainer.CreateIfNotExistsAsync();
            CloudBlobDirectory instanceDirectory = cloudBlobContainer.GetDirectoryReference(directoryName);
            int blobCount = 0;
            BlobContinuationToken blobContinuationToken = null;
            do
            {
                BlobResultSegment results = await instanceDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                blobContinuationToken = results.ContinuationToken;
                blobCount += results.Results.Count();
            } while (blobContinuationToken != null);

            return blobCount;
        }


        [TestMethod]
        public async Task PurgeInstanceHistoryForTimePeriodDeletePartially()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                DateTime startDateTime = DateTime.Now;
                string firstInstanceId = Guid.NewGuid().ToString();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                DateTime endDateTime = DateTime.Now;
                await Task.Delay(5000);
                string secondInstanceId = Guid.NewGuid().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                string thirdInstanceId = Guid.NewGuid().ToString();
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(3, results.Count);
                Assert.IsNotNull(results[0].Output.Equals("\"Done\""));
                Assert.IsNotNull(results[1].Output.Equals("\"Done\""));
                Assert.IsNotNull(results[2].Output.Equals("\"Done\""));


                List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.IsTrue(firstHistoryEvents.Count > 0);

                List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(secondHistoryEvents.Count > 0);

                IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                await client.PurgeInstanceHistoryByTimePeriod(startDateTime, endDateTime, new List<OrchestrationStatus> { OrchestrationStatus.Completed, OrchestrationStatus.Terminated, OrchestrationStatus.Failed, OrchestrationStatus.Running });

                List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
                Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

                List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
                Assert.IsTrue(secondHistoryEventsAfterPurging.Count > 0);

                List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
                Assert.IsTrue(thirdHistoryEventsAfterPurging.Count > 0);

                firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.IsNull(firstOrchestrationStateList.First());

                secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates parallel function execution by enumerating all files in the current directory 
        /// in parallel and getting the sum total of all file sizes.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
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
        [DataRow(false)]
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
                await client.RaiseEventAsync("operation", "incr");
                await client.RaiseEventAsync("operation", "incr");
                await client.RaiseEventAsync("operation", "decr");
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
        /// End-to-end test which validates the ContinueAsNew functionality by implementing character counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestrationForLargeInput(bool enableExtendedSessions)
        {
            await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
        }

        /// <summary>
        /// End-to-end test which validates the deletion of all data generated by the ContinueAsNew functionality in the character counter actor pattern.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ActorOrchestrationDeleteAllLargeMessageBlobs(bool enableExtendedSessions)
        {
            DateTime startDateTime = DateTime.UtcNow;

            Tuple<string, TestOrchestrationClient> resultTuple = await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
            string instanceId = resultTuple.Item1;
            TestOrchestrationClient client = resultTuple.Item2;

            List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
            Assert.IsTrue(historyEvents.Count > 0);

            IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
            Assert.AreEqual(1, orchestrationStateList.Count);
            Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

            int blobCount = await this.GetBlobCount("test-largemessages", instanceId);

            Assert.AreEqual(3, blobCount);

            await client.PurgeInstanceHistoryByTimePeriod(
                startDateTime,
                DateTime.UtcNow,
                new List<OrchestrationStatus>
                {
                    OrchestrationStatus.Completed,
                    OrchestrationStatus.Terminated,
                    OrchestrationStatus.Failed,
                    OrchestrationStatus.Running
                });

            historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
            Assert.AreEqual(0, historyEvents.Count);

            orchestrationStateList = await client.GetStateAsync(instanceId);
            Assert.AreEqual(1, orchestrationStateList.Count);
            Assert.IsNull(orchestrationStateList.First());

            blobCount = await this.GetBlobCount("test-largemessages", instanceId);
            Assert.AreEqual(0, blobCount);
        }

        private async Task<Tuple<string, TestOrchestrationClient>> ValidateCharacterCounterIntegrationTest(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string initialMessage = this.GenerateMediumRandomStringPayload().ToString();
                int counter = initialMessage.Length;
                var initialValue = new Tuple<string, int>(initialMessage, counter);
                TestOrchestrationClient client =
                    await host.StartOrchestrationAsync(typeof(Orchestrations.CharacterCounter), initialValue);

                // Need to wait for the instance to start before sending events to it.
                // TODO: This requirement may not be ideal and should be revisited.
                OrchestrationState orchestrationState =
                    await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Perform some operations
                await client.RaiseEventAsync("operation", "double");
                counter *= 2;

                // TODO: Sleeping to avoid a race condition where multiple ContinueAsNew messages
                //       are processed by the same instance at the same time, resulting in a corrupt
                //       storage failure in DTFx.
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                counter *= 2;
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                counter *= 2;
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                counter *= 2;
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                counter *= 2;
                await Task.Delay(10000);

                // Make sure it's still running and didn't complete early (or fail).
                var status = await client.GetStatusAsync();
                Assert.IsTrue(
                    status?.OrchestrationStatus == OrchestrationStatus.Running ||
                    status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

                // The end message will cause the actor to complete itself.
                await client.RaiseEventAsync("operation", "end");

                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                var result = status?.Output;
                Assert.IsNotNull(result);

                await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, result);
                await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Input);

                await host.StopAsync();

                return new Tuple<string, TestOrchestrationClient>(
                    orchestrationState.OrchestrationInstance.InstanceId,
                    client);
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
        /// End-to-end test which validates the Rewind functionality on more than one orchestration.
        /// </summary>
        [TestMethod]
        public async Task RewindOrchestrationsFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                Orchestrations.FactorialOrchestratorFail.ShouldFail = true;
                await host.StartAsync();

                string singletonInstanceId1 = $"1_Test_{Guid.NewGuid():N}";
                string singletonInstanceId2 = $"2_Test_{Guid.NewGuid():N}";

                var client1 = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FactorialOrchestratorFail),
                    input: 3,
                    instanceId: singletonInstanceId1);

                var statusFail = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.FactorialOrchestratorFail.ShouldFail = false;

                var client2 = await host.StartOrchestrationAsync(
                typeof(Orchestrations.SayHelloWithActivity),
                input: "Catherine",
                instanceId: singletonInstanceId2);

                await client1.RewindAsync("Rewind failed orchestration only");

                var statusRewind = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("6", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates the Rewind functionality with fan in fan out pattern.
        /// </summary>
        [TestMethod]
        public async Task RewindActivityFailFanOut()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                Activities.HelloFailFanOut.ShouldFail1 = false;
                await host.StartAsync();

                string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FanOutFanInRewind),
                    input: 3,
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailFanOut.ShouldFail2 = false;

                await client.RewindAsync("Rewind orchestrator with failed parallel activity.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("\"Done\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }


        /// <summary>
        /// End-to-end test which validates the Rewind functionality on an activity function failure 
        /// with modified (to fail initially) SayHelloWithActivity orchestrator.
        /// </summary>
        [TestMethod]
        public async Task RewindActivityFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string singletonInstanceId = $"{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.SayHelloWithActivityFail),
                    input: "World",
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailActivity.ShouldFail = false;

                await client.RewindAsync("Activity failure test.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("\"Hello, World!\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindMultipleActivityFail()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FactorialMultipleActivityFail),
                    input: 4,
                    instanceId: singletonInstanceId);

                var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.MultiplyMultipleActivityFail.ShouldFail1 = false;

                await client.RewindAsync("Rewind for activity failure 1.");

                statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.MultiplyMultipleActivityFail.ShouldFail2 = false;

                await client.RewindAsync("Rewind for activity failure 2.");

                var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                Assert.AreEqual("24", statusRewind?.Output);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindSubOrchestrationsTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientParent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentWorkflowSubOrchestrationFail),
                    input: true,
                    instanceId: ParentInstanceId);

                var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail1 = false;

                await clientParent.RewindAsync("Rewind first suborchestration failure.");

                statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail2 = false;

                await clientParent.RewindAsync("Rewind second suborchestration failure.");

                var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindSubOrchestrationActivityTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientParent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail),
                    input: true,
                    instanceId: ParentInstanceId);

                var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailSubOrchestrationActivity.ShouldFail1 = false;

                await clientParent.RewindAsync("Rewinding 1: child should still fail.");

                statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailSubOrchestrationActivity.ShouldFail2 = false;

                await clientParent.RewindAsync("Rewinding 2: child should complete.");

                var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task RewindNestedSubOrchestrationTest()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
            {
                await host.StartAsync();

                string GrandparentInstanceId = $"Grandparent_{Guid.NewGuid():N}";
                string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

                var clientGrandparent = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.GrandparentWorkflowNestedActivityFail),
                    input: true,
                    instanceId: GrandparentInstanceId);

                var statusFail = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailNestedSuborchestration.ShouldFail1 = false;

                await clientGrandparent.RewindAsync("Rewind 1: Nested child activity still fails.");

                Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

                Activities.HelloFailNestedSuborchestration.ShouldFail2 = false;

                await clientGrandparent.RewindAsync("Rewind 2: Nested child activity completes.");

                var statusRewind = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
                //Assert.AreEqual("\"Hello, Catherine!\"", statusRewind?.Output);

                await host.StopAsync();
            }
        }

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
        [DataRow(false)]
        public async Task OrchestrationConcurrency(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                Func<Task> orchestrationStarter = async delegate ()
                {
                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                    // Don't send any notification - let the internal timeout expire
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
        [DataRow(false)]
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

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await ValidateBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Output,
                    Encoding.UTF8.GetByteCount(message));

                await host.StopAsync();
            }
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

                // Large message payloads may actually get bigger when stored in blob storage.
                await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Output, (int)(readBytes.Length * 1.3));

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

                // Large message payloads may actually get bigger when stored in blob storage.
                await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Output, (int)(readBytes.Length * 1.3));

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

        /// <summary>
        /// Tests an orchestration that does two consecutive fan-out, fan-ins.
        /// This is a regression test for https://github.com/Azure/durabletask/issues/241.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task DoubleFanOut(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DoubleFanOut), null);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await host.StopAsync();
            }
        }

        private static async Task ValidateBlobUrlAsync(string taskHubName, string instanceId, string value, int originalPayloadSize = 0)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(TestHelpers.GetTestStorageAccountConnectionString());
            Assert.IsTrue(value.StartsWith(account.BlobStorageUri.PrimaryUri.OriginalString));
            Assert.IsTrue(value.Contains("/" + instanceId + "/"));
            Assert.IsTrue(value.EndsWith(".json.gz"));

            string containerName = $"{taskHubName.ToLowerInvariant()}-largemessages";
            CloudBlobClient client = account.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            Assert.IsTrue(await container.ExistsAsync(), $"Blob container {containerName} is expected to exist.");

            await client.GetBlobReferenceFromServerAsync(new Uri(value));
            CloudBlobDirectory instanceDirectory = container.GetDirectoryReference(instanceId);

            string blobName = value.Split('/').Last();
            CloudBlob blob = instanceDirectory.GetBlobReference(blobName);
            Assert.IsTrue(await blob.ExistsAsync(), $"Blob named {blob.Uri} is expected to exist.");

            if (originalPayloadSize > 0)
            {
                await blob.FetchAttributesAsync();
                Assert.IsTrue(blob.Properties.Length < originalPayloadSize, "Blob is expected to be compressed");
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
                    Assert.AreEqual("operation", name, true, "Unknown signal recieved...");
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
