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
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
#if !NET462
    using OpenTelemetry;
    using OpenTelemetry.Trace;
#endif

    [TestClass]
    public class AzureStorageScenarioTests
    {
        public static readonly TimeSpan StandardTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30);

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
                var status = await client.WaitForCompletionAsync(StandardTimeout);

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
                var status = await client.WaitForCompletionAsync(StandardTimeout);

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
                var status = await client.WaitForCompletionAsync(StandardTimeout);

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
                var status = await client.WaitForCompletionAsync(StandardTimeout);

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

        /// <summary>
        /// End-to-end test which runs a slow orchestrator that causes work item renewal
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LongRunningOrchestrator(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions,
                modifySettingsAction: (AzureStorageOrchestrationServiceSettings settings) =>
                {
                    // set a short timeout so we can test that the renewal works
                    settings.ControlQueueVisibilityTimeout = TimeSpan.FromSeconds(10);
                }))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.LongRunningOrchestrator), "0");
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("ok", JToken.Parse(status?.Output));

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

        [TestMethod]
        public async Task GetInstanceIdsByPrefix()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false);
            string instanceIdPrefixGuid = "0abb6ebb-d712-453a-97c4-6c7c1f78f49f";

            string[] instanceIds = new[]
            {
                instanceIdPrefixGuid,
                instanceIdPrefixGuid + "_0_Foo",
                instanceIdPrefixGuid + "_1_Bar",
                instanceIdPrefixGuid + "_Foo",
                instanceIdPrefixGuid + "_Bar",
            };

            // Create multiple instances that we'll try to query back
            await host.StartAsync();

            TestOrchestrationClient client;
            foreach (string instanceId in instanceIds)
            {
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), input: "Greetings!", instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
            }

            // Add one more instance which shouldn't get picked up
            client = await host.StartOrchestrationAsync(
                typeof(Orchestrations.Echo),
                input: "Greetings!",
                instanceId: $"Foo_{instanceIdPrefixGuid}");
            await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            DurableStatusQueryResult queryResult = await host.service.GetOrchestrationStateAsync(
                new OrchestrationInstanceStatusQueryCondition()
                {
                    InstanceIdPrefix = instanceIdPrefixGuid,
                },
                top: instanceIds.Length,
                continuationToken: null);
            Assert.AreEqual(instanceIds.Length, queryResult.OrchestrationState.Count());
            Assert.IsNull(queryResult.ContinuationToken);

            await host.StopAsync();
        }

        [TestMethod]
        public async Task NoInstancesGetAllOrchestrationStatusesNullContinuationToken()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                var queryResult = await host.service.GetOrchestrationStateAsync(
                    new OrchestrationInstanceStatusQueryCondition(),
                    100,
                    null);

                Assert.IsNull(queryResult.ContinuationToken);
                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public async Task EventConversation(bool enableExtendedSessions, bool useFireAndForget)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.EventConversationOrchestration), useFireAndForget);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task AutoStart(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                host.AddAutoStartOrchestrator(typeof(Orchestrations.AutoStartOrchestration.Responder));

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AutoStartOrchestration), "");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual("OK", JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ContinueAsNewThenTimer(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.ContinueAsNewThenTimerOrchestration), 0);
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
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

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
                Assert.IsNull(orchestrationStateList[0]);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ValidateCustomStatusPersists()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();

                string customStatus = "custom_status";
                var client = await host.StartOrchestrationAsync(
                    typeof(Test.Orchestrations.ChangeStatusOrchestration),
                    new string[] { customStatus });
                var state = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, state?.OrchestrationStatus);
                Assert.AreEqual(customStatus, JToken.Parse(state?.Status));

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ValidateNullCustomStatusPersists()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(
                    typeof(Test.Orchestrations.ChangeStatusOrchestration),
                    // First set "custom_status", then set null and make sure it persists
                    new string[] { "custom_status", null });
                var state = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.AreEqual(OrchestrationStatus.Completed, state?.OrchestrationStatus);
                Assert.AreEqual(null, JToken.Parse(state?.Status).Value<string>());

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

                string result = JToken.Parse(results.First(x => x.OrchestrationInstance.InstanceId == instanceId).Output).ToString();
                Assert.AreEqual(message, result);

                await client.PurgeInstanceHistory();

                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

                orchestrationStateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, orchestrationStateList.Count);
                Assert.IsNull(orchestrationStateList[0]);

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
                string result = JToken.Parse(results.First(x => x.OrchestrationInstance.InstanceId == fourthInstanceId).Output).ToString();
                Assert.AreEqual(message, result);

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

                List<HistoryStateEvent> fourthHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(fourthInstanceId);
                Assert.AreEqual(0, fourthHistoryEventsAfterPurging.Count);

                firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
                Assert.AreEqual(1, firstOrchestrationStateList.Count);
                Assert.IsNull(firstOrchestrationStateList[0]);

                secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
                Assert.AreEqual(1, secondOrchestrationStateList.Count);
                Assert.IsNull(secondOrchestrationStateList[0]);

                thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
                Assert.AreEqual(1, thirdOrchestrationStateList.Count);
                Assert.IsNull(thirdOrchestrationStateList[0]);

                fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
                Assert.AreEqual(1, fourthOrchestrationStateList.Count);
                Assert.IsNull(fourthOrchestrationStateList[0]);

                blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
                Assert.AreEqual(0, blobCount);

                await host.StopAsync();
            }
        }

        private async Task<int> GetBlobCount(string containerName, string directoryName)
        {
            var client = new BlobServiceClient(TestHelpers.GetTestStorageAccountConnectionString());

            var containerClient = client.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync();

            return await containerClient.GetBlobsAsync(traits: BlobTraits.Metadata, prefix: directoryName).CountAsync();
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
                Assert.IsNull(firstOrchestrationStateList[0]);

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
            Assert.IsNull(orchestrationStateList[0]);

            blobCount = await this.GetBlobCount("test-largemessages", instanceId);
            Assert.AreEqual(0, blobCount);
        }

        private async Task<Tuple<string, TestOrchestrationClient>> ValidateCharacterCounterIntegrationTest(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string initialMessage = this.GenerateMediumRandomStringPayload().ToString();
                string finalMessage = initialMessage;
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
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
                counter *= 2;

                // TODO: Sleeping to avoid a race condition where multiple ContinueAsNew messages
                //       are processed by the same instance at the same time, resulting in a corrupt
                //       storage failure in DTFx.
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
                counter *= 2;
                await Task.Delay(10000);
                await client.RaiseEventAsync("operation", "double");
                finalMessage = finalMessage + new string(finalMessage.Reverse().ToArray());
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
                var result = JObject.Parse(status?.Output);
                Assert.IsNotNull(result);

                var input = JObject.Parse(status?.Input);
                Assert.AreEqual(finalMessage, input["Item1"].Value<string>());
                Assert.AreEqual(finalMessage.Length, input["Item2"].Value<int>());
                Assert.AreEqual(finalMessage, result["Item1"].Value<string>());
                Assert.AreEqual(counter, result["Item2"].Value<int>());

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
        /// End-to-end test which validates the Suspend-Resume functionality.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task SuspendResumeOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                string originalStatus = "OGstatus";
                string suspendReason = "sleepyOrch";
                string changedStatus = "newStatus";

                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Test.Orchestrations.NextExecution), originalStatus);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                // Test case 1: Suspend changes the status Running->Suspended
                await client.SuspendAsync(suspendReason);
                var status = await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Suspended);
                Assert.AreEqual(OrchestrationStatus.Suspended, status?.OrchestrationStatus);
                Assert.AreEqual(suspendReason, status?.Output);

                // Test case 2: external event does not go through
                await client.RaiseEventAsync("changeStatusNow", changedStatus);
                status = await client.GetStatusAsync();
                Assert.AreEqual(originalStatus, JToken.Parse(status?.Status));

                // Test case 3: external event now goes through
                await client.ResumeAsync("wakeUp");
                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(changedStatus, JToken.Parse(status?.Status));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Test that a suspended orchestration can be terminated.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TerminateSuspendedOrchestration(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

                await client.SuspendAsync("suspend");
                await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Suspended);

                await client.TerminateAsync("terminate");

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual("terminate", status?.Output);

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

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task TimerDelay(bool useUtc)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();
                // by convention, DateTime objects are expected to be in UTC, but previous version of DTFx.AzureStorage
                // performed a implicit conversions to UTC when different timezones where used. This test ensures
                // that behavior is backwards compatible, despite not being recommended.
                var startTime = useUtc ? DateTime.UtcNow : DateTime.Now;
                var delay = TimeSpan.FromSeconds(5);
                var fireAt = startTime.Add(delay);
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeInline), fireAt);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                var actualDelay = DateTime.UtcNow - startTime.ToUniversalTime();
                Assert.IsTrue(
                    actualDelay >= delay && actualDelay < delay + TimeSpan.FromSeconds(10),
                    $"Expected delay: {delay}, ActualDelay: {actualDelay}");

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task OrchestratorStartAtAcceptsAllDateTimeKinds(bool useUtc)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();
                // by convention, DateTime objects are expected to be in UTC, but previous version of DTFx.AzureStorage
                // performed a implicit conversions to UTC when different timezones where used. This test ensures
                // that behavior is backwards compatible, despite not being recommended.

                // set up orchestrator start time
                var currentTime = DateTime.Now;
                var delay = TimeSpan.FromSeconds(5);
                var startAt = currentTime.Add(delay);

                if (useUtc)
                {
                    startAt = startAt.ToUniversalTime();
                }


                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), input: string.Empty, startAt: startAt);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                var orchestratorState = await client.GetStateAsync(client.InstanceId);
                var actualScheduledStartTime = status.ScheduledStartTime;

                // internal representation of DateTime is always UTC
                var expectedScheduledStartTime = startAt.ToUniversalTime();
                Assert.AreEqual(expectedScheduledStartTime, actualScheduledStartTime);
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
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

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
#if NETCOREAPP
            EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
#else
            try
            {
                EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
            }
            catch (FormatException)
            {
                Assert.Inconclusive("Known issue with .NET Framework, EventSourceAnalyzer, and DateTime parameters");
            }
#endif
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
        public async Task LargeQueueTextMessagePayloads_BlobUrl(bool enableExtendedSessions)
        {
            // Small enough to be a small table message, but a large queue message
            const int largeMessageSize = 25 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 3, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Output));
                Assert.AreEqual(message, JToken.Parse(status.Input));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTableTextMessagePayloads_SizeViolation_BlobUrl(bool enableExtendedSessions)
        {
            // Small enough to be a small queue message, but a large table message due to UTF encoding differences of ASCII characters
            const int largeMessageSize = 32 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await ValidateLargeMessageBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Input,
                    Encoding.UTF8.GetByteCount(message));
                await ValidateLargeMessageBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Output,
                    Encoding.UTF8.GetByteCount(message));
                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test that validates large (>60KB) messages stored in blob storage can be retrieved successfully,
        /// when the instance ID includes special characters like '|' that can affect blob URL encoding.
        /// </summary>
        [TestMethod]
        public async Task LargeMessage_WithEscapedInstanceId_CanBeStoredAndFetchedSuccessfully()
        {
            // Genereates a random large message.
            const int largeMessageSize = 60 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();

                // Use an instanceId that contains special characters which must be escaped in URIs
                string id = "test|123:with white spcae";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), input:message, instanceId: id);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // Verify that the output matches the original message (the blob was successfully downloaded and not returned as a URL) 
                StringAssert.Contains(status.Output.ToString(), message);
                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task TagsAreAvailableInOrchestrationState()
        {
            const string TagMessage = "message";
            const string Tag = "tag";

            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false, fetchLargeMessages: true);
            await host.StartAsync();
            var tags = new Dictionary<string, string> { { Tag, TagMessage } };
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), "Hello, world!", tags: tags);
            var statuses = await client.GetStateAsync(client.InstanceId);
            var status = statuses.Single();

            Assert.IsNotNull(status.Tags);
            Assert.AreEqual(1, status.Tags.Count);
            Assert.IsTrue(status.Tags.TryGetValue(Tag, out string actualMessage));
            Assert.AreEqual(TagMessage, actualMessage);

            await host.StopAsync();
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeOverallTextMessagePayloads_BlobUrl(bool enableExtendedSessions)
        {
            const int largeMessageSize = 80 * 1024;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: false))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(numChars: largeMessageSize).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                await ValidateLargeMessageBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Input,
                    Encoding.UTF8.GetByteCount(message));
                await ValidateLargeMessageBlobUrlAsync(
                    host.TaskHub,
                    client.InstanceId,
                    status?.Output,
                    Encoding.UTF8.GetByteCount(message));

                Assert.IsTrue(status.Output.EndsWith("-Result.json.gz"));
                Assert.IsTrue(status.Input.EndsWith("-Input.json.gz"));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads_FetchLargeMessages(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
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
        public async Task LargeTableTextMessagePayloads_FetchLargeMessages(bool enableExtendedSessions)
        {
            // Small enough to be a small queue message, but a large table message due to UTF encoding differences of ASCII characters
            const int largeMessageSize = 32 * 1024;
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB of tag data can be run successfully.
        /// </summary>
        [TestMethod]
        public async Task LargeOrchestrationTags()
        {
            const int largeMessageSize = 64 * 1024;
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false, fetchLargeMessages: true);
            await host.StartAsync();

            string bigMessage = this.GenerateMediumRandomStringPayload(largeMessageSize, utf8ByteSize: 1, utf16ByteSize: 2).ToString();
            var bigTags = new Dictionary<string, string> { { "BigTag", bigMessage } };
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), "Hello, world!", tags: bigTags);
            var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            // TODO: Uncomment these assertions as part of https://github.com/Azure/durabletask/issues/840.
            ////Assert.IsNotNull(status?.Tags);
            ////Assert.AreEqual(bigTags.Count, status.Tags.Count);
            ////Assert.IsTrue(bigTags.TryGetValue("BigTag", out string actualMessage));
            ////Assert.AreEqual(bigMessage, actualMessage);

            await host.StopAsync();
        }

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task NonBlobUriPayload_FetchLargeMessages_RetainsOriginalPayload(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = "https://anygivenurl.azurewebsites.net";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
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
        public async Task LargeTextMessagePayloads_FetchLargeMessages_QueryState(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                //Ensure that orchestration state querying also retrieves messages 
                status = (await client.GetStateAsync(status.OrchestrationInstance.InstanceId)).First();

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status?.Input));
                Assert.AreEqual(message, JToken.Parse(status?.Output));

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test which validates that exception messages that are considered valid Urls in the Uri.TryCreate() method
        /// are handled with an additional Uri format check
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task LargeTextMessagePayloads_URIFormatCheck(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions, fetchLargeMessages: true))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.ThrowException), "durabletask.core.exceptions.taskfailedexception: Task failed with an unhandled exception: This is an invalid operation.)");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                //Ensure that orchestration state querying also retrieves messages 
                status = (await client.GetStateAsync(status.OrchestrationInstance.InstanceId)).First();

                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                Assert.IsTrue(status?.Output.Contains("invalid operation") == true);

                await host.StopAsync();
            }
        }

        private StringBuilder GenerateMediumRandomStringPayload(int numChars = 128 * 1024, short utf8ByteSize = 1, short utf16ByteSize = 2)
        {
            string Chars;
            if (utf16ByteSize != 2 && utf16ByteSize != 4)
            {
                throw new InvalidOperationException($"No characters have byte size {utf16ByteSize} for UTF16");
            }
            else if (utf8ByteSize < 1 || utf8ByteSize > 4)
            {
                throw new InvalidOperationException($"No characters have byte size {utf8ByteSize} for UTF8.");
            }
            else if (utf8ByteSize == 1 && utf16ByteSize == 2)
            {
                // Use a character set that is small for UTF8 and large for UTF16
                // This allows us to produce a smaller string for UTF8 than UTF16.
                Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 {}/<>.";
            }
            else if (utf16ByteSize == 2 && utf8ByteSize == 3)
            {
                // Use a character set that is small for UTF16 and large for UTF8
                // This allows us to produce a smaller string for UTF16 than UTF8.
                Chars = "มันสนุกพี่บุ๋มมันโจ๊ะ";
            }
            else
            {
                throw new InvalidOperationException($"This method has not yet added support for characters of utf8 size {utf8ByteSize} and utf16 size {utf16ByteSize}");
            }

            var random = new Random();
            var sb = new StringBuilder();
            for (int i = 0; i < numChars; i++)
            {
                sb.Append(Chars[random.Next(Chars.Length)]);
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

                byte[] resultBytes = JObject.Parse(status?.Output).ToObject<byte[]>();
                Assert.IsTrue(readBytes.SequenceEqual(resultBytes));

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
                string result = JToken.Parse(status?.Output).ToString();
                Assert.AreEqual(message, result);

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

        private static async Task ValidateLargeMessageBlobUrlAsync(string taskHubName, string instanceId, string value, int originalPayloadSize = 0)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);

            var serviceClient = new BlobServiceClient(TestHelpers.GetTestStorageAccountConnectionString());
            Assert.IsTrue(value.StartsWith(serviceClient.Uri.OriginalString));
            Assert.IsTrue(value.Contains("/" + sanitizedInstanceId + "/"));
            Assert.IsTrue(value.EndsWith(".json.gz"));

            string blobName = value.Split('/').Last();
            Assert.IsTrue(await new Blob(serviceClient, new Uri(value)).ExistsAsync(), $"Blob named {blobName} is expected to exist.");

            string containerName = $"{taskHubName.ToLowerInvariant()}-largemessages";
            BlobContainerClient container = serviceClient.GetBlobContainerClient(containerName);
            Assert.IsTrue(await container.ExistsAsync(), $"Blob container {containerName} is expected to exist.");
            BlobItem blob = await container
                .GetBlobsByHierarchyAsync(BlobTraits.Metadata, prefix: sanitizedInstanceId)
                .Where(x => x.IsBlob && x.Blob.Name == sanitizedInstanceId + "/" + blobName)
                .Select(x => x.Blob)
                .SingleOrDefaultAsync();
            Assert.IsNotNull(blob);

            if (originalPayloadSize > 0)
            {
                Assert.IsTrue(blob.Properties.ContentLength < originalPayloadSize, "Blob is expected to be compressed");
            }
        }

        /// <summary>
        /// Tests the behavior of <see cref="SessionAbortedException"/> from orchestrations and activities.
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task AbortOrchestrationAndActivity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string input = Guid.NewGuid().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AbortSessionOrchestration), input);
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                Assert.IsNotNull(status.Output);
                Assert.AreEqual("True", JToken.Parse(status.Output));
                await host.StopAsync();
            }
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Inline(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeInline), "Current Time!");

                var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                var statusStartingIn30Seconds = clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                await Task.WhenAll(statusStartingNow, statusStartingIn30Seconds);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
                Assert.IsNull(statusStartingNow.Result.ScheduledStartTime);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingIn30Seconds.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingIn30Seconds.Result?.Input));
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.Result.ScheduledStartTime);

                var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
                var startIn30SecondsResult = (DateTime)JToken.Parse(statusStartingIn30Seconds.Result?.Output);

                Assert.IsTrue(startIn30SecondsResult > startNowResult);
                Assert.IsTrue(startIn30SecondsResult >= expectedStartTime);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Activity(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.CurrentTimeActivity), "Current Time!");

                var statusStartingNow = clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                var statusStartingIn30Seconds = clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                await Task.WhenAll(statusStartingNow, statusStartingIn30Seconds);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingNow.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingNow.Result?.Input));
                Assert.IsNull(statusStartingNow.Result.ScheduledStartTime);

                Assert.AreEqual(OrchestrationStatus.Completed, statusStartingIn30Seconds.Result?.OrchestrationStatus);
                Assert.AreEqual("Current Time!", JToken.Parse(statusStartingIn30Seconds.Result?.Input));
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.Result.ScheduledStartTime);

                var startNowResult = (DateTime)JToken.Parse(statusStartingNow.Result?.Output);
                var startIn30SecondsResult = (DateTime)JToken.Parse(statusStartingIn30Seconds.Result?.Output);

                Assert.IsTrue(startIn30SecondsResult > startNowResult);
                Assert.IsTrue(startIn30SecondsResult >= expectedStartTime);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Validates scheduled starts, ensuring they are executed according to defined start date time
        /// </summary>
        /// <param name="enableExtendedSessions"></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ScheduledStart_Activity_GetStatus_Returns_ScheduledStart(bool enableExtendedSessions)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                var clientStartingIn30Seconds = await host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!", startAt: expectedStartTime);
                var clientStartingNow = await host.StartOrchestrationAsync(typeof(Orchestrations.DelayedCurrentTimeActivity), "Delayed Current Time!");

                var statusStartingIn30Seconds = await clientStartingIn30Seconds.GetStatusAsync();
                Assert.IsNotNull(statusStartingIn30Seconds.ScheduledStartTime);
                Assert.AreEqual(expectedStartTime, statusStartingIn30Seconds.ScheduledStartTime);

                var statusStartingNow = await clientStartingNow.GetStatusAsync();
                Assert.IsNull(statusStartingNow.ScheduledStartTime);

                await Task.WhenAll(
                    clientStartingNow.WaitForCompletionAsync(TimeSpan.FromSeconds(35)),
                    clientStartingIn30Seconds.WaitForCompletionAsync(TimeSpan.FromSeconds(65))
                    );

                await host.StopAsync();
            }
        }

        /// <summary>
        /// End-to-end test validating the <see cref="AzureStorageOrchestrationServiceSettings.AllowReplayingTerminalInstances"/> setting.
        /// </summary>
        /// <remarks>
        /// The `AllowReplayingTerminalInstances` setting was introduced to fix a gap in
        /// the Azure Storage provider where the History table and Instance table may get out of sync.
        ///
        /// Namely, suppose we're updating an orchestrator from the Running state to Completed. If the DTFx process crashes
        /// after updating the History table but before updating the Instance table, the orchestrator will be in a terminal
        /// state according to the History table, but not the Instance table. Since the History claims the orchestrator is terminal,
        /// we will *discard* any new events that try to reach the orchestrator, including "Terminate" requests. Therefore, the data
        /// in the Instace table will remain incorrect until it is manually edited.
        ///
        /// To recover from this, users may set `AllowReplayingTerminalInstances` to true. When this is set, DTFx will not discard
        /// events for terminal orchestrators, forcing a replay which eventually updates the instances table to the right state.
        /// </remarks>
        [DataTestMethod]
        [DataRow(true, true, true)]
        [DataRow(true, true, false)]
        [DataRow(true, false, true)]
        [DataRow(true, false, false)]
        [DataRow(false, true, true)]
        [DataRow(false, true, false)]
        [DataRow(false, false, true)]
        [DataRow(false, false, false)]
        public async Task TestAllowReplayingTerminalInstances(bool enableExtendedSessions, bool sendTerminateEvent, bool allowReplayingTerminalInstances)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions,
                allowReplayingTerminalInstances: allowReplayingTerminalInstances))
            {
                await host.StartAsync();

                // Run simple orchestrator to completion, this will help us obtain a valid terminal history for the orchestrator
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), "hello!");
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                var instanceId = client.InstanceId;
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions,
                    allowReplayingTerminalInstances: allowReplayingTerminalInstances);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G")
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);


                // Send event (either terminate or external event) and wait for the event to be processed.
                // The wait is possibly flakey, but unit testing this directly is difficult today
                if (sendTerminateEvent)
                {
                    // we want to test the "Terminate" event explicitly because it's the first thing users
                    // should try when an orchestrator is in a bad state
                    await client.TerminateAsync("Foo");
                }
                else
                {
                    // we test the raise event case because, if `AllowReplayingTerminalInstances` is set to true,
                    // an "unregistered" external event (one that the orchestrator is not expected) allows the orchestrator
                    // to update the Instance table to the correct state without forcing it to end up as "Terminated".
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                if (allowReplayingTerminalInstances)
                {
                    // A replay should have occurred, forcing the instance table to be updated with a terminal status
                    state = await client.GetStateAsync(instanceId);
                    Assert.AreEqual(1, state.Count);

                    status = state.First();
                    OrchestrationStatus expectedStatus = sendTerminateEvent ? OrchestrationStatus.Terminated : OrchestrationStatus.Completed;
                    Assert.AreEqual(expectedStatus, status.OrchestrationStatus);
                }
                else
                {
                    // A replay should not have occurred, the instance table should still have the "Running" status
                    state = await client.GetStateAsync(instanceId);
                    Assert.AreEqual(1, state.Count);

                    status = state.First();
                    Assert.AreEqual(OrchestrationStatus.Running, status.OrchestrationStatus);
                }
                await host.StopAsync();
            }
        }

        [TestMethod]
        [DataRow(VersioningSettings.VersionMatchStrategy.Strict)]
        [DataRow(VersioningSettings.VersionMatchStrategy.CurrentOrOlder)]
        [DataRow(VersioningSettings.VersionMatchStrategy.None)]
        public async Task OrchestrationFailsWithVersionMismatch(VersioningSettings.VersionMatchStrategy matchStrategy)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false, versioningSettings: new VersioningSettings
            {
                Version = "1",
                MatchStrategy = matchStrategy,
                FailureStrategy = VersioningSettings.VersionFailureStrategy.Fail
            }))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", tags: new Dictionary<string, string>(), version: "2");
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                if (matchStrategy == VersioningSettings.VersionMatchStrategy.None)
                {
                    Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                }
                else
                {
                    Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                }

                await host.StopAsync();
            }
        }

        [TestMethod]
        [DataRow(VersioningSettings.VersionMatchStrategy.Strict, "1.0.0")]
        [DataRow(VersioningSettings.VersionMatchStrategy.CurrentOrOlder, "1.0.0")]
        [DataRow(VersioningSettings.VersionMatchStrategy.CurrentOrOlder, "0.9.0")]
        [DataRow(VersioningSettings.VersionMatchStrategy.None, "1.0.0")]
        public async Task OrchestrationSucceedsWithVersion(VersioningSettings.VersionMatchStrategy matchStrategy, string orchestrationVersion)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false, versioningSettings: new VersioningSettings
            {
                Version = "1.0.0",
                MatchStrategy = matchStrategy,
                FailureStrategy = VersioningSettings.VersionFailureStrategy.Fail
            }))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", tags: new Dictionary<string, string>(), version: orchestrationVersion);
                var status = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationRejectsWithVersionMismatch()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false, versioningSettings: new VersioningSettings
            {
                Version = "1",
                MatchStrategy = VersioningSettings.VersionMatchStrategy.Strict,
                FailureStrategy = VersioningSettings.VersionFailureStrategy.Reject
            }))
            {
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", tags: new Dictionary<string, string>(), version: "2");
                // We intend for this to timeout as the work should be getting rejected.
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.IsNull(status);

                // We should either be pending (recently rejected) or running (to be rejected).
                status = await client.GetStatusAsync();
                Assert.IsTrue(OrchestrationStatus.Running == status?.OrchestrationStatus || OrchestrationStatus.Pending == status?.OrchestrationStatus);

                var history = await client.GetOrchestrationHistoryAsync(client.InstanceId);
                Assert.AreEqual(0, history.Count, "A rejected orchestration should have no history as it should never have been started.");

                await host.StopAsync();
            }
        }

#if !NET462
        /// <summary>
        /// End-to-end test which validates a simple orchestrator function that calls an activity function
        /// and checks the OpenTelemetry trace information
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task OpenTelemetry_SayHelloWithActivity(bool enableExtendedSessions)
        {
            var processor = new Mock<BaseProcessor<Activity>>();

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
                {
                    await host.StartAsync();

                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithActivity), "World");
                    var status = await client.WaitForCompletionAsync(StandardTimeout);

                    await host.StopAsync();
                }
            }

            // (1) Explanation about indexes:
            // The orchestration Activity's start at Invocation[1] and each action logs
            // two activities - (Processor.OnStart(Activity) and Processor.OnEnd(Activity)
            // The Activity for orchestration execution is "started" (with the same Id, SpanId, etc.)
            // upon every replay of the orchestration so will have an OnStart invocation for each such replay, 
            // but an OnEnd at the end of orchestration execution.
            // The first OnEnd invocation is at index 2, so we start from there.

            // (2) Additional invocations:
            // processor.Invocations[0] - processor.SetParentProvider(TracerProviderSdk)
            // processor.Invocations[10] - processor.OnShutdown()
            // processor.Invocations[11] - processor.Dispose(true)

            // Create orchestration Activity
            Activity activity2 = (Activity)processor.Invocations[2].Arguments[0];
            // Task execution Activity
            Activity activity5 = (Activity)processor.Invocations[5].Arguments[0];
            // Task completed Activity
            Activity activity8 = (Activity)processor.Invocations[8].Arguments[0];
            // Orchestration execution Activity
            Activity activity9 = (Activity)processor.Invocations[9].Arguments[0];

            // Checking total number activities
            Assert.AreEqual(12, processor.Invocations.Count);

            // Checking tag values
            string activity2TypeValue = activity2.Tags.First(k => (k.Key).Equals("durabletask.type" )).Value;
            string activity5TypeValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity8TypeValue = activity8.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity9TypeValue = activity9.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;

            ActivityKind activity2Kind = activity2.Kind;
            ActivityKind activity5Kind = activity5.Kind;
            ActivityKind activity8Kind = activity8.Kind;
            ActivityKind activity9Kind = activity9.Kind;

            Assert.AreEqual("orchestration", activity2TypeValue);
            Assert.AreEqual("activity", activity5TypeValue);
            Assert.AreEqual("activity", activity8TypeValue);
            Assert.AreEqual("orchestration", activity9TypeValue);
            Assert.AreEqual(ActivityKind.Producer, activity2Kind);
            Assert.AreEqual(ActivityKind.Server, activity5Kind);
            Assert.AreEqual(ActivityKind.Client, activity8Kind);
            Assert.AreEqual(ActivityKind.Server, activity9Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(activity2.SpanId, activity9.ParentSpanId);
            Assert.AreEqual(activity8.SpanId, activity5.ParentSpanId);
            Assert.AreEqual(activity9.SpanId, activity8.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(activity2.TraceId.ToString(), activity5.TraceId.ToString(), activity8.TraceId.ToString(), activity9.TraceId.ToString());
        }

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function that waits for an external event
        /// raised through the RaiseEvent API and checks the OpenTelemetry trace information
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task OpenTelemetry_ExternalEvent_RaiseEvent(bool enableExtendedSessions)
        {
            var processor = new Mock<BaseProcessor<Activity>>();
            string instanceId = null;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
                {
                    await host.StartAsync();

                    var timeout = TimeSpan.FromSeconds(10);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    instanceId = client.InstanceId;

                    // Need to wait for the instance to start before sending events to it.
                    // TODO: This requirement may not be ideal and should be revisited.
                    await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                    await client.RaiseEventAsync("approval", eventData: true);
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    await host.StopAsync();
                }
            }

            // (1) Explanation about indexes:
            // The orchestration Activity's start at Invocation[1] and each action logs
            // two activities - (Processor.OnStart(Activity) and Processor.OnEnd(Activity)
            // The Activity for orchestration execution is "started" (with the same Id, SpanId, etc.)
            // upon every replay of the orchestration so will have an OnStart invocation for each such replay, 
            // but an OnEnd at the end of orchestration execution.
            // The first OnEnd invocation is at index 2, so we start from there.

            // (2) Additional invocations:
            // processor.Invocations[0] - processor.SetParentProvider(TracerProviderSdk)
            // processor.Invocations[8] - processor.OnShutdown()
            // processor.Invocations[9] - processor.Dispose(true)

            // Create orchestration Activity
            Activity activity2 = (Activity)processor.Invocations[2].Arguments[0];
            // External event Activity
            Activity activity5 = (Activity)processor.Invocations[5].Arguments[0];
            // Orchestration execution Activity
            Activity activity7 = (Activity)processor.Invocations[7].Arguments[0];

            // Checking total number activities
            Assert.AreEqual(10, processor.Invocations.Count);

            // Checking tag values
            string activity2TypeValue = activity2.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity5TypeValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity7TypeValue = activity7.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity5TargetInstanceIdValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;

            ActivityKind activity2Kind = activity2.Kind;
            ActivityKind activity5Kind = activity5.Kind;
            ActivityKind activity7Kind = activity7.Kind;

            Assert.AreEqual("orchestration", activity2TypeValue);
            Assert.AreEqual("event", activity5TypeValue);
            Assert.AreEqual("orchestration", activity7TypeValue);
            Assert.AreEqual(instanceId, activity5TargetInstanceIdValue);
            Assert.AreEqual(ActivityKind.Producer, activity2Kind);
            Assert.AreEqual(ActivityKind.Producer, activity5Kind);
            Assert.AreEqual(ActivityKind.Server, activity7Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(activity2.SpanId, activity7.ParentSpanId);

            // Checking trace ID values (the external event from the client is its own trace)
            Assert.AreEqual(activity2.TraceId.ToString(), activity7.TraceId.ToString());
        }

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function that fires a timer and checks the OpenTelemetry trace information
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task OpenTelemetry_TimerFired(bool enableExtendedSessions)
        {
            var processor = new Mock<BaseProcessor<Activity>>();
            string instanceId = null;

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
                {
                    await host.StartAsync();

                    var timeout = TimeSpan.FromSeconds(30);
                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Approval), timeout);
                    instanceId = client.InstanceId;

                    await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                    await host.StopAsync();
                }
            }

            // (1) Explanation about indexes:
            // The orchestration Activity's start at Invocation[1] and each action logs
            // two activities - (Processor.OnStart(Activity) and Processor.OnEnd(Activity)
            // The Activity for orchestration execution is "started" (with the same Id, SpanId, etc.)
            // upon every replay of the orchestration so will have an OnStart invocation for each such replay, 
            // but an OnEnd at the end of orchestration execution.
            // The first OnEnd invocation is at index 2, so we start from there.

            // (2) Additional invocations:
            // processor.Invocations[0] - processor.SetParentProvider(TracerProviderSdk)
            // processor.Invocations[8] - processor.OnShutdown()
            // processor.Invocations[9] - processor.Dispose(true)

            // Create orchestration Activity
            Activity activity2 = (Activity)processor.Invocations[2].Arguments[0];
            // Timer fired Activity
            Activity activity6 = (Activity)processor.Invocations[6].Arguments[0];
            // Orchestration execution Activity
            Activity activity7 = (Activity)processor.Invocations[7].Arguments[0];

            // Checking total number activities
            Assert.AreEqual(10, processor.Invocations.Count);

            // Checking tag values
            string activity2TypeValue = activity2.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity6TypeValue = activity6.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity7TypeValue = activity7.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;

            ActivityKind activity2Kind = activity2.Kind;
            ActivityKind activity6Kind = activity6.Kind;
            ActivityKind activity7Kind = activity7.Kind;

            Assert.AreEqual("orchestration", activity2TypeValue);
            Assert.AreEqual("timer", activity6TypeValue);
            Assert.AreEqual("orchestration", activity7TypeValue);
            Assert.AreEqual(ActivityKind.Producer, activity2Kind);
            Assert.AreEqual(ActivityKind.Internal, activity6Kind);
            Assert.AreEqual(ActivityKind.Server, activity7Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(activity2.SpanId, activity7.ParentSpanId);
            Assert.AreEqual(activity7.SpanId, activity6.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(activity2.TraceId.ToString(), activity6.TraceId.ToString(), activity7.TraceId.ToString());
        }

        /// <summary>
        /// End-to-end test which validates a simple orchestrator function that waits for an external event
        /// raised by calling SendEvent and checks the OpenTelemetry trace information
        /// </summary>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task OpenTelemetry_ExternalEvent_SendEvent(bool enableExtendedSessions)
        {
            var processor = new Mock<BaseProcessor<Activity>>();
            string instanceId = null;
            var responderId = $"@{typeof(Orchestrations.AutoStartOrchestration.Responder).FullName}";

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
                {
                    await host.StartAsync();

                    host.AddAutoStartOrchestrator(typeof(Orchestrations.AutoStartOrchestration.Responder));

                    var client = await host.StartOrchestrationAsync(typeof(Orchestrations.AutoStartOrchestration), "");
                    instanceId = client.InstanceId;
                    var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    await host.StopAsync();
                }
            }

            // (1) Explanation about indexes:
            // The orchestration Activity's start at Invocation[1] and each action logs
            // two activities - (Processor.OnStart(Activity) and Processor.OnEnd(Activity)
            // The Activity for orchestration execution is "started" (with the same Id, SpanId, etc.)
            // upon every replay of the orchestration so will have an OnStart invocation for each such replay, 
            // but an OnEnd at the end of orchestration execution.
            // The first OnEnd invocation is at index 2, so we start from there.

            // (2) Additional invocations:
            // processor.Invocations[0] - processor.SetParentProvider(TracerProviderSdk)
            // processor.Invocations[10] - processor.OnShutdown()
            // processor.Invocations[11] - processor.Dispose(true)

            var invocations = processor.Invocations;
            // Create orchestration (AutoStartOrchestration) Activity
            Activity activity2 = (Activity)processor.Invocations[2].Arguments[0];
            // Send event to AutoStartOrchestration.Responder Activity
            Activity activity5 = (Activity)processor.Invocations[5].Arguments[0];
            // Send event from AutoStartOrchestration.Responder back to AutoStartOrchestration Activity
            Activity activity7 = (Activity)processor.Invocations[7].Arguments[0];
            // Orchestration execution Activity
            Activity activity9 = (Activity)processor.Invocations[9].Arguments[0];

            // Checking total number activities
            Assert.AreEqual(12, processor.Invocations.Count);

            // Checking tag values
            string activity2TypeValue = activity2.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity5TypeValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity7TypeValue = activity7.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity9TypeValue = activity9.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string activity5InstanceIdValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.task.instance_id")).Value;
            string activity5TargetInstanceIdValue = activity5.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;
            string activity7InstanceIdValue = activity7.Tags.First(k => (k.Key).Equals("durabletask.task.instance_id")).Value;
            string activity7TargetInstanceIdValue = activity7.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;

            ActivityKind activity2Kind = activity2.Kind;
            ActivityKind activity5Kind = activity5.Kind;
            ActivityKind activity7Kind = activity7.Kind;
            ActivityKind activity9Kind = activity9.Kind;

            Assert.AreEqual("orchestration", activity2TypeValue);
            Assert.AreEqual("event", activity5TypeValue);
            Assert.AreEqual("event", activity7TypeValue);
            Assert.AreEqual("orchestration", activity9TypeValue);
            Assert.AreEqual(instanceId, activity5InstanceIdValue);
            Assert.AreEqual(responderId, activity5TargetInstanceIdValue);
            Assert.AreEqual(responderId, activity7InstanceIdValue);
            Assert.AreEqual(instanceId, activity7TargetInstanceIdValue);
            Assert.AreEqual(ActivityKind.Producer, activity2Kind);
            Assert.AreEqual(ActivityKind.Producer, activity5Kind);
            Assert.AreEqual(ActivityKind.Producer, activity7Kind);
            Assert.AreEqual(ActivityKind.Server, activity9Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(activity2.SpanId, activity9.ParentSpanId);
            Assert.AreEqual(activity9.SpanId, activity5.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(activity2.TraceId.ToString(), activity5.TraceId.ToString(), activity9.TraceId.ToString());
        }
#endif

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

            internal class LongRunningOrchestrator : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                    if (input == "0")
                    {
                        context.ContinueAsNew("1");
                        return Task.FromResult("");
                    }
                    else
                    {
                        return Task.FromResult("ok");
                    }
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
                                $"{inputData.Item1}{new string(inputData.Item1.Reverse().ToArray())}",
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
                        Task timeoutTask = context.CreateTimer(deadline, true, cts.Token);

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
            internal class ThrowException : TaskOrchestration<string, string>
            {
                public override async Task<string> RunTask(OrchestrationContext context, string message)
                {
                    if (string.IsNullOrEmpty(message))
                    {
                        // This throw happens directly in the orchestration.
                        throw new Exception(message);
                    }

                    // This throw happens in the implementation of an activity.
                    await context.ScheduleTask<string>(typeof(Activities.Throw), message);
                    return null;

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
                    = new TaskCompletionSource<string>();

                // HACK: This is just a hack to communicate result of orchestration back to test
                public static bool OkResult;

                private static string ChannelName = Guid.NewGuid().ToString();

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var responderId = $"@{typeof(Responder).FullName}";
                    var responderInstance = new OrchestrationInstance() { InstanceId = responderId };
                    var requestInformation = new RequestInformation { InstanceId = context.OrchestrationInstance.InstanceId, RequestId = ChannelName };

                    // send the RequestInformation containing RequestId and Instanceid of this orchestration to a not-yet-started orchestration
                    context.SendEvent(responderInstance, ChannelName, requestInformation);

                    // wait for a response event 
                    var message = await tcs.Task;
                    if (message != "hello from autostarted orchestration")
                        throw new Exception("test failed");

                    OkResult = true;

                    return "OK";
                }

                public class RequestInformation
                {
                    [JsonProperty("id")]
                    public string RequestId { get; set; }
                    public string InstanceId { get; set; }
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    if (name == ChannelName)
                    {
                        tcs.TrySetResult(input);
                    }
                }

                public class Responder : TaskOrchestration<string, string, RequestInformation, string>
                {
                    private readonly TaskCompletionSource<string> tcs
                        = new TaskCompletionSource<string>();

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

                    public override void OnEvent(OrchestrationContext context, string name, RequestInformation input)
                    {
                        if (name == ChannelName)
                        {
                            tcs.TrySetResult(input.InstanceId);
                        }
                    }
                }
            }

            [KnownType(typeof(AbortSessionOrchestration.Activity))]
            internal class AbortSessionOrchestration : TaskOrchestration<string, string>
            {
                // This is a hacky way of keeping track of global state
                static bool abortedOrchestration = false;
                static bool abortedActivity = false;

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    if (!abortedOrchestration)
                    {
                        abortedOrchestration = true;
                        throw new SessionAbortedException();
                    }

                    try
                    {
                        await context.ScheduleTask<string>(typeof(Activity), input);
                        return (abortedOrchestration && abortedActivity).ToString();
                    }
                    catch
                    {
                        return "Test failed: The activity's SessionAbortedException should not be visible to the orchestration";
                    }
                    finally
                    {
                        // reset to ensure future executions work correctly
                        abortedOrchestration = false;
                        abortedActivity = false;
                    }
                }

                public class Activity : TaskActivity<string, string>
                {
                    protected override string Execute(TaskContext context, string input)
                    {
                        if (!abortedActivity)
                        {
                            abortedActivity = true;
                            throw new SessionAbortedException();
                        }

                        return input;
                    }
                }
            }

            internal class CurrentTimeInline : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult(context.CurrentUtcDateTime);
                }
            }

            [KnownType(typeof(Activities.CurrentTime))]
            internal class CurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.CurrentTime), input);
                }
            }

            internal class DelayedCurrentTimeInline : TaskOrchestration<DateTime, DateTime>
            {
                public override async Task<DateTime> RunTask(OrchestrationContext context, DateTime fireAt)
                {
                    await context.CreateTimer<bool>(fireAt, true);
                    return context.CurrentUtcDateTime;
                }
            }

            [KnownType(typeof(Activities.DelayedCurrentTime))]
            internal class DelayedCurrentTimeActivity : TaskOrchestration<DateTime, string>
            {
                public override Task<DateTime> RunTask(OrchestrationContext context, string input)
                {
                    return context.ScheduleTask<DateTime>(typeof(Activities.DelayedCurrentTime), input);
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
                        throw new Exception("Simulating unhandled activity function failure...");
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
                        throw new Exception("Simulating unhandled activity function failure...");
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
                        throw new Exception("Simulating unhandled activity function failure...");
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
                        throw new Exception("Simulating unhandled activity function failure...");
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
                        throw new Exception("Simulating unhandled activity function failure...");
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
                static TableClient cachedTable;

                internal static TableClient TestTable
                {
                    get
                    {
                        if (cachedTable == null)
                        {
                            string connectionString = TestHelpers.GetTestStorageAccountConnectionString();
                            TableClient table = new TableServiceClient(connectionString).GetTableClient("TestTable");
                            table.CreateIfNotExistsAsync().Wait();
                            cachedTable = table;
                        }

                        return cachedTable;
                    }
                }

                protected override string Execute(TaskContext context, Tuple<string, string> rowData)
                {
                    var entity = new TableEntity(
                        partitionKey: rowData.Item1,
                        rowKey: $"{rowData.Item2}.{Guid.NewGuid():N}");
                    TestTable.AddEntityAsync(entity).Wait();
                    return null;
                }
            }

            internal class CountTableRows : TaskActivity<string, int>
            {
                protected override int Execute(TaskContext context, string partitionKey)
                {
                    return WriteTableRow.TestTable.Query<TableEntity>(filter: $"PartitionKey eq '{partitionKey}'").Count();
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

            internal class CurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }
                    return DateTime.UtcNow;
                }
            }

            internal class DelayedCurrentTime : TaskActivity<string, DateTime>
            {
                protected override DateTime Execute(TaskContext context, string input)
                {
                    if (string.IsNullOrEmpty(input))
                    {
                        throw new ArgumentNullException(nameof(input));
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    return DateTime.UtcNow;
                }
            }
        }
    }
}
