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
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using DurableTask.Core.Tracing;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
#if !NET48
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
        /// Verifies that the normal checkpoint write path records a child's parent, and that the value is
        /// returned by both a direct instance lookup and a status query. This does not cover the
        /// terminal-history repair path; see
        /// <see cref="ParentInstanceIdTrackingStoreTests.CompletedOrchestrationRepair_PersistsParentInstanceId"/>
        /// for that.
        /// </summary>
        [TestMethod]
        public async Task ParentMetadataIsReturnedByDirectGetAndQuery()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.TaskHubName = "pmf" + Guid.NewGuid().ToString("N").Substring(0, 12)))
            {
                string parentInstanceId = $"parent-{Guid.NewGuid():N}";
                string childInstanceId = parentInstanceId + ":child";
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentOfInlineChild),
                    "input",
                    parentInstanceId);
                OrchestrationState completed = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, completed?.OrchestrationStatus);

                OrchestrationState parent = await host.service.GetOrchestrationStateAsync(parentInstanceId, executionId: null);
                OrchestrationState child = await host.service.GetOrchestrationStateAsync(childInstanceId, executionId: null);
                Assert.IsNull(parent.ParentInstance);
                Assert.AreEqual(parentInstanceId, child.ParentInstance?.OrchestrationInstance.InstanceId);

                DurableStatusQueryResult queryResult = await host.service.GetOrchestrationStateAsync(
                    new OrchestrationInstanceStatusQueryCondition { InstanceIdPrefix = parentInstanceId },
                    top: 10,
                    continuationToken: null);
                OrchestrationState queriedChild = queryResult.OrchestrationState.Single(state =>
                    state.OrchestrationInstance.InstanceId == childInstanceId);
                Assert.AreEqual(parentInstanceId, queriedChild.ParentInstance?.OrchestrationInstance.InstanceId);
                OrchestrationState queriedParent = queryResult.OrchestrationState.Single(state =>
                    state.OrchestrationInstance.InstanceId == parentInstanceId);
                Assert.IsNull(queriedParent.ParentInstance);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task ParentMetadataSurvivesChildContinueAsNew()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.TaskHubName = "pmc" + Guid.NewGuid().ToString("N").Substring(0, 12)))
            {
                string parentInstanceId = $"parent-{Guid.NewGuid():N}";
                string childInstanceId = parentInstanceId + ":child";
                await host.StartAsync();

                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ParentOfContinueAsNewChild),
                    0,
                    parentInstanceId);
                OrchestrationState completed = await client.WaitForCompletionAsync(StandardTimeout);

                Assert.AreEqual(OrchestrationStatus.Completed, completed?.OrchestrationStatus);
                OrchestrationState child = await host.service.GetOrchestrationStateAsync(childInstanceId, executionId: null);
                Assert.AreEqual(1, JToken.Parse(child.Input));
                Assert.AreEqual(parentInstanceId, child.ParentInstance?.OrchestrationInstance.InstanceId);

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
        public async Task PurgeInstanceHistory_InstanceIdWithSingleQuote_Succeeds()
        {
            // Regression test for OData injection: an instance ID containing a single quote
            // must be escaped when building the purge filter. Without escaping, the resulting
            // OData filter is malformed and the purge query fails.
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                string instanceId = "purge'inject-" + Guid.NewGuid();
                await host.StartAsync();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 110, instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 0);

                await client.PurgeInstanceHistory();

                List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEventsAfterPurging.Count);

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
                DateTime startDateTime = DateTime.UtcNow;
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

            return await containerClient.GetBlobsAsync(traits: BlobTraits.Metadata, states: BlobStates.None, prefix: directoryName, cancellationToken: default).CountAsync();
        }

        static async Task<int> GetControlQueueMessageCountAsync(AzureStorageOrchestrationService service)
        {
            int[] messageCounts = await Task.WhenAll(
                service.AllControlQueues.Select(queue => queue.InnerQueue.GetApproximateMessagesCountAsync()));
            return messageCounts.Sum();
        }


        [TestMethod]
        public async Task PurgeMultipleInstancesHistoryByTimePeriod_ScalabilityValidation()
        {
            // This test validates scale improvements: parallel batch delete and pipelined page processing.
            // Runs multiple concurrent orchestrations, then purges all of them by time period.
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;

                // Create multiple orchestration instances concurrently
                const int instanceCount = 5;
                var clients = new List<TestOrchestrationClient>();
                var instanceIds = new List<string>();

                for (int i = 0; i < instanceCount; i++)
                {
                    string instanceId = $"purge-scale-{Guid.NewGuid():N}";
                    instanceIds.Add(instanceId);
                    TestOrchestrationClient client = await host.StartOrchestrationAsync(
                        typeof(Orchestrations.Factorial), 10, instanceId);
                    clients.Add(client);
                }

                // Wait for all orchestrations to complete
                foreach (var client in clients)
                {
                    var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                }

                // Verify all instances have history
                foreach (string instanceId in instanceIds)
                {
                    List<HistoryStateEvent> historyEvents = await clients[0].GetOrchestrationHistoryAsync(instanceId);
                    Assert.IsTrue(historyEvents.Count > 0, $"Instance {instanceId} should have history events");
                }

                // Purge all instances by time period
                await clients[0].PurgeInstanceHistoryByTimePeriod(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus> { OrchestrationStatus.Completed });

                // Verify all history is purged
                foreach (string instanceId in instanceIds)
                {
                    List<HistoryStateEvent> historyEvents = await clients[0].GetOrchestrationHistoryAsync(instanceId);
                    Assert.AreEqual(0, historyEvents.Count, $"Instance {instanceId} should have no history after purge");

                    IList<OrchestrationState> stateList = await clients[0].GetStateAsync(instanceId);
                    Assert.AreEqual(1, stateList.Count);
                    Assert.IsNull(stateList[0], $"Instance {instanceId} state should be null after purge");
                }

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeSingleInstanceWithIdempotency()
        {
            // This test validates that purging the same instance twice doesn't cause errors
            // (testing the idempotent batch delete fallback).
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                string instanceId = Guid.NewGuid().ToString();
                await host.StartAsync();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Factorial), 110, instanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                // First purge should succeed
                await client.PurgeInstanceHistory();

                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEvents.Count);

                // Second purge of the same instance should not throw
                // (the instance row is already gone, so PurgeInstanceHistoryAsync returns 0)
                await client.PurgeInstanceHistory();

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeSingleInstance_WithLargeBlobs_CleansUpBlobs()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                string instanceId = Guid.NewGuid().ToString();
                // Generate a payload large enough to be stored as a blob (>60KB threshold)
                string largeMessage = new string('x', 70 * 1024);

                TestOrchestrationClient client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.Echo), largeMessage, instanceId);
                OrchestrationState status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // Verify blobs exist before purge
                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0, "Should have large message blobs before purge");

                // Purge
                await client.PurgeInstanceHistory();

                // Verify blobs are cleaned up
                blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.AreEqual(0, blobCount, "All large message blobs should be deleted after purge");

                // Verify history is gone
                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEvents.Count);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstance_WithManyHistoryRows_DeletesAll()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();

                string instanceId = Guid.NewGuid().ToString();
                // FanOutFanIn with 50 parallel activities creates 100+ history rows
                TestOrchestrationClient client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.FanOutFanIn), 50, instanceId);
                OrchestrationState status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

                // Verify lots of history exists
                List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.IsTrue(historyEvents.Count > 50, $"Expected many history events, got {historyEvents.Count}");

                // Purge
                await client.PurgeInstanceHistory();

                // Verify clean
                historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
                Assert.AreEqual(0, historyEvents.Count);

                IList<OrchestrationState> stateList = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, stateList.Count);
                Assert.IsNull(stateList[0]);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeAllInstances_HistoryIsCleared()
        {
            // Validates that purging multiple instances clears all history
            // when purging by time period.
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;

                const int totalInstances = 5;
                var clients = new List<TestOrchestrationClient>();
                for (int i = 0; i < totalInstances; i++)
                {
                    string instanceId = $"purge-complete-{Guid.NewGuid():N}";
                    TestOrchestrationClient client = await host.StartOrchestrationAsync(
                        typeof(Orchestrations.Factorial), 10, instanceId);
                    clients.Add(client);
                }

                foreach (var client in clients)
                {
                    var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                }

                DateTime endDateTime = DateTime.UtcNow;
                var statuses = new List<OrchestrationStatus> { OrchestrationStatus.Completed };

                // Purge should complete within the 30s built-in timeout for a small number of instances
                await clients[0].PurgeInstanceHistoryByTimePeriod(
                    startDateTime, endDateTime, statuses);

                // Verify all history is purged
                foreach (var client in clients)
                {
                    List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(
                        client.InstanceId);
                    Assert.AreEqual(0, historyEvents.Count, "History should be purged");
                }

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryForTimePeriodDeletePartially()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;
                string firstInstanceId = Guid.NewGuid().ToString();
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                DateTime endDateTime = DateTime.UtcNow;
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

        [TestMethod]
        public async Task PurgeInstanceHistoryWithTimeoutCompletesAll()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;

                // Start 2 simple orchestrations that complete quickly
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", Guid.NewGuid().ToString());
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", Guid.NewGuid().ToString());
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(2, results.Count);

                // Purge with a generous timeout — should complete all
                PurgeHistoryResult purgeResult = await client.PurgeInstanceHistoryByTimePeriodWithTimeout(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus> { OrchestrationStatus.Completed },
                    timeout: TimeSpan.FromMinutes(5));

                Assert.AreEqual(2, purgeResult.InstancesDeleted);
                Assert.IsTrue(purgeResult.IsComplete.HasValue, "IsComplete should have a value when timeout is specified");
                Assert.IsTrue(purgeResult.IsComplete.Value, "IsComplete should be true when all instances were purged");

                results = await host.GetAllOrchestrationInstancesAsync();
                Assert.AreEqual(0, results.Count);

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryWithTimeoutExpiresReturnsIsCompleteFalse()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;

                // Start several orchestrations so that purge cannot finish in 1 ms
                const int instanceCount = 50;
                TestOrchestrationClient client = null!;
                for (int i = 0; i < instanceCount; i++)
                {
                    client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", Guid.NewGuid().ToString());
                    await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                }

                // Purge with a very short timeout — should expire before all instances are deleted.
                // Using 100ms instead of 1ms to avoid flakiness from OS timer resolution (~15ms on Windows).
                PurgeHistoryResult purgeResult = await client.PurgeInstanceHistoryByTimePeriodWithTimeout(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus> { OrchestrationStatus.Completed },
                    timeout: TimeSpan.FromMilliseconds(100));

                Assert.IsTrue(purgeResult.IsComplete.HasValue, "IsComplete should have a value when timeout is specified");
                Assert.IsFalse(purgeResult.IsComplete.Value, "IsComplete should be false when the timeout expired before all instances were purged");

                await host.StopAsync();
            }
        }

        [TestMethod]
        public async Task PurgeInstanceHistoryWithoutTimeoutReturnsNullIsComplete()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            {
                await host.StartAsync();
                DateTime startDateTime = DateTime.UtcNow;

                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "World", Guid.NewGuid().ToString());
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                // Purge without timeout (backward compat)
                PurgeHistoryResult purgeResult = await client.PurgeInstanceHistoryByTimePeriodWithTimeout(
                    startDateTime,
                    DateTime.UtcNow,
                    new List<OrchestrationStatus> { OrchestrationStatus.Completed },
                    timeout: null);

                Assert.AreEqual(1, purgeResult.InstancesDeleted);
                Assert.IsFalse(purgeResult.IsComplete.HasValue, "IsComplete should be null when no timeout is specified (backward compat)");

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
                // TerminatePendingOrchestration tests terminating a pending orchestration.
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
        /// Test that a pending orchestration can be terminated (including tests with a large termination reason that will need to be
        /// stored in blob storage).
        /// </summary>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TerminatePendingOrchestration(bool enableExtendedSessions, bool largeTerminationReason)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();
                // Schedule a start time to ensure that the orchestration is in a Pending state when we attempt to terminate.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0, startAt: DateTime.UtcNow.AddMinutes(1));
                await client.WaitForStatusChange(TimeSpan.FromSeconds(5), OrchestrationStatus.Pending);

                string message = largeTerminationReason ? this.GenerateMediumRandomStringPayload().ToString() : "terminate";
                await client.TerminateAsync(message);

                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

                if (largeTerminationReason)
                {
                    int blobCount = await this.GetBlobCount("test-largemessages", client.InstanceId);
                    Assert.IsTrue(blobCount > 0);
                }

                // Confirm the pending orchestration was terminated.
                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual(message, status?.Output);

                // Now sleep for a minute and confirm that the orchestration does not start after its scheduled time.
                Thread.Sleep(TimeSpan.FromMinutes(1));

                status = await client.GetStatusAsync();
                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                Assert.AreEqual(message, status?.Output);

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
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), input: message, instanceId: id);
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
                .GetBlobsByHierarchyAsync(traits: BlobTraits.Metadata, states: BlobStates.None, delimiter: null, prefix: sanitizedInstanceId, cancellationToken: default)
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
        [Ignore("Skipping since this functionality has since changed, see TestWorkerFailingDuringCompleteWorkItemCall")]
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

        /// <summary>
        /// Confirm that if a worker fails after committing the new history but before updating the instance state in a call to
        /// <see cref="AzureStorageOrchestrationService.CompleteTaskOrchestrationWorkItemAsync"/> for an orchestration that has 
        /// reached a terminal state, then storage is brought to consistent state by the call to 
        /// <see cref="AzureStorageOrchestrationService.LockNextTaskOrchestrationWorkItemAsync"/>.
        /// Since we cannot simulate a worker failure at this precise point, instead what is done by this test is that we
        /// let an orchestration run to completion, and then manually change the instance table state back to "Running".
        /// We then send an event to the orchestration, which triggers a call to lock the next task work item, at which point
        /// the inconsistent state in storage for the terminal instance is recognized, the instance state is updated, and the work item discarded.
        /// Note that this test does not confirm that orphaned blobs are deleted by the call to lock the next orchestration work item 
        /// in the case of a terminal orchestration with inconsistent state in storage. This is because there is no easy way to mock/inject
        /// the tracking store context object that is part of the orchestration session state which keeps track of the blobs.
        /// </summary>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallCompletedOrchestration(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Run simple orchestrator to completion, this will help us obtain a valid terminal history for the orchestrator
                string input = "hello!";
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), input, tags: new Dictionary<string, string> { { "key", "value" } });
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                var instanceId = client.InstanceId;
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Completed, status.OrchestrationStatus);
                Assert.AreEqual(input, JToken.Parse(status.Output).ToString());
                Assert.AreEqual(input, JToken.Parse(status.Input).ToString());

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Completed, status.OrchestrationStatus);
                Assert.AreEqual(input, JToken.Parse(status.Output).ToString());
                Assert.AreEqual(input, JToken.Parse(status.Input).ToString());
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.Echo)));
                Assert.IsTrue(status.Tags.Contains(new KeyValuePair<string, string>("key", "value")));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Same as <see cref="TestWorkerFailingDuringCompleteWorkItemCallCompletedOrchestration"/> but for a failed orchestration.
        /// </summary>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallFailedOrchestration(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string failureReason = "Failure!";
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ThrowException),
                    input: failureReason);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                var instanceId = client.InstanceId;
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);

                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Failed, status.OrchestrationStatus);
                Assert.AreEqual(failureReason, status.Output);
                Assert.AreEqual(failureReason, JToken.Parse(status.Input).ToString());

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Failed, status.OrchestrationStatus);
                Assert.AreEqual(failureReason, status.Output);
                Assert.AreEqual(failureReason, JToken.Parse(status.Input).ToString());
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.ThrowException)));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Same as <see cref="TestWorkerFailingDuringCompleteWorkItemCallCompletedOrchestration"/> but for a terminated orchestration.
        /// </summary>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallTerminatedOrchestration(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                // Terminate the orchestration
                string reason = "terminate";
                await client.TerminateAsync(reason);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                var instanceId = client.InstanceId;
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Terminated, status.OrchestrationStatus);
                Assert.AreEqual(reason, status.Output);
                Assert.AreEqual(0, int.Parse(status.Input));

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Terminated, status.OrchestrationStatus);
                Assert.AreEqual(reason, status.Output);
                Assert.AreEqual(0, int.Parse(status.Input));
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.Counter)));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

                await host.StopAsync();
            }
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed, false)]
        [DataRow(OrchestrationStatus.Failed, false)]
        [DataRow(OrchestrationStatus.Terminated, false)]
        [DataRow(OrchestrationStatus.Completed, true)]
        [DataRow(OrchestrationStatus.Failed, true)]
        [DataRow(OrchestrationStatus.Terminated, true)]
        public async Task WorkerDeletesMessagesForTerminalOrchestration(
            OrchestrationStatus terminalStatus,
            bool includeExecutionSpecificMessage)
        {
            AzureStorageOrchestrationService service = null;
            bool serviceStarted = false;

            string instanceId = Guid.NewGuid().ToString();
            string executionId = Guid.NewGuid().ToString();
            var orchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            };

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = "TerminalMessages" + Guid.NewGuid().ToString("N").Substring(0, 10),
                ExtendedSessionsEnabled = false,
                UseAppLease = false,
            };

            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();

                // Create a completed orchestration history with the specific terminal status
                OrchestrationHistory emptyHistory = await service.TrackingStore.GetHistoryEventsAsync(
                    instanceId,
                    executionId);
                var runtimeState = new OrchestrationRuntimeState();
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                });
                if (includeExecutionSpecificMessage)
                {
                    runtimeState.AddEvent(new TaskScheduledEvent(0));
                }

                runtimeState.AddEvent(new ExecutionCompletedEvent(1, "output", terminalStatus));
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.TrackingStore.UpdateStateAsync(
                    runtimeState,
                    new OrchestrationRuntimeState(),
                    instanceId,
                    executionId,
                    new OrchestrationETags { HistoryETag = emptyHistory.ETag },
                    emptyHistory.TrackingStoreContext);

                var controlQueue = service.AllControlQueues.Single();
                var sourceInstance = new OrchestrationInstance
                {
                    InstanceId = "source",
                    ExecutionId = "source-execution",
                };

                // Enqueue an external event that targets no specific execution ID and potentially an
                // event that does target the specific execution ID of the terminal orchestration
                if (includeExecutionSpecificMessage)
                {
                    await controlQueue.AddMessageAsync(
                        new TaskMessage
                        {
                            OrchestrationInstance = orchestrationInstance,
                            Event = new TaskCompletedEvent(-1, 0, "result"),
                        },
                        sourceInstance);
                }

                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                        Event = new EventRaisedEvent(-1, string.Empty) { Name = "event" },
                    },
                    sourceInstance);

                await service.StartAsync();
                serviceStarted = true;

                int dequeueAttempts = includeExecutionSpecificMessage ? 2 : 1;
                for (int i = 0; i < dequeueAttempts; i++)
                {
                    using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                    {
                        TaskOrchestrationWorkItem workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                            TimeSpan.FromSeconds(30),
                            timeout.Token);
                        Assert.IsNull(workItem);
                    }
                }

                // Confirm all messages are deleted.
                Assert.AreEqual(
                    0,
                    await GetControlQueueMessageCountAsync(service),
                    "All messages sent to the terminal orchestration should be deleted.");
            }
            finally
            {
                if (serviceStarted)
                {
                    await service.StopAsync(isForced: true);
                }

                if (service != null)
                {
                    await service.DeleteAsync();
                }

                service?.Dispose();
            }
        }

        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task WorkerDoesNotDiscardExternalEventBatchedWithStaleTerminalMessageDuringInstanceReuse(
            bool includeTaskScheduledEvent)
        {
            AzureStorageOrchestrationService service = null;
            bool serviceStarted = false;

            string instanceId = Guid.NewGuid().ToString();
            string oldExecutionId = Guid.NewGuid().ToString();
            string newExecutionId = Guid.NewGuid().ToString();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = "ReusedInstance" + Guid.NewGuid().ToString("N").Substring(0, 10),
                ExtendedSessionsEnabled = false,
                UseAppLease = false,
            };

            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();

                var oldInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = oldExecutionId,
                };

                // First create a terminal orchestration with the old execution ID
                OrchestrationHistory emptyHistory = await service.TrackingStore.GetHistoryEventsAsync(
                    instanceId,
                    oldExecutionId);
                var oldRuntimeState = new OrchestrationRuntimeState();
                oldRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                oldRuntimeState.AddEvent(new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = oldInstance,
                });
                if (includeTaskScheduledEvent)
                {
                    oldRuntimeState.AddEvent(new TaskScheduledEvent(0));
                }

                oldRuntimeState.AddEvent(new ExecutionCompletedEvent(1, "output", OrchestrationStatus.Completed));
                oldRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.TrackingStore.UpdateStateAsync(
                    oldRuntimeState,
                    new OrchestrationRuntimeState(),
                    instanceId,
                    oldExecutionId,
                    new OrchestrationETags { HistoryETag = emptyHistory.ETag },
                    emptyHistory.TrackingStoreContext);

                var newInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = newExecutionId,
                };
                var newExecutionStartedEvent = new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = newInstance,
                    Generation = 1,
                };

                InstanceStatus oldInstanceStatus = await service.TrackingStore.FetchInstanceStatusAsync(instanceId);
                Assert.IsNotNull(oldInstanceStatus);
                Assert.IsTrue(await service.TrackingStore.SetNewExecutionAsync(
                    newExecutionStartedEvent,
                    oldInstanceStatus.ETag,
                    inputPayloadOverride: null));

                InstanceStatus pendingInstance = await service.TrackingStore.FetchInstanceStatusAsync(instanceId);
                Assert.IsNotNull(pendingInstance);
                Assert.AreEqual(newExecutionId, pendingInstance.State.OrchestrationInstance.ExecutionId);
                Assert.AreEqual(OrchestrationStatus.Pending, pendingInstance.State.OrchestrationStatus);

                var controlQueue = service.AllControlQueues.Single();
                var sourceInstance = new OrchestrationInstance
                {
                    InstanceId = "source",
                    ExecutionId = "source-execution",
                };

                // A delayed message for the old execution can arrive after the new ExecutionStarted message.
                // The execution-independent event then joins the newer message batch targeting the old execution
                // rather than the message batch containing the ExecutionStarted event for the new execution.
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = oldInstance,
                        Event = new TaskCompletedEvent(-1, 0, "result"),
                    },
                    sourceInstance);
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                        Event = new EventRaisedEvent(-1, string.Empty) { Name = "event" },
                    },
                    sourceInstance);
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = newInstance,
                        Event = newExecutionStartedEvent,
                    },
                    sourceInstance);

                // Fetch the messages directly so their pending-batch assignment is deterministic. The new
                // ExecutionStarted message was enqueued after the stale messages and must form its own batch,
                // while the external event joins the batch created by the stale TaskCompleted message.
                var queuedMessages = new List<MessageData>();
                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    while (queuedMessages.Count < 3)
                    {
                        queuedMessages.AddRange(await controlQueue.GetMessagesAsync(timeout.Token));
                    }
                }

                Assert.AreEqual(3, queuedMessages.Count);
                MessageData executionStartedMessage = queuedMessages.Single(
                    message => message.TaskMessage.Event is ExecutionStartedEvent);
                MessageData staleTaskCompletedMessage = queuedMessages.Single(
                    message => message.TaskMessage.Event is TaskCompletedEvent);
                MessageData externalEventMessage = queuedMessages.Single(
                    message => message.TaskMessage.Event is EventRaisedEvent);

                // Initialize the service lifecycle without allowing its queue listener to compete with
                // the messages that this test assigns to pending batches explicitly.
                settings.ControlQueueBufferThreshold = 0;
                await service.StartAsync();
                serviceStarted = true;

                var sessionManagerField = typeof(AzureStorageOrchestrationService).GetField(
                    "orchestrationSessionManager",
                    BindingFlags.Instance | BindingFlags.NonPublic);
                Assert.IsNotNull(sessionManagerField);
                var sessionManager = (OrchestrationSessionManager)sessionManagerField.GetValue(service);

                Guid traceActivityId = Guid.NewGuid();
                sessionManager.AddMessageToPendingOrchestration(
                    controlQueue,
                    new[] { staleTaskCompletedMessage, externalEventMessage },
                    traceActivityId,
                    CancellationToken.None);
                sessionManager.AddMessageToPendingOrchestration(
                    controlQueue,
                    new[] { executionStartedMessage },
                    traceActivityId,
                    CancellationToken.None);

                bool externalEventDelivered = false;
                bool staleTerminalBatchProcessed = false;
                for (int attempt = 0; attempt < 3 && !externalEventDelivered; attempt++)
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    TaskOrchestrationWorkItem workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromSeconds(30),
                        timeout.Token);

                    // The stale terminal batch does not produce a work item. The TaskCompleted event is either
                    // discarded or removed as out of order, and the EventRaised event is abandoned and retried.
                    if (workItem == null)
                    {
                        staleTerminalBatchProcessed = true;
                        continue;
                    }

                    if (workItem.NewMessages.Any(message => message.Event is EventRaisedEvent))
                    {
                        // Either the ExecutionStartedEvent batch was committed first, in which case the execution ID is
                        // already part of the orchestration runtime state and this EventRaisedEvent is retried by itself
                        // after the work item with the stale TaskCompletedEvent is abandoned.
                        // Or the ExecutionStartedEvent batch was dequeued second, in which case the EventRaisedEvent has
                        // already been abandoned and is retried with the ExecutionStartedEvent as part of the new messages too
                        string executionId = workItem.OrchestrationRuntimeState.OrchestrationInstance?.ExecutionId ??
                            workItem.NewMessages.Single(message => message.Event is ExecutionStartedEvent)
                                .OrchestrationInstance.ExecutionId;

                        Assert.AreEqual(newExecutionId, executionId);
                        Assert.IsFalse(workItem.NewMessages.Any(message => message.Event is TaskCompletedEvent));
                        externalEventDelivered = true;
                        await service.AbandonTaskOrchestrationWorkItemAsync(workItem);
                        await service.ReleaseTaskOrchestrationWorkItemAsync(workItem);
                        break;
                    }

                    // If the new ExecutionStarted batch is dequeued first, commit it so that the deferred
                    // external event can subsequently be loaded against the new execution's history.
                    Assert.AreEqual(1, workItem.NewMessages.Count);
                    Assert.IsInstanceOfType(workItem.NewMessages.Single().Event, typeof(ExecutionStartedEvent));
                    Assert.AreEqual(newExecutionId, workItem.NewMessages.Single().OrchestrationInstance.ExecutionId);

                    OrchestrationRuntimeState newRuntimeState = workItem.OrchestrationRuntimeState;
                    newRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                    newRuntimeState.AddEvent(newExecutionStartedEvent);
                    newRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                    await service.CompleteTaskOrchestrationWorkItemAsync(
                        workItem,
                        newRuntimeState,
                        new List<TaskMessage>(),
                        new List<TaskMessage>(),
                        new List<TaskMessage>(),
                        null,
                        null);
                    await service.ReleaseTaskOrchestrationWorkItemAsync(workItem);
                }

                Assert.IsTrue(
                    staleTerminalBatchProcessed,
                    "The stale execution-specific message should be processed against the old terminal history.");
                Assert.IsTrue(
                    externalEventDelivered,
                    "The execution-independent event should be delivered to the recreated execution.");
            }
            finally
            {
                if (serviceStarted)
                {
                    await service.StopAsync(isForced: true);
                }

                if (service != null)
                {
                    await service.DeleteAsync();
                }

                service?.Dispose();
            }
        }

        /// <summary>
        /// Same as <see cref="TestWorkerFailingDuringCompleteWorkItemCallCompletedOrchestration"/> but for an orchestration with large input
        /// and output, which will need to be stored in blob storage.
        /// </summary>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallLargeInputOutput(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                var instanceId = client.InstanceId;
                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0);

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Completed, status.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status.Output).ToString());
                Assert.AreEqual(message, JToken.Parse(status.Input).ToString());

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Completed, status.OrchestrationStatus);
                Assert.AreEqual(message, JToken.Parse(status.Output).ToString());
                Assert.AreEqual(message, JToken.Parse(status.Input).ToString());
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.Echo)));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Same as <see cref="TestWorkerFailingDuringCompleteWorkItemCallTerminatedOrchestration"/> but for a large termination reason that
        /// will need to be stored in blob storage.
        /// </summary>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallLargeTerminationReason(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                // Using the counter orchestration because it will wait indefinitely for input.
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);
                await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));
                // Terminate the orchestration
                await client.TerminateAsync(message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                var instanceId = client.InstanceId;
                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0);

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Terminated, status.OrchestrationStatus);
                Assert.AreEqual(message, status.Output);
                Assert.AreEqual(0, int.Parse(status.Input));

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Terminated, status.OrchestrationStatus);
                Assert.AreEqual(message, status.Output);
                Assert.AreEqual(0, int.Parse(status.Input));
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.Counter)));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

                await host.StopAsync();
            }
        }

        /// <summary>
        /// Same as <see cref="TestWorkerFailingDuringCompleteWorkItemCallFailedOrchestration"/> but for a large exception message that will need
        /// to be stored in blob storage.
        /// </summary>
        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task TestWorkerFailingDuringCompleteWorkItemCallLargeFailureReason(bool enableExtendedSessions, bool terminate)
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
            {
                await host.StartAsync();

                string message = this.GenerateMediumRandomStringPayload().ToString();
                var client = await host.StartOrchestrationAsync(
                    typeof(Orchestrations.ThrowException),
                    input: message);
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);
                string executionId = status.OrchestrationInstance.ExecutionId;

                var instanceId = client.InstanceId;
                int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
                Assert.IsTrue(blobCount > 0);

                // Simulate having an "out of date" Instance table, by setting it's runtime status to "Running".
                // This simulates the scenario where the History table was updated, but not the Instance table.
                AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                    enableExtendedSessions);
                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new TableEntity(instanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    ["Output"] = "null",
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                // Assert that the status in the Instance table reads "Running"
                IList<OrchestrationState> state = await client.GetStateAsync(instanceId);
                OrchestrationStatus forcedStatus = state.First().OrchestrationStatus;
                Assert.AreEqual(OrchestrationStatus.Running, forcedStatus);

                // The type of event sent should not matter - the event itself should be discarded, and the instance table updated
                // to reflect the status in the history table.
                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Failed, status.OrchestrationStatus);
                Assert.AreEqual(message, status.Output);
                Assert.AreEqual(message, JToken.Parse(status.Input).ToString());

                // Now simulate there being no instance entity (which can be the case for suborchestrations that complete in one execution), and try again
                await instanceTable.DeleteEntityAsync(entity, Azure.ETag.All);

                if (terminate)
                {
                    await client.TerminateAsync("testing");
                }
                else
                {
                    await client.RaiseEventAsync("Foo", "Bar");
                }
                await Task.Delay(TimeSpan.FromSeconds(30));

                // A replay should have occurred, forcing the instance table to be updated with a terminal status
                state = await client.GetStateAsync(instanceId);
                Assert.AreEqual(1, state.Count);

                status = state.First();
                Assert.AreEqual(OrchestrationStatus.Failed, status.OrchestrationStatus);
                Assert.AreEqual(message, status.Output);
                Assert.AreEqual(message, JToken.Parse(status.Input).ToString());
                Assert.IsTrue(status.Name.Contains(nameof(Orchestrations.ThrowException)));
                Assert.AreEqual(executionId, status.OrchestrationInstance.ExecutionId);

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

        /// <summary>
        /// Confirm that:
        /// 1. If <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/> is true, and a worker attempts to update the instance table with a stale
        /// etag upon completing a work item, a SessionAbortedException is thrown which wraps the inner DurableTaskStorageException, which has the correct status code
        /// (precondition failed).
        /// The specific scenario tested is if the worker stalled after updating the history table but before updating the instance table. When it attempts to update
        /// the instance table with a stale etag, it will fail.
        /// 2. If <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/> is false for the above scenario, then the call to update the instance table
        /// will go through, and the instance table will be updated with a "stale" status.
        /// </summary>
        /// <remarks>
        /// Since it is impossible to force stalling, we simulate the above scenario by manually updating the instance table before the worker
        /// attempts to complete the work item. The history table update will go through, but the instance table update will fail since "another worker
        /// has since updated" the instance table.
        /// </remarks>
        /// <param name="useInstanceEtag">The value to use for <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task WorkerAttemptingToUpdateInstanceTableAfterStalling(bool useInstanceEtag)
        {
            AzureStorageOrchestrationService service = null;
            try
            {
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance_id",
                    ExecutionId = "execution_id",
                };

                ExecutionStartedEvent startedEvent = new(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                    ScheduledStartTime = DateTime.UtcNow,
                };

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    PartitionCount = 1,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                    TaskHubName = TestHelpers.GetTestTaskHubName(),
                    ExtendedSessionsEnabled = false,
                    UseInstanceTableEtag = useInstanceEtag
                };

                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync();

                // Create the orchestration and get the first work item and start "working" on it
                await service.CreateTaskOrchestrationAsync(
                    new TaskMessage()
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = startedEvent
                    });
                var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(5),
                    CancellationToken.None);
                var runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(startedEvent);
                runtimeState.AddEvent(new TaskScheduledEvent(0));
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                AzureStorageClient azureStorageClient = new AzureStorageClient(settings);

                // Now manually update the instance to have status "Completed"
                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                TableEntity entity = new(orchestrationInstance.InstanceId, "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Completed.ToString("G"),
                };
                await instanceTable.MergeEntityAsync(entity, Azure.ETag.All);

                if (useInstanceEtag)
                {
                    // Confirm an exception is thrown due to the etag mismatch for the instance table when the worker attempts to complete the work item
                    SessionAbortedException exception = await Assert.ThrowsExceptionAsync<SessionAbortedException>(async () =>
                        await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null)
                    );
                    Assert.IsInstanceOfType(exception.InnerException, typeof(DurableTaskStorageException));
                    DurableTaskStorageException dtse = (DurableTaskStorageException)exception.InnerException;
                    Assert.AreEqual((int)HttpStatusCode.PreconditionFailed, dtse.HttpStatusCode);
                }
                else
                {
                    await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null);

                    var queryCondition = new OrchestrationInstanceStatusQueryCondition
                    {
                        InstanceId = "instance_id",
                        FetchInput = false,
                    };

                    ODataCondition odata = queryCondition.ToOData();
                    OrchestrationInstanceStatus instanceTableEntity = await instanceTable
                        .ExecuteQueryAsync<OrchestrationInstanceStatus>(odata.Filter, 1, odata.Select, CancellationToken.None)
                        .FirstOrDefaultAsync();

                    // Confirm the instance table was updated with a "stale" status
                    Assert.IsNotNull(instanceTableEntity);
                    Assert.AreEqual(OrchestrationStatus.Running.ToString(), instanceTableEntity.RuntimeStatus);
                }
            }
            finally
            {
                await service?.StopAsync(isForced: true);
            }
        }

        /// <summary>
        /// Confirm that:
        /// 1. If <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/> is true, and a worker attempts to update the instance table with a stale
        /// etag upon completing a work item for a suborchestration, a SessionAbortedException is thrown which wraps the inner DurableTaskStorageException, which has
        /// the correct status code (conflict).
        /// The specific scenario tested is if the worker stalled after updating the history table but before updating the instance table for the first work item
        /// for a suborchestration. When it attempts to insert a new entity into the instance table for the suborchestration (since for a suborchestration,
        /// the instance entity is only created upon completion of the first work item), it will fail.
        /// 2. If <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/> is false for the above scenario, then the call to update the instance table
        /// will go through, and the instance table will be updated with a "stale" status.
        /// </summary>
        /// <remarks>
        /// Since it is impossible to force stalling, we simulate the above scenario by manually updating the instance table before the worker
        /// attempts to complete the work item. The history table update will go through, but the instance table update will fail since "another worker
        /// has since updated" the instance table.
        /// </remarks>
        /// <param name="useInstanceEtag">The value to use for <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/></param>
        /// <returns></returns>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task WorkerAttemptingToUpdateInstanceTableAfterStallingForSubOrchestration(bool useInstanceEtag)
        {
            AzureStorageOrchestrationService service = null;
            try
            {
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance_id",
                    ExecutionId = "execution_id",
                };

                ExecutionStartedEvent startedEvent = new(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                    ScheduledStartTime = DateTime.UtcNow,
                };

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    PartitionCount = 1,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                    TaskHubName = TestHelpers.GetTestTaskHubName(),
                    ExtendedSessionsEnabled = false,
                    UseInstanceTableEtag = useInstanceEtag
                };

                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync();

                // Create the orchestration and get the first work item and start "working" on it
                await service.CreateTaskOrchestrationAsync(
                    new TaskMessage()
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = startedEvent
                    });
                var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(5),
                    CancellationToken.None);
                var runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(startedEvent);
                runtimeState.AddEvent(new SubOrchestrationInstanceCreatedEvent(0)
                {
                    Name = "suborchestration",
                    InstanceId = "sub_instance_id"
                });
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                // Create the task message to start the suborchestration
                var subOrchestrationExecutionStartedEvent = new ExecutionStartedEvent(-1, string.Empty)
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = "sub_instance_id",
                        ExecutionId = Guid.NewGuid().ToString("N")
                    },
                    ParentInstance = new ParentInstance
                    {
                        OrchestrationInstance = runtimeState.OrchestrationInstance,
                        Name = runtimeState.Name,
                        Version = runtimeState.Version,
                        TaskScheduleId = 0,
                    },
                    Name = "suborchestration"
                };
                List<TaskMessage> orchestratorMessages =
                new() {
                new TaskMessage()
                {
                    OrchestrationInstance = subOrchestrationExecutionStartedEvent.OrchestrationInstance,
                    Event = subOrchestrationExecutionStartedEvent,
                }
                };

                // Complete the first work item, which will send the execution started message for the suborchestration
                await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), orchestratorMessages, new List<TaskMessage>(), null, null);

                // Now get the work item for the suborchestration and "work" on it
                workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(5),
                    CancellationToken.None);
                runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(subOrchestrationExecutionStartedEvent);
                runtimeState.AddEvent(new TaskScheduledEvent(0));
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                AzureStorageClient azureStorageClient = new(settings);
                Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
                // Now manually update the suborchestration to have status "Completed"
                TableEntity entity = new("sub_instance_id", "")
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Completed.ToString("G"),
                };
                await instanceTable.InsertEntityAsync(entity);

                if (useInstanceEtag)
                {
                    // Confirm an exception is thrown because the worker attempts to insert a new entity for the suborchestration into the instance table
                    // when one already exists
                    SessionAbortedException exception = await Assert.ThrowsExceptionAsync<SessionAbortedException>(async () =>
                        await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null)
                    );
                    Assert.IsInstanceOfType(exception.InnerException, typeof(DurableTaskStorageException));
                    DurableTaskStorageException dtse = (DurableTaskStorageException)exception.InnerException;
                    Assert.AreEqual((int)HttpStatusCode.Conflict, dtse.HttpStatusCode);
                }
                else
                {
                    await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null);

                    var queryCondition = new OrchestrationInstanceStatusQueryCondition
                    {
                        InstanceId = "sub_instance_id",
                        FetchInput = false,
                    };

                    ODataCondition odata = queryCondition.ToOData();
                    OrchestrationInstanceStatus instanceTableEntity = await instanceTable
                        .ExecuteQueryAsync<OrchestrationInstanceStatus>(odata.Filter, 1, odata.Select, CancellationToken.None)
                        .FirstOrDefaultAsync();

                    // Confirm the instance table was updated with a "stale" status
                    Assert.IsNotNull(instanceTableEntity);
                    Assert.AreEqual(OrchestrationStatus.Running.ToString(), instanceTableEntity.RuntimeStatus);
                }

            }
            finally
            {
                await service?.StopAsync(isForced: true);
            }
        }

        [DataTestMethod]
        [DataRow(true, false)]
        [DataRow(false, false)]
        [DataRow(true, true)]
        [DataRow(false, true)]
        public async Task WorkerAttemptingToDequeueMessageForNonExistentInstance(
            bool extendedSessionsEnabled,
            bool sendExternalEvent)
        {
            AzureStorageOrchestrationService service = null;
            try
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    PartitionCount = 1,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                    TaskHubName = TestHelpers.GetTestTaskHubName(),
                    ExtendedSessionsEnabled = extendedSessionsEnabled,
                };

                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync();

                TaskMessage message;
                if (sendExternalEvent)
                {
                    message = new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = "instance_id",
                            ExecutionId = null,
                        },
                        Event = new EventRaisedEvent(-1, string.Empty)
                        {
                            Name = "event",
                        },
                    };
                }
                else
                {
                    message = new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = "instance_id",
                            ExecutionId = "execution_id",
                        },
                        Event = new TaskCompletedEvent(-1, 0, string.Empty)
                        {
                            Timestamp = DateTime.UtcNow - TimeSpan.FromMinutes(1),
                        },
                    };
                }

                await service.SendTaskOrchestrationMessageAsync(
                    message);

                // Task responses are given five benefit-of-the-doubt abandonments before deletion. External events are
                // not out-of-order messages, so an event for a nonexistent instance should be deleted immediately.
                int dequeueAttempts = sendExternalEvent ? 1 : 6;
                for (int i = 0; i < dequeueAttempts; i++)
                {
                    var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromMinutes(1),
                        CancellationToken.None);
                    Assert.IsNull(workItem);
                }

                Assert.AreEqual(
                    0,
                    await GetControlQueueMessageCountAsync(service),
                    "The message for the nonexistent orchestration should be deleted rather than abandoned.");
            }
            finally
            {
                await service?.StopAsync(isForced: true);
            }
        }

        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task WorkerAttemptingToDequeueMessageWithNoTaskScheduledInHistory(bool extendedSessionsEnabled, bool addTaskScheduledEvent)
        {
            AzureStorageOrchestrationService service = null;
            try
            {
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = Guid.NewGuid().ToString(),
                    ExecutionId = Guid.NewGuid().ToString(),
                };

                ExecutionStartedEvent startedEvent = new(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                    ScheduledStartTime = DateTime.UtcNow,
                };

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    PartitionCount = 1,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                    TaskHubName = TestHelpers.GetTestTaskHubName(),
                    ExtendedSessionsEnabled = extendedSessionsEnabled
                };

                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync();

                // Create the orchestration and get the first work item and start "working" on it
                await service.CreateTaskOrchestrationAsync(
                    new TaskMessage()
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = startedEvent
                    });
                var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(5),
                    CancellationToken.None);
                var runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(startedEvent);
                if (addTaskScheduledEvent)
                {
                    runtimeState.AddEvent(new TaskScheduledEvent(0));
                }
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null);
                
                // Necessary to force a new work item to be generated for the next message
                await service.ReleaseTaskOrchestrationWorkItemAsync(workItem);

                // Send a task completed for a different task scheduled ID, message should be abandoned
                await service.SendTaskOrchestrationMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = new TaskCompletedEvent(-1, 1, string.Empty)
                    });
                 workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
                Assert.IsNull(workItem);

                if (addTaskScheduledEvent)
                {
                    // Send a task completed for the same task scheduled ID, this should work
                    await service.SendTaskOrchestrationMessageAsync(
                        new TaskMessage
                        {
                            OrchestrationInstance = orchestrationInstance,
                            Event = new TaskCompletedEvent(-1, 0, string.Empty)
                        });
                    workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                       TimeSpan.FromMinutes(1),
                       CancellationToken.None);
                    Assert.IsNotNull(workItem);
                }
            }
            finally
            {
                await service?.StopAsync(isForced: true);
            }
        }

        [TestMethod]
        public async Task WorkerDoesNotDiscardExternalEventBatchedWithMessageForMissingExecution()
        {
            AzureStorageOrchestrationService service = null;
            AzureStorageOrchestrationService retryService = null;
            bool serviceStarted = false;
            bool retryServiceStarted = false;

            string instanceId = Guid.NewGuid().ToString();
            string currentExecutionId = Guid.NewGuid().ToString();
            string futureExecutionId = Guid.NewGuid().ToString();
            DateTime fireAt = DateTime.UtcNow;

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = "MixedBatch" + Guid.NewGuid().ToString("N").Substring(0, 12),
                ExtendedSessionsEnabled = false,
                ControlQueueVisibilityTimeout = TimeSpan.FromSeconds(5),
                UseAppLease = false,
            };

            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();

                // First manually generate a history for a particular "old" execution ID
                OrchestrationHistory emptyHistory = await service.TrackingStore.GetHistoryEventsAsync(
                    instanceId,
                    currentExecutionId);
                var currentRuntimeState = new OrchestrationRuntimeState();
                currentRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                currentRuntimeState.AddEvent(new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = instanceId,
                        ExecutionId = currentExecutionId,
                    },
                });
                currentRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.TrackingStore.UpdateStateAsync(
                    currentRuntimeState,
                    new OrchestrationRuntimeState(),
                    instanceId,
                    currentExecutionId,
                    new OrchestrationETags { HistoryETag = emptyHistory.ETag },
                    emptyHistory.TrackingStoreContext);

                var controlQueue = service.AllControlQueues.Single();
                var sourceInstance = new OrchestrationInstance
                {
                    InstanceId = "source",
                    ExecutionId = "source-execution",
                };

                // Next, enqueue two messages - one is a TimerFired targeting a "future" execution that has
                // not been committed yet, and another is a generic EventRaisedEvent that is execution-independent
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = instanceId,
                            ExecutionId = futureExecutionId,
                        },
                        Event = new TimerFiredEvent(-1, fireAt) { TimerId = 0 },
                    },
                    sourceInstance);
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                        Event = new EventRaisedEvent(-1, string.Empty) { Name = "event" },
                    },
                    sourceInstance);

                await service.StartAsync();
                serviceStarted = true;

                // Now confirm that both messages remain on the queue, even though no history was fetched for the
                // "future" execution ID attached to the TimerFired that has not yet been committed to the history table.
                // Critically, we want to abandon both the TimerFired *and* the EventRaisedEvent even though the latter
                // targets no specific execution ID.
                // No work item is generated since no history was fetched, but both messages will be retried again after
                // the visibility timeout expires.
                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    TaskOrchestrationWorkItem workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromSeconds(30),
                        timeout.Token);
                    Assert.IsNull(workItem);
                }

                Assert.AreEqual(
                    2,
                    await controlQueue.InnerQueue.GetApproximateMessagesCountAsync(),
                    "The out-of-order timer and execution-independent event should both remain on the queue.");

                await service.StopAsync(isForced: true);
                serviceStarted = false;

                // Now manually update the history to have the "future" execution ID
                OrchestrationHistory currentHistory = await service.TrackingStore.GetHistoryEventsAsync(
                    instanceId,
                    currentExecutionId);
                var futureRuntimeState = new OrchestrationRuntimeState();
                futureRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                futureRuntimeState.AddEvent(new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = instanceId,
                        ExecutionId = futureExecutionId,
                    },
                });
                futureRuntimeState.AddEvent(new TimerCreatedEvent(0, fireAt));
                futureRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.TrackingStore.UpdateStateAsync(
                    futureRuntimeState,
                    new OrchestrationRuntimeState(currentHistory.Events),
                    instanceId,
                    futureExecutionId,
                    new OrchestrationETags { HistoryETag = currentHistory.ETag },
                    currentHistory.TrackingStoreContext);

                await Task.Delay(settings.ControlQueueVisibilityTimeout + TimeSpan.FromSeconds(1));

                retryService = new AzureStorageOrchestrationService(settings);
                await retryService.StartAsync();
                retryServiceStarted = true;

                // This time when the history is fetched for the execution ID attached to the TimerFired, there *is*
                // a stored history, so both messages can be processed and attached to the work item
                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    TaskOrchestrationWorkItem workItem = await retryService.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromSeconds(30),
                        timeout.Token);

                    Assert.IsNotNull(workItem);
                    Assert.AreEqual(futureExecutionId, workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId);
                    Assert.AreEqual(2, workItem.NewMessages.Count);
                    Assert.IsTrue(workItem.NewMessages.Any(message => message.Event is TimerFiredEvent));
                    Assert.IsTrue(workItem.NewMessages.Any(message => message.Event is EventRaisedEvent));
                }
            }
            finally
            {
                if (retryServiceStarted)
                {
                    await retryService.StopAsync(isForced: true);
                }

                if (serviceStarted)
                {
                    await service.StopAsync(isForced: true);
                }

                if (service != null)
                {
                    await service.DeleteAsync();
                }

                retryService?.Dispose();
                service?.Dispose();
            }
        }

        [TestMethod]
        public async Task WorkerDiscardsExecutionIndependentMessagesWhenPendingInstanceIsTerminated()
        {
            AzureStorageOrchestrationService service = null;
            bool serviceStarted = false;

            string instanceId = Guid.NewGuid().ToString();
            string executionId = Guid.NewGuid().ToString();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = "PendingTermination" + Guid.NewGuid().ToString("N").Substring(0, 8),
                ExtendedSessionsEnabled = false,
                ControlQueueVisibilityTimeout = TimeSpan.FromSeconds(5),
                UseAppLease = false,
            };

            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();

                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                };
                var executionStartedEvent = new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                };

                // Create only the pending instance row, without committing any history or enqueueing its start message.
                Assert.IsTrue(await service.TrackingStore.SetNewExecutionAsync(executionStartedEvent, null, null));
                InstanceStatus status = await service.TrackingStore.FetchInstanceStatusAsync(instanceId);
                Assert.AreEqual(OrchestrationStatus.Pending, status.State.OrchestrationStatus);
                Assert.AreEqual(
                    0,
                    (await service.TrackingStore.GetHistoryEventsAsync(instanceId, executionId)).Events.Count);

                var controlQueue = service.AllControlQueues.Single();
                var sourceInstance = new OrchestrationInstance
                {
                    InstanceId = "source",
                    ExecutionId = "source-execution",
                };

                // Now terminate the pending orchestration
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                        Event = new ExecutionTerminatedEvent(-1, "terminate"),
                    },
                    sourceInstance);

                await service.StartAsync();
                serviceStarted = true;

                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    TaskOrchestrationWorkItem workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromSeconds(30),
                        timeout.Token);
                    Assert.IsNull(workItem);
                }

                status = await service.TrackingStore.FetchInstanceStatusAsync(instanceId);
                Assert.AreEqual(OrchestrationStatus.Terminated, status.State.OrchestrationStatus);
                Assert.AreEqual(
                    0,
                    await controlQueue.InnerQueue.GetApproximateMessagesCountAsync(),
                    "The termination message should be deleted after terminating the pending instance.");

                // Now try to send an external event to the terminated pending instance. It should be deleted rather than abandoned.
                await controlQueue.AddMessageAsync(
                    new TaskMessage
                    {
                        OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                        Event = new EventRaisedEvent(-1, string.Empty) { Name = "event" },
                    },
                    sourceInstance);

                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    TaskOrchestrationWorkItem workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                        TimeSpan.FromSeconds(30),
                        timeout.Token);
                    Assert.IsNull(workItem);
                }

                Assert.AreEqual(
                    0,
                    await controlQueue.InnerQueue.GetApproximateMessagesCountAsync(),
                    "The external event sent to the terminated pending instance should be deleted.");
            }
            finally
            {
                if (serviceStarted)
                {
                    await service.StopAsync(isForced: true);
                }

                if (service != null)
                {
                    await service.DeleteAsync();
                }

                service?.Dispose();
            }
        }

        [DataTestMethod]
        [DataRow(true, true)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(false, false)]
        public async Task WorkerAttemptingToDequeueMessageWithNoEventSentInHistory(bool extendedSessionsEnabled, bool addEventSentEvent)
        {
            AzureStorageOrchestrationService service = null;
            try
            {
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = Guid.NewGuid().ToString(),
                    ExecutionId = Guid.NewGuid().ToString(),
                };

                ExecutionStartedEvent startedEvent = new(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                    ScheduledStartTime = DateTime.UtcNow,
                };

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    PartitionCount = 1,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                    TaskHubName = TestHelpers.GetTestTaskHubName(),
                    ExtendedSessionsEnabled = extendedSessionsEnabled
                };

                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync();

                // Create the orchestration and get the first work item and start "working" on it
                await service.CreateTaskOrchestrationAsync(
                    new TaskMessage()
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = startedEvent
                    });
                var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                    TimeSpan.FromMinutes(5),
                    CancellationToken.None);
                var runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(startedEvent);
                string requestId = Guid.NewGuid().ToString();
                if (addEventSentEvent)
                {
                    runtimeState.AddEvent(new EventSentEvent(-1)
                    {
                        Input = $"{{ \"id\": \"{requestId}\" }}"
                    });
                }
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                await service.CompleteTaskOrchestrationWorkItemAsync(workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null);

                // Necessary to force a new work item to be generated for the next message
                await service.ReleaseTaskOrchestrationWorkItemAsync(workItem);

                // Send an event raised for a different request ID, message should be abandoned
                await service.SendTaskOrchestrationMessageInternalAsync(
                    sourceInstance: new OrchestrationInstance()
                    {
                        InstanceId = "@test@myEntity"
                    },
                    controlQueue: service.OwnedControlQueues.Single(),
                    new TaskMessage
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = new EventRaisedEvent(-1, string.Empty)
                        {
                            Name = Guid.NewGuid().ToString()
                        }
                    });
                workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                   TimeSpan.FromMinutes(1),
                   CancellationToken.None);
                Assert.IsNull(workItem);

                if (addEventSentEvent)
                {
                    // Send an event raised for the same request ID, this should work
                    await service.SendTaskOrchestrationMessageAsync(
                        new TaskMessage
                        {
                            OrchestrationInstance = orchestrationInstance,
                            Event = new EventRaisedEvent(-1, string.Empty)
                            {
                                Name = requestId
                            }
                        });
                    workItem = await service.LockNextTaskOrchestrationWorkItemAsync(
                       TimeSpan.FromMinutes(1),
                       CancellationToken.None);
                    Assert.IsNotNull(workItem);
                }
            }
            finally
            {
                await service?.StopAsync(isForced: true);
            }
        }

#if !NET48
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

            // Collect only the OnEnd activities from the processor invocations.
            // Other invocations (SetParentProvider, OnStart, OnShutdown, OnForceFlush, Dispose)
            // vary across OpenTelemetry SDK versions, so we filter by method name.
            var endedActivities = processor.Invocations
                .Where(i => i.Method.Name == "OnEnd")
                .Select(i => (Activity)i.Arguments[0])
                .ToList();

            Assert.AreEqual(4, endedActivities.Count);

            // Create orchestration Activity
            Activity createOrchestration = endedActivities[0];
            // Task execution Activity
            Activity taskExecution = endedActivities[1];
            // Task completed Activity
            Activity taskCompleted = endedActivities[2];
            // Orchestration execution Activity
            Activity orchestrationExecution = endedActivities[3];

            // Checking tag values
            string createOrchestrationTypeValue = createOrchestration.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string taskExecutionTypeValue = taskExecution.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string taskCompletedTypeValue = taskCompleted.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string orchestrationExecutionTypeValue = orchestrationExecution.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;

            Assert.AreEqual("orchestration", createOrchestrationTypeValue);
            Assert.AreEqual("activity", taskExecutionTypeValue);
            Assert.AreEqual("activity", taskCompletedTypeValue);
            Assert.AreEqual("orchestration", orchestrationExecutionTypeValue);
            Assert.AreEqual(ActivityKind.Producer, createOrchestration.Kind);
            Assert.AreEqual(ActivityKind.Server, taskExecution.Kind);
            Assert.AreEqual(ActivityKind.Client, taskCompleted.Kind);
            Assert.AreEqual(ActivityKind.Server, orchestrationExecution.Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(createOrchestration.SpanId, orchestrationExecution.ParentSpanId);
            Assert.AreEqual(taskCompleted.SpanId, taskExecution.ParentSpanId);
            Assert.AreEqual(orchestrationExecution.SpanId, taskCompleted.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(createOrchestration.TraceId.ToString(), taskExecution.TraceId.ToString(), taskCompleted.TraceId.ToString(), orchestrationExecution.TraceId.ToString());
        }

        [TestMethod]
        public async Task OpenTelemetry_ForeignParentSurvivesStorageRestart()
        {
            const string foreignTraceId = "0af7651916cd43dd8448eb211c80319c";
            const string foreignSpanId = "b7ad6b7169203331";
            var processor = new Mock<BaseProcessor<Activity>>();
            var settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = $"OtelParent{Guid.NewGuid():N}".Substring(0, 20);

            var serviceBeforeRestart = new AzureStorageOrchestrationService(settings);
            AzureStorageOrchestrationService serviceAfterRestart = null;
            TaskHubWorker worker = null;

            await serviceBeforeRestart.CreateAsync();

            try
            {
                using (Sdk.CreateTracerProviderBuilder()
                    .AddSource("DurableTask.Core")
                    .AddProcessor(processor.Object)
                    .Build())
                {
                    var instance = new OrchestrationInstance
                    {
                        InstanceId = Guid.NewGuid().ToString("N"),
                        ExecutionId = Guid.NewGuid().ToString("N"),
                    };
                    var startedEvent = new ExecutionStartedEvent(-1, JsonConvert.SerializeObject("World"))
                    {
                        Name = NameVersionHelper.GetDefaultName(typeof(Orchestrations.SayHelloWithActivity)),
                        Version = NameVersionHelper.GetDefaultVersion(typeof(Orchestrations.SayHelloWithActivity)),
                        OrchestrationInstance = instance,
                        ParentTraceContext = new DistributedTraceContext(
                            $"00-{foreignTraceId}-{foreignSpanId}-01",
                            "vendor=value"),
                    };

                    await serviceBeforeRestart.CreateTaskOrchestrationAsync(new TaskMessage
                    {
                        OrchestrationInstance = instance,
                        Event = startedEvent,
                    });

                    serviceAfterRestart = new AzureStorageOrchestrationService(settings);
                    var client = new TaskHubClient(serviceAfterRestart);
                    worker = new TaskHubWorker(serviceAfterRestart);
                    worker.AddTaskOrchestrations(typeof(Orchestrations.SayHelloWithActivity));
                    worker.AddTaskActivities(typeof(Activities.Hello));
                    await worker.StartAsync();

                    OrchestrationState state = await client.WaitForOrchestrationAsync(instance, StandardTimeout);
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual("\"Hello, World!\"", state.Output);

                    await worker.StopAsync();
                    worker = null;

                    List<Activity> endedActivities = GetEndedActivities(processor, instance.InstanceId);
                    Assert.AreEqual(3, endedActivities.Count);

                    Activity orchestration = GetActivity(
                        endedActivities,
                        ActivityKind.Server,
                        "orchestration",
                        startedEvent.Name);
                    Activity activityClient = GetActivity(
                        endedActivities,
                        ActivityKind.Client,
                        "activity",
                        NameVersionHelper.GetDefaultName(typeof(Activities.Hello)));
                    Activity activityServer = GetActivity(
                        endedActivities,
                        ActivityKind.Server,
                        "activity",
                        NameVersionHelper.GetDefaultName(typeof(Activities.Hello)));

                    Assert.AreEqual(foreignTraceId, orchestration.TraceId.ToString());
                    Assert.AreEqual(foreignSpanId, orchestration.ParentSpanId.ToString());
                    Assert.AreEqual(orchestration.SpanId, activityClient.ParentSpanId);
                    Assert.AreEqual(activityClient.SpanId, activityServer.ParentSpanId);

                    foreach (Activity activity in endedActivities)
                    {
                        Assert.AreEqual(foreignTraceId, activity.TraceId.ToString());
                        Assert.AreEqual("vendor=value", activity.TraceStateString);
                    }
                }
            }
            finally
            {
                if (worker != null)
                {
                    await worker.StopAsync();
                }

                if (serviceAfterRestart != null)
                {
                    await serviceAfterRestart.DeleteAsync();
                }
                else
                {
                    await serviceBeforeRestart.DeleteAsync();
                }
            }
        }

        [TestMethod]
        public async Task OpenTelemetry_ActivityRetryCreatesDistinctAttemptSpans()
        {
            var processor = new Mock<BaseProcessor<Activity>>();
            Activities.HelloRetryOnce.Reset();

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
            {
                await host.StartAsync();
                TestOrchestrationClient client;
                try
                {
                    client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloWithRetryActivity), "World");
                    OrchestrationState state = await client.WaitForCompletionAsync(StandardTimeout);

                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual("\"Hello, World!\"", state.Output);
                }
                finally
                {
                    await host.StopAsync();
                }

                List<Activity> endedActivities = GetEndedActivities(processor, client.InstanceId);
                string orchestrationName = NameVersionHelper.GetDefaultName(typeof(Orchestrations.SayHelloWithRetryActivity));
                string activityName = NameVersionHelper.GetDefaultName(typeof(Activities.HelloRetryOnce));

                Activity create = GetActivity(endedActivities, ActivityKind.Producer, "orchestration", orchestrationName);
                Activity orchestration = GetActivity(endedActivities, ActivityKind.Server, "orchestration", orchestrationName);
                List<Activity> activityClients = GetActivities(endedActivities, ActivityKind.Client, "activity", activityName);
                List<Activity> activityServers = GetActivities(endedActivities, ActivityKind.Server, "activity", activityName);

                Assert.AreEqual(2, activityClients.Count);
                Assert.AreEqual(2, activityServers.Count);
                Assert.AreEqual(2, activityClients.Select(a => a.SpanId).Distinct().Count());
                Assert.AreEqual(2, activityServers.Select(a => a.SpanId).Distinct().Count());
                Assert.AreEqual(1, activityClients.Count(a => a.Status == ActivityStatusCode.Error));
                Assert.AreEqual(1, activityServers.Count(a => a.Status == ActivityStatusCode.Error));

                foreach (Activity activityClient in activityClients)
                {
                    Assert.AreEqual(orchestration.SpanId, activityClient.ParentSpanId);
                    Assert.AreEqual(create.TraceId, activityClient.TraceId);
                    Activity activityServer = activityServers.Single(a => a.ParentSpanId == activityClient.SpanId);
                    Assert.AreEqual(activityClient.TraceId, activityServer.TraceId);
                }

                Assert.AreEqual(create.TraceId, orchestration.TraceId);
                Assert.AreEqual(create.SpanId, orchestration.ParentSpanId);
            }
        }

        [TestMethod]
        public async Task OpenTelemetry_SubOrchestrationLinksClientAndServerSpans()
        {
            var processor = new Mock<BaseProcessor<Activity>>();

            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
            using (Sdk.CreateTracerProviderBuilder()
                .AddSource("DurableTask.Core")
                .AddProcessor(processor.Object)
                .Build())
            {
                await host.StartAsync();
                TestOrchestrationClient client;
                try
                {
                    client = await host.StartOrchestrationAsync(typeof(Orchestrations.ParentWithSubOrchestration), "World");
                    OrchestrationState state = await client.WaitForCompletionAsync(StandardTimeout);

                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual("\"Hello, World!\"", state.Output);
                }
                finally
                {
                    await host.StopAsync();
                }

                List<Activity> allEndedActivities = GetEndedActivities(processor);
                string parentName = NameVersionHelper.GetDefaultName(typeof(Orchestrations.ParentWithSubOrchestration));
                Activity create = GetActivity(
                    allEndedActivities.Where(a => GetTag(a, "durabletask.task.instance_id") == client.InstanceId),
                    ActivityKind.Producer,
                    "orchestration",
                    parentName);
                List<Activity> endedActivities = allEndedActivities
                    .Where(a => a.TraceId == create.TraceId)
                    .ToList();
                Assert.AreEqual(4, endedActivities.Count);

                string childName = NameVersionHelper.GetDefaultName(typeof(Orchestrations.ChildSayHelloInline));
                Activity parent = GetActivity(endedActivities, ActivityKind.Server, "orchestration", parentName);
                Activity childClient = GetActivity(endedActivities, ActivityKind.Client, "orchestration", childName);
                Activity childServer = GetActivity(endedActivities, ActivityKind.Server, "orchestration", childName);

                Assert.AreEqual(create.SpanId, parent.ParentSpanId);
                Assert.AreEqual(parent.SpanId, childClient.ParentSpanId);
                Assert.AreEqual(childClient.SpanId, childServer.ParentSpanId);
                Assert.AreEqual(client.InstanceId, GetTag(childClient, "durabletask.task.instance_id"));
                Assert.AreNotEqual(client.InstanceId, GetTag(childServer, "durabletask.task.instance_id"));

                foreach (Activity activity in endedActivities)
                {
                    Assert.AreEqual(create.TraceId, activity.TraceId);
                }
            }
        }

        static List<Activity> GetEndedActivities(Mock<BaseProcessor<Activity>> processor, string instanceId = null)
        {
            IEnumerable<Activity> activities = processor.Invocations
                .Where(i => i.Method.Name == "OnEnd")
                .Select(i => (Activity)i.Arguments[0]);

            if (instanceId != null)
            {
                activities = activities.Where(a => GetTag(a, "durabletask.task.instance_id") == instanceId);
            }

            return activities.ToList();
        }

        static Activity GetActivity(
            IEnumerable<Activity> activities,
            ActivityKind kind,
            string taskType,
            string taskName)
        {
            return GetActivities(activities, kind, taskType, taskName).Single();
        }

        static List<Activity> GetActivities(
            IEnumerable<Activity> activities,
            ActivityKind kind,
            string taskType,
            string taskName)
        {
            return activities
                .Where(a => a.Kind == kind)
                .Where(a => GetTag(a, "durabletask.type") == taskType)
                .Where(a => GetTag(a, "durabletask.task.name") == taskName)
                .ToList();
        }

        static string GetTag(Activity activity, string name)
        {
            return Convert.ToString(activity.GetTagItem(name));
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

            // Collect only the OnEnd activities from the processor invocations.
            var endedActivities = processor.Invocations
                .Where(i => i.Method.Name == "OnEnd")
                .Select(i => (Activity)i.Arguments[0])
                .ToList();

            Assert.AreEqual(3, endedActivities.Count);

            // Create orchestration Activity
            Activity createOrchestration = endedActivities[0];
            // External event Activity
            Activity externalEvent = endedActivities[1];
            // Orchestration execution Activity
            Activity orchestrationExecution = endedActivities[2];

            // Checking tag values
            string createOrchestrationTypeValue = createOrchestration.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string externalEventTypeValue = externalEvent.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string orchestrationExecutionTypeValue = orchestrationExecution.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string externalEventTargetInstanceIdValue = externalEvent.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;

            Assert.AreEqual("orchestration", createOrchestrationTypeValue);
            Assert.AreEqual("event", externalEventTypeValue);
            Assert.AreEqual("orchestration", orchestrationExecutionTypeValue);
            Assert.AreEqual(instanceId, externalEventTargetInstanceIdValue);
            Assert.AreEqual(ActivityKind.Producer, createOrchestration.Kind);
            Assert.AreEqual(ActivityKind.Producer, externalEvent.Kind);
            Assert.AreEqual(ActivityKind.Server, orchestrationExecution.Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(createOrchestration.SpanId, orchestrationExecution.ParentSpanId);

            // Checking trace ID values (the external event from the client is its own trace)
            Assert.AreEqual(createOrchestration.TraceId.ToString(), orchestrationExecution.TraceId.ToString());
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

            // Collect only the OnEnd activities from the processor invocations.
            var endedActivities = processor.Invocations
                .Where(i => i.Method.Name == "OnEnd")
                .Select(i => (Activity)i.Arguments[0])
                .ToList();

            Assert.AreEqual(3, endedActivities.Count);

            // Create orchestration Activity
            Activity createOrchestration = endedActivities[0];
            // Timer fired Activity
            Activity timerFired = endedActivities[1];
            // Orchestration execution Activity
            Activity orchestrationExecution = endedActivities[2];

            // Checking tag values
            string createOrchestrationTypeValue = createOrchestration.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string timerFiredTypeValue = timerFired.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string orchestrationExecutionTypeValue = orchestrationExecution.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;

            Assert.AreEqual("orchestration", createOrchestrationTypeValue);
            Assert.AreEqual("timer", timerFiredTypeValue);
            Assert.AreEqual("orchestration", orchestrationExecutionTypeValue);
            Assert.AreEqual(ActivityKind.Producer, createOrchestration.Kind);
            Assert.AreEqual(ActivityKind.Internal, timerFired.Kind);
            Assert.AreEqual(ActivityKind.Server, orchestrationExecution.Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(createOrchestration.SpanId, orchestrationExecution.ParentSpanId);
            Assert.AreEqual(orchestrationExecution.SpanId, timerFired.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(createOrchestration.TraceId.ToString(), timerFired.TraceId.ToString(), orchestrationExecution.TraceId.ToString());
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

            // Collect only the OnEnd activities from the processor invocations.
            var endedActivities = processor.Invocations
                .Where(i => i.Method.Name == "OnEnd")
                .Select(i => (Activity)i.Arguments[0])
                .ToList();

            Assert.AreEqual(4, endedActivities.Count);

            // Create orchestration (AutoStartOrchestration) Activity
            Activity createOrchestration = endedActivities[0];
            // Send event to AutoStartOrchestration.Responder Activity
            Activity sendEvent = endedActivities[1];
            // Send event from AutoStartOrchestration.Responder back to AutoStartOrchestration Activity
            Activity sendEventBack = endedActivities[2];
            // Orchestration execution Activity
            Activity orchestrationExecution = endedActivities[3];

            // Checking tag values
            string createOrchestrationTypeValue = createOrchestration.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string sendEventTypeValue = sendEvent.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string sendEventBackTypeValue = sendEventBack.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string orchestrationExecutionTypeValue = orchestrationExecution.Tags.First(k => (k.Key).Equals("durabletask.type")).Value;
            string sendEventInstanceIdValue = sendEvent.Tags.First(k => (k.Key).Equals("durabletask.task.instance_id")).Value;
            string sendEventTargetInstanceIdValue = sendEvent.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;
            string sendEventBackInstanceIdValue = sendEventBack.Tags.First(k => (k.Key).Equals("durabletask.task.instance_id")).Value;
            string sendEventBackTargetInstanceIdValue = sendEventBack.Tags.First(k => (k.Key).Equals("durabletask.event.target_instance_id")).Value;

            Assert.AreEqual("orchestration", createOrchestrationTypeValue);
            Assert.AreEqual("event", sendEventTypeValue);
            Assert.AreEqual("event", sendEventBackTypeValue);
            Assert.AreEqual("orchestration", orchestrationExecutionTypeValue);
            Assert.AreEqual(instanceId, sendEventInstanceIdValue);
            Assert.AreEqual(responderId, sendEventTargetInstanceIdValue);
            Assert.AreEqual(responderId, sendEventBackInstanceIdValue);
            Assert.AreEqual(instanceId, sendEventBackTargetInstanceIdValue);
            Assert.AreEqual(ActivityKind.Producer, createOrchestration.Kind);
            Assert.AreEqual(ActivityKind.Producer, sendEvent.Kind);
            Assert.AreEqual(ActivityKind.Producer, sendEventBack.Kind);
            Assert.AreEqual(ActivityKind.Server, orchestrationExecution.Kind);

            // Checking span ID correlation between parent and child
            Assert.AreEqual(createOrchestration.SpanId, orchestrationExecution.ParentSpanId);
            Assert.AreEqual(orchestrationExecution.SpanId, sendEvent.ParentSpanId);

            // Checking trace ID values
            Assert.AreEqual(createOrchestration.TraceId.ToString(), sendEvent.TraceId.ToString(), orchestrationExecution.TraceId.ToString());
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

            [KnownType(typeof(Activities.HelloRetryOnce))]
            internal class SayHelloWithRetryActivity : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(10), 2);
                    return context.ScheduleWithRetry<string>(typeof(Activities.HelloRetryOnce), retryOptions, input);
                }
            }

            [KnownType(typeof(ChildSayHelloInline))]
            internal class ParentWithSubOrchestration : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.CreateSubOrchestrationInstance<string>(typeof(ChildSayHelloInline), input);
                }
            }

            internal class ChildSayHelloInline : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult($"Hello, {input}!");
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

            [KnownType(typeof(InlineChild))]
            internal class ParentOfInlineChild : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return context.CreateSubOrchestrationInstance<string>(
                        typeof(InlineChild),
                        context.OrchestrationInstance.InstanceId + ":child",
                        input);
                }
            }

            internal class InlineChild : TaskOrchestration<string, string>
            {
                public override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    return Task.FromResult(input);
                }
            }

            [KnownType(typeof(ContinueAsNewChild))]
            internal class ParentOfContinueAsNewChild : TaskOrchestration<int, int>
            {
                public override Task<int> RunTask(OrchestrationContext context, int input)
                {
                    return context.CreateSubOrchestrationInstance<int>(
                        typeof(ContinueAsNewChild),
                        context.OrchestrationInstance.InstanceId + ":child",
                        input);
                }
            }

            internal class ContinueAsNewChild : TaskOrchestration<int, int>
            {
                public override Task<int> RunTask(OrchestrationContext context, int input)
                {
                    if (input == 0)
                    {
                        context.ContinueAsNew(1);
                    }

                    return Task.FromResult(input);
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

            internal class HelloRetryOnce : TaskActivity<string, string>
            {
                static int attemptCount;

                internal static void Reset()
                {
                    Volatile.Write(ref attemptCount, 0);
                }

                protected override string Execute(TaskContext context, string input)
                {
                    if (Interlocked.Increment(ref attemptCount) == 1)
                    {
                        throw new InvalidOperationException("Failing the first activity attempt.");
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
