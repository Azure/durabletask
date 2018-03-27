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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationHubTableClientTests
    {
        TaskHubClient client;
        AzureTableClient tableClient;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            var r = new Random();
            tableClient = new AzureTableClient("test00" + r.Next(0, 10000),
                "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:10002/");
            tableClient.CreateTableIfNotExistsAsync().Wait();

            client = TestHelpers.CreateTaskHubClient();

            taskHub = TestHelpers.CreateTaskHub();

            taskHub.orchestrationService.CreateAsync(true).Wait();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            tableClient.DeleteTableIfExistsAsync().Wait();
            taskHub.StopAsync(true).Wait();
            taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        [TestMethod]
        public async Task BasicInstanceStoreTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "DONT_THROW");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            OrchestrationState runtimeState = await client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(runtimeState.OrchestrationStatus, OrchestrationStatus.Completed);
            Assert.AreEqual(runtimeState.OrchestrationInstance.InstanceId, id.InstanceId);
            Assert.AreEqual(runtimeState.OrchestrationInstance.ExecutionId, id.ExecutionId);
            Assert.AreEqual("DurableTask.ServiceBus.Tests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration", runtimeState.Name);
            Assert.AreEqual(runtimeState.Version, string.Empty);
            Assert.AreEqual(runtimeState.Input, "\"DONT_THROW\"");
            Assert.AreEqual(runtimeState.Output, "\"Spartacus\"");

            string history = await client.GetOrchestrationHistoryAsync(id);
            Assert.IsTrue(!string.IsNullOrEmpty(history));
            Assert.IsTrue(history.Contains("ExecutionStartedEvent"));
        }

        [TestMethod]
        public async Task MultipleInstanceStoreTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id1 = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "WAIT_THROW");
            OrchestrationInstance id2 = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "WAIT_DONTTHROW");

            await TestHelpers.WaitForInstanceAsync(client, id1, 60, false);
            await TestHelpers.WaitForInstanceAsync(client, id2, 60, false);

            OrchestrationState runtimeState1 = await client.GetOrchestrationStateAsync(id1);
            OrchestrationState runtimeState2 = await client.GetOrchestrationStateAsync(id2);
            Assert.AreEqual(OrchestrationStatus.Pending, runtimeState1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Pending, runtimeState2.OrchestrationStatus);

            await TestHelpers.WaitForInstanceAsync(client, id1, 60);
            await TestHelpers.WaitForInstanceAsync(client, id2, 60);

            runtimeState1 = await client.GetOrchestrationStateAsync(id1);
            runtimeState2 = await client.GetOrchestrationStateAsync(id2);
            Assert.AreEqual(OrchestrationStatus.Failed, runtimeState1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, runtimeState2.OrchestrationStatus);
        }

        [TestMethod]
        public async Task TerminateInstanceStoreTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "WAIT");

            await TestHelpers.WaitForInstanceAsync(client, id, 60, false);
            OrchestrationState runtimeState = await client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(OrchestrationStatus.Pending, runtimeState.OrchestrationStatus);

            await client.TerminateInstanceAsync(id);
            await TestHelpers.WaitForInstanceAsync(client, id, 60);
            runtimeState = await client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(OrchestrationStatus.Terminated, runtimeState.OrchestrationStatus);
        }

        [TestMethod]
        public async Task IntermediateStateInstanceStoreTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "WAIT");

            await TestHelpers.WaitForInstanceAsync(client, id, 60, false);

            OrchestrationState runtimeState = await client.GetOrchestrationStateAsync(id);
            Assert.IsNotNull(runtimeState);
            Assert.AreEqual(OrchestrationStatus.Pending, runtimeState.OrchestrationStatus);
            Assert.AreEqual(id.InstanceId, runtimeState.OrchestrationInstance.InstanceId);
            Assert.AreEqual(id.ExecutionId, runtimeState.OrchestrationInstance.ExecutionId);
            Assert.AreEqual("DurableTask.ServiceBus.Tests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration", runtimeState.Name);
            Assert.AreEqual(runtimeState.Version, string.Empty);
            Assert.AreEqual(runtimeState.Input, "\"WAIT\"");
            Assert.AreEqual(runtimeState.Output, null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            runtimeState = await client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(runtimeState.OrchestrationStatus, OrchestrationStatus.Completed);
        }

        [TestMethod]
        public async Task FailingInstanceStoreTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (InstanceStoreTestOrchestration),
                "THROW");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            var status = await client.GetOrchestrationStateAsync(id);
            Assert.IsTrue(status.OrchestrationStatus == OrchestrationStatus.Failed);
        }

        [TestMethod]
        public async Task OrchestrationEventHistoryTest()
        {
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> entitiesInst0Gen0 = CreateHistoryEntities(tableClient, "0", "0",
                10);
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> entitiesInst0Gen1 = CreateHistoryEntities(tableClient, "0", "1",
                10);
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> entitiesInst1Gen0 = CreateHistoryEntities(tableClient, "1", "0",
                10);
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> entitiesInst1Gen1 = CreateHistoryEntities(tableClient, "1", "1",
                10);

            IEnumerable<AzureTableOrchestrationHistoryEventEntity> histInst0Gen0Returned =
                await tableClient.ReadOrchestrationHistoryEventsAsync("0", "0");
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> histInst0Gen1Returned =
                await tableClient.ReadOrchestrationHistoryEventsAsync("0", "1");
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> histInst1Gen0Returned =
                await tableClient.ReadOrchestrationHistoryEventsAsync("1", "0");
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> histInst1Gen1Returned =
                await tableClient.ReadOrchestrationHistoryEventsAsync("1", "1");

            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen0, histInst0Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen1, histInst0Gen1Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen0, histInst1Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen1, histInst1Gen1Returned));
        }

        [TestMethod]
        public async Task OrchestrationStateTest()
        {
            IEnumerable<AzureTableOrchestrationStateEntity> entitiesInst0Gen0 = CreateStateEntities(tableClient, "0", "0");
            IEnumerable<AzureTableOrchestrationStateEntity> entitiesInst0Gen1 = CreateStateEntities(tableClient, "0", "1");
            IEnumerable<AzureTableOrchestrationStateEntity> entitiesInst1Gen0 = CreateStateEntities(tableClient, "1", "0");
            IEnumerable<AzureTableOrchestrationStateEntity> entitiesInst1Gen1 = CreateStateEntities(tableClient, "1", "1");

            IEnumerable<AzureTableOrchestrationStateEntity> histInst0Gen0Returned = await tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("0", "0"));

            IEnumerable<AzureTableOrchestrationStateEntity> histInst0Gen1Returned = await tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("0", "1"));

            IEnumerable<AzureTableOrchestrationStateEntity> histInst1Gen0Returned = await tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("1", "0"));

            IEnumerable<AzureTableOrchestrationStateEntity> histInst1Gen1Returned = await tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("1", "1"));

            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen0, histInst0Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen1, histInst0Gen1Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen0, histInst1Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen1, histInst1Gen1Returned));
        }

        bool CompareEnumerations(IEnumerable<AzureTableCompositeTableEntity> expected,
            IEnumerable<AzureTableCompositeTableEntity> actual)
        {
            IEnumerator<AzureTableCompositeTableEntity> expectedEnumerator = expected.GetEnumerator();
            IEnumerator<AzureTableCompositeTableEntity> actualEnumerator = actual.GetEnumerator();

            while (expectedEnumerator.MoveNext())
            {
                if (!actualEnumerator.MoveNext())
                {
                    Debug.WriteLine("actual enumeration does not have enough elements");
                    return false;
                }

                bool match = false;
                Trace.WriteLine("Expected: " + expectedEnumerator.Current);
                Trace.WriteLine("Actual: " + actualEnumerator.Current);
                if (expectedEnumerator.Current is AzureTableOrchestrationHistoryEventEntity)
                {
                    match = CompareHistoryEntity(expectedEnumerator.Current as AzureTableOrchestrationHistoryEventEntity,
                        actualEnumerator.Current as AzureTableOrchestrationHistoryEventEntity);
                }
                else
                {
                    match = CompareStateEntity(expectedEnumerator.Current as AzureTableOrchestrationStateEntity,
                        actualEnumerator.Current as AzureTableOrchestrationStateEntity);
                }

                if (!match)
                {
                    Debug.WriteLine("Actual different from expected. \n\tActual : " + actualEnumerator.Current +
                                    "\n\tExpected: " + expectedEnumerator.Current);
                    return false;
                }
            }

            if (actualEnumerator.MoveNext())
            {
                Trace.WriteLine("actual enumeration has more elements than expected");
                return false;
            }

            return true;
        }

        bool CompareHistoryEntity(AzureTableOrchestrationHistoryEventEntity expected, AzureTableOrchestrationHistoryEventEntity actual)
        {
            // TODO : history comparison!
            return expected.InstanceId.Equals(actual.InstanceId) && expected.ExecutionId.Equals(actual.ExecutionId) &&
                   expected.SequenceNumber == actual.SequenceNumber;
        }

        bool CompareStateEntity(AzureTableOrchestrationStateEntity expected, AzureTableOrchestrationStateEntity actual)
        {
            return
                expected.State.OrchestrationInstance.InstanceId.Equals(actual.State.OrchestrationInstance.InstanceId) &&
                expected.State.OrchestrationInstance.ExecutionId.Equals(actual.State.OrchestrationInstance.ExecutionId) &&
                expected.State.Name.Equals(actual.State.Name) &&
                expected.State.CreatedTime.Equals(actual.State.CreatedTime) &&
                expected.State.LastUpdatedTime.Equals(actual.State.LastUpdatedTime) &&
                ((expected.State.CompletedTime == null && actual.State.CompletedTime == null) ||
                 expected.State.CompletedTime.Equals(actual.State.CompletedTime)) &&
                expected.State.Status.Equals(actual.State.Status) &&
                expected.State.Input.Equals(actual.State.Input) &&
                ((string.IsNullOrEmpty(expected.State.Output) && string.IsNullOrEmpty(actual.State.Output)) ||
                 expected.State.Output.Equals(actual.State.Output));
        }

        IEnumerable<AzureTableOrchestrationHistoryEventEntity> CreateHistoryEntities(AzureTableClient client, string instanceId,
            string genId, int count)
        {
            var historyEntities = new List<AzureTableOrchestrationHistoryEventEntity>();
            for (int i = 0; i < count; i++)
            {
                var eeStartedEvent = new ExecutionStartedEvent(-1, "EVENT_" + instanceId + "_" + genId + "_" + i);

                historyEntities.Add(new AzureTableOrchestrationHistoryEventEntity(instanceId, genId, i, DateTime.Now,
                    eeStartedEvent));
            }
            client.WriteEntitiesAsync(historyEntities).Wait();
            return historyEntities;
        }

        IEnumerable<AzureTableOrchestrationStateEntity> CreateStateEntities(AzureTableClient client, string instanceId, string genId)
        {
            var entities = new List<AzureTableOrchestrationStateEntity>();
            var runtimeState = new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = genId
                },
                Name = "FooOrch",
                Version = "1.0",
                CompletedTime = DateTime.UtcNow,
                CreatedTime = DateTime.UtcNow,
                LastUpdatedTime = DateTime.UtcNow,
                Status = "Screwed",
                Input = "INPUT_" + instanceId + "_" + genId,
                Output = null
            };

            entities.Add(new AzureTableOrchestrationStateEntity(runtimeState));
            client.WriteEntitiesAsync(entities).Wait();
            return entities;
        }

        public sealed class Activity1 : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Spartacus";
            }
        }

        public class InstanceStoreTestOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = await context.ScheduleTask<string>(typeof (Activity1));
                if (string.Equals(input, "THROW", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("BADFOOD");
                }
                if (string.Equals(input, "WAIT", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                }
                else if (string.Equals(input, "WAIT_THROW", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                    throw new InvalidOperationException("BADFOOD");
                }
                else if (string.Equals(input, "WAIT_DONTTHROW", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                }
                return result;
            }
        }
    }
}