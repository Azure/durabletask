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

namespace FrameworkUnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.History;
    using DurableTask.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationHubTableClientTests
    {
        TaskHubClient client;
        TableClient tableClient;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            var r = new Random();
            tableClient = new TableClient("test00" + r.Next(0, 10000),
                "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:10002/");
            tableClient.CreateTableIfNotExists();

            client = TestHelpers.CreateTaskHubClient();

            taskHub = TestHelpers.CreateTaskHub();

            taskHub.DeleteHub();
            taskHub.CreateHubIfNotExists();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            tableClient.DeleteTableIfExists();
            taskHub.Stop(true);
            taskHub.DeleteHub();
        }

        [TestMethod]
        public void BasicInstanceStoreTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "DONT_THROW");

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            OrchestrationState runtimeState = client.GetOrchestrationState(id);
            Assert.AreEqual(runtimeState.OrchestrationStatus, OrchestrationStatus.Completed);
            Assert.AreEqual(runtimeState.OrchestrationInstance.InstanceId, id.InstanceId);
            Assert.AreEqual(runtimeState.OrchestrationInstance.ExecutionId, id.ExecutionId);
            Assert.AreEqual(runtimeState.Name,
                "FrameworkUnitTests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration");
            Assert.AreEqual(runtimeState.Version, string.Empty);
            Assert.AreEqual(runtimeState.Input, "\"DONT_THROW\"");
            Assert.AreEqual(runtimeState.Output, "\"Spartacus\"");

            string history = client.GetOrchestrationHistory(id);
            Assert.IsTrue(!string.IsNullOrEmpty(history));
            Assert.IsTrue(history.Contains("ExecutionStartedEvent"));
        }

        [TestMethod]
        public void MultipleInstanceStoreTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT_THROW");
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT_DONTTHROW");

            TestHelpers.WaitForInstance(client, id1, 60, false);
            TestHelpers.WaitForInstance(client, id2, 60, false);

            OrchestrationState runtimeState1 = client.GetOrchestrationState(id1);
            OrchestrationState runtimeState2 = client.GetOrchestrationState(id2);
            Assert.AreEqual(runtimeState1.OrchestrationStatus, OrchestrationStatus.Running);
            Assert.AreEqual(runtimeState2.OrchestrationStatus, OrchestrationStatus.Running);

            TestHelpers.WaitForInstance(client, id1, 60);
            TestHelpers.WaitForInstance(client, id2, 60);

            runtimeState1 = client.GetOrchestrationState(id1);
            runtimeState2 = client.GetOrchestrationState(id2);
            Assert.AreEqual(runtimeState1.OrchestrationStatus, OrchestrationStatus.Failed);
            Assert.AreEqual(runtimeState2.OrchestrationStatus, OrchestrationStatus.Completed);
        }

        [TestMethod]
        public void TerminateInstanceStoreTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT");

            TestHelpers.WaitForInstance(client, id, 60, false);
            OrchestrationState runtimeState = client.GetOrchestrationState(id);
            Assert.AreEqual(OrchestrationStatus.Running, runtimeState.OrchestrationStatus);

            client.TerminateInstance(id);
            TestHelpers.WaitForInstance(client, id, 60);
            runtimeState = client.GetOrchestrationState(id);
            Assert.AreEqual(OrchestrationStatus.Terminated, runtimeState.OrchestrationStatus);
        }

        [TestMethod]
        public void IntermediateStateInstanceStoreTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT");

            TestHelpers.WaitForInstance(client, id, 60, false);

            OrchestrationState runtimeState = client.GetOrchestrationState(id);
            Assert.IsNotNull(runtimeState);
            Assert.AreEqual(runtimeState.OrchestrationStatus, OrchestrationStatus.Running);
            Assert.AreEqual(runtimeState.OrchestrationInstance.InstanceId, id.InstanceId);
            Assert.AreEqual(runtimeState.OrchestrationInstance.ExecutionId, id.ExecutionId);
            Assert.AreEqual(runtimeState.Name,
                "FrameworkUnitTests.OrchestrationHubTableClientTests+InstanceStoreTestOrchestration");
            Assert.AreEqual(runtimeState.Version, string.Empty);
            Assert.AreEqual(runtimeState.Input, "\"WAIT\"");
            Assert.AreEqual(runtimeState.Output, null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            runtimeState = client.GetOrchestrationState(id);
            Assert.AreEqual(runtimeState.OrchestrationStatus, OrchestrationStatus.Completed);
        }

        [TestMethod]
        public void FailingInstanceStoreTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "THROW");

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(client.GetOrchestrationState(id).OrchestrationStatus == OrchestrationStatus.Failed);
        }

        [TestMethod]
        public void OrchestrationEventHistoryTest()
        {
            IEnumerable<OrchestrationHistoryEventEntity> entitiesInst0Gen0 = CreateHistoryEntities(tableClient, "0", "0",
                10);
            IEnumerable<OrchestrationHistoryEventEntity> entitiesInst0Gen1 = CreateHistoryEntities(tableClient, "0", "1",
                10);
            IEnumerable<OrchestrationHistoryEventEntity> entitiesInst1Gen0 = CreateHistoryEntities(tableClient, "1", "0",
                10);
            IEnumerable<OrchestrationHistoryEventEntity> entitiesInst1Gen1 = CreateHistoryEntities(tableClient, "1", "1",
                10);

            IEnumerable<OrchestrationHistoryEventEntity> histInst0Gen0Returned =
                tableClient.ReadOrchestrationHistoryEventsAsync("0", "0").Result;
            IEnumerable<OrchestrationHistoryEventEntity> histInst0Gen1Returned =
                tableClient.ReadOrchestrationHistoryEventsAsync("0", "1").Result;
            IEnumerable<OrchestrationHistoryEventEntity> histInst1Gen0Returned =
                tableClient.ReadOrchestrationHistoryEventsAsync("1", "0").Result;
            IEnumerable<OrchestrationHistoryEventEntity> histInst1Gen1Returned =
                tableClient.ReadOrchestrationHistoryEventsAsync("1", "1").Result;

            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen0, histInst0Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen1, histInst0Gen1Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen0, histInst1Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen1, histInst1Gen1Returned));
        }

        [TestMethod]
        public void OrchestrationStateTest()
        {
            IEnumerable<OrchestrationStateEntity> entitiesInst0Gen0 = CreateStateEntities(tableClient, "0", "0");
            IEnumerable<OrchestrationStateEntity> entitiesInst0Gen1 = CreateStateEntities(tableClient, "0", "1");
            IEnumerable<OrchestrationStateEntity> entitiesInst1Gen0 = CreateStateEntities(tableClient, "1", "0");
            IEnumerable<OrchestrationStateEntity> entitiesInst1Gen1 = CreateStateEntities(tableClient, "1", "1");

            IEnumerable<OrchestrationStateEntity> histInst0Gen0Returned = tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("0", "0")).Result;

            IEnumerable<OrchestrationStateEntity> histInst0Gen1Returned = tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("0", "1")).Result;

            IEnumerable<OrchestrationStateEntity> histInst1Gen0Returned = tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("1", "0")).Result;

            IEnumerable<OrchestrationStateEntity> histInst1Gen1Returned = tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter("1", "1")).Result;

            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen0, histInst0Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst0Gen1, histInst0Gen1Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen0, histInst1Gen0Returned));
            Assert.IsTrue(CompareEnumerations(entitiesInst1Gen1, histInst1Gen1Returned));
        }

        bool CompareEnumerations(IEnumerable<CompositeTableEntity> expected,
            IEnumerable<CompositeTableEntity> actual)
        {
            IEnumerator<CompositeTableEntity> expectedEnumerator = expected.GetEnumerator();
            IEnumerator<CompositeTableEntity> actualEnumerator = actual.GetEnumerator();

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
                if (expectedEnumerator.Current is OrchestrationHistoryEventEntity)
                {
                    match = CompareHistoryEntity(expectedEnumerator.Current as OrchestrationHistoryEventEntity,
                        actualEnumerator.Current as OrchestrationHistoryEventEntity);
                }
                else
                {
                    match = CompareStateEntity(expectedEnumerator.Current as OrchestrationStateEntity,
                        actualEnumerator.Current as OrchestrationStateEntity);
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

        bool CompareHistoryEntity(OrchestrationHistoryEventEntity expected, OrchestrationHistoryEventEntity actual)
        {
            // TODO : history comparison!
            return expected.InstanceId.Equals(actual.InstanceId) && expected.ExecutionId.Equals(actual.ExecutionId) &&
                   expected.SequenceNumber == actual.SequenceNumber;
        }

        bool CompareStateEntity(OrchestrationStateEntity expected, OrchestrationStateEntity actual)
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

        IEnumerable<OrchestrationHistoryEventEntity> CreateHistoryEntities(TableClient client, string instanceId,
            string genId, int count)
        {
            var historyEntities = new List<OrchestrationHistoryEventEntity>();
            for (int i = 0; i < count; i++)
            {
                var eeStartedEvent = new ExecutionStartedEvent(-1, "EVENT_" + instanceId + "_" + genId + "_" + i);

                historyEntities.Add(new OrchestrationHistoryEventEntity(instanceId, genId, i, DateTime.Now,
                    eeStartedEvent));
            }
            client.WriteEntitesAsync(historyEntities).Wait();
            return historyEntities;
        }

        IEnumerable<OrchestrationStateEntity> CreateStateEntities(TableClient client, string instanceId, string genId)
        {
            var entities = new List<OrchestrationStateEntity>();
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

            entities.Add(new OrchestrationStateEntity(runtimeState));
            client.WriteEntitesAsync(entities).Wait();
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