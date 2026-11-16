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
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class AzureTableTrackingStoreTest
    {
        [TestMethod]
        public async Task QueryStatus_WithContinuationToken_NoInputToken()
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";

            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };

            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Strict, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new AzureTableTrackingStore(stats, table);

            DateTime expectedCreatedDateFrom = DateTime.UtcNow;
            DateTime expectedCreatedDateTo = expectedCreatedDateFrom.AddHours(1);
            var inputState = new List<OrchestrationStatus>
            {
                OrchestrationStatus.Running,
                OrchestrationStatus.Completed,
                OrchestrationStatus.Failed,
            };
            var expected = new List<OrchestrationInstanceStatus>
            {
                new OrchestrationInstanceStatus
                {
                    PartitionKey = "child",
                    ParentInstanceId = "parent",
                    Name = "foo",
                    RuntimeStatus = "Running"
                },
                new OrchestrationInstanceStatus
                {
                    PartitionKey = "top-level",
                    ParentInstanceId = "",
                    Name = "bar",
                    RuntimeStatus = "Completed"
                },
                new OrchestrationInstanceStatus
                {
                    PartitionKey = "legacy",
                    Name = "baz",
                    RuntimeStatus = "Failed"
                }
            };

            string expectedFilter = string.Format(
                CultureInfo.InvariantCulture,
                "({0} ge datetime'{1:O}') and ({0} le datetime'{2:O}') and ({3} eq '{4:G}' or {3} eq '{5:G}' or {3} eq '{6:G}')",
                nameof(OrchestrationInstanceStatus.CreatedTime),
                expectedCreatedDateFrom,
                expectedCreatedDateTo,
                nameof(OrchestrationInstanceStatus.RuntimeStatus),
                OrchestrationStatus.Running,
                OrchestrationStatus.Completed,
                OrchestrationStatus.Failed);

            tableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(expectedFilter, null, null, tokenSource.Token))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(
                    new[]
                    {
                        Page<OrchestrationInstanceStatus>.FromValues(expected, null, new Mock<Response>().Object)
                    }));

            // .ExecuteQueryAsync<OrchestrationInstanceStatus>(filter, select, cancellationToken)
            var actual = await trackingStore
                .GetStateAsync(expectedCreatedDateFrom, expectedCreatedDateTo, inputState, tokenSource.Token)
                .ToListAsync();

            Assert.AreEqual(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.AreEqual(expected[i].Name, actual[i].Name);
                Assert.AreEqual(Enum.Parse(typeof(OrchestrationStatus), expected[i].RuntimeStatus), actual[i].OrchestrationStatus);
            }

            // Child rows resolve to their parent; rows written for a top-level orchestration store an
            // empty value to clear any stale parent, and legacy rows omit the property entirely. The
            // latter two must both surface as a null ParentInstance.
            Assert.AreEqual("parent", actual[0].ParentInstance.OrchestrationInstance.InstanceId);
            Assert.IsNull(actual[1].ParentInstance);
            Assert.IsNull(actual[2].ParentInstance);
        }

        [TestMethod]
        public async Task InstanceStoreBackedTrackingStore_PersistsParentOnCreation()
        {
            const string ParentInstanceId = "parent";
            OrchestrationStateInstanceEntity writtenState = null;
            var instanceStore = new Mock<IOrchestrationServiceInstanceStore>(MockBehavior.Strict);
            instanceStore
                .Setup(store => store.WriteEntitiesAsync(It.IsAny<IEnumerable<InstanceEntityBase>>()))
                .Callback<IEnumerable<InstanceEntityBase>>(entities => writtenState = entities.Single() as OrchestrationStateInstanceEntity)
                .ReturnsAsync(new object());

            var trackingStore = new InstanceStoreBackedTrackingStore(instanceStore.Object);
            var startedEvent = new ExecutionStartedEvent(0, null)
            {
                Name = "child",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "child",
                    ExecutionId = "execution",
                },
                ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = ParentInstanceId,
                        ExecutionId = "parent-execution",
                    },
                },
            };

            bool created = await trackingStore.SetNewExecutionAsync(startedEvent, null, null);

            Assert.IsTrue(created);
            Assert.IsNotNull(writtenState);
            Assert.AreSame(startedEvent.ParentInstance, writtenState.State.ParentInstance);
            Assert.AreEqual(ParentInstanceId, writtenState.State.ParentInstance.OrchestrationInstance.InstanceId);
        }

        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ActivityStartedTime_PersistsInHistoryAndHistoryJson(bool failed)
        {
            const string InstanceId = "instance";
            const string ExecutionId = "execution";
            DateTime scheduledTime = new DateTime(2026, 1, 2, 12, 0, 0, DateTimeKind.Utc);
            DateTime startedTime = scheduledTime.AddMinutes(12);
            DateTime completedTime = scheduledTime.AddMinutes(15);
            var settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = "started" + Guid.NewGuid().ToString("N");
            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName.ToLowerInvariant() + "-largemessages");
            var trackingStore = new AzureTableTrackingStore(storageClient, messageManager);
            using var service = new AzureStorageOrchestrationService(settings);
            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(new ExecutionStartedEvent(0, null)
            {
                Name = "orchestration",
                OrchestrationInstance = new OrchestrationInstance { InstanceId = InstanceId, ExecutionId = ExecutionId },
                Timestamp = scheduledTime,
            });
            runtimeState.AddEvent(new TaskScheduledEvent(1, "activity", "v1") { Timestamp = scheduledTime });
            HistoryEvent response = failed
                ? new TaskFailedEvent(2, 1, "failure", "details") { StartedTime = startedTime, Timestamp = completedTime }
                : new TaskCompletedEvent(2, 1, "result") { StartedTime = startedTime, Timestamp = completedTime };
            runtimeState.AddEvent(response);
            runtimeState.AddEvent(new TaskScheduledEvent(3, "legacy", "v1") { Timestamp = scheduledTime });
            runtimeState.AddEvent(new TaskCompletedEvent(4, 3, "legacy") { Timestamp = completedTime });

            try
            {
                await trackingStore.CreateAsync();
                OrchestrationHistory empty = await trackingStore.GetHistoryEventsAsync(InstanceId, ExecutionId);
                await trackingStore.UpdateStateAsync(
                    runtimeState, runtimeState, InstanceId, ExecutionId, new OrchestrationETags(), empty.TrackingStoreContext);

                var tableClient = new TableServiceClient(TestHelpers.GetTestStorageAccountConnectionString())
                    .GetTableClient(settings.TaskHubName + "History");
                TableEntity row = (await tableClient.GetEntityAsync<TableEntity>(InstanceId, "0000000000000002")).Value;
                Assert.AreEqual(startedTime, row.GetDateTime("StartedTime"));
                Assert.AreEqual(completedTime, row.GetDateTime("_Timestamp"));
                TableEntity legacyRow = (await tableClient.GetEntityAsync<TableEntity>(InstanceId, "0000000000000004")).Value;
                Assert.IsFalse(legacyRow.ContainsKey("StartedTime"));

                OrchestrationHistory history = await trackingStore.GetHistoryEventsAsync(InstanceId, ExecutionId);
                HistoryEvent actual = history.Events[2];
                Assert.AreEqual(response.EventType, actual.EventType);
                Assert.AreEqual(startedTime, actual is TaskFailedEvent failure ? failure.StartedTime : ((TaskCompletedEvent)actual).StartedTime);
                Assert.IsNull(((TaskCompletedEvent)history.Events[4]).StartedTime);

                JArray json = JArray.Parse(await service.GetOrchestrationHistoryAsync(InstanceId, ExecutionId));
                Assert.AreEqual(scheduledTime, (DateTime)json[1]["Timestamp"]);
                Assert.AreEqual(startedTime, (DateTime)json[2]["StartedTime"]);
                Assert.AreEqual(completedTime, (DateTime)json[2]["Timestamp"]);
                Assert.AreEqual(1, (int)json[2]["TaskScheduledId"]);
                Assert.IsNull(json[4]["StartedTime"]);
            }
            finally
            {
                await trackingStore.DeleteAsync();
            }
        }
    }
}
