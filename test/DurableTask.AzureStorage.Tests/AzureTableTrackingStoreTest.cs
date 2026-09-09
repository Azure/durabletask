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
        public async Task UpdateStatusForRewind_ReplacesFullEntityUsingCurrentEtag()
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            const string PreservedProperty = "preserved";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };

            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var storedEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("current-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Failed.ToString(),
                ["Output"] = "stale output",
                ["PreservedProperty"] = PreservedProperty,
            };
            tableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageable<TableEntity>.FromPages(
                    new[]
                    {
                        Page<TableEntity>.FromValues(
                            new[] { storedEntity },
                            continuationToken: null,
                            new Mock<Response>().Object),
                    }));

            TableEntity replacedEntity = null;
            ETag replaceEtag = default;
            TableUpdateMode updateMode = default;
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    It.IsAny<ETag>(),
                    It.IsAny<TableUpdateMode>(),
                    tokenSource.Token))
                .Callback<TableEntity, ETag, TableUpdateMode, CancellationToken>((entity, etag, mode, _) =>
                {
                    replacedEntity = entity;
                    replaceEtag = etag;
                    updateMode = mode;
                })
                .ReturnsAsync(new Mock<Response>().Object);

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await trackingStore.UpdateStatusForRewindAsync(
                InstanceId,
                ExecutionId,
                storedEntity.ETag,
                tokenSource.Token);

            Assert.AreEqual(TableUpdateMode.Replace, updateMode);
            Assert.AreEqual(storedEntity.ETag, replaceEtag);
            Assert.AreEqual(PreservedProperty, replacedEntity["PreservedProperty"]);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), replacedEntity["RuntimeStatus"]);
            Assert.IsFalse(replacedEntity.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Pending)]
        [DataRow(OrchestrationStatus.Running)]
        [DataRow(OrchestrationStatus.Suspended)]
        public async Task UpdateStatusForRewind_ResetsUnchangedPreFailureProjection(
            OrchestrationStatus currentStatus)
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("rewind-start-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = currentStatus.ToString(),
            };
            if (currentStatus != OrchestrationStatus.Pending)
            {
                currentEntity["Output"] = "pre-failure output";
            }

            tableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(currentEntity));

            TableEntity replacedEntity = null;
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    currentEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token))
                .Callback<TableEntity, ETag, TableUpdateMode, CancellationToken>(
                    (entity, _, _, _) => replacedEntity = entity)
                .ReturnsAsync(new Mock<Response>().Object);

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await trackingStore.UpdateStatusForRewindAsync(
                InstanceId,
                ExecutionId,
                currentEntity.ETag,
                tokenSource.Token);

            Assert.IsNotNull(replacedEntity);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), replacedEntity["RuntimeStatus"]);
            Assert.IsFalse(replacedEntity.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Pending, true, false)]
        [DataRow(OrchestrationStatus.Running, true, true)]
        [DataRow(OrchestrationStatus.Suspended, true, true)]
        [DataRow(OrchestrationStatus.Completed, true, true)]
        [DataRow(OrchestrationStatus.Completed, false, true)]
        [DataRow(OrchestrationStatus.ContinuedAsNew, true, true)]
        [DataRow(OrchestrationStatus.ContinuedAsNew, false, true)]
        [DataRow(OrchestrationStatus.Canceled, true, true)]
        [DataRow(OrchestrationStatus.Canceled, false, true)]
        [DataRow(OrchestrationStatus.Terminated, true, true)]
        [DataRow(OrchestrationStatus.Terminated, false, true)]
        public async Task UpdateStatusForRewind_PreservesAdvancedState(
            OrchestrationStatus currentStatus,
            bool changedSinceRewindStarted,
            bool hasOutput)
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            var rewindStartETag = new ETag("rewind-start-etag");
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = changedSinceRewindStarted ? new ETag("advanced-etag") : rewindStartETag,
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = currentStatus.ToString(),
            };
            if (hasOutput)
            {
                currentEntity["Output"] = "new output";
            }

            tableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(currentEntity));

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await trackingStore.UpdateStatusForRewindAsync(
                InstanceId,
                ExecutionId,
                rewindStartETag,
                tokenSource.Token);

            Assert.AreEqual(hasOutput, currentEntity.ContainsKey("Output"));
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    It.IsAny<ETag>(),
                    It.IsAny<TableUpdateMode>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Pending)]
        public async Task UpdateStatusForRewind_ResetsChangedNonEquivalentProjection(
            OrchestrationStatus currentStatus)
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            var rewindStartETag = new ETag("rewind-start-etag");
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("changed-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = currentStatus.ToString(),
                ["Output"] = "concurrent output",
            };
            tableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(currentEntity));
            TableEntity replacedEntity = null;
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    currentEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token))
                .Callback<TableEntity, ETag, TableUpdateMode, CancellationToken>(
                    (entity, _, _, _) => replacedEntity = entity)
                .ReturnsAsync(new Mock<Response>().Object);

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await trackingStore.UpdateStatusForRewindAsync(
                InstanceId,
                ExecutionId,
                rewindStartETag,
                tokenSource.Token);

            Assert.IsNotNull(replacedEntity);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), replacedEntity["RuntimeStatus"]);
            Assert.IsFalse(replacedEntity.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Pending)]
        public async Task UpdateStatusForRewind_PropagatesEtagConflict(
            OrchestrationStatus currentStatus)
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };

            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var staleEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("stale-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Failed.ToString(),
                ["Output"] = "stale output",
            };
            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("current-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = currentStatus.ToString(),
                ["Output"] = "current failure output",
            };
            tableClient
                .SetupSequence(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(staleEntity))
                .Returns(AsyncPageableFromEntity(currentEntity));
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    staleEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token))
                .ThrowsAsync(new RequestFailedException(412, "The entity changed."));

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                () => trackingStore.UpdateStatusForRewindAsync(
                    InstanceId,
                    ExecutionId,
                    staleEntity.ETag,
                    tokenSource.Token));
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    staleEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token),
                Times.Once);
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    ETag.All,
                    It.IsAny<TableUpdateMode>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_RejectsDifferentExecutionBeforeWrite()
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExpectedExecutionId = "execution-1";
            const string CurrentExecutionId = "execution-2";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("current-etag"),
                ["ExecutionId"] = CurrentExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Completed.ToString(),
                ["Output"] = "new output",
            };
            tableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(currentEntity));

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            DurableTaskStorageException conflict =
                await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                    () => trackingStore.UpdateStatusForRewindAsync(
                        InstanceId,
                        ExpectedExecutionId,
                        new ETag("rewind-start-etag"),
                        tokenSource.Token));

            StringAssert.Contains(conflict.Message, ExpectedExecutionId);
            StringAssert.Contains(conflict.Message, CurrentExecutionId);
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    It.IsAny<ETag>(),
                    It.IsAny<TableUpdateMode>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Pending, false)]
        [DataRow(OrchestrationStatus.Running, true)]
        [DataRow(OrchestrationStatus.Completed, true)]
        public async Task UpdateStatusForRewind_AcceptsEquivalentEtagConflict(
            OrchestrationStatus currentStatus,
            bool hasNewOutput)
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExecutionId = "execution-1";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var staleEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("stale-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Failed.ToString(),
                ["Output"] = "stale output",
            };
            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("current-etag"),
                ["ExecutionId"] = ExecutionId,
                ["RuntimeStatus"] = currentStatus.ToString(),
            };
            if (hasNewOutput)
            {
                currentEntity["Output"] = "new output";
            }

            tableClient
                .SetupSequence(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(staleEntity))
                .Returns(AsyncPageableFromEntity(currentEntity));
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    staleEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token))
                .ThrowsAsync(new RequestFailedException(412, "The entity changed."));

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            await trackingStore.UpdateStatusForRewindAsync(
                InstanceId,
                ExecutionId,
                staleEntity.ETag,
                tokenSource.Token);

            Assert.AreEqual(hasNewOutput, currentEntity.ContainsKey("Output"));
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    It.IsAny<ETag>(),
                    It.IsAny<TableUpdateMode>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_RejectsExecutionChangeAfterEtagConflict()
        {
            const string TableName = "MockTable";
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string InstanceId = "rewind-instance";
            const string ExpectedExecutionId = "execution-1";
            const string CurrentExecutionId = "execution-2";
            using var tokenSource = new CancellationTokenSource();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };
            var azureStorageClient = new AzureStorageClient(settings);
            var tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            var staleEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("stale-etag"),
                ["ExecutionId"] = ExpectedExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Failed.ToString(),
                ["Output"] = "stale output",
            };
            var currentEntity = new TableEntity(InstanceId, string.Empty)
            {
                ETag = new ETag("current-etag"),
                ["ExecutionId"] = CurrentExecutionId,
                ["RuntimeStatus"] = OrchestrationStatus.Completed.ToString(),
                ["Output"] = "new output",
            };
            tableClient
                .SetupSequence(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    tokenSource.Token))
                .Returns(AsyncPageableFromEntity(staleEntity))
                .Returns(AsyncPageableFromEntity(currentEntity));
            tableClient
                .Setup(t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    staleEntity.ETag,
                    TableUpdateMode.Replace,
                    tokenSource.Token))
                .ThrowsAsync(new RequestFailedException(412, "The entity changed."));

            var table = new Table(azureStorageClient, tableServiceClient.Object, TableName);
            var trackingStore = new AzureTableTrackingStore(new AzureStorageOrchestrationServiceStats(), table);

            DurableTaskStorageException conflict =
                await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                    () => trackingStore.UpdateStatusForRewindAsync(
                        InstanceId,
                        ExpectedExecutionId,
                        staleEntity.ETag,
                        tokenSource.Token));

            StringAssert.Contains(conflict.Message, ExpectedExecutionId);
            StringAssert.Contains(conflict.Message, CurrentExecutionId);
            Assert.AreEqual("new output", currentEntity["Output"]);
            tableClient.Verify(
                t => t.UpdateEntityAsync(
                    It.IsAny<TableEntity>(),
                    It.IsAny<ETag>(),
                    It.IsAny<TableUpdateMode>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
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

        static AsyncPageable<TableEntity> AsyncPageableFromEntity(TableEntity entity)
        {
            return AsyncPageable<TableEntity>.FromPages(
                new[]
                {
                    Page<TableEntity>.FromValues(
                        new[] { entity },
                        continuationToken: null,
                        new Mock<Response>().Object),
                });
        }
    }
}
