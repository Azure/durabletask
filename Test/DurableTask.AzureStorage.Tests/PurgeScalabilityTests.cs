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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class PurgeScalabilityTests
    {
        const string ConnectionString = "UseDevelopmentStorage=true";
        const string InstancesTableName = "TestInstances";
        const string HistoryTableName = "TestHistory";

        /// <summary>
        /// Verifies that the DeleteHistoryAsync pipeline processes instances
        /// and that the storageRequests counter accumulates correctly.
        /// Previously there was a bug where rowsDeleted was counted as storageRequests.
        /// </summary>
        [TestMethod]
        public async Task DeleteHistoryAsync_ViaPublicApi_AccumulatesStatistics()
        {
            // Arrange - create a tracking store with mocked tables
            var (trackingStore, instancesTableClient, historyTableClient) = CreateTrackingStoreWithMockedTables();

            // Setup: Instances table query returns 2 instances across 1 page
            var instance1 = new OrchestrationInstanceStatus
            {
                PartitionKey = "instance1",
                RowKey = "",
                RuntimeStatus = "Completed",
                CreatedTime = DateTime.UtcNow.AddHours(-2),
            };
            var instance2 = new OrchestrationInstanceStatus
            {
                PartitionKey = "instance2",
                RowKey = "",
                RuntimeStatus = "Completed",
                CreatedTime = DateTime.UtcNow.AddHours(-1),
            };

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(
                        new List<OrchestrationInstanceStatus> { instance1, instance2 },
                        null,
                        new Mock<Response>().Object)
                }));

            // History table query: each instance has 5 history rows
            historyTableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(),
                    It.IsAny<int?>(),
                    It.IsAny<IEnumerable<string>>(),
                    It.IsAny<CancellationToken>()))
                .Returns((string filter, int? maxPerPage, IEnumerable<string>? select, CancellationToken ct) =>
                {
                    string pk = filter.Contains("instance1") ? "instance1" : "instance2";
                    var entities = Enumerable.Range(0, 5)
                        .Select(i => new TableEntity(pk, $"rk_{i}") { ETag = ETag.All })
                        .ToList();
                    return AsyncPageable<TableEntity>.FromPages(new[]
                    {
                        Page<TableEntity>.FromValues(entities, null, new Mock<Response>().Object)
                    });
                });

            // History batch delete succeeds
            historyTableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync((IEnumerable<TableTransactionAction> actions, CancellationToken _) =>
                {
                    int count = actions.Count();
                    var responses = Enumerable.Range(0, count).Select(_ => new Mock<Response>().Object).ToList();
                    return Response.FromValue<IReadOnlyList<Response>>(responses, new Mock<Response>().Object);
                });

            // Instances table delete succeeds
            instancesTableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Mock<Response>().Object);

            // Act
            PurgeHistoryResult result = await trackingStore.PurgeInstanceHistoryAsync(
                createdTimeFrom: DateTime.UtcNow.AddHours(-3),
                createdTimeTo: DateTime.UtcNow,
                runtimeStatus: new[] { OrchestrationStatus.Completed });

            // Assert
            Assert.AreEqual(2, result.InstancesDeleted);
            Assert.AreEqual(10, result.RowsDeleted); // 5 history rows per instance × 2 instances
        }

        /// <summary>
        /// Verifies that the purge pipeline limits concurrent instance purges
        /// to a bounded number (MaxPurgeInstanceConcurrency = 100).
        /// With only 10 instances, all should run concurrently but not exceed the limit.
        /// </summary>
        [TestMethod]
        public async Task DeleteHistoryAsync_RespectsMaxConcurrency()
        {
            // Arrange
            int concurrentCount = 0;
            int maxObservedConcurrency = 0;

            var (trackingStore, instancesTableClient, historyTableClient) = CreateTrackingStoreWithMockedTables();

            // 10 instances to delete
            var instances = Enumerable.Range(0, 10).Select(i =>
                new OrchestrationInstanceStatus
                {
                    PartitionKey = $"instance{i}",
                    RowKey = "",
                    RuntimeStatus = "Completed",
                    CreatedTime = DateTime.UtcNow.AddHours(-1),
                }).ToList();

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(instances, null, new Mock<Response>().Object)
                }));

            // History: empty for each instance (no rows to delete)
            historyTableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<TableEntity>.FromPages(new[]
                {
                    Page<TableEntity>.FromValues(new List<TableEntity>(), null, new Mock<Response>().Object)
                }));

            // Instances delete: track concurrency
            instancesTableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()))
                .Returns(async (string pk, string rk, ETag etag, CancellationToken ct) =>
                {
                    int current = Interlocked.Increment(ref concurrentCount);
                    int snapshot;
                    do
                    {
                        snapshot = Volatile.Read(ref maxObservedConcurrency);
                    }
                    while (current > snapshot && Interlocked.CompareExchange(ref maxObservedConcurrency, current, snapshot) != snapshot);

                    await Task.Delay(30); // Simulate latency
                    Interlocked.Decrement(ref concurrentCount);
                    return new Mock<Response>().Object;
                });

            // Act
            PurgeHistoryResult result = await trackingStore.PurgeInstanceHistoryAsync(
                createdTimeFrom: DateTime.UtcNow.AddHours(-3),
                createdTimeTo: DateTime.UtcNow,
                runtimeStatus: new[] { OrchestrationStatus.Completed });

            // Assert
            Assert.AreEqual(10, result.InstancesDeleted);
            // With 10 instances and MaxPurgeInstanceConcurrency=100, all 10 should be able to run concurrently
            Assert.IsTrue(
                maxObservedConcurrency <= 100,
                $"Max observed concurrency ({maxObservedConcurrency}) should not exceed MaxPurgeInstanceConcurrency (100)");
        }

        /// <summary>
        /// Verifies that single-instance purge removes the TimestampProperty from the projection columns.
        /// </summary>
        [TestMethod]
        public async Task PurgeInstanceHistory_ProjectsOnlyPKAndRK()
        {
            // Arrange
            var (trackingStore, instancesTableClient, historyTableClient) = CreateTrackingStoreWithMockedTables();

            // Setup instances table to return one instance
            var instance = new OrchestrationInstanceStatus
            {
                PartitionKey = "testInstance",
                RowKey = "",
                RuntimeStatus = "Completed",
            };

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(
                        new List<OrchestrationInstanceStatus> { instance }, null, new Mock<Response>().Object)
                }));

            // Capture the actual select columns passed to the history query
            IEnumerable<string>? capturedSelect = null;
            historyTableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Callback((string filter, int? maxPerPage, IEnumerable<string>? select, CancellationToken ct) =>
                {
                    capturedSelect = select;
                })
                .Returns(AsyncPageable<TableEntity>.FromPages(new[]
                {
                    Page<TableEntity>.FromValues(new List<TableEntity>(), null, new Mock<Response>().Object)
                }));

            instancesTableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Mock<Response>().Object);

            // Act
            await trackingStore.PurgeInstanceHistoryAsync("testInstance");

            // Assert: Projection should NOT include Timestamp
            Assert.IsNotNull(capturedSelect, "Select projection was not provided");
            var selectList = capturedSelect!.ToList();
            Assert.IsTrue(selectList.Contains("PartitionKey"), "Should project PartitionKey");
            Assert.IsTrue(selectList.Contains("RowKey"), "Should project RowKey");
            Assert.IsFalse(selectList.Contains("Timestamp"), "Should NOT project Timestamp");
        }

        /// <summary>
        /// Verifies that a single-instance purge deletes instance from instances table.
        /// </summary>
        [TestMethod]
        public async Task PurgeInstanceHistory_ByInstanceId_DeletesInstance()
        {
            // Arrange
            var (trackingStore, instancesTableClient, historyTableClient) = CreateTrackingStoreWithMockedTables();

            var instance = new OrchestrationInstanceStatus
            {
                PartitionKey = "myInstance",
                RowKey = "",
                RuntimeStatus = "Completed",
            };

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(
                        new List<OrchestrationInstanceStatus> { instance }, null, new Mock<Response>().Object)
                }));

            historyTableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<TableEntity>.FromPages(new[]
                {
                    Page<TableEntity>.FromValues(new List<TableEntity>(), null, new Mock<Response>().Object)
                }));

            instancesTableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Mock<Response>().Object);

            // Act
            PurgeHistoryResult result = await trackingStore.PurgeInstanceHistoryAsync("myInstance");

            // Assert
            Assert.AreEqual(1, result.InstancesDeleted);
            instancesTableClient.Verify(
                t => t.DeleteEntityAsync("myInstance", string.Empty, ETag.All, It.IsAny<CancellationToken>()),
                Times.Once);
        }

        /// <summary>
        /// Verifies that purging a non-existent instance returns zero.
        /// </summary>
        [TestMethod]
        public async Task PurgeInstanceHistory_InstanceNotFound_ReturnsZero()
        {
            // Arrange
            var (trackingStore, instancesTableClient, _) = CreateTrackingStoreWithMockedTables();

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(
                        new List<OrchestrationInstanceStatus>(), null, new Mock<Response>().Object)
                }));

            // Act
            PurgeHistoryResult result = await trackingStore.PurgeInstanceHistoryAsync("nonexistent");

            // Assert
            Assert.AreEqual(0, result.InstancesDeleted);
            Assert.AreEqual(0, result.RowsDeleted);
        }

        /// <summary>
        /// Verifies that history batch delete uses parallel execution (via DeleteBatchParallelAsync).
        /// </summary>
        [TestMethod]
        public async Task PurgeInstanceHistory_WithManyHistoryRows_UsesParallelBatchDelete()
        {
            // Arrange
            var (trackingStore, instancesTableClient, historyTableClient) = CreateTrackingStoreWithMockedTables();

            var instance = new OrchestrationInstanceStatus
            {
                PartitionKey = "testPK",
                RowKey = "",
                RuntimeStatus = "Completed",
            };

            instancesTableClient
                .Setup(t => t.QueryAsync<OrchestrationInstanceStatus>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<OrchestrationInstanceStatus>.FromPages(new[]
                {
                    Page<OrchestrationInstanceStatus>.FromValues(
                        new List<OrchestrationInstanceStatus> { instance }, null, new Mock<Response>().Object)
                }));

            // Return 250 history entities (which should be split into 3 batches: 100+100+50)
            var historyEntities = Enumerable.Range(0, 250)
                .Select(i => new TableEntity("testPK", $"rk_{i:D5}") { ETag = ETag.All })
                .ToList();

            historyTableClient
                .Setup(t => t.QueryAsync<TableEntity>(
                    It.IsAny<string>(), It.IsAny<int?>(), It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
                .Returns(AsyncPageable<TableEntity>.FromPages(new[]
                {
                    Page<TableEntity>.FromValues(historyEntities, null, new Mock<Response>().Object)
                }));

            // Track batch deletes
            int batchDeleteCallCount = 0;
            historyTableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync((IEnumerable<TableTransactionAction> actions, CancellationToken _) =>
                {
                    Interlocked.Increment(ref batchDeleteCallCount);
                    int count = actions.Count();
                    var responses = Enumerable.Range(0, count).Select(_ => new Mock<Response>().Object).ToList();
                    return Response.FromValue<IReadOnlyList<Response>>(responses, new Mock<Response>().Object);
                });

            instancesTableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new Mock<Response>().Object);

            // Act
            PurgeHistoryResult result = await trackingStore.PurgeInstanceHistoryAsync("testPK");

            // Assert
            Assert.AreEqual(1, result.InstancesDeleted);
            Assert.AreEqual(250, result.RowsDeleted);
            // Should have made 3 batch delete calls (100 + 100 + 50)
            Assert.AreEqual(3, batchDeleteCallCount, "Expected 3 batch transactions for 250 entities");
        }

        #region Helper Methods

        static (AzureTableTrackingStore trackingStore, Mock<TableClient> instancesTableClient, Mock<TableClient> historyTableClient)
            CreateTrackingStoreWithMockedTables(Action<AzureStorageOrchestrationServiceSettings>? modifySettings = null)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
                FetchLargeMessageDataEnabled = false,
            };

            modifySettings?.Invoke(settings);

            var azureStorageClient = new AzureStorageClient(settings);

            // Create mocked instances table
            var instancesServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var instancesTableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, InstancesTableName);
            instancesTableClient.Setup(t => t.Name).Returns(InstancesTableName);
            instancesServiceClient.Setup(t => t.GetTableClient(InstancesTableName)).Returns(instancesTableClient.Object);
            var instancesTable = new Table(azureStorageClient, instancesServiceClient.Object, InstancesTableName);

            // Create mocked history table
            var historyServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            var historyTableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, HistoryTableName);
            historyTableClient.Setup(t => t.Name).Returns(HistoryTableName);
            historyServiceClient.Setup(t => t.GetTableClient(HistoryTableName)).Returns(historyTableClient.Object);
            var historyTable = new Table(azureStorageClient, historyServiceClient.Object, HistoryTableName);

            // Create mock message manager that returns 1 storage operation (no blobs to delete)
            var messageManager = new Mock<MessageManager>(
                settings, azureStorageClient, "test-largemessages") { CallBase = false };
            messageManager
                .Setup(m => m.DeleteLargeMessageBlobs(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(1);

            // Create tracking store using the internal test constructor
            var trackingStore = new AzureTableTrackingStore(
                azureStorageClient,
                historyTable,
                instancesTable,
                messageManager.Object);

            return (trackingStore, instancesTableClient, historyTableClient);
        }

        #endregion
    }
}