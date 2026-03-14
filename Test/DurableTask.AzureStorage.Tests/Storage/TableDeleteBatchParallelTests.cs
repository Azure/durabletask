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
namespace DurableTask.AzureStorage.Tests.Storage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class TableDeleteBatchParallelTests
    {
        const string ConnectionString = "UseDevelopmentStorage=true";
        const string TableName = "TestTable";

        [TestMethod]
        public async Task DeleteBatchParallelAsync_EmptyBatch_ReturnsEmptyResults()
        {
            Table table = CreateTableWithMockedClient(out _, out _);
            var entities = new List<TableEntity>();

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(0, results.Responses.Count);
            Assert.AreEqual(0, results.RequestCount);
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_SingleBatch_SubmitsOneTransaction()
        {
            var entities = CreateTestEntities("pk", count: 50);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.Is<IEnumerable<TableTransactionAction>>(a => a.Count() == 50),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateMockBatchResponse(50));

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(50, results.Responses.Count);
            tableClient.Verify(
                t => t.SubmitTransactionAsync(It.IsAny<IEnumerable<TableTransactionAction>>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_MultipleBatches_SplitsIntoChunksOf100()
        {
            var entities = CreateTestEntities("pk", count: 250);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync((IEnumerable<TableTransactionAction> batch, CancellationToken _) =>
                    CreateMockBatchResponse(batch.Count()));

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(250, results.Responses.Count);
            tableClient.Verify(
                t => t.SubmitTransactionAsync(It.IsAny<IEnumerable<TableTransactionAction>>(), It.IsAny<CancellationToken>()),
                Times.Exactly(3));
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_SubmitsBatchesConcurrently()
        {
            var entities = CreateTestEntities("pk", count: 500); // 5 batches of 100
            int concurrentCount = 0;
            int maxConcurrent = 0;

            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async (IEnumerable<TableTransactionAction> batch, CancellationToken _) =>
                {
                    int current = Interlocked.Increment(ref concurrentCount);
                    int snapshot;
                    do
                    {
                        snapshot = Volatile.Read(ref maxConcurrent);
                    }
                    while (current > snapshot && Interlocked.CompareExchange(ref maxConcurrent, current, snapshot) != snapshot);

                    await Task.Delay(50);
                    Interlocked.Decrement(ref concurrentCount);

                    return CreateMockBatchResponse(batch.Count());
                });

            await table.DeleteBatchParallelAsync(entities);

            // All 5 batches should run concurrently since there's no internal semaphore
            Assert.IsTrue(
                maxConcurrent > 1,
                $"Expected concurrent execution, but max concurrent was {maxConcurrent}");
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_BatchFails404_FallsBackToIndividualDeletes()
        {
            var entities = CreateTestEntities("pk", count: 3);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new RequestFailedException(404, "Entity not found"));

            var mockResponse = new Mock<Response>();
            tableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<ETag>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockResponse.Object);

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(3, results.Responses.Count);
            tableClient.Verify(
                t => t.DeleteEntityAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<ETag>(), It.IsAny<CancellationToken>()),
                Times.Exactly(3));
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_IndividualDeleteSkips404()
        {
            var entities = CreateTestEntities("pk", count: 3);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new RequestFailedException(404, "Entity not found"));

            int callCount = 0;
            var mockResponse = new Mock<Response>();
            tableClient
                .Setup(t => t.DeleteEntityAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<ETag>(),
                    It.IsAny<CancellationToken>()))
                .Returns((string pk, string rk, ETag ifMatch, CancellationToken ct) =>
                {
                    int call = Interlocked.Increment(ref callCount);
                    if (call == 2)
                    {
                        throw new RequestFailedException(404, "Entity already deleted");
                    }
                    return Task.FromResult(mockResponse.Object);
                });

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(2, results.Responses.Count);
            Assert.AreEqual(3, results.RequestCount);
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_ExactlyOneBatch_NoBoundaryIssues()
        {
            var entities = CreateTestEntities("pk", count: 100);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.Is<IEnumerable<TableTransactionAction>>(a => a.Count() == 100),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(CreateMockBatchResponse(100));

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(100, results.Responses.Count);
            tableClient.Verify(
                t => t.SubmitTransactionAsync(It.IsAny<IEnumerable<TableTransactionAction>>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_101Entities_CreatesTwoBatches()
        {
            var entities = CreateTestEntities("pk", count: 101);
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync((IEnumerable<TableTransactionAction> batch, CancellationToken _) =>
                    CreateMockBatchResponse(batch.Count()));

            TableTransactionResults results = await table.DeleteBatchParallelAsync(entities);

            Assert.AreEqual(101, results.Responses.Count);
            tableClient.Verify(
                t => t.SubmitTransactionAsync(It.IsAny<IEnumerable<TableTransactionAction>>(), It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [TestMethod]
        public async Task DeleteBatchParallelAsync_CancellationToken_IsPropagated()
        {
            var entities = CreateTestEntities("pk", count: 200);
            using var cts = new CancellationTokenSource();
            Table table = CreateTableWithMockedClient(out _, out Mock<TableClient> tableClient);

            int batchesSubmitted = 0;
            tableClient
                .Setup(t => t.SubmitTransactionAsync(
                    It.IsAny<IEnumerable<TableTransactionAction>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async (IEnumerable<TableTransactionAction> batch, CancellationToken ct) =>
                {
                    int count = Interlocked.Increment(ref batchesSubmitted);
                    if (count == 1)
                    {
                        cts.Cancel();
                    }
                    ct.ThrowIfCancellationRequested();
                    return CreateMockBatchResponse(batch.Count());
                });

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => table.DeleteBatchParallelAsync(entities, cts.Token));
        }

        #region Helper Methods

        static Table CreateTableWithMockedClient(
            out Mock<TableServiceClient> tableServiceClient,
            out Mock<TableClient> tableClient)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(ConnectionString),
            };

            var azureStorageClient = new AzureStorageClient(settings);

            tableServiceClient = new Mock<TableServiceClient>(MockBehavior.Strict, ConnectionString);
            tableClient = new Mock<TableClient>(MockBehavior.Loose, ConnectionString, TableName);
            tableClient.Setup(t => t.Name).Returns(TableName);
            tableServiceClient.Setup(t => t.GetTableClient(TableName)).Returns(tableClient.Object);

            return new Table(azureStorageClient, tableServiceClient.Object, TableName);
        }

        static List<TableEntity> CreateTestEntities(string partitionKey, int count)
        {
            var entities = new List<TableEntity>(count);
            for (int i = 0; i < count; i++)
            {
                entities.Add(new TableEntity(partitionKey, $"rk_{i:D5}")
                {
                    ETag = ETag.All,
                });
            }
            return entities;
        }

        static Response<IReadOnlyList<Response>> CreateMockBatchResponse(int count)
        {
            var responses = new List<Response>();
            for (int i = 0; i < count; i++)
            {
                responses.Add(new Mock<Response>().Object);
            }
            return Response.FromValue<IReadOnlyList<Response>>(responses, new Mock<Response>().Object);
        }

        #endregion
    }
}
