// Copyright Microsoft Corporation
// Licensed under the Apache License, Version 2.0.

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Specialized;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public class ClientPartitionTests
    {
        readonly string connection = TestHelpers.GetTestStorageAccountConnectionString();

        [DataTestMethod]
        [DataRow(16, 4, "Table")]
        [DataRow(4, 16, "Table")]
        [DataRow(4, 4, "Table")]
        [DataRow(16, 4, "Safe")]
        [DataRow(4, 16, "Safe")]
        [DataRow(4, 4, "Safe")]
        [DataRow(16, 4, "Legacy")]
        [DataRow(4, 16, "Legacy")]
        [DataRow(4, 4, "Legacy")]
        public async Task ExistingHubClientPreservesTopologyAndRouting(int clientPartitions, int targetPartitions, string manager)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, targetPartitions, manager));
            var clientSettings = this.Settings(hub, clientPartitions, manager);
            using var service = new AzureStorageOrchestrationService(clientSettings);
            try
            {
                await target.CreateIfNotExistsAsync();
                string[] before = await this.QueueNamesAsync(hub);
                Assert.AreEqual(targetPartitions, before.Length);
                Assert.AreEqual(targetPartitions, await this.PartitionCountAsync(hub, manager));

                var client = new TaskHubClient(service);
                await client.CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                await client.RaiseEventAsync(new OrchestrationInstance { InstanceId = "partition-probe" }, "Probe", "payload");

                CollectionAssert.AreEqual(before, await this.QueueNamesAsync(hub));
                Assert.AreEqual(targetPartitions, await this.PartitionCountAsync(hub, manager));
                Assert.AreEqual(clientPartitions, clientSettings.PartitionCount, "Client discovery must not mutate caller settings.");

                string expectedQueue = AzureStorageOrchestrationService.GetControlQueueName(
                    hub, (int)(Fnv1aHashHelper.ComputeHash("partition-probe") % targetPartitions));
                var queues = new QueueServiceClient(this.connection);
                foreach (string queueName in before)
                {
                    int messages = (await queues.GetQueueClient(queueName).PeekMessagesAsync(2)).Value.Length;
                    Assert.AreEqual(queueName == expectedQueue ? 2 : 0, messages, queueName);
                }
            }
            finally
            {
                await this.CleanupAsync(hub, manager);
            }
        }

        [DataTestMethod]
        [DataRow("Table")]
        [DataRow("Safe")]
        [DataRow("Legacy")]
        public async Task ConcurrentClientQueriesDoNotReconfigureExistingHub(string manager)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, manager));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, manager));
            try
            {
                await target.CreateIfNotExistsAsync();
                var client = new TaskHubClient(service);
                await Task.WhenAll(Enumerable.Range(0, 8).Select(i => client.GetOrchestrationStateAsync("missing" + i)));
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);
                Assert.AreEqual(4, await this.PartitionCountAsync(hub, manager));
            }
            finally
            {
                await this.CleanupAsync(hub, manager);
            }
        }

        [DataTestMethod]
        [DataRow(4, "Table")]
        [DataRow(16, "Table")]
        [DataRow(4, "Safe")]
        [DataRow(16, "Safe")]
        [DataRow(4, "Legacy")]
        [DataRow(16, "Legacy")]
        public async Task ClientCreatesMissingHubAndCanRecreateAfterDelete(int partitions, string manager)
        {
            string hub = NewHub();
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, partitions, manager));
            var client = new TaskHubClient(service);
            try
            {
                Assert.AreEqual(0, (await this.QueueNamesAsync(hub)).Length);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    await client.CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                    Assert.AreEqual(partitions, (await this.QueueNamesAsync(hub)).Length);
                    Assert.AreEqual(partitions, await this.PartitionCountAsync(hub, manager));
                    await service.DeleteAsync();
                }
            }
            finally
            {
                await this.CleanupAsync(hub, manager);
            }
        }

        [DataTestMethod]
        [DataRow("Table")]
        [DataRow("Safe")]
        [DataRow("Legacy")]
        public async Task ExplicitCreationRetainsConfiguredWorkerPartitions(string manager)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, manager));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, manager));
            try
            {
                await target.CreateIfNotExistsAsync();
                var client = new TaskHubClient(service);
                await client.GetOrchestrationStateAsync("missing");
                await service.CreateIfNotExistsAsync();
                Assert.AreEqual(16, (await this.QueueNamesAsync(hub)).Length);
                Assert.AreEqual(16, await this.PartitionCountAsync(hub, manager));
                await client.CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                var queue = new QueueClient(this.connection, AzureStorageOrchestrationService.GetControlQueueName(hub, 14));
                Assert.AreEqual(1, (await queue.PeekMessagesAsync(1)).Value.Length);
            }
            finally
            {
                await this.CleanupAsync(hub, manager);
            }
        }

        [DataTestMethod]
        [DataRow("Table")]
        [DataRow("Safe")]
        [DataRow("Legacy")]
        public async Task ConcurrentClientsCreateMissingHub(string manager)
        {
            string hub = NewHub();
            var services = Enumerable.Range(0, 8)
                .Select(_ => new AzureStorageOrchestrationService(this.Settings(hub, 16, manager)))
                .ToArray();
            try
            {
                await Task.WhenAll(services.Select((service, i) => new TaskHubClient(service)
                    .CreateOrchestrationInstanceAsync("Probe", string.Empty, "probe" + i, "hello")));
                Assert.AreEqual(16, (await this.QueueNamesAsync(hub)).Length);
                Assert.AreEqual(16, await this.PartitionCountAsync(hub, manager));
                var expected = Enumerable.Range(0, 8)
                    .GroupBy(i => Fnv1aHashHelper.ComputeHash("probe" + i) % 16)
                    .ToDictionary(g => (int)g.Key, g => g.Count());
                var queues = new QueueServiceClient(this.connection);
                for (int i = 0; i < 16; i++)
                {
                    var queue = queues.GetQueueClient(AzureStorageOrchestrationService.GetControlQueueName(hub, i));
                    Assert.AreEqual(expected.TryGetValue(i, out int count) ? count : 0,
                        (await queue.PeekMessagesAsync(8)).Value.Length, $"partition {i}");
                }
            }
            finally
            {
                foreach (var service in services)
                {
                    service.Dispose();
                }
                await this.CleanupAsync(hub, manager);
            }
        }

        [DataTestMethod]
        [DataRow("null")]
        [DataRow("{broken")]
        [DataRow("{\"TaskHubName\":\"wrong\",\"PartitionCount\":4}")]
        [DataRow("{\"PartitionCount\":0}")]
        [DataRow("{\"PartitionCount\":17}")]
        public async Task InvalidWorkerMetadataIsNotReplacedAndCanBeRetried(string metadata)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, "Table"));
            try
            {
                await target.CreateIfNotExistsAsync();
                await this.WriteWorkerMetadataAsync(hub, metadata);
                Exception failure = null;
                try
                {
                    await new TaskHubClient(service).GetOrchestrationStateAsync("missing");
                }
                catch (Exception e)
                {
                    failure = e;
                }
                Assert.IsNotNull(failure, "Malformed metadata must not be interpreted as an absent hub.");
                Assert.AreEqual(metadata, await this.ReadWorkerMetadataAsync(hub));
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);

                await this.WriteWorkerMetadataAsync(hub, $"{{\"TaskHubName\":\"{hub}\",\"PartitionCount\":4}}");
                await new TaskHubClient(service).GetOrchestrationStateAsync("missing");
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        [DataTestMethod]
        [DataRow("Table")]
        [DataRow("Safe")]
        [DataRow("Legacy")]
        public async Task UnmarkedExistingHubRetainsLegacyInitialization(string manager)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, manager));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, manager));
            try
            {
                await target.CreateIfNotExistsAsync();
                await this.WriteWorkerMetadataAsync(hub, null);
                await new TaskHubClient(service).GetOrchestrationStateAsync("missing");
                Assert.AreEqual(16, (await this.QueueNamesAsync(hub)).Length);
                Assert.AreEqual(16, await this.PartitionCountAsync(hub, manager));
                Assert.IsNull(await this.ReadWorkerMetadataAsync(hub), "Clients must not publish authoritative worker settings.");
            }
            finally
            {
                await this.CleanupAsync(hub, manager);
            }
        }

        [TestMethod]
        public async Task ImplicitCreationDoesNotPublishWorkerMetadata()
        {
            string hub = NewHub();
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            try
            {
                await new TaskHubClient(service).GetOrchestrationStateAsync("missing");
                Assert.IsNull(await this.ReadWorkerMetadataAsync(hub));
                await service.CreateIfNotExistsAsync();
                Assert.AreEqual(4, JObject.Parse(await this.ReadWorkerMetadataAsync(hub)).Value<int>("PartitionCount"));
                await service.DeleteAsync();
                Assert.IsNull(await this.ReadWorkerMetadataAsync(hub));
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        [DataTestMethod]
        [DataRow("Table", "Safe")]
        [DataRow("Safe", "Table")]
        [DataRow("Legacy", "Table")]
        public async Task MarkedHubClientDoesNotCreateItsOwnPartitionManager(string targetManager, string clientManager)
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, targetManager));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, clientManager));
            try
            {
                await target.CreateIfNotExistsAsync();
                await new TaskHubClient(service).GetOrchestrationStateAsync("missing");
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);
                Assert.AreEqual(4, await this.PartitionCountAsync(hub, targetManager));
                if (clientManager == "Table")
                {
                    var tables = new TableServiceClient(this.connection);
                    await foreach (var table in tables.QueryAsync(filter: $"TableName eq '{hub}Partitions'"))
                    {
                        Assert.Fail("A client must not introduce a different partition-manager store.");
                    }
                }
            }
            finally
            {
                await this.CleanupAsync(hub, targetManager);
                if (clientManager != targetManager)
                {
                    await this.CleanupAsync(hub, clientManager);
                }
            }
        }

        [TestMethod]
        public async Task ClientDuringWorkerCreationDoesNotCachePartialPartitionTable()
        {
            string hub = NewHub();
            var policy = new PausePartitionCreationPolicy(hub + "Partitions");
            var tableOptions = new TableClientOptions();
            tableOptions.AddPolicy(policy, HttpPipelinePosition.PerCall);
            var targetSettings = this.Settings(hub, 16, "Table");
            targetSettings.StorageAccountClientProvider = new StorageAccountClientProvider(
                StorageServiceClientProvider.ForBlob(this.connection),
                StorageServiceClientProvider.ForQueue(this.connection),
                StorageServiceClientProvider.ForTable(this.connection, tableOptions));
            using var target = new AzureStorageOrchestrationService(targetSettings);
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 16, "Table"));
            Task creation = target.CreateIfNotExistsAsync();
            try
            {
                Assert.AreSame(policy.FirstFourCreated, await Task.WhenAny(policy.FirstFourCreated, Task.Delay(TimeSpan.FromSeconds(10))));
                Assert.AreEqual(16, JObject.Parse(await this.ReadWorkerMetadataAsync(hub)).Value<int>("PartitionCount"));
                await new TaskHubClient(service).CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                policy.Release();
                await creation;
                var queue = new QueueClient(this.connection, AzureStorageOrchestrationService.GetControlQueueName(hub, 14));
                Assert.AreEqual(1, (await queue.PeekMessagesAsync(1)).Value.Length,
                    "A matching 16-partition client must not route using a partially populated four-row lease table.");
            }
            finally
            {
                policy.Release();
                await creation;
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task InterruptedWorkerCreationCanBeRetried()
        {
            string hub = NewHub();
            var policy = new PausePartitionCreationPolicy(hub + "Partitions") { FailAfterRelease = true };
            var tableOptions = new TableClientOptions();
            tableOptions.AddPolicy(policy, HttpPipelinePosition.PerCall);
            var settings = this.Settings(hub, 16, "Table");
            settings.StorageAccountClientProvider = new StorageAccountClientProvider(
                StorageServiceClientProvider.ForBlob(this.connection),
                StorageServiceClientProvider.ForQueue(this.connection),
                StorageServiceClientProvider.ForTable(this.connection, tableOptions));
            using var target = new AzureStorageOrchestrationService(settings);
            using var clientService = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            Task creation = target.CreateIfNotExistsAsync();
            try
            {
                Assert.AreSame(policy.FirstFourCreated, await Task.WhenAny(policy.FirstFourCreated, Task.Delay(TimeSpan.FromSeconds(10))));
                policy.Release();
                await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => creation);
                await new TaskHubClient(clientService).CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                Assert.AreEqual(4, await this.PartitionCountAsync(hub, "Table"), "The client must not take over lease provisioning.");
                Assert.AreEqual(1, (await new QueueClient(this.connection,
                    AzureStorageOrchestrationService.GetControlQueueName(hub, 14)).PeekMessagesAsync(1)).Value.Length);

                policy.FailAfterRelease = false;
                await target.CreateIfNotExistsAsync();
                Assert.AreEqual(16, await this.PartitionCountAsync(hub, "Table"));
            }
            finally
            {
                policy.Release();
                try
                {
                    await creation;
                }
                catch (InvalidOperationException)
                {
                    // The injected provisioning failure is expected; cleanup still owns this hub.
                }
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task UnreadableWorkerMetadataDoesNotFallBackAndCanBeRetried()
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            var policy = new FailMetadataReadPolicy();
            var options = new QueueClientOptions();
            options.AddPolicy(policy, HttpPipelinePosition.PerCall);
            var settings = this.Settings(hub, 16, "Table");
            settings.StorageAccountClientProvider = new StorageAccountClientProvider(
                StorageServiceClientProvider.ForBlob(this.connection),
                StorageServiceClientProvider.ForQueue(this.connection, options),
                StorageServiceClientProvider.ForTable(this.connection));
            using var service = new AzureStorageOrchestrationService(settings);
            try
            {
                await target.CreateIfNotExistsAsync();
                var client = new TaskHubClient(service);
                await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(() => client.GetOrchestrationStateAsync("missing"));
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);
                policy.Fail = false;
                await client.GetOrchestrationStateAsync("missing");
                Assert.AreEqual(4, (await this.QueueNamesAsync(hub)).Length);
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task ExplicitInitializationDoesNotRediscoverWorkerMetadata()
        {
            string hub = NewHub();
            var policy = new FailMetadataReadPolicy { Fail = false };
            var options = new QueueClientOptions();
            options.AddPolicy(policy, HttpPipelinePosition.PerCall);
            var settings = this.Settings(hub, 4, "Table");
            settings.StorageAccountClientProvider = new StorageAccountClientProvider(
                StorageServiceClientProvider.ForBlob(this.connection),
                StorageServiceClientProvider.ForQueue(this.connection, options),
                StorageServiceClientProvider.ForTable(this.connection));
            using var service = new AzureStorageOrchestrationService(settings);
            try
            {
                await service.CreateIfNotExistsAsync();
                policy.Fail = true;
                Assert.IsNull(await new TaskHubClient(service).GetOrchestrationStateAsync("missing"));
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task DiscoveredClientQueuesDoNotBecomeRecreatedWorkerLeases()
        {
            string hub = NewHub();
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 16, "Table"));
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            try
            {
                await target.CreateIfNotExistsAsync();
                await new TaskHubClient(service).CreateOrchestrationInstanceAsync("Probe", string.Empty, "partition-probe", "hello");
                await service.CreateAsync();
                var actual = new List<string>();
                await foreach (TableEntity row in new TableClient(this.connection, hub + "Partitions").QueryAsync<TableEntity>())
                {
                    actual.Add(row.RowKey);
                }
                CollectionAssert.AreEquivalent(
                    Enumerable.Range(0, 4).Select(i => AzureStorageOrchestrationService.GetControlQueueName(hub, i)).ToArray(),
                    actual.ToArray());
                Assert.AreEqual(4, service.AllControlQueues.Count());
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task HubDeletionRemovesMetadataWhenAppLeaseContainerCannotBeDeleted()
        {
            string hub = NewHub();
            using var service = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            var container = new BlobContainerClient(this.connection, hub.ToLowerInvariant() + "-applease");
            var lease = container.GetBlobLeaseClient();
            try
            {
                await service.CreateIfNotExistsAsync();
                await lease.AcquireAsync(TimeSpan.FromSeconds(60));
                await service.DeleteAsync();
                Assert.IsTrue(await container.ExistsAsync(), "Legacy app-lease cleanup is best-effort while a lease is active.");
                Assert.IsNull(await this.ReadWorkerMetadataAsync(hub), "Deleted hubs must not leave authoritative topology behind.");
            }
            finally
            {
                if (await container.ExistsAsync())
                {
                    await lease.ReleaseAsync();
                }
                await this.CleanupAsync(hub, "Table");
            }
        }

        [TestMethod]
        public async Task WorkerPublicationPreservesUnrelatedQueueMetadata()
        {
            string hub = NewHub();
            var queue = new QueueClient(this.connection, hub.ToLowerInvariant() + "-workitems");
            using var target = new AzureStorageOrchestrationService(this.Settings(hub, 4, "Table"));
            using var clientService = new AzureStorageOrchestrationService(this.Settings(hub, 16, "Table"));
            try
            {
                await queue.CreateIfNotExistsAsync(new Dictionary<string, string> { ["application"] = "preserved" });
                await target.CreateIfNotExistsAsync();
                string published = await this.ReadWorkerMetadataAsync(hub);
                await new TaskHubClient(clientService).GetOrchestrationStateAsync("missing");
                Assert.AreEqual(published, await this.ReadWorkerMetadataAsync(hub));
                Assert.AreEqual("preserved", (await queue.GetPropertiesAsync()).Value.Metadata["application"]);
            }
            finally
            {
                await this.CleanupAsync(hub, "Table");
            }
        }

        static string NewHub() => "ClientPartitions" + Guid.NewGuid().ToString("N");

        async Task<string> ReadWorkerMetadataAsync(string hub)
        {
            var queue = new QueueClient(this.connection, hub.ToLowerInvariant() + "-workitems");
            if (!await queue.ExistsAsync())
            {
                return null;
            }
            var metadata = (await queue.GetPropertiesAsync()).Value.Metadata;
            return metadata.TryGetValue("durabletask_taskhub", out string value) ? value : null;
        }

        async Task WriteWorkerMetadataAsync(string hub, string value)
        {
            var queue = new QueueClient(this.connection, hub.ToLowerInvariant() + "-workitems");
            var metadata = (await queue.GetPropertiesAsync()).Value.Metadata;
            if (value == null)
            {
                metadata.Remove("durabletask_taskhub");
            }
            else
            {
                metadata["durabletask_taskhub"] = value;
            }
            await queue.SetMetadataAsync(metadata);
        }

        AzureStorageOrchestrationServiceSettings Settings(string hub, int partitions, string manager) =>
            new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                TaskHubName = hub,
                PartitionCount = partitions,
                UseTablePartitionManagement = manager == "Table",
                UseLegacyPartitionManagement = manager == "Legacy",
            };

        async Task CleanupAsync(string hub, string manager)
        {
            using var cleanup = new AzureStorageOrchestrationService(this.Settings(hub, 16, manager));
            await cleanup.DeleteAsync();
        }

        async Task<string[]> QueueNamesAsync(string hub)
        {
            var queues = new QueueServiceClient(this.connection);
            var names = new List<string>();
            await foreach (var queue in queues.GetQueuesAsync(prefix: hub.ToLowerInvariant() + "-control-"))
            {
                names.Add(queue.Name);
            }
            return names.OrderBy(n => n, StringComparer.Ordinal).ToArray();
        }

        async Task<int> PartitionCountAsync(string hub, string manager)
        {
            if (manager != "Table")
            {
                var blob = new BlobClient(this.connection, hub.ToLowerInvariant() + "-leases", "taskhub.json");
                return JObject.Parse((await blob.DownloadContentAsync()).Value.Content.ToString()).Value<int>("PartitionCount");
            }

            var table = new TableClient(this.connection, hub + "Partitions");
            int count = 0;
            await foreach (TableEntity row in table.QueryAsync<TableEntity>())
            {
                count++;
            }
            return count;
        }

        class PausePartitionCreationPolicy : HttpPipelinePolicy
        {
            readonly string tableName;
            readonly TaskCompletionSource<bool> firstFourCreated = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            readonly TaskCompletionSource<bool> released = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int started;
            int completed;

            public PausePartitionCreationPolicy(string tableName) => this.tableName = tableName;

            public Task FirstFourCreated => this.firstFourCreated.Task;

            public bool FailAfterRelease { get; set; }

            public void Release() => this.released.TrySetResult(true);

            public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
                throw new NotSupportedException("This test uses asynchronous table operations.");

            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                bool partitionInsert = message.Request.Method == RequestMethod.Post
                    && message.Request.Uri.ToUri().AbsolutePath.EndsWith("/" + this.tableName, StringComparison.Ordinal);
                if (partitionInsert && Interlocked.Increment(ref this.started) > 4)
                {
                    await this.released.Task;
                    if (this.FailAfterRelease)
                    {
                        throw new InvalidOperationException("Injected partition provisioning failure.");
                    }
                }
                await ProcessNextAsync(message, pipeline);
                if (partitionInsert && Interlocked.Increment(ref this.completed) == 4)
                {
                    this.firstFourCreated.TrySetResult(true);
                }
            }

        }

        class FailMetadataReadPolicy : HttpPipelinePolicy
        {
            public bool Fail { get; set; } = true;

            public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
                throw new NotSupportedException("This test uses asynchronous queue operations.");

            public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                if (this.Fail && message.Request.Uri.ToUri().AbsolutePath.EndsWith("-workitems", StringComparison.Ordinal))
                {
                    throw new Azure.RequestFailedException(403, "Injected metadata authorization failure.");
                }
                return ProcessNextAsync(message, pipeline);
            }
        }
    }
}
