// Copyright Microsoft Corporation
// Licensed under the Apache License, Version 2.0.

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class ClientStorageInitializationTests
    {
        readonly string connection = TestHelpers.GetTestStorageAccountConnectionString();

        [DataTestMethod]
        [DataRow("history", false)]
        [DataRow("history", true)]
        [DataRow("instance", false)]
        [DataRow("instance", true)]
        [DataRow("range", false)]
        [DataRow("range", true)]
        [DataRow("interface-instance", false)]
        [DataRow("interface-instance", true)]
        [DataRow("interface-range", false)]
        [DataRow("interface-range", true)]
        [DataRow("timeout", false)]
        [DataRow("timeout", true)]
        [DataRow("legacy", false)]
        [DataRow("legacy", true)]
        [DataRow("download", false)]
        [DataRow("download", true)]
        public async Task ClientStorageOperationRequiresMetadataBeforeAccess(string operation, bool existing)
        {
            string hub = "ClientStorage" + Guid.NewGuid().ToString("N");
            using var target = this.CreateService(hub);
            var traffic = new StorageAccessPolicy(hub);
            using var client = this.CreateService(hub, traffic);
            var instance = new OrchestrationInstance { InstanceId = "probe", ExecutionId = "missing" };
            string blobUri = new BlobClient(this.connection, hub.ToLowerInvariant() + "-largemessages", "probe/input").Uri.AbsoluteUri;
            string history = null;
            try
            {
                if (existing)
                {
                    await target.CreateIfNotExistsAsync();
                    instance = await new TaskHubClient(target).CreateOrchestrationInstanceAsync("Probe", "", "probe", "hello");
                    history = await target.GetOrchestrationHistoryAsync(instance.InstanceId, instance.ExecutionId);
                    await this.RemoveMetadataAsync(hub);
                }

                InvalidOperationException error = await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                    () => InvokeAsync(client, operation, instance, blobUri));
                StringAssert.Contains(error.Message, "CreateIfNotExistsAsync");
                Assert.AreEqual(0, traffic.NonMetadataRequests.Count, "Rejected operation accessed hub storage beyond metadata.");
                if (existing)
                {
                    Assert.AreEqual(history, await target.GetOrchestrationHistoryAsync(instance.InstanceId, instance.ExecutionId));
                    Assert.AreEqual(OrchestrationStatus.Pending, (await new TaskHubClient(target).GetOrchestrationStateAsync("probe")).OrchestrationStatus);
                }

                await target.CreateIfNotExistsAsync();
                if (!existing)
                {
                    instance = await new TaskHubClient(target).CreateOrchestrationInstanceAsync("Probe", "", "probe", "hello");
                }
                if (operation == "download")
                {
                    await this.UploadPayloadAsync(hub);
                }
                if (operation == "legacy")
                {
                    await Assert.ThrowsExceptionAsync<NotSupportedException>(() => InvokeAsync(client, operation, instance, blobUri));
                }
                else
                {
                    await InvokeAsync(client, operation, instance, blobUri);
                }
            }
            finally
            {
                await target.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task PurgeDeadlineIncludesInitializationWithoutCancellingOtherCallers()
        {
            string hub = "PurgeDeadline" + Guid.NewGuid().ToString("N");
            using var target = this.CreateService(hub);
            var traffic = new StorageAccessPolicy(hub) { PauseMetadata = true };
            using var client = this.CreateService(hub, traffic);
            try
            {
                await target.CreateIfNotExistsAsync();
                await new TaskHubClient(target).CreateOrchestrationInstanceAsync("Probe", "", "probe", "hello");
                Task<PurgeResult> purge = ((IOrchestrationServicePurgeClient)client).PurgeInstanceStateAsync(
                    new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null) { Timeout = TimeSpan.FromMilliseconds(100) });
                Assert.AreSame(traffic.MetadataReached, await Task.WhenAny(traffic.MetadataReached, Task.Delay(TimeSpan.FromSeconds(10))));
                Task<OrchestrationState> otherCaller = new TaskHubClient(client).GetOrchestrationStateAsync("probe");
                Assert.AreSame(purge, await Task.WhenAny(purge, Task.Delay(TimeSpan.FromSeconds(5))));
                PurgeResult result = await purge;
                Assert.AreEqual(0, result.DeletedInstanceCount);
                Assert.AreEqual(false, result.IsComplete);
                Assert.AreEqual(0, traffic.NonMetadataRequests.Count);
                Assert.IsFalse(otherCaller.IsCompleted, "The shared initialization should still be waiting, not cancelled by the purge.");

                traffic.Release();
                Assert.IsNotNull(await otherCaller);
                Assert.IsNotNull(await new TaskHubClient(target).GetOrchestrationStateAsync("probe"),
                    "A timed-out purge must not resume deletion after initialization completes.");
                PurgeResult retry = await ((IOrchestrationServicePurgeClient)client).PurgeInstanceStateAsync(
                    new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null) { Timeout = TimeSpan.FromSeconds(10) });
                Assert.AreEqual(1, retry.DeletedInstanceCount);
                Assert.AreEqual(true, retry.IsComplete);
            }
            finally
            {
                traffic.Release();
                await target.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ExpiredPurgeNeverStartsTrackingDeletion()
        {
            string hub = "ExpiredPurge" + Guid.NewGuid().ToString("N");
            using var target = this.CreateService(hub);
            var traffic = new StorageAccessPolicy(hub) { PauseMetadata = true };
            using var client = this.CreateService(hub, traffic);
            try
            {
                await target.CreateIfNotExistsAsync();
                await new TaskHubClient(target).CreateOrchestrationInstanceAsync("Probe", "", "probe", "hello");
                PurgeResult result = await ((IOrchestrationServicePurgeClient)client).PurgeInstanceStateAsync(
                    new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null) { Timeout = TimeSpan.Zero });
                Assert.AreEqual(0, result.DeletedInstanceCount);
                Assert.AreEqual(false, result.IsComplete);
                Assert.AreEqual(0, traffic.NonMetadataRequests.Count);
                Assert.AreEqual(0, traffic.MetadataReads, "An already-expired purge must not start initialization.");
                traffic.Release();
                Assert.IsNotNull(await new TaskHubClient(client).GetOrchestrationStateAsync("probe"));
            }
            finally
            {
                traffic.Release();
                await target.DeleteAsync();
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task TimedPurgePreservesInitializationErrorsBeforeDeadline(bool authorizationFailure)
        {
            string hub = "PurgeError" + Guid.NewGuid().ToString("N");
            using var target = this.CreateService(hub);
            var traffic = new StorageAccessPolicy(hub) { FailMetadata = authorizationFailure };
            using var client = this.CreateService(hub, traffic);
            try
            {
                await target.CreateIfNotExistsAsync();
                if (!authorizationFailure)
                {
                    var queue = new QueueClient(this.connection, hub.ToLowerInvariant() + "-workitems");
                    var metadata = (await queue.GetPropertiesAsync()).Value.Metadata;
                    metadata["durabletask_taskhub"] = "{broken";
                    await queue.SetMetadataAsync(metadata);
                }
                Func<Task> purge = () => ((IOrchestrationServicePurgeClient)client).PurgeInstanceStateAsync(
                    new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null) { Timeout = TimeSpan.FromSeconds(10) });
                if (authorizationFailure)
                {
                    var error = await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(purge);
                    Assert.AreEqual(403, error.HttpStatusCode);
                }
                else
                {
                    await Assert.ThrowsExceptionAsync<JsonReaderException>(purge);
                }
                Assert.AreEqual(0, traffic.NonMetadataRequests.Count);
            }
            finally
            {
                await target.DeleteAsync();
            }
        }

        static async Task InvokeAsync(AzureStorageOrchestrationService service, string operation, OrchestrationInstance instance, string blobUri)
        {
            switch (operation)
            {
                case "history":
                    await service.GetOrchestrationHistoryAsync(instance.InstanceId, instance.ExecutionId);
                    break;
                case "instance":
                    await service.PurgeInstanceHistoryAsync(instance.InstanceId);
                    break;
                case "range":
                    await service.PurgeInstanceHistoryAsync(DateTime.UtcNow.AddDays(-1), null, null);
                    break;
                case "interface-instance":
                    await ((IOrchestrationServicePurgeClient)service).PurgeInstanceStateAsync(instance.InstanceId);
                    break;
                case "interface-range":
                case "timeout":
                    var filter = new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null);
                    if (operation == "timeout")
                    {
                        filter.Timeout = TimeSpan.FromSeconds(10);
                    }
                    await ((IOrchestrationServicePurgeClient)service).PurgeInstanceStateAsync(filter);
                    break;
                case "legacy":
                    await service.PurgeOrchestrationHistoryAsync(DateTime.UtcNow, OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);
                    break;
                case "download":
                    Assert.AreEqual("payload", await service.DownloadBlobAsync(blobUri));
                    break;
                default:
                    throw new ArgumentException(nameof(operation));
            }
        }

        AzureStorageOrchestrationService CreateService(string hub, StorageAccessPolicy policy = null)
        {
            var blob = new BlobClientOptions();
            var queue = new QueueClientOptions();
            var table = new TableClientOptions();
            if (policy != null)
            {
                blob.AddPolicy(policy, HttpPipelinePosition.PerCall);
                queue.AddPolicy(policy, HttpPipelinePosition.PerCall);
                table.AddPolicy(policy, HttpPipelinePosition.PerCall);
            }
            return new AzureStorageOrchestrationService(new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = hub,
                PartitionCount = 4,
                UseTablePartitionManagement = true,
                StorageAccountClientProvider = new StorageAccountClientProvider(
                    StorageServiceClientProvider.ForBlob(this.connection, blob),
                    StorageServiceClientProvider.ForQueue(this.connection, queue),
                    StorageServiceClientProvider.ForTable(this.connection, table)),
            });
        }

        async Task RemoveMetadataAsync(string hub)
        {
            var queue = new QueueClient(this.connection, hub.ToLowerInvariant() + "-workitems");
            var metadata = (await queue.GetPropertiesAsync()).Value.Metadata;
            metadata.Remove("durabletask_taskhub");
            await queue.SetMetadataAsync(metadata);
        }

        async Task UploadPayloadAsync(string hub)
        {
            var container = new BlobContainerClient(this.connection, hub.ToLowerInvariant() + "-largemessages");
            await container.CreateIfNotExistsAsync();
            using var content = new MemoryStream();
            using (var gzip = new GZipStream(content, CompressionLevel.Optimal, leaveOpen: true))
            {
                byte[] bytes = Encoding.UTF8.GetBytes("payload");
                await gzip.WriteAsync(bytes, 0, bytes.Length);
            }
            content.Position = 0;
            await container.GetBlobClient("probe/input").UploadAsync(content);
        }

        class StorageAccessPolicy : HttpPipelinePolicy
        {
            readonly string metadataPath;
            readonly TaskCompletionSource<bool> reached = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            readonly TaskCompletionSource<bool> released = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int metadataReads;

            public StorageAccessPolicy(string hub) => this.metadataPath = "/" + hub.ToLowerInvariant() + "-workitems";
            public ConcurrentQueue<string> NonMetadataRequests { get; } = new ConcurrentQueue<string>();
            public bool PauseMetadata { get; set; }
            public bool FailMetadata { get; set; }
            public int MetadataReads => this.metadataReads;
            public Task MetadataReached => this.reached.Task;
            public void Release() => this.released.TrySetResult(true);
            public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) => throw new NotSupportedException();
            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                bool metadataRead = (message.Request.Method == RequestMethod.Get || message.Request.Method == RequestMethod.Head)
                    && message.Request.Uri.ToUri().AbsolutePath.EndsWith(this.metadataPath, StringComparison.Ordinal);
                if (metadataRead)
                {
                    Interlocked.Increment(ref this.metadataReads);
                }
                if (!metadataRead)
                {
                    this.NonMetadataRequests.Enqueue(message.Request.Method + " " + message.Request.Uri.ToUri().AbsolutePath);
                }
                else if (this.FailMetadata)
                {
                    throw new Azure.RequestFailedException(403, "Injected metadata authorization failure.");
                }
                else if (this.PauseMetadata)
                {
                    this.reached.TrySetResult(true);
                    await this.released.Task;
                }
                await ProcessNextAsync(message, pipeline);
            }
        }
    }
}
