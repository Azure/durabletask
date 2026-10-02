// Copyright Microsoft Corporation
// Licensed under the Apache License, Version 2.0.

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Storage.Blobs;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LeaseContainerDeletionTests
    {
        readonly string connection = TestHelpers.GetTestStorageAccountConnectionString();

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task BlobPartitionManagerDeletesSharedContainerOnce(bool legacy)
        {
            string hub = "SingleDelete" + Guid.NewGuid().ToString("N");
            var observer = new ContainerDeletePolicy(hub);
            using var service = this.CreateService(hub, legacy, observer);
            await service.CreateIfNotExistsAsync();
            var container = new BlobContainerClient(this.connection, hub.ToLowerInvariant() + "-leases");
            var names = new ConcurrentBag<string>();
            await foreach (var blob in container.GetBlobsAsync())
            {
                names.Add(blob.Name);
            }
            if (!legacy)
            {
                Assert.IsTrue(names.Any(n => n.StartsWith("intent/", StringComparison.Ordinal)));
                Assert.IsTrue(names.Any(n => n.StartsWith("ownership/", StringComparison.Ordinal)));
            }

            Task deletion = service.DeleteAsync();
            try
            {
                Assert.AreSame(observer.FirstDelete, await Task.WhenAny(observer.FirstDelete, Task.Delay(TimeSpan.FromSeconds(10))));
                observer.Release();
                await deletion;
                Assert.AreEqual(1, observer.Requests.Count, "One container must not receive concurrent duplicate DELETE requests.");
                Assert.AreEqual(1, observer.Requests.Distinct().Count());
                Assert.IsFalse(await container.ExistsAsync());
                await service.DeleteAsync();
                Assert.AreEqual(2, observer.Requests.Count, "A later idempotent delete should issue only one request of its own.");
            }
            finally
            {
                observer.Release();
                Console.WriteLine($"Lease container DELETE requests: {observer.Requests.Count}; distinct container paths: {observer.Requests.Distinct().Count()}.");
                await container.DeleteIfExistsAsync();
            }
        }

        [DataTestMethod]
        [DataRow(403, "AuthorizationPermissionMismatch")]
        [DataRow(409, "ConcurrentContainerOperationInProgress")]
        public async Task SafeDeletePropagatesStorageFailureAndCanBeRetried(int status, string errorCode)
        {
            string hub = "DeleteFailure" + Guid.NewGuid().ToString("N");
            var observer = new ContainerDeletePolicy(hub) { FailureStatus = status, FailureCode = errorCode };
            observer.Release();
            using var service = this.CreateService(hub, legacy: false, observer);
            try
            {
                await service.CreateIfNotExistsAsync();
                DurableTaskStorageException error = await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(() => service.DeleteAsync());
                Assert.AreEqual(status, error.HttpStatusCode);
                Assert.AreEqual(errorCode, error.ErrorCode);
                Assert.AreEqual(1, observer.Requests.Count);
                observer.FailureStatus = null;
                await service.DeleteAsync();
                Assert.AreEqual(2, observer.Requests.Count);
                Assert.IsFalse(await new BlobContainerClient(this.connection, hub.ToLowerInvariant() + "-leases").ExistsAsync());
            }
            finally
            {
                observer.Release();
                await new BlobContainerClient(this.connection, hub.ToLowerInvariant() + "-leases").DeleteIfExistsAsync();
            }
        }

        AzureStorageOrchestrationService CreateService(string hub, bool legacy, ContainerDeletePolicy observer)
        {
            var options = new BlobClientOptions();
            options.AddPolicy(observer, HttpPipelinePosition.PerCall);
            return new AzureStorageOrchestrationService(new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = hub,
                PartitionCount = 4,
                UseTablePartitionManagement = false,
                UseLegacyPartitionManagement = legacy,
                StorageAccountClientProvider = new StorageAccountClientProvider(
                    StorageServiceClientProvider.ForBlob(this.connection, options),
                    StorageServiceClientProvider.ForQueue(this.connection),
                    StorageServiceClientProvider.ForTable(this.connection)),
            });
        }

        class ContainerDeletePolicy : HttpPipelinePolicy
        {
            readonly string suffix;
            readonly TaskCompletionSource<bool> firstDelete = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            readonly TaskCompletionSource<bool> released = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int active;

            public ContainerDeletePolicy(string hub) => this.suffix = "/" + hub.ToLowerInvariant() + "-leases";
            public ConcurrentQueue<string> Requests { get; } = new ConcurrentQueue<string>();
            public Task FirstDelete => this.firstDelete.Task;
            public int? FailureStatus { get; set; }
            public string FailureCode { get; set; }
            public void Release() => this.released.TrySetResult(true);
            public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
                throw new NotSupportedException("Tests use asynchronous storage requests.");

            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                if (message.Request.Method != RequestMethod.Delete ||
                    !message.Request.Uri.ToUri().AbsolutePath.EndsWith(this.suffix, StringComparison.Ordinal))
                {
                    await ProcessNextAsync(message, pipeline);
                    return;
                }
                this.Requests.Enqueue(message.Request.Uri.ToUri().AbsolutePath);
                if (Interlocked.CompareExchange(ref this.active, 1, 0) != 0)
                {
                    throw new RequestFailedException(409, "Duplicate concurrent container deletion.", "ConcurrentContainerOperationInProgress", null);
                }
                try
                {
                    this.firstDelete.TrySetResult(true);
                    await this.released.Task;
                    if (this.FailureStatus.HasValue)
                    {
                        throw new RequestFailedException(this.FailureStatus.Value, "Injected container deletion failure.", this.FailureCode, null);
                    }
                    await ProcessNextAsync(message, pipeline);
                }
                finally
                {
                    Interlocked.Exchange(ref this.active, 0);
                }
            }
        }
    }
}
