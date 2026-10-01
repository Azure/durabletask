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
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.AzureStorage.Linq;
    using DurableTask.AzureStorage.Net;

    class BlobContainer
    {
        readonly AzureStorageClient azureStorageClient;
        readonly string containerName;
        readonly BlobContainerClient originalBlobContainerClient;
        BlobContainerClient? cachedBlobContainerClient;

        // During migration we use a blob container client that relies on short-lived SAS tokens
        BlobContainerClient blobContainerClient => Volatile.Read(ref this.cachedBlobContainerClient)
            ?? LazyInitializer.EnsureInitialized(ref this.cachedBlobContainerClient, () => this.azureStorageClient.GetBlobContainerClient(this.originalBlobContainerClient))!;

        public BlobContainer(AzureStorageClient azureStorageClient, BlobServiceClient blobServiceClient, string name)
        {
            this.azureStorageClient = azureStorageClient;
            this.containerName = name;

            this.originalBlobContainerClient = blobServiceClient.GetBlobContainerClient(this.containerName);
        }

        public Blob GetBlobReference(string blobName, string? blobPrefix = null)
        {
            string fullBlobName = blobPrefix != null ? UriPath.Combine(blobPrefix, blobName) : blobName;
            return this.azureStorageClient.GetBlobReference(this.containerName, fullBlobName);
        }

        public async Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            // This call must use the original blob container client because the migration SAS tokens lack the permissions for this operation,
            // so we must ensure access explicitly
            this.azureStorageClient.EnsureAccess();
            // Creating the container requires the original credentials. Blob data operations still use the migration SAS.
            // TODO: Any encryption scope?
            // If we received null, then the response must have been a 409 (Conflict) and the container must already exist
            Response<BlobContainerInfo> response = await this.originalBlobContainerClient.CreateIfNotExistsAsync(PublicAccessType.None, cancellationToken: cancellationToken).DecorateFailure();
            return response != null;
        }

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            // This call must use the original blob container client because the migration SAS tokens lack the permissions for this operation,
            // so we must ensure access explicitly
            this.azureStorageClient.EnsureAccess();
            // Container properties and leases do not support user delegation SAS. These management
            // operations never mutate orchestration data; all blob content operations use SAS.
            return await this.originalBlobContainerClient.ExistsAsync(cancellationToken).DecorateFailure();
        }

        public async Task<bool> DeleteIfExistsAsync(string? appLeaseId = null, CancellationToken cancellationToken = default)
        {
            this.azureStorageClient.ThrowIfMigrationResourceDeletion();
            BlobRequestConditions? conditions = null;
            if (appLeaseId != null)
            {
                conditions = new BlobRequestConditions { LeaseId = appLeaseId };
            }

            return await this.blobContainerClient.DeleteIfExistsAsync(conditions, cancellationToken).DecorateFailure();
        }

        public AsyncPageable<Blob> ListBlobsAsync(string? prefix = null, CancellationToken cancellationToken = default)
        {
            // GetBlobsAsync will return directories if hierarchical namespace (HNS) is enabled,
            // so we must filter them out by checking for a special piece of metadata called "hdi_isfolder"
            return this.blobContainerClient
                .GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix, cancellationToken)
                .DecorateFailure()
                .TransformPages(p => p.Values
                    .Where(b => !IsHnsFolder(b))
                    .Select(b => this.GetBlobReference(b.Name)));
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId, CancellationToken cancellationToken = default)
        {
            // This call must use the original blob container client because the migration SAS tokens lack the permissions for this operation,
            // so we must ensure access explicitly
            this.azureStorageClient.EnsureAccess();
            BlobLease lease = await this.originalBlobContainerClient
                .GetBlobLeaseClient(leaseId)
                .AcquireAsync(leaseInterval, cancellationToken: cancellationToken)
                .DecorateFailure();

            return lease.LeaseId;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId, CancellationToken cancellationToken = default)
        {
            // This call must use the original blob container client because the migration SAS tokens lack the permissions for this operation,
            // so we must ensure access explicitly
            this.azureStorageClient.EnsureAccess();
            BlobLease lease = await this.originalBlobContainerClient
                .GetBlobLeaseClient(currentLeaseId)
                .ChangeAsync(proposedLeaseId, cancellationToken: cancellationToken)
                .DecorateFailure();

            return lease.LeaseId;
        }

        public Task RenewLeaseAsync(string leaseId, CancellationToken cancellationToken = default)
        {
            // This call must use the original blob container client because the migration SAS tokens lack the permissions for this operation,
            // so we must ensure access explicitly
            this.azureStorageClient.EnsureAccess();
            return this.originalBlobContainerClient
                .GetBlobLeaseClient(leaseId)
                .RenewAsync(cancellationToken: cancellationToken)
                .DecorateFailure();
        }

        public Uri GetBlobContainerUri()
        {
            return this.originalBlobContainerClient.Uri;
        }

        static bool IsHnsFolder(BlobItem item)
        {
            // Check the optional "hdi_isfolder" value in the metadata to determine whether
            // the blob is actually a directory. See https://github.com/Azure/azure-sdk-for-python/issues/24814
            return item.Metadata != null
                && item.Metadata.TryGetValue("hdi_isfolder", out string value)
                && bool.TryParse(value, out bool isFolder)
                && isFolder;
        }
    }
}
