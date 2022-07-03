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
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;

    class BlobContainer
    {
        readonly AzureStorageClient azureStorageClient;
        readonly string containerName;
        readonly BlobContainerClient blobContainerClient;

        public BlobContainer(AzureStorageClient azureStorageClient, BlobServiceClient blobServiceClient, string name)
        {
            this.azureStorageClient = azureStorageClient;
            this.containerName = name;

            this.blobContainerClient = blobServiceClient.GetBlobContainerClient(this.containerName);
        }

        public Blob GetBlobReference(string blobName, string? blobPrefix = null)
        {
            var fullBlobName = blobPrefix != null ? Path.Combine(blobPrefix, blobName) : blobName;
            return this.azureStorageClient.GetBlobReference(this.containerName, fullBlobName);
        }

        public async Task<bool> CreateIfNotExistsAsync()
        {
            Response<BlobContainerInfo> info = await this.azureStorageClient.GetBlobStorageRequestResponse( // TODO: Any encryption scope?
                cancellationToken => this.blobContainerClient.CreateIfNotExistsAsync(PublicAccessType.None, cancellationToken: cancellationToken),
                "Create Container");

            // If we received null, then the response must have been a 409 (Conflict)
            // and the container must already exist
            return info != null;
        }

        public Task<bool> ExistsAsync() =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blobContainerClient.ExistsAsync(cancellationToken),
                "Container Exists");

        public async Task<bool> DeleteIfExistsAsync(string? appLeaseId = null)
        {
            BlobRequestConditions? conditions = null;
            if (appLeaseId != null)
            {
                conditions = new BlobRequestConditions { LeaseId = appLeaseId };
            }

            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                cancellationToken => this.blobContainerClient.DeleteIfExistsAsync(conditions, cancellationToken),
                "Delete Container");
        }

        public async IAsyncEnumerable<Blob> ListBlobsAsync(string? prefix = null)
        {
            Page<BlobItem>? page = null;
            do
            {
                page = await this.azureStorageClient.MakePaginatedBlobStorageRequest(
                    cancellationToken => this.blobContainerClient.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix, cancellationToken),
                    "Container GetBlobs");

                foreach (BlobItem blobItem in page?.Values ?? Array.Empty<BlobItem>())
                {
                    yield return this.GetBlobReference(blobItem.Name);
                }
            }
            while (page?.ContinuationToken != null);
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId)
        {
            BlobLease lease = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blobContainerClient.GetBlobLeaseClient(leaseId).AcquireAsync(leaseInterval, cancellationToken: cancellationToken),
                "Container AcquireLease");

            return lease.LeaseId;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId)
        {
            BlobLease lease = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blobContainerClient.GetBlobLeaseClient(currentLeaseId).ChangeAsync(proposedLeaseId, cancellationToken: cancellationToken),
                "Container ChangeLease");

            return lease.LeaseId;
        }

        public Task RenewLeaseAsync(string leaseId) =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blobContainerClient.GetBlobLeaseClient(leaseId).RenewAsync(cancellationToken: cancellationToken),
                "Container RenewLease");
    }
}
