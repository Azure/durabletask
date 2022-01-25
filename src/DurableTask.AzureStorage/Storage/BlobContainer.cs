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
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    class BlobContainer
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudBlobClient blobClient;
        readonly string containerName;
        readonly CloudBlobContainer cloudBlobContainer;

        public BlobContainer(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, string name)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.containerName = name;

            this.cloudBlobContainer = this.blobClient.GetContainerReference(this.containerName);
        }

        public Blob GetBlobReference(string blobName, string? blobPrefix = null)
        {
            var fullBlobName = blobPrefix != null ? Path.Combine(blobPrefix, blobName) : blobName;
            return this.azureStorageClient.GetBlobReference(this.containerName, fullBlobName);
        }

        public async Task<bool> CreateIfNotExistsAsync()
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                (context, cancellationToken) => this.cloudBlobContainer.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Off, null, context, cancellationToken),
                "Create Container");
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                (context, cancellationToken) => this.cloudBlobContainer.ExistsAsync(null, context, cancellationToken),
                "Container Exists");
        }

        public async Task<bool> DeleteIfExistsAsync(string? appLeaseId = null)
        {
            AccessCondition? accessCondition = null;
            if (appLeaseId != null)
            {
                accessCondition = new AccessCondition() { LeaseId = appLeaseId };
            }

            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                (context, cancellationToken) => this.cloudBlobContainer.DeleteIfExistsAsync(accessCondition, null, context, cancellationToken),
                "Delete Container");
        }

        public async Task<IEnumerable<Blob>> ListBlobsAsync(string? blobDirectory = null)
        {
            BlobContinuationToken? continuationToken = null;
            Func<OperationContext, CancellationToken, Task<BlobResultSegment>> listBlobsFunction;
            if (blobDirectory != null)
            {
                var cloudBlobDirectory = this.cloudBlobContainer.GetDirectoryReference(blobDirectory);

                listBlobsFunction = (context, cancellationToken) => cloudBlobDirectory.ListBlobsSegmentedAsync(
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: context,
                    cancellationToken: cancellationToken);
            }
            else
            {
                listBlobsFunction = (context, cancellationToken) => this.cloudBlobContainer.ListBlobsSegmentedAsync(
                    null,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: context,
                    cancellationToken: cancellationToken);
            }

            var blobList = new List<Blob>();
            do
            {
                BlobResultSegment segment = await this.azureStorageClient.MakeBlobStorageRequest(listBlobsFunction, "ListBlobs");

                continuationToken = segment.ContinuationToken;

                foreach (IListBlobItem listBlobItem in segment.Results)
                {
                    CloudBlockBlob cloudBlockBlob = (CloudBlockBlob)listBlobItem;
                    var blobName = cloudBlockBlob.Name;
                    Blob blob = this.GetBlobReference(blobName);
                    blobList.Add(blob);
                }
            }
            while (continuationToken != null);

            return blobList;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId)
        {
            AccessCondition accessCondition = new AccessCondition() { LeaseId = currentLeaseId };

            return await this.azureStorageClient.MakeBlobStorageRequest<string>(
                (context, cancellationToken) => this.cloudBlobContainer.ChangeLeaseAsync(proposedLeaseId, accessCondition, null, context, cancellationToken),
                "Container ChangeLease");
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string proposedLeaseId)
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<string>(
                (context, cancellationToken) => this.cloudBlobContainer.AcquireLeaseAsync(leaseInterval, proposedLeaseId, null, null, context, cancellationToken),
                "Container AcquireLease");
        }

        public async Task RenewLeaseAsync(string leaseId)
        {
            AccessCondition accessCondition = new AccessCondition() { LeaseId = leaseId };
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlobContainer.RenewLeaseAsync(accessCondition, null, context, cancellationToken),
                "Container RenewLease");
        }
    }
}
