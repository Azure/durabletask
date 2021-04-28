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

        [Obsolete("Use AzureStorageClient.GetBlobContainerReference()")]
        public BlobContainer(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, string name)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.containerName = name;

            this.cloudBlobContainer = this.blobClient.GetContainerReference(this.containerName);
        }

        public Blob GetBlobReference(string blobName, string blobPrefix = null)
        {
            return this.azureStorageClient.GetBlobReference(this.containerName, Path.Combine(blobPrefix, blobName));
        }

        public async Task<bool> CreateIfNotExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(() => this.cloudBlobContainer.CreateIfNotExistsAsync(), "Create Container");
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(() => this.cloudBlobContainer.ExistsAsync(), "Container Exists");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(() => this.cloudBlobContainer.DeleteIfExistsAsync(), "Delete Container");
        }

        public async Task<IEnumerable<Blob>> ListBlobsAsync(string blobDirectory = null)
        {
            BlobContinuationToken continuationToken = null;
            Func<OperationContext, CancellationToken, Task<BlobResultSegment>> listBlobsFunction;
            if (blobDirectory != null)
            {
                var cloudBlobDirectory = this.cloudBlobContainer.GetDirectoryReference(blobDirectory);

                listBlobsFunction = (context, timeoutToken) => cloudBlobDirectory.ListBlobsSegmentedAsync(
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: context,
                    cancellationToken: timeoutToken);
            }
            else
            {
                listBlobsFunction = (context, timeoutToken) => this.cloudBlobContainer.ListBlobsSegmentedAsync(
                    null,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: continuationToken,
                    options: null,
                    operationContext: context,
                    cancellationToken: timeoutToken);
            }

            var blobList = new List<Blob>();
            do
            {
                BlobResultSegment segment = await this.azureStorageClient.MakeStorageRequest(listBlobsFunction, "ListBlobs");

                continuationToken = segment.ContinuationToken;

                foreach (IListBlobItem listBlobItem in segment.Results)
                {
                    CloudBlockBlob cloudBlockBlob = listBlobItem as CloudBlockBlob;
                    var blobName = cloudBlockBlob.Name;
                    Blob blob = this.GetBlobReference(blobName);
                    blobList.Add(blob);
                }
            }
            while (continuationToken != null);

            return blobList;
        }
    }
}
