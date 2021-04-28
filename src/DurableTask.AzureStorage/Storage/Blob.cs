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
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    class Blob
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudBlobClient blobClient;
        readonly string blobDirectory;
        readonly string fullBlobPath;
        readonly CloudBlockBlob cloudBlockBlob;

        public string Name { get; }

        [Obsolete("Use BlobContainer.GetBlobReference() or AzureStorageClient.GetBlobReference()")]
        public Blob(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, string containerName, string blobName, string blobDirectory = null)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.Name = blobName;
            this.blobDirectory = blobDirectory;
            this.fullBlobPath = this.blobDirectory != null ? Path.Combine(this.blobDirectory, this.Name) : blobName;

            this.cloudBlockBlob = this.blobClient.GetContainerReference(containerName).GetBlockBlobReference(fullBlobPath);
        }

        [Obsolete("Use AzureStorageClient.GetBlobReference()")]
        public Blob(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, Uri blobUri)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.cloudBlockBlob = new CloudBlockBlob(blobUri, blobClient.Credentials);
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(() => this.cloudBlockBlob.ExistsAsync(), "Blob Exists");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(() => this.cloudBlockBlob.DeleteIfExistsAsync(), "Blob Delete");
        }

        public async Task UploadTextAsync(string content, string leaseId = null, bool ifDoesntExist = false)
        {
            AccessCondition accessCondition = null;
            if (ifDoesntExist)
            {
                accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");
            }
            else if (leaseId != null)
            {
                accessCondition = AccessCondition.GenerateLeaseCondition(leaseId);
            }

            Func<Task> storageFunction;
            if (accessCondition != null)
            {
                storageFunction = () => this.cloudBlockBlob.UploadTextAsync(content, null, accessCondition, null, null);
            }
            else
            {
                storageFunction = () => this.cloudBlockBlob.UploadTextAsync(content);
            }

            await this.azureStorageClient.MakeStorageRequest(storageFunction, "Blob UploadText");
        }

        public async Task UploadFromByteArrayAsync(byte[] buffer, int index, int byteCount)
        {
            await this.azureStorageClient.MakeStorageRequest(() => this.cloudBlockBlob.UploadFromByteArrayAsync(buffer, index, byteCount), "Blob UploadFromByeArray");
        }

        public async Task<string> DownloadTextAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest(() => this.cloudBlockBlob.DownloadTextAsync(), "Blob DownloadText");
        }

        public async Task DownloadToStreamAsync(MemoryStream memory)
        {
            await this.cloudBlockBlob.DownloadToStreamAsync(memory);
        }

        public async Task FetchAttributesAsync()
        {
            await this.azureStorageClient.MakeStorageRequest(() => this.cloudBlockBlob.FetchAttributesAsync(), "Blob FetchAttributes");
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId)
        {
            return await this.azureStorageClient.MakeStorageRequest<string>(() => this.cloudBlockBlob.AcquireLeaseAsync(leaseInterval, leaseId), "Blob AcquireLease");
        }


        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId)
        {
            return await this.azureStorageClient.MakeStorageRequest<string>(() => this.cloudBlockBlob.ChangeLeaseAsync(proposedLeaseId, accessCondition: AccessCondition.GenerateLeaseCondition(currentLeaseId)), "Blob ChangeLease");
        }

        public async Task RenewLeaseAsync(string leaseId)
        {
            var requestOptions = new BlobRequestOptions { ServerTimeout = azureStorageClient.Settings.LeaseRenewInterval };
            await this.azureStorageClient.MakeStorageRequest(() => this.cloudBlockBlob.RenewLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId), options: requestOptions, operationContext: null), "Blob RenewLease");
        }

        public async Task ReleaseLeaseAsync(string leaseId)
        {
            await this.azureStorageClient.MakeStorageRequest(() => this.cloudBlockBlob.ReleaseLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId)), "Blob ReleaseLease");
        }

        public async Task<bool> IsLeased()
        {
            await this.FetchAttributesAsync();
            return this.cloudBlockBlob.Properties.LeaseState == LeaseState.Leased;
        }

        public string GetAbsoluteUri()
        {
            return this.cloudBlockBlob.Uri.AbsoluteUri;
        }
    }
}
