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
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    class Blob
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudBlobClient blobClient;
        readonly CloudBlockBlob cloudBlockBlob;

        public Blob(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, string containerName, string blobName, string? blobDirectory = null)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.Name = blobName;
            var fullBlobPath = blobDirectory != null ? Path.Combine(blobDirectory, this.Name) : blobName;

            this.cloudBlockBlob = this.blobClient.GetContainerReference(containerName).GetBlockBlobReference(fullBlobPath);
        }

        public Blob(AzureStorageClient azureStorageClient, CloudBlobClient blobClient, Uri blobUri)
        {
            this.azureStorageClient = azureStorageClient;
            this.blobClient = blobClient;
            this.cloudBlockBlob = new CloudBlockBlob(blobUri, blobClient.Credentials);
        }

        public string? Name { get; }

        public bool IsLeased => this.cloudBlockBlob.Properties.LeaseState == LeaseState.Leased;

        public string AbsoluteUri => this.cloudBlockBlob.Uri.AbsoluteUri;

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                (context, cancellationToken) => this.cloudBlockBlob.ExistsAsync(null, context, cancellationToken),
                "Blob Exists");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<bool>(
                (context, cancellationToken) => this.cloudBlockBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, context, cancellationToken),
                "Blob Delete");
        }

        public async Task UploadTextAsync(string content, string? leaseId = null, bool ifDoesntExist = false)
        {
            AccessCondition? accessCondition = null;
            if (ifDoesntExist)
            {
                accessCondition = AccessCondition.GenerateIfNoneMatchCondition("*");
            }
            else if (leaseId != null)
            {
                accessCondition = AccessCondition.GenerateLeaseCondition(leaseId);
            }

            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.UploadTextAsync(content, null, accessCondition, null, context, cancellationToken),
                "Blob UploadText");
        }

        public async Task UploadFromByteArrayAsync(byte[] buffer, int index, int byteCount)
        {
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.UploadFromByteArrayAsync(buffer, index, byteCount, null, null, context, cancellationToken),
                "Blob UploadFromByeArray");
        }

        public async Task<string> DownloadTextAsync()
        {
            return await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.DownloadTextAsync(null, null, null, context, cancellationToken),
                "Blob DownloadText");
        }

        public async Task DownloadToStreamAsync(MemoryStream target)
        {
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.DownloadToStreamAsync(target, null, null, context, cancellationToken),
                "Blob DownloadToStream");
        }

        public async Task FetchAttributesAsync()
        {
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.FetchAttributesAsync(null, null, context, cancellationToken),
                "Blob FetchAttributes");
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId)
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<string>(
                (context, cancellationToken) => this.cloudBlockBlob.AcquireLeaseAsync(leaseInterval, leaseId, null, null, context, cancellationToken),
                "Blob AcquireLease");
        }


        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId)
        {
            return await this.azureStorageClient.MakeBlobStorageRequest<string>(
                (context, cancellationToken) => this.cloudBlockBlob.ChangeLeaseAsync(proposedLeaseId, accessCondition: AccessCondition.GenerateLeaseCondition(currentLeaseId), null, context, cancellationToken),
                "Blob ChangeLease");
        }

        public async Task RenewLeaseAsync(string leaseId)
        {
            var requestOptions = new BlobRequestOptions { ServerTimeout = azureStorageClient.Settings.LeaseRenewInterval };
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.RenewLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId), requestOptions, context, cancellationToken),
                "Blob RenewLease",
                force: true); // lease renewals should not be throttled
        }

        public async Task ReleaseLeaseAsync(string leaseId)
        {
            await this.azureStorageClient.MakeBlobStorageRequest(
                (context, cancellationToken) => this.cloudBlockBlob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(leaseId), null, context, cancellationToken),
                "Blob ReleaseLease");
        }
    }
}
