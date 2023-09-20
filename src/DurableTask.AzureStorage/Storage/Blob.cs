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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;

    class Blob
    {
        readonly BlockBlobClient blockBlobClient;

        public Blob(BlobServiceClient blobServiceClient, string containerName, string blobName)
        {
            this.blockBlobClient = blobServiceClient
                .GetBlobContainerClient(containerName)
                .GetBlockBlobClient(blobName);
        }

        public Blob(BlobServiceClient blobServiceClient, Uri blobUri)
        {
            if (!blobUri.AbsoluteUri.StartsWith(blobServiceClient.Uri.AbsoluteUri, StringComparison.Ordinal))
            {
                throw new ArgumentException("Blob is not present in the storage account", nameof(blobUri));
            }

            var builder = new BlobUriBuilder(blobUri);
            this.blockBlobClient = blobServiceClient
                .GetBlobContainerClient(builder.BlobContainerName)
                .GetBlockBlobClient(builder.BlobName);
        }

        public string Name => this.blockBlobClient.Name;

        public Uri Uri => this.blockBlobClient.Uri;

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.blockBlobClient.ExistsAsync(cancellationToken).DecorateFailure();
        }

        public async Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.blockBlobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken).DecorateFailure();
        }

        public async Task<bool> IsLeasedAsync(CancellationToken cancellationToken = default)
        {
            BlobProperties properties = await this.blockBlobClient.GetPropertiesAsync(cancellationToken: cancellationToken).DecorateFailure();
            return properties.LeaseState == LeaseState.Leased;
        }

        public async Task UploadTextAsync(string content, string? leaseId = null, bool ifDoesntExist = false, CancellationToken cancellationToken = default)
        {
            BlobRequestConditions? conditions = null;
            if (ifDoesntExist)
            {
                conditions = new BlobRequestConditions { IfNoneMatch = ETag.All };
            }
            else if (leaseId != null)
            {
                conditions = new BlobRequestConditions { LeaseId = leaseId };
            }

            using var buffer = new MemoryStream(Encoding.UTF8.GetBytes(content), writable: false);
            await this.blockBlobClient.UploadAsync(buffer, conditions: conditions, cancellationToken: cancellationToken).DecorateFailure();
        }

        public async Task UploadFromByteArrayAsync(byte[] buffer, int index, int byteCount, CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream(buffer, index, byteCount, writable: false);
            await this.blockBlobClient.UploadAsync(stream, cancellationToken: cancellationToken).DecorateFailure();
        }

        public async Task<Stream> OpenWriteAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                return await this.blockBlobClient.OpenWriteAsync(overwrite: true, cancellationToken: cancellationToken);
            }
            catch (RequestFailedException rfe)
            {
                throw new DurableTaskStorageException(rfe);
            }
        }

        public async Task<string> DownloadTextAsync(CancellationToken cancellationToken = default)
        {
            using BlobDownloadStreamingResult result = await this.blockBlobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).DecorateFailure();

            using var reader = new StreamReader(result.Content, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        public Task DownloadToStreamAsync(MemoryStream target, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient.DownloadToAsync(target, cancellationToken: cancellationToken).DecorateFailure();
        }

        public async Task<BlobDownloadStreamingResult> DownloadStreamingAsync(CancellationToken cancellationToken = default)
        {
            return await this.blockBlobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).DecorateFailure();
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId, CancellationToken cancellationToken = default)
        {
            BlobLease lease = await this.blockBlobClient
                .GetBlobLeaseClient(leaseId)
                .AcquireAsync(leaseInterval, cancellationToken: cancellationToken)
                .DecorateFailure();

            return lease.LeaseId;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId, CancellationToken cancellationToken = default)
        {
            BlobLease lease = await this.blockBlobClient
                .GetBlobLeaseClient(currentLeaseId)
                .ChangeAsync(proposedLeaseId, cancellationToken: cancellationToken)
                .DecorateFailure();

            return lease.LeaseId;
        }

        public Task RenewLeaseAsync(string leaseId, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient
                .GetBlobLeaseClient(leaseId)
                .RenewAsync(cancellationToken: cancellationToken)
                .DecorateFailure();
        }

        public Task ReleaseLeaseAsync(string leaseId, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient
                .GetBlobLeaseClient(leaseId)
                .ReleaseAsync(cancellationToken: cancellationToken)
                .DecorateFailure();
        }
    }
}
