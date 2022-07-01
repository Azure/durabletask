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
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;

    class Blob
    {
        readonly AzureStorageClient azureStorageClient;
        readonly BlockBlobClient blockBlobClient;

        public Blob(AzureStorageClient azureStorageClient, BlobServiceClient blobClient, string containerName, string blobName, string? blobDirectory = null)
        {
            this.azureStorageClient = azureStorageClient;
            this.Name = blobName;
            var fullBlobPath = blobDirectory != null ? Path.Combine(blobDirectory, this.Name) : blobName;

            this.blockBlobClient = blobClient.GetBlobContainerClient(containerName).GetBlockBlobClient(fullBlobPath);
        }

        public Blob(AzureStorageClient azureStorageClient, BlobServiceClient blobClient, Uri blobUri)
        {
            if (!blobUri.IsAbsoluteUri)
                throw new ArgumentException("Blob URI must be absolute", nameof(blobUri));

            if (blobUri.Scheme != blobClient.Uri.Scheme)
                throw new ArgumentException("Blob URI has a different scheme from blob client,", nameof(blobUri));

            if (blobUri.Host != blobClient.Uri.Host)
                throw new ArgumentException("Blob URI has a different host from blob client,", nameof(blobUri));

            if (blobUri.Port != blobClient.Uri.Port)
                throw new ArgumentException("Blob URI has a different port from blob client,", nameof(blobUri));

            if (blobUri.Segments.Length < 3)
                throw new ArgumentException("Blob URI is missing the container.", nameof(blobUri));

            this.azureStorageClient = azureStorageClient;

            // Uri.Segments splits on the '/' in the path such that it is always the last character.
            // Eg. 'https://foo.blob.core.windows.net/bar/baz/dog.json' => [ '/', 'bar/', 'baz/', 'dog.json' ]
            string container = blobUri.Segments[1];
            this.blockBlobClient = blobClient
                .GetBlobContainerClient(container.Substring(0, container.Length - 1))
                .GetBlockBlobClient(string.Join("", blobUri.Segments.Skip(2)));
        }

        public string? Name { get; }

        public string AbsoluteUri => this.blockBlobClient.Uri.AbsoluteUri;

        public Task<bool> ExistsAsync() =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.ExistsAsync(cancellationToken),
                "Blob Exists");

        public Task<bool> DeleteIfExistsAsync() =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken),
                "Blob Delete");

        public async Task<bool> IsLeasedAsync()
        {
            BlobProperties properties = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.GetPropertiesAsync(cancellationToken: cancellationToken),
                "Blob Properties");

            return properties.LeaseState == LeaseState.Leased;
        }

        public Task UploadTextAsync(string content, string? leaseId = null, bool ifDoesntExist = false)
        {
            BlobRequestConditions? conditions = null;
            if (ifDoesntExist)
            {
                conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") };
            }
            else if (leaseId != null)
            {
                conditions = new BlobRequestConditions { LeaseId = leaseId };
            }

            return this.azureStorageClient.MakeBlobStorageRequest(
                async cancellationToken =>
                {
                    using var buffer = new MemoryStream(Encoding.UTF8.GetBytes(content));
                    return await this.blockBlobClient.UploadAsync(buffer, conditions: conditions, cancellationToken: cancellationToken);
                },
                "Blob Upload");
        }

        public Task UploadFromByteArrayAsync(byte[] buffer, int index, int byteCount) =>
            this.azureStorageClient.MakeBlobStorageRequest(
                async cancellationToken =>
                {
                    using MemoryStream stream = new MemoryStream(buffer, index, byteCount);
                    return await this.blockBlobClient.UploadAsync(stream, cancellationToken: cancellationToken);
                },
                "Blob Upload");

        public async Task<string> DownloadTextAsync()
        {
            BlobDownloadStreamingResult result = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.DownloadStreamingAsync(cancellationToken: cancellationToken),
                "Blob Download");

            using StreamReader reader = new StreamReader(result.Content, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        public Task DownloadToStreamAsync(MemoryStream target) =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.DownloadToAsync(target, cancellationToken: cancellationToken),
                "Blob DownloadToStream");

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId)
        {
            BlobLease lease = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.GetBlobLeaseClient(leaseId).AcquireAsync(leaseInterval, cancellationToken: cancellationToken),
                "Blob AcquireLease");

            return lease.LeaseId;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId)
        {
            BlobLease lease = await this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.GetBlobLeaseClient(currentLeaseId).ChangeAsync(proposedLeaseId, cancellationToken: cancellationToken),
                "Blob ChangeLease");

            return lease.LeaseId;
        }

        public Task RenewLeaseAsync(string leaseId) =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.GetBlobLeaseClient(leaseId).RenewAsync(cancellationToken: cancellationToken),
                "Blob RenewLease",
                force: true); // lease renewals should not be throttled

        public Task ReleaseLeaseAsync(string leaseId) =>
            this.azureStorageClient.MakeBlobStorageRequest(
                cancellationToken => this.blockBlobClient.GetBlobLeaseClient(leaseId).ReleaseAsync(cancellationToken: cancellationToken),
                "Blob ReleaseLease");
    }
}
