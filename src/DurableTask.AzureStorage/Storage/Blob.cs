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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Blobs.Specialized;

    class Blob
    {
        readonly AzureStorageClient azureStorageClient;
        readonly BlockBlobClient blockBlobClient;

        public Blob(AzureStorageClient azureStorageClient, BlobServiceClient blobServiceClient, string containerName, string blobName)
        {
            this.azureStorageClient = azureStorageClient;
            this.Name = blobName;

            this.blockBlobClient = blobServiceClient.GetBlobContainerClient(containerName).GetBlockBlobClient(blobName);
        }

        public Blob(AzureStorageClient azureStorageClient, BlobServiceClient blobServiceClient, Uri blobUri)
        {
            if (!blobUri.IsAbsoluteUri)
                throw new ArgumentException("Blob URI must be absolute", nameof(blobUri));

            if (blobUri.Scheme != blobServiceClient.Uri.Scheme)
                throw new ArgumentException("Blob URI has a different scheme from blob client,", nameof(blobUri));

            if (blobUri.Host != blobServiceClient.Uri.Host)
                throw new ArgumentException("Blob URI has a different host from blob client,", nameof(blobUri));

            if (blobUri.Port != blobServiceClient.Uri.Port)
                throw new ArgumentException("Blob URI has a different port from blob client,", nameof(blobUri));

            if (blobUri.Segments.Length < 3)
                throw new ArgumentException("Blob URI is missing the container.", nameof(blobUri));

            this.azureStorageClient = azureStorageClient;

            // Uri.Segments splits on the '/' in the path such that it is always the last character.
            // Eg. 'https://foo.blob.core.windows.net/bar/baz/dog.json' => [ '/', 'bar/', 'baz/', 'dog.json' ]
            string container = blobUri.Segments[1];
            this.blockBlobClient = blobServiceClient
                .GetBlobContainerClient(container.Substring(0, container.Length - 1))
                .GetBlockBlobClient(string.Join("", blobUri.Segments.Skip(2)));
        }

        public string? Name { get; }

        public string AbsoluteUri => this.blockBlobClient.Uri.AbsoluteUri;

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.blockBlobClient.ExistsAsync(cancellationToken);
        }

        public async Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.blockBlobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken);
        }

        public async Task<bool> IsLeasedAsync(CancellationToken cancellationToken = default)
        {
            BlobProperties properties = await this.blockBlobClient.GetPropertiesAsync(cancellationToken: cancellationToken);
            return properties.LeaseState == LeaseState.Leased;
        }

        public async Task UploadTextAsync(string content, string? leaseId = null, bool ifDoesntExist = false, CancellationToken cancellationToken = default)
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

            using var buffer = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await this.blockBlobClient.UploadAsync(buffer, conditions: conditions, cancellationToken: cancellationToken);
        }

        public async Task UploadFromByteArrayAsync(byte[] buffer, int index, int byteCount, CancellationToken cancellationToken = default)
        {
            using var stream = new MemoryStream(buffer, index, byteCount);
            await this.blockBlobClient.UploadAsync(stream, cancellationToken: cancellationToken);
        }

        public async Task<string> DownloadTextAsync(CancellationToken cancellationToken = default)
        {
            BlobDownloadStreamingResult result = await this.blockBlobClient.DownloadStreamingAsync(cancellationToken: cancellationToken);

            using var reader = new StreamReader(result.Content, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        public Task DownloadToStreamAsync(MemoryStream target, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient.DownloadToAsync(target, cancellationToken: cancellationToken);
        }

        public async Task<string> AcquireLeaseAsync(TimeSpan leaseInterval, string leaseId, CancellationToken cancellationToken = default)
        {
            BlobLease lease = await this.blockBlobClient.GetBlobLeaseClient(leaseId).AcquireAsync(leaseInterval, cancellationToken: cancellationToken);
            return lease.LeaseId;
        }

        public async Task<string> ChangeLeaseAsync(string proposedLeaseId, string currentLeaseId, CancellationToken cancellationToken = default)
        {
            BlobLease lease = await this.blockBlobClient.GetBlobLeaseClient(currentLeaseId).ChangeAsync(proposedLeaseId, cancellationToken: cancellationToken);
            return lease.LeaseId;
        }

        public Task RenewLeaseAsync(string leaseId, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient.GetBlobLeaseClient(leaseId).RenewAsync(cancellationToken: cancellationToken);
        }

        public Task ReleaseLeaseAsync(string leaseId, CancellationToken cancellationToken = default)
        {
            return this.blockBlobClient.GetBlobLeaseClient(leaseId).ReleaseAsync(cancellationToken: cancellationToken)
        }
    }
}
