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

namespace DurableTask.ServiceBus.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;

    /// <summary>
    /// A client to access the Azure blob storage.
    /// </summary>
    public class BlobStorageClient
    {
        // container prefix is in the format of {hubName}-dtfx. It is not part of the blob key.
        // the container full name is in the format of {hubName}-dtfx-{streamType}-{DateTime};
        // the streamType is the type of the stream, either 'message' or 'session';
        // the date time is in the format of yyyyMMdd.
        readonly string containerNamePrefix;
        readonly BlobServiceClient blobServiceClient;

        const int MaxRetries = 3;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);
        static readonly TimeSpan DeltaBackOff = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Construct a blob storage client instance with hub name and connection string
        /// </summary>
        /// <param name="hubName">The hub name</param>
        /// <param name="connectionString">The connection string</param>
        public BlobStorageClient(string hubName, string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Invalid connection string", nameof(connectionString));
            }

            if (string.IsNullOrWhiteSpace(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            this.blobServiceClient = new BlobServiceClient(connectionString);

            // make the hub name lower case since it will be used as part of the prefix of the container name,
            // which only allows lower case letters
            this.containerNamePrefix = BlobStorageClientHelper.BuildContainerNamePrefix(hubName.ToLower());
        }

        /// <summary>
        /// Construct a blob storage client instance with hub name uri endpoint and a token credential
        /// </summary>
        /// <param name="hubName">The hub name</param>
        /// <param name="endpoint">Uri Endpoint for the blob store</param>
        /// <param name="credential">Token Credentials required for accessing the blob store</param>
        public BlobStorageClient(string hubName, Uri endpoint, TokenCredential credential)
        {
            if (string.IsNullOrWhiteSpace(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            if (endpoint == null)
            {
                throw new ArgumentException("Invalid endpoint", nameof(endpoint));
            }

            if (credential == null)
            {
                throw new ArgumentException("Invalid credentials", nameof(credential));
            }

            this.blobServiceClient = new BlobServiceClient(endpoint, credential);

            // make the hub name lower case since it will be used as part of the prefix of the container name,
            // which only allows lower case letters
            this.containerNamePrefix = BlobStorageClientHelper.BuildContainerNamePrefix(hubName.ToLower());
        }

        /// <summary>
        /// Upload the stream into the blob storage using the specified key.
        /// </summary>
        /// <param name="key">The key to uniquely locate and access the blob</param>
        /// <param name="stream">The stream to be uploaded</param>
        /// <returns></returns>
        public async Task UploadStreamBlobAsync(string key, Stream stream)
        {
            BlobStorageClientHelper.ParseKey(key, out string containerNameSuffix, out string blobName);
            var cloudBlob = await GetCloudBlobClientAsync(containerNameSuffix, blobName);
            await cloudBlob.UploadAsync(stream, overwrite: true);
        }

        /// <summary>
        /// Download the blob from the storage using key.
        /// </summary>
        /// <param name="key">The key to uniquely locate and access the blob</param>
        /// <returns>A downloaded stream</returns>
        public async Task<Stream> DownloadStreamAsync(string key)
        {
            BlobStorageClientHelper.ParseKey(key, out string containerNameSuffix, out string blobName);

            var cloudBlobClient = await GetCloudBlobClientAsync(containerNameSuffix, blobName);
            Stream targetStream = new MemoryStream();
            await cloudBlobClient.DownloadToAsync(targetStream);
            targetStream.Position = 0;
            return targetStream;
        }

        private async Task<BlobClient> GetCloudBlobClientAsync(string containerNameSuffix, string blobName)
        {
            string containerName = BlobStorageClientHelper.BuildContainerName(this.containerNamePrefix, containerNameSuffix);
            var cloudBlobContainerClient = this.blobServiceClient.GetBlobContainerClient(containerName);
            await cloudBlobContainerClient.CreateIfNotExistsAsync();
            return cloudBlobContainerClient.GetBlobClient(blobName);
        }

        /// <summary>
        /// List all containers of the blob storage, whose prefix is containerNamePrefix, i.e., {hubName}-dtfx.
        /// </summary>
        /// <returns>A list of Azure blob containers</returns>
        public async Task<IEnumerable<BlobContainerItem>> ListContainersAsync()
        {
            List<BlobContainerItem> results = new List<BlobContainerItem>();
            var response = this.blobServiceClient.GetBlobContainersAsync();
            await foreach (var container in response)
            {
                results.Add(container);
            }
            return results;
        }
        

        /// <summary>
        /// Delete all containers that are older than the input threshold date.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The specified date threshold</param>
        /// <returns></returns>
        public async Task DeleteExpiredContainersAsync(DateTime thresholdDateTimeUtc)
        {
            IEnumerable<BlobContainerItem> containers = await ListContainersAsync();
            var tasks = containers.Where(container => BlobStorageClientHelper.IsContainerExpired(container.Name, thresholdDateTimeUtc)).ToList().Select(container => this.blobServiceClient.DeleteBlobContainerAsync(container.Name));
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Delete blob containers with the containerNamePrefix as prefix.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteBlobStoreContainersAsync()
        {
            IEnumerable<BlobContainerItem> containers = await this.ListContainersAsync();
            var tasks = containers.ToList().Select(container => this.blobServiceClient.DeleteBlobContainerAsync(container.Name));
            await Task.WhenAll(tasks);
        }
    }
}
