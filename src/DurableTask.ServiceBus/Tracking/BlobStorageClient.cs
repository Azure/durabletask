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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

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
        readonly CloudBlobClient blobClient;

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
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("Invalid connection string", nameof(connectionString));
            }

            if (string.IsNullOrEmpty(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            this.blobClient = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
            this.blobClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry(DeltaBackOff, MaxRetries);
            this.blobClient.DefaultRequestOptions.MaximumExecutionTime = MaximumExecutionTime;

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
            string containerNameSuffix;
            string blobName;
            BlobStorageClientHelper.ParseKey(key, out containerNameSuffix, out blobName);
            var cloudBlob = await this.GetCloudBlockBlobReferenceAsync(containerNameSuffix, blobName);
            await cloudBlob.UploadFromStreamAsync(stream);
        }

        /// <summary>
        /// Download the blob from the storage using key.
        /// </summary>
        /// <param name="key">The key to uniquely locate and access the blob</param>
        /// <returns>A downloaded stream</returns>
        public async Task<Stream> DownloadStreamAsync(string key)
        {
            string containerNameSuffix;
            string blobName;
            BlobStorageClientHelper.ParseKey(key, out containerNameSuffix, out blobName);

            var cloudBlob = await this.GetCloudBlockBlobReferenceAsync(containerNameSuffix, blobName);
            Stream targetStream = new MemoryStream();
            await cloudBlob.DownloadToStreamAsync(targetStream);
            targetStream.Position = 0;
            return targetStream;
        }

        async Task<ICloudBlob> GetCloudBlockBlobReferenceAsync(string containerNameSuffix, string blobName)
        {
            string containerName = BlobStorageClientHelper.BuildContainerName(this.containerNamePrefix, containerNameSuffix);
            var cloudBlobContainer = this.blobClient.GetContainerReference(containerName);
            await cloudBlobContainer.CreateIfNotExistsAsync();
            return cloudBlobContainer.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// List all containers of the blob storage, whose prefix is containerNamePrefix, i.e., {hubName}-dtfx.
        /// </summary>
        /// <returns>A list of Azure blob containers</returns>
        public IEnumerable<CloudBlobContainer> ListContainers()
        {
            return this.blobClient.ListContainers(this.containerNamePrefix);
        }

        /// <summary>
        /// Delete all contianers that are older than the input threshold date.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The specified date threshold</param>
        /// <returns></returns>
        public async Task DeleteExpiredContainersAsync(DateTime thresholdDateTimeUtc)
        {
            IEnumerable<CloudBlobContainer> containers = ListContainers();
            var tasks = containers.Where(container => BlobStorageClientHelper.IsContainerExpired(container.Name, thresholdDateTimeUtc)).ToList().Select(container => container.DeleteIfExistsAsync());
            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Delete blob containers with the containerNamePrefix as prefix.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteBlobStoreContainersAsync()
        {
            IEnumerable<CloudBlobContainer> containers = this.ListContainers();
            var tasks = containers.ToList().Select(container => container.DeleteIfExistsAsync());
            await Task.WhenAll(tasks);
        }
    }
}
