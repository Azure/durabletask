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

namespace DurableTask.AzureStorage.Partitioning
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;

    class BlobLease : Lease
    {
        private readonly string accountName;
        private readonly AzureStorageOrchestrationServiceStats stats;
        private readonly AzureStorageOrchestrationServiceSettings settings;

        public BlobLease() { }

        public BlobLease(BlobLease blobLease)
        {
            this.PartitionId = blobLease.PartitionId;
            this.Blob = blobLease.Blob;
            this.Owner = blobLease.Owner;
            this.Epoch = blobLease.Epoch;
            this.Token = blobLease.Token;
        }

        public BlobLease(
            string partitionId,
            CloudBlobDirectory leaseDirectory,
            string accountName,
            AzureStorageOrchestrationServiceStats stats,
            AzureStorageOrchestrationServiceSettings settings)
        {
            this.PartitionId = partitionId;
            this.Blob = leaseDirectory.GetBlockBlobReference(partitionId);
            this.accountName = accountName;
            this.stats = stats;
            this.settings = settings;
        }

        /// <summary>Determines whether the lease is expired.</summary>
        /// <returns>true if the lease is expired; otherwise, false.</returns>
        public override bool IsExpired() => this.Blob.Properties.LeaseState != LeaseState.Leased;


        // This property is a reference to the blob itself, so we do not want to serialize it when
        // writing/reading from the blob content.
        [JsonIgnore]
        internal CloudBlockBlob Blob { get; private set; }

        public override async Task DownloadLeaseAsync()
        {

            var serializedLease = await TimeoutHandler.ExecuteWithTimeout(
                operationName: "DownloadBlobLease",
                account: this.accountName,
                settings: this.settings,
                operation: async (context, cancelToken) =>
                {
                    // We use DownloadToStreamAsync() because unlike many of the other Download*() APIs, this fetches
                    // the attributes without a second API call. See https://stackoverflow.com/a/23749639/9035640
                    using (var memoryStream = new MemoryStream())
                    {
                        await this.Blob.DownloadToStreamAsync(memoryStream, null, null, context, cancelToken);
                        memoryStream.Position = 0;
                        using (StreamReader reader = new StreamReader(memoryStream, Encoding.UTF8))
                        {
                            return await reader.ReadToEndAsync();
                        }
                    }
                });

            this.stats.StorageRequests.Increment();

            BlobLease lease = JsonConvert.DeserializeObject<BlobLease>(serializedLease);
            this.Epoch = lease.Epoch;
            this.Owner = lease.Owner;
            this.Token = lease.Token;
        }
    }
}
