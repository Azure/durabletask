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
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    class BlobLease
    {
        private readonly AzureStorageOrchestrationServiceStats stats;

        public BlobLease() { }

        public BlobLease(BlobLease blobLease)
        {
            this.PartitionId = blobLease.PartitionId;
            this.LeaseType = blobLease.LeaseType;
            this.Blob = blobLease.Blob;
            this.Owner = blobLease.Owner;
            this.Epoch = blobLease.Epoch;
            this.Token = blobLease.Token;
        }

        public BlobLease(
            string partitionId,
            CloudBlobDirectory leaseDirectory,
            string leaseType,
            AzureStorageOrchestrationServiceStats stats)
        {
            this.PartitionId = partitionId;
            this.LeaseType = leaseType;
            this.Blob = leaseDirectory.GetBlockBlobReference(partitionId);
            this.stats = stats;
        }

        /// <summary>
        /// The type of lease this is.
        /// </summary>
        [JsonIgnore]
        public string LeaseType { get; set; }

        /// <summary>Gets the ID of the partition to which this lease belongs.</summary>
        /// <value>The partition identifier.</value>
        [JsonIgnore]
        public string PartitionId { get; set; }

        /// <summary>Gets or sets the host owner for the partition.</summary>
        /// <value>The host owner of the partition.</value>
        public string Owner { get; set; }

        /// <summary>Gets or sets the lease token that manages concurrency between hosts. You can use this token to guarantee single access to any resource needed by the 
        /// <see cref="DurableTask.AzureStorage.AzureStorageOrchestrationService" /> object.</summary> 
        /// <value>The lease token.</value>
        public string Token { get; set; }

        /// <summary>Gets or sets the epoch year of the lease, which is a value 
        /// you can use to determine the most recent owner of a partition between competing nodes.</summary> 
        /// <value>The epoch year of the lease.</value>
        public long Epoch { get; set; }

        /// <summary>Determines whether the lease is expired.</summary>
        /// <returns>true if the lease is expired; otherwise, false.</returns>
        public bool IsExpired => this.Blob.Properties.LeaseState != LeaseState.Leased;


        [JsonIgnore]
        internal CloudBlockBlob Blob { get; private set; }

        public async Task DownloadLeaseAsync()
        {
            string serializedLease = await this.Blob.DownloadTextAsync();
            this.stats.StorageRequests.Increment();

            // Workaround: for some reason storage client reports incorrect blob properties after downloading the blob
            await this.Blob.FetchAttributesAsync();
            this.stats.StorageRequests.Increment();

            BlobLease lease = JsonConvert.DeserializeObject<BlobLease>(serializedLease);
            this.Epoch = lease.Epoch;
            this.Owner = lease.Owner;
            this.Token = lease.Token;
        }

        public override bool Equals(object obj)
        {
            return obj is BlobLease lease &&
                   LeaseType == lease.LeaseType &&
                   PartitionId == lease.PartitionId;
        }

        public override int GetHashCode()
        {
            int hashCode = 1696262853;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(LeaseType);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PartitionId);
            return hashCode;
        }
    }
}
