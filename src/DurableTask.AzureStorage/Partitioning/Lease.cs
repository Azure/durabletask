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
    /// <summary>Contains partition ownership information.</summary>
    class Lease
    {
        /// <summary>Initializes a new instance of the <see cref="DurableTask.AzureStorage.Partitioning.Lease" /> class.</summary>
        public Lease()
        {
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="DurableTask.AzureStorage.Partitioning.Lease" /> class with the specified 
        /// <see cref="DurableTask.AzureStorage.Partitioning.Lease(DurableTask.AzureStorage.Partitioning.Lease)" /> value as reference.</summary> 
        /// <param name="source">The specified <see cref="DurableTask.AzureStorage.Partitioning.Lease(DurableTask.AzureStorage.Partitioning.Lease)" /> instance where its property values will be copied from.</param>
        public Lease(Lease source)
        {
            this.PartitionId = source.PartitionId;
            this.Owner = source.Owner;
            this.Token = source.Token;
            this.Epoch = source.Epoch;
        }

        /// <summary>Gets the ID of the partition to which this lease belongs.</summary>
        /// <value>The partition identifier.</value>
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
        public virtual bool IsExpired()
        {
            return false;
        }

        /// <summary>Determines whether this instance is equal to the specified object.</summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>true if this instance is equal to the specified object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            if (object.ReferenceEquals(this, obj))
            {
                return true;
            }

            Lease lease = obj as Lease;
            if (lease == null)
            {
                return false;
            }

            return string.Equals(this.PartitionId, lease.PartitionId);
        }

        /// <summary>Returns the hash code of the current instance.</summary>
        /// <returns>The hash code of the current instance.</returns>
        public override int GetHashCode()
        {
            return this.PartitionId.GetHashCode();
        }
    }
}
