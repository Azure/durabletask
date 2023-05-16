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
    using Azure.Data.Tables;
    using System;
    using Azure;

    /// <summary>
    /// This class defines the lease that will be saved in the Table storage.
    /// </summary>
    public class TableLease : ITableEntity
    {
        /// <summary>
        /// Empty string. Not used for now.
        /// </summary>
        public string PartitionKey { get; set; } = null;

        /// <summary>
        /// The name of the partition/control queue.
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// The current owner name for this lease. 
        /// </summary>
        public string CurrentOwner { get; set; }

        /// <summary>
        /// The name of the worker that is stealing, or null if nobody is trying to steal it.
        /// </summary>
        public string NextOwner { get; set; }

        /// <summary>
        /// The timestamp at which the partition was originally acquired by this worker. 
        /// </summary>
        public DateTime? OwnedSince { get; set; }

        /// <summary>
        /// The timestamp at which the partition was last renewed.
        /// </summary>
        public DateTime? LastRenewal { get; set; }

        /// <summary>
        /// The timestamp at which the partition lease expires.
        /// </summary>
        public DateTime? ExpiresAt { get; set; }

        /// <summary>
        /// True if the partition is being drained; False otherwise.
        /// </summary>
        public bool IsDraining { get; set; } = false;

        /// <summary>
        /// Required atrribute of Azure.Data.Tables storage entity. Not used. 
        /// </summary>
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// Unique identifier used to version entities and ensure concurrency safety in Azure.Data.Tables.
        /// </summary>
        public ETag ETag { get; set; }
    }
}
