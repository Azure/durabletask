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
    using Azure;
    using Azure.Data.Tables;

    /// <summary>
    /// Abstract class for composite entities for Azure table
    /// </summary>
    public abstract class AzureTableCompositeTableEntity : ITableEntity
    {
        /// <summary>
        /// Gets or sets the task timestamp on the entity
        /// </summary>
        public DateTime TaskTimeStamp { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// Gets or sets the entity partition key
        /// </summary>
        public string PartitionKey { get; set; }

        /// <summary>
        /// Gets or sets the entity row key
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// Gets or sets the row timestamp
        /// </summary>
        public DateTimeOffset? Timestamp { get; set; } = DateTimeOffset.Now;

        /// <summary>
        /// Gets or sets the entity etag
        /// </summary>
        public ETag ETag { get; set; }

        internal abstract IEnumerable<ITableEntity> BuildDenormalizedEntities();
    }
}