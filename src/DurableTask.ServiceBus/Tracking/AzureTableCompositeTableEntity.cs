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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

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
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.Now;

        /// <summary>
        /// Gets or sets the entity etag
        /// </summary>
        public string ETag { get; set; }


        /// <summary>
        /// Read an entity properties based on the supplied dictionary or entity properties
        /// </summary>
        /// <param name="properties">Dictionary of properties to read for the entity</param>
        /// <param name="operationContext">The operation context</param>
        public abstract void ReadEntity(IDictionary<string, EntityProperty> properties,
            OperationContext operationContext);


        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        /// <param name="operationContext">The operation context</param>
        public abstract IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext);

        internal abstract IEnumerable<ITableEntity> BuildDenormalizedEntities();


        /// <summary>
        /// 
        /// </summary>
        protected T GetValue<T>(string key, IDictionary<string, EntityProperty> properties,
            Func<EntityProperty, T> extract)
        {
            EntityProperty ep;
            if (!properties.TryGetValue(key, out ep))
            {
                return default(T);
            }
            return extract(ep);
        }
    }
}