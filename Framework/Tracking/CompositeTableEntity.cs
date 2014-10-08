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

namespace DurableTask.Tracking
{
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public abstract class CompositeTableEntity : ITableEntity
    {
        public DateTime TaskTimeStamp { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        public abstract void ReadEntity(IDictionary<string, EntityProperty> properties,
            OperationContext operationContext);

        public abstract IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext);
        internal abstract IEnumerable<ITableEntity> BuildDenormalizedEntities();

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