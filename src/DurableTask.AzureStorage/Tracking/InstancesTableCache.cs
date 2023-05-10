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

namespace DurableTask.AzureStorage.Tracking
{
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.Table;

    class InstancesTableCache
    {
        private const int NumberOfRecords = 2400;
        private MRUCache<string, DynamicTableEntity> mruCache;

        public InstancesTableCache()
        {
            this.mruCache = new MRUCache<string, DynamicTableEntity>(NumberOfRecords);
        }

        public void AddToCache(DynamicTableEntity entity)
        {
            entity.Properties.Remove("Input");
            this.mruCache.Add(entity.PartitionKey, entity);
        }

        public void UpdateCache(DynamicTableEntity entity)
        {
            if (this.mruCache.TryGet(entity.PartitionKey, out DynamicTableEntity storedEntity))
            {
                foreach (KeyValuePair<string, EntityProperty> property in entity.Properties)
                {
                    if (storedEntity.Properties.ContainsKey(property.Key))
                    {
                        storedEntity.Properties[property.Key] = property.Value;
                    }
                    else if (property.Key != "Input")
                    {
                        storedEntity.Properties.Add(property.Key, property.Value);
                    }
                }

                this.mruCache.Add(storedEntity.PartitionKey, storedEntity);
            }
        }

        public void RemoveFromCache(string instanceId)
        {
            this.mruCache.TryRemove(instanceId);
        }

        public bool TryGetInstanceInCache(string instanceId, out DynamicTableEntity entity)
        {
            return this.mruCache.TryGet(instanceId, out entity);
        }
    }
}
