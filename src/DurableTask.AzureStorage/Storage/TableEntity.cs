using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace DurableTask.AzureStorage.Storage
{
    class TableEntity
    {
        private DynamicTableEntity dynamicTableEntity;

        public TableEntity(string partitionKey, string rowKey)
        {
            this.dynamicTableEntity = new DynamicTableEntity(partitionKey, rowKey);
        }

        public IDictionary<string, EntityProperty> Properties
        { 
            get
            {
                return ConvertToDurableEntityProperty(this.dynamicTableEntity.Properties);
            }
            set 
            {
                this.dynamicTableEntity.Properties = ConvertFromDurableEntityProperty(value);
            }
        }

        private IDictionary<string, EntityProperty> ConvertToDurableEntityProperty(IDictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> properties)
        {
            IDictionary<string, EntityProperty> dictionary = new Dictionary<string, EntityProperty>();
            foreach(KeyValuePair<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> pair in properties)
            {
                dictionary.Add(pair.Key, new EntityProperty(pair.Value));
            }

            return dictionary;
        }

        private IDictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> ConvertFromDurableEntityProperty(IDictionary<string, EntityProperty> properties)
        {
            IDictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty> dictionary = new Dictionary<string, Microsoft.WindowsAzure.Storage.Table.EntityProperty>();
            foreach (KeyValuePair<string, EntityProperty> pair in properties)
            {
                dictionary.Add(pair.Key, pair.Value.CloudEntityProperty);
            }

            return dictionary;
        }

        public string PartitionKey => this.dynamicTableEntity.PartitionKey;
        public string RowKey => this.dynamicTableEntity.RowKey;
        public DateTimeOffset Timestamp => this.dynamicTableEntity.Timestamp;
        public string ETag => this.dynamicTableEntity.ETag;
    }
}
