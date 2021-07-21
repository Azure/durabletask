using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace DurableTask.AzureStorage.Storage
{
    class TableEntity
    {

        public DynamicTableEntity DynamicTableEntity { get; }

        private string sanitizedInstanceId;
        private string v;

        public TableEntity(string sanitizedInstanceId, string v)
        {
            this.sanitizedInstanceId = sanitizedInstanceId;
            this.v = v;
        }

        public string PartitionKey { get; internal set; }
        public string RowKey { get; internal set; }
        public IDictionary<string, EntityProperty> Properties { get; }
        public object Timestamp { get; internal set; }
        public string ETag { get; internal set; }
    }
}
