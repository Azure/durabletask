using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class DynamicTableEntity
    {
        private string sanitizedInstanceId;
        private string v;

        public DynamicTableEntity(string sanitizedInstanceId, string v)
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
