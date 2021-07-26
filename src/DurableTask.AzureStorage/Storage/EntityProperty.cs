using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class EntityProperty
    {
        public EntityProperty(string decompressedMessage)
        {
            this.CloudEntityProperty = new Microsoft.WindowsAzure.Storage.Table.EntityProperty(decompressedMessage);
        }

        public EntityProperty(DateTime utcNow)
        {
            this.CloudEntityProperty = new Microsoft.WindowsAzure.Storage.Table.EntityProperty(utcNow);
        }

        public EntityProperty(DateTime? scheduledStartTime)
        {
            this.CloudEntityProperty = new Microsoft.WindowsAzure.Storage.Table.EntityProperty(scheduledStartTime);
        }

        public EntityProperty(bool isFinalBatch)
        {
            this.CloudEntityProperty = new Microsoft.WindowsAzure.Storage.Table.EntityProperty(isFinalBatch);
        }

        public EntityProperty(Microsoft.WindowsAzure.Storage.Table.EntityProperty entityProperty)
        {
            this.CloudEntityProperty = entityProperty;
        }

        public Microsoft.WindowsAzure.Storage.Table.EntityProperty CloudEntityProperty { get; }

        public string StringValue => this.CloudEntityProperty.StringValue;
        public DateTime? DateTime => this.CloudEntityProperty.DateTime;
        public object Int32Value => this.CloudEntityProperty.Int32Value;
        public object PropertyType => this.CloudEntityProperty.PropertyType;
    }
}
