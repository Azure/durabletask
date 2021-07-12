using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class EntityProperty
    {
        private string decompressedMessage;
        private DateTime utcNow;
        private DateTime? scheduledStartTime;
        private bool isFinalBatch;

        public EntityProperty(string decompressedMessage)
        {
            this.decompressedMessage = decompressedMessage;
        }

        public EntityProperty(DateTime utcNow)
        {
            this.utcNow = utcNow;
        }

        public EntityProperty(DateTime? scheduledStartTime)
        {
            this.scheduledStartTime = scheduledStartTime;
        }

        public EntityProperty(bool isFinalBatch)
        {
            this.isFinalBatch = isFinalBatch;
        }

        public string StringValue { get; internal set; }
        public DateTime? DateTime { get; internal set; }
        public object Int32Value { get; internal set; }
        public object PropertyType { get; internal set; }
    }
}
