using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace DurableTask.AzureStorage.Storage
{
    class QueueMessage
    {
        public CloudQueueMessage CloudQueueMessage { get; }

        public string Message { get; }

        public string Id { get; }

        public int DequeueCount => this.CloudQueueMessage.DequeueCount;

        public DateTimeOffset? InsertionTime => this.CloudQueueMessage.InsertionTime;

        public DateTimeOffset? NextVisibleTime => this.CloudQueueMessage.NextVisibleTime;

        public QueueMessage(CloudQueueMessage cloudQueueMessage)
        {
            this.CloudQueueMessage = cloudQueueMessage;
            this.Message = this.CloudQueueMessage.AsString;
            this.Id = this.CloudQueueMessage.Id;
        }

        public QueueMessage(string message)
        {
            this.CloudQueueMessage = new CloudQueueMessage(message);
            this.Message = this.CloudQueueMessage.AsString;
            this.Id = this.CloudQueueMessage.Id;
        }
    }
}
