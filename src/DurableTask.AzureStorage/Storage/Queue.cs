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
#nullable enable
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Queue;

    class Queue
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudQueueClient queueClient;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly CloudQueue cloudQueue;

        public Queue(AzureStorageClient azureStorageClient, CloudQueueClient queueClient, string queueName)
        {
            this.azureStorageClient = azureStorageClient;
            this.queueClient = queueClient;
            this.stats = this.azureStorageClient.Stats;
            this.Name = queueName;

            this.cloudQueue = this.queueClient.GetQueueReference(this.Name);
        }

        public string Name { get; }

        public Uri Uri => this.cloudQueue.Uri;

        public int? ApproximateMessageCount => this.cloudQueue.ApproximateMessageCount;

        public async Task AddMessageAsync(QueueMessage queueMessage, TimeSpan? visibilityDelay, Guid? clientRequestId = null)
        {
            // Infinite time to live
            TimeSpan? timeToLive = TimeSpan.FromSeconds(-1);
#if NET462
            // WindowsAzure.Storage 7.2.1 does not allow infinite time to live. Passing in null will default the time to live to 7 days.
            timeToLive = null;
#endif
            await this.azureStorageClient.MakeQueueStorageRequest(
                (context, cancellationToken) => this.cloudQueue.AddMessageAsync(
                    queueMessage.CloudQueueMessage,
                    timeToLive,
                    visibilityDelay,
                    null,
                    context),
                "Queue AddMessage",
                clientRequestId?.ToString());

            this.stats.MessagesSent.Increment();
        }

        public async Task UpdateMessageAsync(QueueMessage queueMessage, TimeSpan visibilityTimeout, Guid? clientRequestId = null)
        {
            await this.azureStorageClient.MakeQueueStorageRequest(
                (context, cancellationToken) => this.cloudQueue.UpdateMessageAsync(
                    queueMessage.CloudQueueMessage,
                    visibilityTimeout,
                    MessageUpdateFields.Visibility,
                    null,
                    context),
                "Queue UpdateMessage",
                clientRequestId?.ToString());

            this.stats.MessagesUpdated.Increment();
        }

        public async Task DeleteMessageAsync(QueueMessage queueMessage, Guid? clientRequestId = null)
        {
            await this.azureStorageClient.MakeQueueStorageRequest(
                (context, cancellationToken) => this.cloudQueue.DeleteMessageAsync(
                    queueMessage.CloudQueueMessage,
                    null,
                    context),
                "Queue DeleteMessage",
                clientRequestId?.ToString());
        }

        public async Task<QueueMessage?> GetMessageAsync(TimeSpan visibilityTimeout, CancellationToken callerCancellationToken)
        {
            var cloudQueueMessage = await this.azureStorageClient.MakeQueueStorageRequest<CloudQueueMessage>(
                async (context, timeoutCancellationToken) =>
                {
                    using (var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, timeoutCancellationToken))
                    {
                        return await this.cloudQueue.GetMessageAsync(
                            visibilityTimeout,
                            null,
                            context,
                            finalLinkedCts.Token);
                    }
                },
                "Queue GetMessage");

            if (cloudQueueMessage == null)
            {
                return null;
            }

            this.stats.MessagesRead.Increment();
            return new QueueMessage(cloudQueueMessage);
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeQueueStorageRequest<bool>(
                (context, cancellationToken) => this.cloudQueue.ExistsAsync(null, context, cancellationToken),
                "Queue Exists");
        }

        public async Task<bool> CreateIfNotExistsAsync()
        {
            return await this.azureStorageClient.MakeQueueStorageRequest<bool>(
                (context, cancellationToken) => this.cloudQueue.CreateIfNotExistsAsync(null, context, cancellationToken),
                "Queue Create");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeQueueStorageRequest<bool>(
                (context, cancellationToken) => this.cloudQueue.DeleteIfExistsAsync(null, context, cancellationToken),
                "Queue Delete");
        }

        public async Task<IEnumerable<QueueMessage>> GetMessagesAsync(int batchSize, TimeSpan visibilityTimeout, CancellationToken callerCancellationToken)
        {
            var cloudQueueMessages = await this.azureStorageClient.MakeQueueStorageRequest<IEnumerable<CloudQueueMessage>>(
                async (context, timeoutCancellationToken) =>
                {
                    using (var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, timeoutCancellationToken))
                    {
                        return await this.cloudQueue.GetMessagesAsync(
                            batchSize,
                            visibilityTimeout,
                            null,
                            context,
                            finalLinkedCts.Token);
                    }
                },
                "Queue GetMessages");

            var queueMessages = new List<QueueMessage>();
            foreach (CloudQueueMessage cloudQueueMessage in cloudQueueMessages)
            {
                queueMessages.Add(new QueueMessage(cloudQueueMessage));
                this.stats.MessagesRead.Increment();
            }

            return queueMessages;
        }

        public async Task FetchAttributesAsync()
        {
            await this.azureStorageClient.MakeQueueStorageRequest(
                (context, cancellationToken) => this.cloudQueue.FetchAttributesAsync(null, context, cancellationToken),
                "Queue FetchAttributes");
        }

        public async Task<IEnumerable<QueueMessage>> PeekMessagesAsync(int batchSize)
        {
            var cloudQueueMessages = await this.azureStorageClient.MakeQueueStorageRequest<IEnumerable<CloudQueueMessage>>(
                (context, cancellationToken) => this.cloudQueue.PeekMessagesAsync(batchSize, null, context, cancellationToken),
                "Queue PeekMessages");

            var queueMessages = new List<QueueMessage>();
            foreach (CloudQueueMessage cloudQueueMessage in cloudQueueMessages)
            {
                queueMessages.Add(new QueueMessage(cloudQueueMessage));
                this.stats.MessagesRead.Increment();
            }

            return queueMessages;
        }

        public async Task<QueueMessage?> PeekMessageAsync()
        {
            var queueMessage = await this.cloudQueue.PeekMessageAsync();
            return queueMessage == null ? null : new QueueMessage(queueMessage);
        }
    }
}
