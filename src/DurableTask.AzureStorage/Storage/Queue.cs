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
    using Azure;
    using Azure.Storage.Queues;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Monitoring;

    class Queue
    {
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly QueueClient queueClient;

        public Queue(AzureStorageClient azureStorageClient, QueueServiceClient queueServiceClient, string queueName)
        {
            this.azureStorageClient = azureStorageClient;
            this.stats = this.azureStorageClient.Stats;
            this.Name = queueName;

            this.queueClient = queueServiceClient.GetQueueClient(this.Name);
        }

        public string Name { get; }

        public Uri Uri => this.queueClient.Uri;

        public async Task<int> GetApproximateMessagesCountAsync()
        {
            QueueProperties properties = await this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.GetPropertiesAsync(cancellationToken),
                "Queue GetProperties");

            return properties.ApproximateMessagesCount;
        }

        public async Task AddMessageAsync(string message, TimeSpan? visibilityDelay, Guid? clientRequestId = null)
        {
            await this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.SendMessageAsync(
                    message,
                    visibilityDelay,
                    TimeSpan.FromSeconds(-1), // Infinite time to live
                    cancellationToken),
                "Queue AddMessage",
                clientRequestId?.ToString());

            this.stats.MessagesSent.Increment();
        }

        public async Task UpdateMessageAsync(QueueMessage queueMessage, TimeSpan visibilityTimeout, Guid? clientRequestId = null)
        {
            await this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.UpdateMessageAsync(
                    queueMessage.MessageId,
                    queueMessage.PopReceipt,
                    visibilityTimeout: visibilityTimeout,
                    cancellationToken: cancellationToken),
                "Queue UpdateMessage",
                clientRequestId?.ToString());

            this.stats.MessagesUpdated.Increment();
        }

        public Task DeleteMessageAsync(QueueMessage queueMessage, Guid? clientRequestId = null) =>
            this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.DeleteMessageAsync(
                    queueMessage.MessageId,
                    queueMessage.PopReceipt,
                    cancellationToken),
                "Queue DeleteMessage",
                clientRequestId?.ToString());

        public async Task<QueueMessage?> GetMessageAsync(TimeSpan visibilityTimeout, CancellationToken callerCancellationToken)
        {
            QueueMessage message = await this.azureStorageClient.MakeQueueStorageRequest(
                async timeoutCancellationToken =>
                {
                    using var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, timeoutCancellationToken);
                    return await this.queueClient.ReceiveMessageAsync(visibilityTimeout, finalLinkedCts.Token);
                },
                "Queue ReceiveMessage");

            if (message == null)
            {
                return null;
            }

            this.stats.MessagesRead.Increment();
            return message;
        }

        public Task<bool> ExistsAsync() =>
            this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.ExistsAsync(cancellationToken),
                "Queue Exists");

        public async Task<bool> CreateIfNotExistsAsync()
        {
            Response response = await this.azureStorageClient.GetQueueStorageRequestResponse(
                cancellationToken => this.queueClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken),
                "Queue Create");

            // If we received null, then the response must have been a 409 (Conflict)
            // and the queue must already exist
            return response != null;
        }

        public Task<bool> DeleteIfExistsAsync() =>
            this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.DeleteIfExistsAsync(cancellationToken),
                "Queue Delete");

        public async Task<IReadOnlyCollection<QueueMessage>> GetMessagesAsync(int batchSize, TimeSpan visibilityTimeout, CancellationToken callerCancellationToken)
        {
            QueueMessage[] messages = await this.azureStorageClient.MakeQueueStorageRequest(
                async cancellationToken =>
                {
                    using var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, cancellationToken);
                    return await this.queueClient.ReceiveMessagesAsync(batchSize, visibilityTimeout, finalLinkedCts.Token);
                },
                "Queue GetMessages");

            this.stats.MessagesRead.Increment(messages.Length);
            return messages;
        }

        public async Task<IReadOnlyCollection<PeekedMessage>> PeekMessagesAsync(int batchSize)
        {
            PeekedMessage[] messages = await this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.PeekMessagesAsync(batchSize, cancellationToken),
                "Queue PeekMessages");

            this.stats.MessagesRead.Increment(messages.Length);
            return messages;
        }

        public Task<PeekedMessage?> PeekMessageAsync() =>
            this.azureStorageClient.MakeQueueStorageRequest(
                cancellationToken => this.queueClient.PeekMessageAsync(cancellationToken),
                "Queue PeekMessage");
    }
}
