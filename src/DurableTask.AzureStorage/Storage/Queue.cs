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
            this.queueClient = queueServiceClient.GetQueueClient(queueName);
        }

        public string Name => this.queueClient.Name;

        public Uri Uri => this.queueClient.Uri;

        public async Task<int> GetApproximateMessagesCountAsync(CancellationToken cancellationToken = default)
        {
            QueueProperties properties = await this.queueClient.GetPropertiesAsync(cancellationToken).DecorateFailure();
            return properties.ApproximateMessagesCount;
        }

        public async Task AddMessageAsync(string message, TimeSpan? visibilityDelay, Guid? clientRequestId = null, CancellationToken cancellationToken = default)
        {
            using IDisposable scope = OperationContext.CreateClientRequestScope(clientRequestId);
            await this.queueClient
                .SendMessageAsync(
                    message,
                    visibilityDelay,
                    TimeSpan.FromSeconds(-1), // Infinite time to live
                    cancellationToken)
                .DecorateFailure();

            this.stats.MessagesSent.Increment();
        }

        public async Task<UpdateReceipt> UpdateMessageAsync(QueueMessage queueMessage, TimeSpan visibilityTimeout, Guid? clientRequestId = null, CancellationToken cancellationToken = default)
        {
            using IDisposable scope = OperationContext.CreateClientRequestScope(clientRequestId);
            UpdateReceipt receipt = await this.queueClient
                .UpdateMessageAsync(
                    queueMessage.MessageId,
                    queueMessage.PopReceipt,
                    visibilityTimeout: visibilityTimeout,
                    cancellationToken: cancellationToken)
                .DecorateFailure();

            this.stats.MessagesUpdated.Increment();
            return receipt;
        }

        public async Task DeleteMessageAsync(QueueMessage queueMessage, Guid? clientRequestId = null, CancellationToken cancellationToken = default)
        {
            using IDisposable scope = OperationContext.CreateClientRequestScope(clientRequestId);
            await this.queueClient
                .DeleteMessageAsync(
                    queueMessage.MessageId,
                    queueMessage.PopReceipt,
                    cancellationToken)
                .DecorateFailure();

            this.stats.MessagesUpdated.Increment();
        }

        public async Task<QueueMessage?> GetMessageAsync(TimeSpan visibilityTimeout, CancellationToken cancellationToken = default)
        {
            QueueMessage message = await this.queueClient.ReceiveMessageAsync(visibilityTimeout, cancellationToken).DecorateFailure();

            if (message == null)
            {
                return null;
            }

            this.stats.MessagesRead.Increment();
            return message;
        }

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.queueClient.ExistsAsync(cancellationToken).DecorateFailure();
        }

        public async Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            // If we received null, then the response must have been a 409 (Conflict) and the queue must already exist
            Response response = await this.queueClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).DecorateFailure();
            return response != null;
        }

        public async Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            return await this.queueClient.DeleteIfExistsAsync(cancellationToken).DecorateFailure();
        }

        public async Task<IReadOnlyCollection<QueueMessage>> GetMessagesAsync(int batchSize, TimeSpan visibilityTimeout, CancellationToken cancellationToken = default)
        {
            QueueMessage[] messages = await this.queueClient.ReceiveMessagesAsync(batchSize, visibilityTimeout, cancellationToken).DecorateFailure();
            this.stats.MessagesRead.Increment(messages.Length);
            return messages;
        }

        public async Task<IReadOnlyCollection<PeekedMessage>> PeekMessagesAsync(int batchSize, CancellationToken cancellationToken = default)
        {
            PeekedMessage[] messages = await this.queueClient.PeekMessagesAsync(batchSize, cancellationToken).DecorateFailure();
            this.stats.MessagesRead.Increment(messages.Length);
            return messages;
        }

        public async Task<PeekedMessage?> PeekMessageAsync(CancellationToken cancellationToken = default)
        {
            return await this.queueClient.PeekMessageAsync(cancellationToken).DecorateFailure();
        }
    }
}
