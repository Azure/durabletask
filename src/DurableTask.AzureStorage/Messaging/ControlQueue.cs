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
namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Storage;

    class ControlQueue : TaskHubQueue, IDisposable
    {
        static readonly List<MessageData> EmptyMessageList = new List<MessageData>();

        readonly CancellationTokenSource releaseTokenSource;
        readonly CancellationToken releaseCancellationToken;
        private readonly AzureStorageOrchestrationServiceStats stats;

        public ControlQueue(
            AzureStorageClient azureStorageClient,
            string queueName,
            MessageManager messageManager)
            : base(azureStorageClient, queueName, messageManager)
        {
            this.releaseTokenSource = new CancellationTokenSource();
            this.releaseCancellationToken = this.releaseTokenSource.Token;
            this.stats = this.azureStorageClient.Stats;
        }

        public bool IsReleased { get; private set; }

        protected override TimeSpan MessageVisibilityTimeout => this.settings.ControlQueueVisibilityTimeout;

        public async Task<IReadOnlyList<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
        {
            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(this.releaseCancellationToken, cancellationToken))
            {
                bool pendingOrchestratorMessageLimitReached = false;
                bool isWaitingForMoreMessages = false;

                while (!linkedCts.IsCancellationRequested)
                {
                    // Pause dequeuing if the total number of locked messages gets too high.
                    long pendingOrchestratorMessages = this.stats.PendingOrchestratorMessages.Count;
                    if (pendingOrchestratorMessages >= this.settings.ControlQueueBufferThreshold)
                    {
                        if (!pendingOrchestratorMessageLimitReached)
                        {
                            pendingOrchestratorMessageLimitReached = true;
                            this.settings.Logger.PendingOrchestratorMessageLimitReached(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.Name,
                                pendingOrchestratorMessages);
                        }

                        await Task.Delay(TimeSpan.FromSeconds(1));
                        continue;
                    }

                    pendingOrchestratorMessageLimitReached = false;

                    try
                    {
                        IEnumerable<QueueMessage> batch = await this.storageQueue.GetMessagesAsync(
                            this.settings.ControlQueueBatchSize,
                            this.settings.ControlQueueVisibilityTimeout,
                            linkedCts.Token);

                        if (!batch.Any())
                        {
                            if (!isWaitingForMoreMessages)
                            {
                                isWaitingForMoreMessages = true;
                                this.settings.Logger.WaitingForMoreMessages(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.storageQueue.Name);
                            }

                            await this.backoffHelper.WaitAsync(linkedCts.Token);
                            continue;
                        }

                        isWaitingForMoreMessages = false;

                        var batchMessages = new ConcurrentBag<MessageData>();
                        await batch.ParallelForEachAsync(async delegate (QueueMessage queueMessage)
                        {
                            MessageData messageData;
                            try
                            {
                                messageData = await this.messageManager.DeserializeQueueMessageAsync(
                                    queueMessage,
                                    this.storageQueue.Name);
                            }
                            catch (Exception e)
                            {
                                // We have limited information about the details of the message
                                // since we failed to deserialize it.
                                this.settings.Logger.MessageFailure(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    queueMessage.MessageId /* MessageId */,
                                    string.Empty /* InstanceId */,
                                    string.Empty /* ExecutionId */,
                                    this.storageQueue.Name,
                                    string.Empty /* EventType */,
                                    0 /* TaskEventId */,
                                    e.ToString());

                                // Abandon the message so we can try it again later.
                                // Note: We will fetch the message again from the queue before retrying, so no need to read the receipt
                                _ = await this.AbandonMessageAsync(queueMessage);
                                return;
                            }

                            // Check to see whether we've already dequeued this message.
                            if (!this.stats.PendingOrchestratorMessages.TryAdd(queueMessage.MessageId, 1))
                            {
                                // This message is already loaded in memory and is therefore a duplicate.
                                // We will continue to process it because we need the updated pop receipt.
                                this.settings.Logger.DuplicateMessageDetected(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    messageData.TaskMessage.Event.EventType.ToString(),
                                    Utils.GetTaskEventId(messageData.TaskMessage.Event),
                                    queueMessage.MessageId,
                                    messageData.TaskMessage.OrchestrationInstance.InstanceId,
                                    messageData.TaskMessage.OrchestrationInstance.ExecutionId,
                                    this.Name,
                                    queueMessage.DequeueCount,
                                    queueMessage.PopReceipt);
                            }

                            batchMessages.Add(messageData);
                        });

                        this.backoffHelper.Reset();
                        
                        // Try to preserve insertion order when processing
                        IReadOnlyList<MessageData> sortedMessages = batchMessages.OrderBy(m => m, MessageOrderingComparer.Default).ToList();
                        foreach (MessageData message in sortedMessages)
                        {
                            AzureStorageOrchestrationService.TraceMessageReceived(
                                this.settings,
                                message,
                                this.storageAccountName);
                        }

                        return sortedMessages;
                    }
                    catch (Exception e)
                    {
                        if (!linkedCts.IsCancellationRequested)
                        {
                            this.settings.Logger.MessageFailure(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                string.Empty /* MessageId */,
                                string.Empty /* InstanceId */,
                                string.Empty /* ExecutionId */,
                                this.storageQueue.Name,
                                string.Empty /* EventType */,
                                0 /* TaskEventId */,
                                e.ToString());

                            await this.backoffHelper.WaitAsync(linkedCts.Token);
                        }
                    }
                }

                this.Release(cancellationToken.IsCancellationRequested ? CloseReason.Shutdown : CloseReason.LeaseLost, $"ControlQueue GetMessagesAsync cancelled by: {(this.releaseCancellationToken.IsCancellationRequested ? "control queue released token cancelled" : "")} {(cancellationToken.IsCancellationRequested ? "shutdown token cancelled" : "")}");
                return EmptyMessageList;
            }
        }

        // This overload is intended for cases where we aren't able to deserialize an instance of MessageData.
        public Task<UpdateReceipt?> AbandonMessageAsync(QueueMessage queueMessage)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(queueMessage.MessageId, out _);
            return base.AbandonMessageAsync(
                queueMessage,
                taskMessage: null,
                instance: null,
                traceActivityId: null,
                sequenceNumber: -1);
        }

        public override Task AbandonMessageAsync(MessageData message, SessionBase? session = null)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.MessageId, out _);
            return base.AbandonMessageAsync(message, session);
        }

        public override Task DeleteMessageAsync(MessageData message, SessionBase? session = null)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.MessageId, out _);
            return base.DeleteMessageAsync(message, session);
        }

        public void Release(CloseReason? reason, string caller)
        {
            if (!this.IsReleased)
            {
                this.releaseTokenSource.Cancel();

                this.IsReleased = true;

                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    this.Name,
                    $"{caller} is releasing partition {this.Name} for reason: {reason}");
            }
        }

        public virtual void Dispose()
        {
            this.releaseTokenSource.Dispose();
        }

        class MessageOrderingComparer : IComparer<MessageData>
        {
            public static readonly MessageOrderingComparer Default = new MessageOrderingComparer();

            public int Compare(MessageData x, MessageData y)
            {
                // Azure Storage is the ultimate authority on the order in which messages were received.
                // Insertion time only has full second precision, however, so it's not always useful.
                DateTimeOffset insertionTimeX = x.OriginalQueueMessage.InsertedOn.GetValueOrDefault();
                DateTimeOffset insertionTimeY = y.OriginalQueueMessage.InsertedOn.GetValueOrDefault();
                if (insertionTimeX != insertionTimeY)
                {
                    return insertionTimeX.CompareTo(insertionTimeY);
                }

                // As a tie-breaker, messages will be ordered based on client-side sequence numbers.
                if (x.SequenceNumber != y.SequenceNumber)
                {
                    return x.SequenceNumber.CompareTo(y.SequenceNumber);
                }

                return 0;
            }
        }
    }
}
