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

namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    class ControlQueue : TaskHubQueue, IDisposable
    {
        static readonly List<MessageData> EmptyMessageList = new List<MessageData>();

        readonly CancellationTokenSource releaseTokenSource;
        readonly CancellationToken releaseCancellationToken;

        public ControlQueue(
            CloudQueue storageQueue,
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats,
            MessageManager messageManager)
            : base(storageQueue, settings, stats, messageManager)
        {
            this.releaseTokenSource = new CancellationTokenSource();
            this.releaseCancellationToken = this.releaseTokenSource.Token;
        }

        public bool IsReleased { get; private set; }

        protected override QueueRequestOptions QueueRequestOptions => this.settings.ControlQueueRequestOptions;

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
                        OperationContext context = new OperationContext { ClientRequestID = Guid.NewGuid().ToString() };
                        IEnumerable<CloudQueueMessage> batch = await TimeoutHandler.ExecuteWithTimeout(
                            "GetMessages",
                            context.ClientRequestID,
                            this.storageAccountName,
                            this.settings,
                            () =>
                            {
                                return this.storageQueue.GetMessagesAsync(
                                    this.settings.ControlQueueBatchSize,
                                    this.settings.ControlQueueVisibilityTimeout,
                                    this.settings.ControlQueueRequestOptions,
                                    context,
                                    linkedCts.Token);
                            });

                        this.stats.StorageRequests.Increment();

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
                        await batch.ParallelForEachAsync(async delegate (CloudQueueMessage queueMessage)
                        {
                            this.stats.MessagesRead.Increment();

                            MessageData messageData = await this.messageManager.DeserializeQueueMessageAsync(
                                queueMessage,
                                this.storageQueue.Name);

                            // Check to see whether we've already dequeued this message.
                            if (!this.stats.PendingOrchestratorMessages.TryAdd(queueMessage.Id, 1))
                            {
                                // This message is already loaded in memory and is therefore a duplicate.
                                // We will continue to process it because we need the updated pop receipt.
                                this.settings.Logger.DuplicateMessageDetected(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    messageData.TaskMessage.Event.EventType.ToString(),
                                    Utils.GetTaskEventId(messageData.TaskMessage.Event),
                                    queueMessage.Id,
                                    messageData.TaskMessage.OrchestrationInstance.InstanceId,
                                    messageData.TaskMessage.OrchestrationInstance.ExecutionId,
                                    this.Name,
                                    queueMessage.DequeueCount);
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

                this.IsReleased = true;
                return EmptyMessageList;
            }
        }

        public override Task AbandonMessageAsync(MessageData message, SessionBase session)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.Id, out _);
            return base.AbandonMessageAsync(message, session);
        }

        public override Task DeleteMessageAsync(MessageData message, SessionBase session)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.Id, out _);
            return base.DeleteMessageAsync(message, session);
        }

        public void Release()
        {
            this.releaseTokenSource.Cancel();

            // Note that we also set IsReleased to true when the dequeue loop ends, so this is
            // somewhat redundant. This one was added mostly to make tests run more predictably.
            this.IsReleased = true;
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
                if (x.OriginalQueueMessage.InsertionTime < y.OriginalQueueMessage.InsertionTime)
                {
                    return -1;
                }
                else if (x.OriginalQueueMessage.InsertionTime > y.OriginalQueueMessage.InsertionTime)
                {
                    return 1;
                }

                // As a tie-breaker, messages will be ordered based on client-side sequence numbers.
                if (x.SequenceNumber < y.SequenceNumber)
                {
                    return -1;
                }
                else if (x.SequenceNumber > y.SequenceNumber)
                {
                    return 1;
                }

                return 0;
            }
        }
    }
}
