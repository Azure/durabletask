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
                    long pendingOrchestratorMessages = this.stats.PendingOrchestratorMessages.Value;
                    if (pendingOrchestratorMessages >= this.settings.ControlQueueBufferThreshold)
                    {
                        if (!pendingOrchestratorMessageLimitReached)
                        {
                            pendingOrchestratorMessageLimitReached = true;
                            AnalyticsEventSource.Log.PendingOrchestratorMessageLimitReached(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                pendingOrchestratorMessages,
                                Utils.ExtensionVersion);
                        }

                        await Task.Delay(TimeSpan.FromSeconds(1));
                        continue;
                    }

                    pendingOrchestratorMessageLimitReached = false;

                    try
                    {
                        IEnumerable<CloudQueueMessage> batch = await this.storageQueue.GetMessagesAsync(
                            this.settings.ControlQueueBatchSize,
                            this.settings.ControlQueueVisibilityTimeout,
                            this.settings.ControlQueueRequestOptions,
                            null /* operationContext */,
                            linkedCts.Token);

                        this.stats.StorageRequests.Increment();

                        if (!batch.Any())
                        {
                            if (!isWaitingForMoreMessages)
                            {
                                isWaitingForMoreMessages = true;
                                AnalyticsEventSource.Log.WaitingForMoreMessages(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.storageQueue.Name,
                                    Utils.ExtensionVersion);
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

                            batchMessages.Add(messageData);
                        });

                        this.backoffHelper.Reset();
                        
                        // Try to preserve insertion order when processing
                        IReadOnlyList<MessageData> sortedMessages = batchMessages.OrderBy(m => m, MessageOrderingComparer.Default).ToList();
                        foreach (MessageData message in sortedMessages)
                        {
                            AzureStorageOrchestrationService.TraceMessageReceived(
                                message,
                                this.storageAccountName,
                                this.settings.TaskHubName);
                        }

                        return sortedMessages;
                    }
                    catch (Exception e)
                    {
                        if (!linkedCts.IsCancellationRequested)
                        {
                            AnalyticsEventSource.Log.MessageFailure(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                string.Empty /* MessageId */,
                                string.Empty /* InstanceId */,
                                string.Empty /* ExecutionId */,
                                this.storageQueue.Name,
                                string.Empty /* EventType */,
                                0 /* TaskEventId */,
                                e.ToString(),
                                Utils.ExtensionVersion);

                            await this.backoffHelper.WaitAsync(linkedCts.Token);
                        }
                    }
                }

                this.IsReleased = true;
                return EmptyMessageList;
            }
        }

        public void Release()
        {
            this.releaseTokenSource.Cancel();
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
