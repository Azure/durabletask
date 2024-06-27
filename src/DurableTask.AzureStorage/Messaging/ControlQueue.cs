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
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage.Table;

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

        internal async Task<InstanceStatus?> FetchInstanceStatusInternalAsync(string instanceId, bool fetchInput)
        {
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            var queryCondition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceId = instanceId,
                FetchInput = fetchInput,
            };

            string instancesTableName = settings.InstanceTableName;

            var instancesTable = this.azureStorageClient.GetTableReference(instancesTableName);

            var tableEntitiesResponseInfo = await instancesTable.ExecuteQueryAsync(queryCondition.ToTableQuery<DynamicTableEntity>());

            var tableEntity = tableEntitiesResponseInfo.ReturnedEntities.FirstOrDefault();

            OrchestrationState? orchestrationState = null;
            if (tableEntity != null)
            {
                orchestrationState = await this.ConvertFromAsync(tableEntity);
            }

            this.settings.Logger.FetchedInstanceStatus(
                this.storageAccountName,
                this.settings.TaskHubName,
                instanceId,
                orchestrationState?.OrchestrationInstance.ExecutionId ?? string.Empty,
                orchestrationState?.OrchestrationStatus.ToString() ?? "NotFound",
                tableEntitiesResponseInfo.ElapsedMilliseconds);

            if (tableEntity == null || orchestrationState == null)
            {
                return null;
            }

            return new InstanceStatus(orchestrationState, tableEntity.ETag);
        }

        async Task<OrchestrationState> ConvertFromAsync(DynamicTableEntity tableEntity)
        {
            var orchestrationInstanceStatus = await CreateOrchestrationInstanceStatusAsync(tableEntity.Properties);
            var instanceId = KeySanitation.UnescapePartitionKey(tableEntity.PartitionKey);
            return await ConvertFromAsync(orchestrationInstanceStatus, instanceId);
        }

        private async Task<OrchestrationInstanceStatus> CreateOrchestrationInstanceStatusAsync(IDictionary<string, EntityProperty> properties)
        {
            var instance = new OrchestrationInstanceStatus();
            EntityProperty property;

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.ExecutionId), out property))
            {
                instance.ExecutionId = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Name), out property))
            {
                instance.Name = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Version), out property))
            {
                instance.Version = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Input), out property))
            {
                instance.Input = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Output), out property))
            {
                instance.Output = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.CustomStatus), out property))
            {
                instance.CustomStatus = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.CreatedTime), out property) && property.DateTime is { } createdTime)
            {
                instance.CreatedTime = createdTime;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.LastUpdatedTime), out property) && property.DateTime is { } lastUpdatedTime)
            {
                instance.LastUpdatedTime = lastUpdatedTime;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.CompletedTime), out property))
            {
                instance.CompletedTime = property.DateTime;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.RuntimeStatus), out property))
            {
                instance.RuntimeStatus = property.StringValue;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.ScheduledStartTime), out property))
            {
                instance.ScheduledStartTime = property.DateTime;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Generation), out property) && property.Int32Value is { } generation)
            {
                instance.Generation = generation;
            }

            if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Tags), out property) && property.StringValue is { Length: > 0 } tags)
            {
                instance.Tags = TagsSerializer.Deserialize(tags);
            }
            else if (properties.TryGetValue(nameof(OrchestrationInstanceStatus.Tags) + "BlobName", out property) && property.StringValue is { Length: > 0 } blob)
            {
                var blobContents = await messageManager.DownloadAndDecompressAsBytesAsync(blob);
                instance.Tags = TagsSerializer.Deserialize(blobContents);
            }

            return instance;
        }

        async Task<OrchestrationState> ConvertFromAsync(OrchestrationInstanceStatus orchestrationInstanceStatus, string instanceId)
        {
            var orchestrationState = new OrchestrationState();
            if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
            {
                // This is not expected, but could happen if there is invalid data in the Instances table.
                orchestrationState.OrchestrationStatus = (OrchestrationStatus)(-1);
            }

            orchestrationState.OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = orchestrationInstanceStatus.ExecutionId,
            };

            orchestrationState.Name = orchestrationInstanceStatus.Name;
            orchestrationState.Version = orchestrationInstanceStatus.Version;
            orchestrationState.Status = orchestrationInstanceStatus.CustomStatus;
            orchestrationState.CreatedTime = orchestrationInstanceStatus.CreatedTime;
            orchestrationState.CompletedTime = orchestrationInstanceStatus.CompletedTime.GetValueOrDefault();
            orchestrationState.LastUpdatedTime = orchestrationInstanceStatus.LastUpdatedTime;
            orchestrationState.Input = orchestrationInstanceStatus.Input;
            orchestrationState.Output = orchestrationInstanceStatus.Output;
            orchestrationState.ScheduledStartTime = orchestrationInstanceStatus.ScheduledStartTime;
            orchestrationState.Generation = orchestrationInstanceStatus.Generation;
            orchestrationState.Tags = orchestrationInstanceStatus.Tags;

            if (this.settings.FetchLargeMessageDataEnabled)
            {
                if (MessageManager.TryGetLargeMessageReference(orchestrationState.Input, out Uri blobUrl))
                {
                    string json = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobUrl);

                    // Depending on which blob this is, we interpret it differently.
                    if (blobUrl.AbsolutePath.EndsWith("ExecutionStarted.json.gz"))
                    {
                        // The downloaded content is an ExecutedStarted message payload that
                        // was created when the orchestration was started.
                        MessageData msg = this.messageManager.DeserializeMessageData(json);
                        if (msg?.TaskMessage?.Event is ExecutionStartedEvent startEvent)
                        {
                            orchestrationState.Input = startEvent.Input;
                        }
                        else
                        {
                            this.settings.Logger.GeneralWarning(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                $"Orchestration input blob URL '{blobUrl}' contained unrecognized data.",
                                instanceId);
                        }
                    }
                    else
                    {
                        // The downloaded content is the raw input JSON
                        orchestrationState.Input = json;
                    }
                }

                orchestrationState.Output = await this.messageManager.FetchLargeMessageIfNecessary(orchestrationState.Output);
            }

            return orchestrationState;
        }

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
                                // try to de-serialize message
                                messageData = await this.messageManager.DeserializeQueueMessageAsync(
                                    queueMessage,
                                    this.storageQueue.Name);

                                // if successful, check if it's a poison message. If so, we handle it
                                // and log metadata about it as the de-serialization succeeded.
                                await this.HandleIfPoisonMessageAsync(messageData);

                                var instanceId = messageData.TaskMessage.OrchestrationInstance.InstanceId;
                                InstanceStatus? status = await FetchInstanceStatusInternalAsync(instanceId, false);
                                if (status != null)
                                {
                                    if (status.State.OrchestrationStatus == OrchestrationStatus.Terminated)
                                    {
                                        // delete the message, the orchestration is terminated, we won't load it's history
                                        // TODO: possibly do not delete, but move to poison message table?
                                        await this.InnerQueue.DeleteMessageAsync(queueMessage);
                                    }
                                }
                            }
                            catch (Exception exception)
                            {
                                // Deserialization errors can be persistent, so we check if this is a poison message.
                                bool isPoisonMessage = await this.TryHandlingDeserializationPoisonMessage(queueMessage, exception);
                                if (isPoisonMessage)
                                {
                                    // we have already handled the poison message, so we move on.
                                    return;
                                }

                                // This is not a poison message (at least not yet), so we abandon it to retry later.
                                await this.AbandonMessageAsync(queueMessage, exception);
                                return;
                            }

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
        public Task AbandonMessageAsync(QueueMessage queueMessage, Exception exception)
        {

            // We have limited information about the details of the message
            // since we failed to deserialize it.
            this.settings.Logger.MessageFailure(
                this.storageAccountName,
                this.settings.TaskHubName,
                queueMessage.Id /* MessageId */,
                string.Empty /* InstanceId */,
                string.Empty /* ExecutionId */,
                this.storageQueue.Name,
                string.Empty /* EventType */,
                0 /* TaskEventId */,
                exception.ToString());

            this.stats.PendingOrchestratorMessages.TryRemove(queueMessage.Id, out _);
            return base.AbandonMessageAsync(
                queueMessage,
                taskMessage: null,
                instance: null,
                traceActivityId: null,
                sequenceNumber: -1);
        }

        public override Task AbandonMessageAsync(MessageData message, SessionBase? session = null)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.Id, out _);
            return base.AbandonMessageAsync(message, session);
        }

        public override Task DeleteMessageAsync(MessageData message, SessionBase? session = null)
        {
            this.stats.PendingOrchestratorMessages.TryRemove(message.OriginalQueueMessage.Id, out _);
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
                DateTimeOffset insertionTimeX = x.OriginalQueueMessage.InsertionTime.GetValueOrDefault();
                DateTimeOffset insertionTimeY = y.OriginalQueueMessage.InsertionTime.GetValueOrDefault();
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
