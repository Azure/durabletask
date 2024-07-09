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
    using System.Reflection;
    using System.Runtime.ExceptionServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using DurableTask.Core.History;


    abstract class TaskHubQueue
    {
        static long messageSequenceNumber;

        protected readonly AzureStorageClient azureStorageClient;
        protected readonly Queue storageQueue;
        protected readonly MessageManager messageManager;
        protected readonly string storageAccountName;
        protected readonly AzureStorageOrchestrationServiceSettings settings;
        protected readonly BackoffPollingHelper backoffHelper;

        public TaskHubQueue(
            AzureStorageClient azureStorageClient,
            string queueName,
            MessageManager messageManager)
        {
            this.azureStorageClient = azureStorageClient;
            this.messageManager = messageManager;
            this.storageAccountName = azureStorageClient.QueueAccountName;
            this.settings = azureStorageClient.Settings;

            this.storageQueue = this.azureStorageClient.GetQueueReference(queueName);

            TimeSpan minPollingDelay = TimeSpan.FromMilliseconds(50);
            TimeSpan maxPollingDelay = this.settings.MaxQueuePollingInterval;
            if (maxPollingDelay < minPollingDelay)
            {
                maxPollingDelay = minPollingDelay;
            }

            this.backoffHelper = new BackoffPollingHelper(minPollingDelay, maxPollingDelay);
        }

        public string Name => this.storageQueue.Name;

        public Uri Uri => this.storageQueue.Uri;

        protected abstract TimeSpan MessageVisibilityTimeout { get; }

        // Intended only for use by unit tests
        internal Queue InnerQueue => this.storageQueue;

        /// <summary>
        /// Adds message to a queue
        /// </summary>
        /// <param name="message">Instance of <see cref="TaskMessage"/></param>
        /// <param name="sourceSession">Instance of <see cref="SessionBase"/></param>
        /// <returns></returns>
        public Task AddMessageAsync(TaskMessage message, SessionBase sourceSession)
        {
            return this.AddMessageAsync(message, sourceSession.Instance, sourceSession);
        }

        /// <summary>
        /// Adds message to a queue
        /// </summary>
        /// <param name="message">Instance of <see cref="TaskMessage"/></param>
        /// <param name="sourceInstance">Instnace of <see cref="OrchestrationInstance"/></param>
        /// <returns></returns>
        public Task<MessageData> AddMessageAsync(TaskMessage message, OrchestrationInstance sourceInstance)
        {
            return this.AddMessageAsync(message, sourceInstance, session: null);
        }

        async Task<MessageData> AddMessageAsync(TaskMessage taskMessage, OrchestrationInstance sourceInstance, SessionBase? session)
        {
            MessageData data;
            try
            {
                // We transfer to a new trace activity ID every time a new outbound queue message is created.
                Guid outboundTraceActivityId = Guid.NewGuid();
                data = new MessageData(
                    taskMessage,
                    outboundTraceActivityId,
                    this.storageQueue.Name,
                    session?.GetCurrentEpisode(),
                    sourceInstance);
                data.SequenceNumber = Interlocked.Increment(ref messageSequenceNumber);

                // Inject Correlation TraceContext on a queue.
                CorrelationTraceClient.Propagate(
                    () => { data.SerializableTraceContext = GetSerializableTraceContext(taskMessage); });
                
                string rawContent = await this.messageManager.SerializeMessageDataAsync(data);

                this.settings.Logger.SendingMessage(
                    outboundTraceActivityId,
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(taskMessage.Event),
                    sourceInstance.InstanceId,
                    sourceInstance.ExecutionId,
                    data.TotalMessageSizeBytes,
                    data.QueueName /* PartitionId */,
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId,
                    data.SequenceNumber,
                    data.Episode.GetValueOrDefault(-1));

                await this.storageQueue.AddMessageAsync(
                    rawContent,
                    GetVisibilityDelay(taskMessage),
                    session?.TraceActivityId);

                // Wake up the queue polling thread
                this.backoffHelper.Reset();
            }
            catch (DurableTaskStorageException e)
            {
                this.settings.Logger.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    string.Empty /* MessageId */,
                    sourceInstance.InstanceId,
                    sourceInstance.ExecutionId,
                    this.storageQueue.Name,
                    taskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(taskMessage.Event),
                    e.ToString());
                throw;
            }

            return data;
        }

        public async Task HandleIfPoisonMessageAsync(MessageData messageData)
        {
            QueueMessage queueMessage = messageData.OriginalQueueMessage;
            int maxThreshold = this.settings.PoisonMessageDeuqueCountThreshold;

            if (queueMessage.DequeueCount > maxThreshold)
            {
                // Create the poison message table if it doesn't exist
                string poisonMessageTableName = this.settings.TaskHubName.ToLowerInvariant() + "Poison";
                Table poisonMessagesTable = this.azureStorageClient.GetTableReference(poisonMessageTableName);
                await poisonMessagesTable.CreateIfNotExistsAsync();

                // provide guidance, which is backend-specific
                string guidance = $"Queue message ID '{queueMessage.MessageId}' was dequeued {queueMessage.DequeueCount} times," +
                    $" which is greater than the threshold poison message threshold ({maxThreshold}). " +
                    $"The message has been moved to the '{poisonMessageTableName}' table for manual review. " +
                    $"This will fail the consuming orchestrator, activity, or entity";
                messageData.TaskMessage.Event.PoisonGuidance = guidance;

                // Add the message to the poison message table
                TableEntity tableEntity = new TableEntity(queueMessage.MessageId, this.Name)
                {
                    ["RawMessage"] = queueMessage.Body,
                    ["Reason"] = guidance
                };

                await poisonMessagesTable.InsertEntityAsync(tableEntity);

                // Delete the message from the queue
                await this.storageQueue.DeleteMessageAsync(queueMessage);

                // Since isPoison is `true`, we'll override the deserialized message
                messageData.TaskMessage.Event.IsPoison = true;

                this.settings.Logger.PoisonMessageDetected(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    messageData.TaskMessage.Event.EventType.ToString(),
                    messageData.TaskMessage.Event.EventId,
                    messageData.OriginalQueueMessage.MessageId,
                    messageData.TaskMessage.OrchestrationInstance.InstanceId,
                    messageData.TaskMessage.OrchestrationInstance.ExecutionId,
                    this.Name,
                    messageData.OriginalQueueMessage.DequeueCount);
            }
        }

        public async Task<bool> TryHandlingDeserializationPoisonMessage(QueueMessage queueMessage, Exception deserializationException)
        {
            var maxThreshold = this.settings.PoisonMessageDeuqueCountThreshold;
            bool isPoisonMessage = queueMessage.DequeueCount > maxThreshold;

            if (isPoisonMessage)
            {
                isPoisonMessage = true;
                string guidance = $"Queue message ID '{queueMessage.MessageId}' was dequeued {queueMessage.DequeueCount} times," +
                    $" which is greater than the threshold poison message threshold ({maxThreshold}). " +
                    $"A de-serialization error ocurred: \n {deserializationException}";

                // Create poison message table if it doesn't exist and add the poison message
                TableEntity tableEntity = new TableEntity(queueMessage.MessageId, this.Name)
                {
                    ["RawMessage"] = queueMessage.Body,
                    ["Reason"] = guidance
                };

                string poisonMessageTableName = this.settings.TaskHubName.ToLowerInvariant() + "Poison";
                Table poisonMessagesTable = this.azureStorageClient.GetTableReference(poisonMessageTableName);
                await poisonMessagesTable.CreateIfNotExistsAsync();

                await poisonMessagesTable.InsertEntityAsync(tableEntity);

                // Delete the message from the queue
                await this.storageQueue.DeleteMessageAsync(queueMessage);

                this.settings.Logger.PoisonMessageDetected(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    string.Empty,
                    0,
                    string.Empty,
                    string.Empty,
                    string.Empty,
                    this.Name,
                    queueMessage.DequeueCount);
            }
            return isPoisonMessage;
        }


        static string? GetSerializableTraceContext(TaskMessage taskMessage)
        {
            TraceContextBase traceContext = CorrelationTraceContext.Current;
            if (traceContext != null)
            {
                if (CorrelationTraceContext.GenerateDependencyTracking)
                {
                    PropertyInfo nameProperty = taskMessage.Event.GetType().GetProperty("Name");
                    string name = (nameProperty == null) ? TraceConstants.DependencyDefault : (string)nameProperty.GetValue(taskMessage.Event);

                    var dependencyTraceContext = TraceContextFactory.Create($"{TraceConstants.Orchestrator} {name}");
                    dependencyTraceContext.TelemetryType = TelemetryType.Dependency;
                    dependencyTraceContext.SetParentAndStart(traceContext);
                    dependencyTraceContext.OrchestrationTraceContexts.Push(dependencyTraceContext);
                    return dependencyTraceContext.SerializableTraceContext;
                }
                else
                {
                    return traceContext.SerializableTraceContext;
                }
            }

            // TODO this might not happen, however, in case happen, introduce NullObjectTraceContext.
            return null; 
        }

        static TimeSpan? GetVisibilityDelay(TaskMessage taskMessage)
        {
            TimeSpan? initialVisibilityDelay = null;
            if (taskMessage.Event is TimerFiredEvent timerEvent)
            {
                initialVisibilityDelay = timerEvent.FireAt.Subtract(DateTime.UtcNow);
                if (initialVisibilityDelay < TimeSpan.Zero)
                {
                    initialVisibilityDelay = TimeSpan.Zero;
                }
            }
            else if (taskMessage.Event is ExecutionStartedEvent executionStartedEvent)
            {
                if (executionStartedEvent.ScheduledStartTime.HasValue)
                {
                    initialVisibilityDelay = executionStartedEvent.ScheduledStartTime.Value.Subtract(DateTime.UtcNow);
                    if (initialVisibilityDelay < TimeSpan.Zero)
                    {
                        initialVisibilityDelay = TimeSpan.Zero;
                    }
                }
            }

            // Special functionality for entity messages with a delivery delay 
            if (DurableTask.Core.Common.Entities.IsDelayedEntityMessage(taskMessage, out DateTime due))
            {
                initialVisibilityDelay = due - DateTime.UtcNow;
                if (initialVisibilityDelay < TimeSpan.Zero)
                {
                    initialVisibilityDelay = TimeSpan.Zero;
                }
            }

            return initialVisibilityDelay;
        }

        public virtual async Task AbandonMessageAsync(MessageData message, SessionBase? session = null)
        {
            QueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;
            OrchestrationInstance instance = taskMessage.OrchestrationInstance;
            long sequenceNumber = message.SequenceNumber;

            UpdateReceipt? receipt = await this.AbandonMessageAsync(
                queueMessage,
                taskMessage,
                instance,
                session?.TraceActivityId,
                sequenceNumber);

            // If we've successfully abandoned the message, update the pop receipt
            // (even though we'll likely no longer interact with this message)
            if (receipt is not null)
            {
                message.Update(receipt);
            }
        }

        protected async Task<UpdateReceipt?> AbandonMessageAsync(
            QueueMessage queueMessage,
            TaskMessage? taskMessage,
            OrchestrationInstance? instance,
            Guid? traceActivityId,
            long sequenceNumber)
        {
            string instanceId = instance?.InstanceId ?? string.Empty;
            string executionId = instance?.ExecutionId ?? string.Empty;
            string eventType = taskMessage?.Event.EventType.ToString() ?? string.Empty;
            int taskEventId = taskMessage != null ? Utils.GetTaskEventId(taskMessage.Event) : -1;

            // Exponentially backoff a given queue message until a maximum visibility delay of 10 minutes.
            // Once it hits the maximum, log the message as a poison message.
            const int maxSecondsToWait = 600;
            int numSecondsToWait = queueMessage.DequeueCount <= 30 ? 
                Math.Min((int)Math.Pow(2, queueMessage.DequeueCount), maxSecondsToWait) :
                maxSecondsToWait;
            if (numSecondsToWait == maxSecondsToWait)
            {
                this.settings.Logger.PoisonMessageDetected(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    eventType,
                    taskEventId,
                    queueMessage.MessageId,
                    instanceId,
                    executionId,
                    this.storageQueue.Name,
                    queueMessage.DequeueCount);
            }

            this.settings.Logger.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                eventType,
                taskEventId,
                queueMessage.MessageId,
                instanceId,
                executionId,
                this.storageQueue.Name,
                sequenceNumber,
                queueMessage.PopReceipt,
                numSecondsToWait);

            try
            {
                // We "abandon" the message by settings its visibility timeout using an exponential backoff algorithm.
                // This allows it to be reprocessed on this node or another node at a later time, hopefully successfully.
                return await this.storageQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.FromSeconds(numSecondsToWait),
                    traceActivityId);
            }
            catch (Exception e)
            {
                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(
                    e,
                    queueMessage.MessageId,
                    instanceId,
                    executionId,
                    eventType,
                    taskEventId,
                    details: $"Caller: {nameof(AbandonMessageAsync)}",
                    queueMessage.PopReceipt);

                return null;
            }
        }

        public async Task RenewMessageAsync(MessageData message, SessionBase session)
        {
            QueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;
            OrchestrationInstance instance = taskMessage.OrchestrationInstance;

            this.settings.Logger.RenewingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                instance.InstanceId,
                instance.ExecutionId,
                this.storageQueue.Name,
                message.TaskMessage.Event.EventType.ToString(),
                Utils.GetTaskEventId(message.TaskMessage.Event),
                queueMessage.MessageId,
                queueMessage.PopReceipt,
                (int)this.MessageVisibilityTimeout.TotalSeconds);

            try
            {
                await this.storageQueue.UpdateMessageAsync(
                    message,
                    this.MessageVisibilityTimeout,
                    session?.TraceActivityId);
            }
            catch (Exception e)
            {
                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(e, message, $"Caller: {nameof(RenewMessageAsync)}");
            }
        }

        public virtual async Task DeleteMessageAsync(MessageData message, SessionBase? session = null)
        {
            QueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;

            this.settings.Logger.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                Utils.GetTaskEventId(taskMessage.Event),
                queueMessage.MessageId,
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                this.storageQueue.Name,
                message.SequenceNumber,
                queueMessage.PopReceipt);

            bool haveRetried = false;
            while (true)
            {
                try
                {
                    await this.storageQueue.DeleteMessageAsync(queueMessage, session?.TraceActivityId);
                }
                catch (Exception e)
                {
                    if (!haveRetried && this.IsMessageGoneException(e))
                    {
                        haveRetried = true;
                        continue;
                    }

                    this.HandleMessagingExceptions(e, message, $"Caller: {nameof(DeleteMessageAsync)}");
                }

                break;
            }
        }

        private bool IsMessageGoneException(Exception e)
        {
            DurableTaskStorageException? storageException = e as DurableTaskStorageException;
            return storageException?.HttpStatusCode == 404;
        }

        void HandleMessagingExceptions(Exception e, MessageData message, string details)
        {
            string messageId = message.OriginalQueueMessage.MessageId;
            string instanceId = message.TaskMessage.OrchestrationInstance.InstanceId;
            string executionId = message.TaskMessage.OrchestrationInstance.ExecutionId;
            string eventType = message.TaskMessage.Event.EventType.ToString() ?? string.Empty;
            int taskEventId = Utils.GetTaskEventId(message.TaskMessage.Event);

            this.HandleMessagingExceptions(e, messageId, instanceId, executionId, eventType, taskEventId, details, message.OriginalQueueMessage.PopReceipt);
        }

        void HandleMessagingExceptions(
            Exception e,
            string messageId,
            string instanceId,
            string executionId,
            string eventType,
            int taskEventId,
            string details,
            string popReceipt)
        {
            if (this.IsMessageGoneException(e))
            {
                // Message may have been processed and deleted already.
                this.settings.Logger.MessageGone(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    messageId,
                    instanceId,
                    executionId,
                    this.storageQueue.Name,
                    eventType,
                    taskEventId,
                    details,
                    popReceipt);
            }
            else
            {
                this.settings.Logger.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    messageId,
                    instanceId,
                    executionId,
                    this.storageQueue.Name,
                    eventType,
                    taskEventId,
                    e.ToString());

                // Rethrow the original exception, preserving the callstack.
                ExceptionDispatchInfo.Capture(e).Throw();
            }
        }

        public async Task CreateIfNotExistsAsync()
        {
            try
            {
                if (await this.storageQueue.CreateIfNotExistsAsync())
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        this.storageQueue.Name,
                        $"Created {this.GetType().Name} named {this.Name}.");
                }
            }
            catch (Exception e)
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
                throw;
            }
        }

        public async Task DeleteIfExistsAsync()
        {
            try
            {
                if (await this.storageQueue.DeleteIfExistsAsync())
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        this.storageQueue.Name,
                        $"Deleted {this.GetType().Name} named {this.Name}.");
                }
            }
            catch (Exception e)
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
                throw;
            }
        }
    }
}
