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
        protected readonly string poisonMessageContainerName;

        BlobContainer? poisonMessageContainer;


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
            this.poisonMessageContainerName = $"{this.settings.TaskHubName.ToLowerInvariant()}-{this.settings.PoisonMessageStorageContainerNameSuffix}";
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
            const int maxSecondsToWait = 600;
            int numSecondsToWait = queueMessage.DequeueCount <= 30 ? 
                Math.Min((int)Math.Pow(2, queueMessage.DequeueCount), maxSecondsToWait) :
                maxSecondsToWait;

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
                string details = $"Caller: {nameof(RenewMessageAsync)}";
                if (e is DurableTaskStorageException storageException && storageException.ErrorCode != null)
                {
                    details += $", ErrorCode: {storageException.ErrorCode}";
                }

                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(e, message, details);
            }
        }

        public virtual async Task DeleteMessageAsync(MessageData message, SessionBase? session = null)
        {
            TaskMessage taskMessage = message.TaskMessage;

            bool haveRetried = false;
            while (true)
            {
                this.settings.Logger.DeletingMessage(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(taskMessage.Event),
                    message.OriginalQueueMessage.MessageId,
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId,
                    this.storageQueue.Name,
                    message.SequenceNumber,
                    message.OriginalQueueMessage.PopReceipt);

                try
                {
                    await this.storageQueue.DeleteMessageAsync(message.OriginalQueueMessage, session?.TraceActivityId);
                }
                catch (Exception e)
                {
                    // Delete operations can transiently fail if a delete operation races with a
                    // message update operation. In this case, we retry the delete operation.
                    if (!haveRetried && (IsMessageGoneException(e) || IsPopReceiptMismatch(e)))
                    {
                        haveRetried = true;
                        continue;
                    }

                    string details = $"Caller: {nameof(DeleteMessageAsync)}";
                    if (e is DurableTaskStorageException storageException && storageException.ErrorCode != null)
                    {
                        details += $", ErrorCode: {storageException.ErrorCode}";
                    }

                    this.HandleMessagingExceptions(e, message, details);
                }

                break;
            }
        }

        protected async Task<bool> CheckForAndHandlePoisonMessageAsync(
            string blobNamePrefix,
            QueueMessage queueMessage,
            CancellationToken cancellationToken,
            OrchestrationInstance? orchestrationInstance = null,
            string eventType = "",
            int taskEventId = -1)
        {
            if (!this.settings.IsPoisonMessageStorageEnabled || queueMessage.DequeueCount <= this.settings.MaxDequeueCount)
            {
                return false;
            }

            try
            {
                BlobContainer container = this.poisonMessageContainer ??=
                    this.azureStorageClient.GetBlobContainerReference(this.poisonMessageContainerName);

                string blobName = CreateBlobName(orchestrationInstance, blobNamePrefix, queueMessage.MessageId);

                await container.CreateIfNotExistsAsync(cancellationToken);

                Blob blob = container.GetBlobReference(blobName);
                await blob.UploadTextAsync(queueMessage.Body.ToString(), cancellationToken: cancellationToken);

                this.settings.Logger.PoisonMessageDetected(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    eventType,
                    taskEventId,
                    queueMessage.MessageId,
                    orchestrationInstance?.InstanceId ?? string.Empty,
                    orchestrationInstance?.ExecutionId ?? string.Empty,
                    this.storageQueue.Name,
                    queueMessage.DequeueCount,
                    blobName);

                await this.storageQueue.DeleteMessageAsync(queueMessage, cancellationToken: cancellationToken);
            }
            catch (Exception e)
            {
                this.settings.Logger.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    queueMessage.MessageId,
                    orchestrationInstance?.InstanceId ?? string.Empty,
                    orchestrationInstance?.ExecutionId ?? string.Empty,
                    this.storageQueue.Name,
                    eventType,
                    taskEventId,
                    $"Error when attempting to store poison message. Error: {e}");

                return false;
            }
            return true;
        }

        static string CreateBlobName(OrchestrationInstance? orchestrationInstance, string blobNamePrefix, string messageId)
        {
            // Replace any invalid characters with a dash
            string sanitizedInstanceId = SanitizeString(orchestrationInstance?.InstanceId, '-');
            blobNamePrefix += sanitizedInstanceId;

            if (!string.IsNullOrEmpty(orchestrationInstance?.ExecutionId))
            {
                blobNamePrefix += $"_{SanitizeString(orchestrationInstance!.ExecutionId, '-')}";
            }

            // Blob name length limit is 1024 characters and we attach an extra character (_) and the message ID at the end
            // From https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata?#blob-names
            int maxPrefixLength = 1024 - messageId.Length - 1;
            if (blobNamePrefix.Length > maxPrefixLength)
            {
                blobNamePrefix = blobNamePrefix.Substring(0, maxPrefixLength);

                // Avoid leaving an unpaired surrogate at the end of the truncated prefix.
                if (char.IsHighSurrogate(blobNamePrefix, blobNamePrefix.Length - 1))
                {
                    blobNamePrefix = blobNamePrefix.Substring(0, blobNamePrefix.Length - 1);
                }
            }
            string blobName = $"{blobNamePrefix}_{messageId}";

            // A blob name may contain at most 254 path segment delimiters ('/'). Replace any '/' beyond the
            // first 254 with a dash so the blob name remains valid.
            // From https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata?#blob-names
            const int MaxForwardSlashes = 254;
            int forwardSlashCount = 0;
            char[] blobNameChars = blobName.ToCharArray();
            for (int i = 0; i < blobNameChars.Length; i++)
            {
                if (blobNameChars[i] == '/' && ++forwardSlashCount > MaxForwardSlashes)
                {
                    blobNameChars[i] = '-';
                }
            }
            return new string(blobNameChars);
        }

        static string SanitizeString(string? input, char replacement)
        {
            if (input == null)
            {
                return string.Empty;
            }

            // From https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata?#unicode-characters-not-recommended-for-use-in-container-or-blob-names
            static bool IsInvalidCodePoint(int scalar)
            {
                if (scalar == 0x0080 ||
                    (scalar >= 0x0082 && scalar <= 0x008C) ||
                    scalar == 0x008E ||
                    (scalar >= 0x0091 && scalar <= 0x009C) ||
                    (scalar >= 0x009E && scalar <= 0x009F) ||
                    (scalar >= 0xFDD1 && scalar <= 0xFDDC) ||
                    (scalar >= 0xFDDE && scalar <= 0xFDEF) ||
                    (scalar >= 0xFFF0 && scalar <= 0xFFFF))
                {
                    return true;
                }

                return scalar == 0x1FFFE || scalar == 0x1FFFF ||
                        scalar == 0x2FFFE || scalar == 0x2FFFF ||
                        scalar == 0x3FFFE || scalar == 0x3FFFF ||
                        scalar == 0x5FFFE || scalar == 0x5FFFF ||
                        scalar == 0x6FFFE || scalar == 0x6FFFF ||
                        scalar == 0x7FFFE || scalar == 0x7FFFF ||
                        scalar == 0x9FFFE || scalar == 0x9FFFF ||
                        scalar == 0xAFFFE || scalar == 0xAFFFF ||
                        scalar == 0xBFFFE || scalar == 0xBFFFF ||
                        scalar == 0xDFFFE || scalar == 0xDFFFF ||
                        scalar == 0xEFFFE || scalar == 0xEFFFF ||
                        scalar == 0xFFFFE || scalar == 0xFFFFF;
            }

            var sb = new StringBuilder(input.Length);

            for (int i = 0; i < input.Length; i++)
            {
                char c = input[i];
                int scalar;

                // Since the .NET version is too low to iterate the runes, we manually detect and combine surrogate pairs here
                if (char.IsHighSurrogate(c) && i + 1 < input.Length && char.IsLowSurrogate(input[i + 1]))
                {
                    scalar = char.ConvertToUtf32(c, input[i + 1]);
                    i++;
                }
                else if (char.IsSurrogate(c))
                {
                    // Unpaired surrogate. These are not valid Unicode scalar values, so char.ConvertFromUtf32 below
                    // would throw for them. In practice an unpaired surrogate never reaches this method: instance and
                    // execution IDs are round-tripped through Azure Storage queues, which substitute unpaired
                    // surrogates (and other characters that are not valid in the storage encoding) with the Unicode
                    // replacement character (U+FFFD) before we read the message back. We therefore don't enumerate
                    // surrogate code points in IsInvalidCodePoint above; we just replace any that somehow appear here
                    // so that blob-name generation can never throw.
                    sb.Append(replacement);
                    continue;
                }
                else
                {
                    scalar = c;
                }

                if (IsInvalidCodePoint(scalar))
                {
                    sb.Append(replacement);
                }
                else
                {
                    sb.Append(char.ConvertFromUtf32(scalar));
                }
            }

            return sb.ToString();
        }

        static bool IsMessageGoneException(Exception e)
        {
            DurableTaskStorageException? storageException = e as DurableTaskStorageException;
            return storageException?.HttpStatusCode == 404;
        }

        static bool IsPopReceiptMismatch(Exception e)
        {
            if (e is DurableTaskStorageException storageException)
            {
                return storageException.IsPopReceiptMismatch;
            }

            return false;
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
            if (IsMessageGoneException(e))
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
