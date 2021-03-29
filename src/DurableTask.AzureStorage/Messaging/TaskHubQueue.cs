﻿//  ----------------------------------------------------------------------------------
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
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    abstract class TaskHubQueue
    {
        static long messageSequenceNumber;

        protected readonly string storageAccountName;
        protected readonly AzureStorageOrchestrationServiceSettings settings;
        protected readonly AzureStorageOrchestrationServiceStats stats;
        protected readonly CloudQueue storageQueue;

        protected readonly BackoffPollingHelper backoffHelper;
        protected readonly MessageManager messageManager;

        public TaskHubQueue(
            CloudQueue storageQueue,
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats,
            MessageManager messageManager)
        {
            this.storageQueue = storageQueue;
            this.storageAccountName = storageQueue.ServiceClient.Credentials.AccountName;
            this.settings = settings;
            this.stats = stats;
            this.messageManager = messageManager;

            TimeSpan minPollingDelay = TimeSpan.FromMilliseconds(50);
            TimeSpan maxPollingDelay = settings.MaxQueuePollingInterval;
            if (maxPollingDelay < minPollingDelay)
            {
                maxPollingDelay = minPollingDelay;
            }

            this.backoffHelper = new BackoffPollingHelper(minPollingDelay, maxPollingDelay);
        }

        public string Name => this.storageQueue.Name;

        public Uri Uri => this.storageQueue.Uri;

        protected abstract QueueRequestOptions QueueRequestOptions { get; }

        protected abstract TimeSpan MessageVisibilityTimeout { get; }

        // Intended only for use by unit tests
        internal CloudQueue InnerQueue => this.storageQueue;

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

                CloudQueueMessage queueMessage = new CloudQueueMessage(rawContent);

                this.settings.Logger.SendingMessage(
                    outboundTraceActivityId,
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(taskMessage.Event),
                    sourceInstance.InstanceId,
                    sourceInstance.ExecutionId,
                    Encoding.UTF8.GetByteCount(rawContent),
                    data.QueueName /* PartitionId */,
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId,
                    data.SequenceNumber,
                    data.Episode.GetValueOrDefault(-1));

                await this.storageQueue.AddMessageAsync(
                    queueMessage,
                    null /* timeToLive */,
                    GetVisibilityDelay(taskMessage),
                    this.QueueRequestOptions,
                    session?.StorageOperationContext);

                this.stats.MessagesSent.Increment();

                // Wake up the queue polling thread
                this.backoffHelper.Reset();
            }
            catch (StorageException e)
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
            finally
            {
                this.stats.StorageRequests.Increment();
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

        public virtual async Task AbandonMessageAsync(MessageData message, SessionBase? session)
        {
            CloudQueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;
            OrchestrationInstance instance = taskMessage.OrchestrationInstance;

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
                    taskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(taskMessage.Event),
                    queueMessage.Id,
                    instance.InstanceId,
                    instance.ExecutionId,
                    this.storageQueue.Name,
                    queueMessage.DequeueCount);
            }

            this.settings.Logger.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                Utils.GetTaskEventId(taskMessage.Event),
                queueMessage.Id,
                instance.InstanceId,
                instance.ExecutionId,
                this.storageQueue.Name,
                message.SequenceNumber,
                numSecondsToWait);

            try
            {
                // We "abandon" the message by settings its visibility timeout using an exponential backoff algorithm.
                // This allows it to be reprocessed on this node or another node at a later time, hopefully successfully.
                await this.storageQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.FromSeconds(numSecondsToWait),
                    MessageUpdateFields.Visibility,
                    this.QueueRequestOptions,
                    session?.StorageOperationContext ?? new OperationContext());

                this.stats.MessagesUpdated.Increment();
            }
            catch (Exception e)
            {
                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(e, message, $"Caller: {nameof(AbandonMessageAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task RenewMessageAsync(MessageData message, SessionBase session)
        {
            CloudQueueMessage queueMessage = message.OriginalQueueMessage;
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
                queueMessage.Id,
                (int)this.MessageVisibilityTimeout.TotalSeconds);

            try
            {
                await this.storageQueue.UpdateMessageAsync(
                    queueMessage,
                    this.MessageVisibilityTimeout,
                    MessageUpdateFields.Visibility,
                    this.QueueRequestOptions,
                    session.StorageOperationContext);

                this.stats.MessagesUpdated.Increment();
            }
            catch (Exception e)
            {
                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(e, message, $"Caller: {nameof(RenewMessageAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public virtual async Task DeleteMessageAsync(MessageData message, SessionBase session)
        {
            CloudQueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;

            this.settings.Logger.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                Utils.GetTaskEventId(taskMessage.Event),
                queueMessage.Id,
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                this.storageQueue.Name,
                message.SequenceNumber);

            bool haveRetried = false;
            while (true)
            {
                try
                {
                    await this.storageQueue.DeleteMessageAsync(
                        queueMessage,
                        this.QueueRequestOptions,
                        session?.StorageOperationContext ?? new OperationContext());
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
                finally
                {
                    this.stats.StorageRequests.Increment();
                }

                break;
            }
        }

        private bool IsMessageGoneException(Exception e)
        {
            StorageException? storageException = e as StorageException;
            return storageException?.RequestInformation?.HttpStatusCode == 404;
        }

        void HandleMessagingExceptions(Exception e, MessageData message, string details)
        {
            if (this.IsMessageGoneException(e))
            {
                // Message may have been processed and deleted already.
                this.settings.Logger.MessageGone(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    message.OriginalQueueMessage.Id,
                    message.TaskMessage.OrchestrationInstance.InstanceId,
                    message.TaskMessage.OrchestrationInstance.ExecutionId,
                    this.storageQueue.Name,
                    message.TaskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(message.TaskMessage.Event),
                    details);
            }
            else
            {
                this.settings.Logger.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    message.OriginalQueueMessage.Id,
                    message.TaskMessage.OrchestrationInstance.InstanceId,
                    message.TaskMessage.OrchestrationInstance.ExecutionId,
                    this.storageQueue.Name,
                    message.TaskMessage.Event.EventType.ToString(),
                    Utils.GetTaskEventId(message.TaskMessage.Event),
                    e.ToString());

                // Rethrow the original exception, preserving the callstack.
                ExceptionDispatchInfo.Capture(e).Throw();
            }
        }

        public async Task CreateIfNotExistsAsync()
        {
            try
            {
                if (await this.storageQueue.CreateIfNotExistsAsync(this.QueueRequestOptions, null))
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
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task DeleteIfExistsAsync()
        {
            try
            {
                if (await this.storageQueue.DeleteIfExistsAsync(this.QueueRequestOptions, null))
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
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }
    }
}
