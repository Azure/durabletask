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

namespace DurableTask.AzureStorage.Messaging
{
    using System;
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

            TimeSpan maxPollingDelay = AzureStorageOrchestrationService.MaxQueuePollingDelay;
            TimeSpan minPollingDelayThreshold = TimeSpan.FromMilliseconds(500);
            this.backoffHelper = new BackoffPollingHelper(maxPollingDelay, minPollingDelayThreshold);
        }

        public string Name => this.storageQueue.Name;

        public Uri Uri => this.storageQueue.Uri;

        protected abstract QueueRequestOptions QueueRequestOptions { get; }

        protected abstract TimeSpan MessageVisibilityTimeout { get; }

        // Intended only for use by unit tests
        internal CloudQueue InnerQueue => this.storageQueue;

        public Task AddMessageAsync(TaskMessage message, SessionBase sourceSession)
        {
            return this.AddMessageAsync(message, sourceSession.Instance, sourceSession);
        }

        public Task AddMessageAsync(TaskMessage message, OrchestrationInstance sourceInstance)
        {
            return this.AddMessageAsync(message, sourceInstance, session: null);
        }

        async Task AddMessageAsync(TaskMessage message, OrchestrationInstance sourceInstance, SessionBase session)
        {
            try
            {
                await this.storageQueue.AddMessageAsync(
                    await this.CreateOutboundQueueMessageAsync(sourceInstance, this.storageQueue.Name, message),
                    null /* timeToLive */,
                    GetVisibilityDelay(message),
                    this.QueueRequestOptions,
                    session?.StorageOperationContext);

                this.stats.MessagesSent.Increment();

                // Wake up the queue polling thread
                this.backoffHelper.Reset();
            }
            catch (StorageException e)
            {
                AnalyticsEventSource.Log.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    string.Empty,
                    sourceInstance.InstanceId,
                    sourceInstance.ExecutionId,
                    this.storageQueue.Name,
                    e.ToString(),
                    Utils.ExtensionVersion);
                throw;
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
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

            return initialVisibilityDelay;
        }

        public async Task AbandonMessageAsync(MessageData message, SessionBase session)
        {
            CloudQueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;
            OrchestrationInstance instance = taskMessage.OrchestrationInstance;

            AnalyticsEventSource.Log.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                queueMessage.Id,
                instance.InstanceId,
                instance.ExecutionId,
                this.storageQueue.Name,
                message.SequenceNumber,
                Utils.ExtensionVersion);

            try
            {
                // We "abandon" the message by settings its visibility timeout to zero.
                // This allows it to be reprocessed on this node or another node.
                await this.storageQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    this.QueueRequestOptions,
                    session.StorageOperationContext);

                this.stats.MessagesUpdated.Increment();
            }
            catch (Exception e)
            {
                // Message may have been processed and deleted already.
                this.HandleMessagingExceptions(e, queueMessage.Id, session.Instance, $"Caller: {nameof(AbandonMessageAsync)}");
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

            AnalyticsEventSource.Log.RenewingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                instance.InstanceId,
                instance.ExecutionId,
                this.storageQueue.Name,
                message.TaskMessage.Event.EventType.ToString(),
                queueMessage.Id,
                (int)this.MessageVisibilityTimeout.TotalSeconds,
                Utils.ExtensionVersion);

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
                this.HandleMessagingExceptions(e, queueMessage.Id, session.Instance, $"Caller: {nameof(RenewMessageAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task DeleteMessageAsync(MessageData message, SessionBase session)
        {
            CloudQueueMessage queueMessage = message.OriginalQueueMessage;
            TaskMessage taskMessage = message.TaskMessage;

            AnalyticsEventSource.Log.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                queueMessage.Id,
                session.Instance.InstanceId,
                session.Instance.ExecutionId,
                this.storageQueue.Name,
                message.SequenceNumber,
                Utils.ExtensionVersion);

            try
            {
                await this.storageQueue.DeleteMessageAsync(
                    queueMessage,
                    this.QueueRequestOptions,
                    session.StorageOperationContext);
            }
            catch (Exception e)
            {
                this.HandleMessagingExceptions(e, queueMessage.Id, session.Instance, $"Caller: {nameof(DeleteMessageAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        void HandleMessagingExceptions(Exception e, string messageId, OrchestrationInstance instance, string details)
        {
            StorageException storageException = e as StorageException;
            if (storageException?.RequestInformation?.HttpStatusCode == 404)
            {
                // Message may have been processed and deleted already.
                AnalyticsEventSource.Log.MessageGone(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    messageId,
                    instance.InstanceId,
                    instance.ExecutionId,
                    this.storageQueue.Name,
                    details,
                    Utils.ExtensionVersion);
            }
            else
            {
                AnalyticsEventSource.Log.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    messageId,
                    instance.InstanceId,
                    instance.ExecutionId,
                    this.storageQueue.Name,
                    e.ToString(),
                    Utils.ExtensionVersion);

                // Rethrow the original exception, preserving the callstack.
                ExceptionDispatchInfo.Capture(e).Throw();
            }
        }

        Task<CloudQueueMessage> CreateOutboundQueueMessageAsync(
            OrchestrationInstance sourceInstance,
            string queueName,
            TaskMessage taskMessage)
        {
            return CreateOutboundQueueMessageAsync(
                this.messageManager,
                sourceInstance,
                this.storageAccountName,
                this.settings.TaskHubName,
                queueName,
                taskMessage);
        }

        static async Task<CloudQueueMessage> CreateOutboundQueueMessageAsync(
            MessageManager messageManager,
            OrchestrationInstance sourceInstance,
            string storageAccountName,
            string taskHub,
            string queueName,
            TaskMessage taskMessage)
        {
            // We transfer to a new trace activity ID every time a new outbound queue message is created.
            Guid outboundTraceActivityId = Guid.NewGuid();

            var data = new MessageData(taskMessage, outboundTraceActivityId, queueName);
            data.SequenceNumber = Interlocked.Increment(ref messageSequenceNumber);

            string rawContent = await messageManager.SerializeMessageDataAsync(data);

            AnalyticsEventSource.Log.SendingMessage(
                outboundTraceActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                sourceInstance.InstanceId,
                sourceInstance.ExecutionId,
                Encoding.Unicode.GetByteCount(rawContent),
                data.QueueName /* PartitionId */,
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                data.SequenceNumber,
                Utils.ExtensionVersion);

            return new CloudQueueMessage(rawContent);
        }

        public async Task CreateIfNotExistsAsync()
        {
            try
            {
                if (await this.storageQueue.CreateIfNotExistsAsync(this.QueueRequestOptions, null))
                {
                    AnalyticsEventSource.Log.PartitionManagerInfo(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        this.storageQueue.Name,
                        $"Created {this.GetType().Name} named {this.Name}.",
                        Utils.ExtensionVersion);
                }
            }
            catch (Exception e)
            {
                AnalyticsEventSource.Log.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    string.Empty /* messageId */,
                    string.Empty /* instanceId */,
                    string.Empty /* executionId */,
                    this.storageQueue.Name,
                    e.ToString(),
                    Utils.ExtensionVersion);
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
                    AnalyticsEventSource.Log.PartitionManagerInfo(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        this.storageQueue.Name,
                        $"Deleted {this.GetType().Name} named {this.Name}.",
                        Utils.ExtensionVersion);
                }
            }
            catch (Exception e)
            {
                AnalyticsEventSource.Log.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    string.Empty /* messageId */,
                    string.Empty /* instanceId */,
                    string.Empty /* executionId */,
                    this.storageQueue.Name,
                    e.ToString(),
                    Utils.ExtensionVersion);
                throw;
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }
    }
}
