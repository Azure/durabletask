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

namespace DurableTask.ServiceBus
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Tracking;
    using DurableTask.Core.Serializing;
    using DurableTask.ServiceBus.Common;
    using DurableTask.ServiceBus.Settings;
    using DurableTask.ServiceBus.Stats;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Orchestration Service and Client implementation using Azure Service Bus
    /// Takes an optional instance store for storing state and history
    /// </summary>
    public class ServiceBusOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        // This is the Max number of messages which can be processed in a single transaction.
        // Current ServiceBus limit is 100 so it has to be lower than that.  
        // This also has an impact on prefetch count as PrefetchCount cannot be greater than this value
        // as every fetched message also creates a tracking message which counts towards this limit.
        const int MaxMessageCount = 80;
        const int SessionStreamWarningSizeInBytes = 150 * 1024;
        const int StatusPollingIntervalInSeconds = 2;
        const int DuplicateDetectionWindowInHours = 4;

        /// <summary>
        /// Orchestration service settings 
        /// </summary>
        public readonly ServiceBusOrchestrationServiceSettings Settings;

        /// <summary>
        /// Instance store for state and history tracking
        /// </summary>
        public readonly IOrchestrationServiceInstanceStore InstanceStore;

        /// <summary>
        /// Blob store for oversized messages and sessions
        /// </summary>
        public readonly IOrchestrationServiceBlobStore BlobStore;

        /// <summary>
        /// Statistics for the orchestration service
        /// </summary>
        public readonly ServiceBusOrchestrationServiceStats ServiceStats;

        static readonly DataConverter DataConverter = new JsonDataConverter();
        readonly string connectionString;
        readonly string hubName;

        MessagingFactory workerSenderMessagingFactory;
        MessagingFactory orchestratorSenderMessagingFactory;
        readonly MessagingFactory orchestratorBatchSenderMessagingFactory;
        MessagingFactory trackingSenderMessagingFactory;
        MessagingFactory workerQueueClientMessagingFactory;
        MessagingFactory orchestratorQueueClientMessagingFactory;
        MessagingFactory trackingQueueClientMessagingFactory;

        MessageSender orchestratorSender;
        readonly MessageSender orchestrationBatchMessageSender;
        QueueClient orchestratorQueueClient;
        MessageSender workerSender;
        QueueClient workerQueueClient;
        MessageSender trackingSender;
        QueueClient trackingQueueClient;
        readonly string workerEntityName;
        readonly string orchestratorEntityName;
        readonly string trackingEntityName;
        readonly WorkItemDispatcher<TrackingWorkItem> trackingDispatcher;
        readonly JumpStartManager jumpStartManager;
        readonly NamespaceManager namespaceManager;
        readonly ServiceBusConnectionStringBuilder sbConnectionStringBuilder;

        ConcurrentDictionary<string, ServiceBusOrchestrationSession> orchestrationSessions;
        ConcurrentDictionary<string, BrokeredMessage> orchestrationMessages;
        CancellationTokenSource cancellationTokenSource;

        /// <summary>
        ///     Create a new ServiceBusOrchestrationService to the given service bus connection string and hub name
        /// </summary>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="hubName">Hub name to use with the connection string</param>
        /// <param name="instanceStore">Instance store Provider, where state and history messages will be stored</param>
        /// <param name="blobStore">Blob store Provider, where oversized messages and sessions will be stored</param>
        /// <param name="settings">Settings object for service and client</param>
        public ServiceBusOrchestrationService(
            string connectionString,
            string hubName,
            IOrchestrationServiceInstanceStore instanceStore,
            IOrchestrationServiceBlobStore blobStore,
            ServiceBusOrchestrationServiceSettings settings)
        {
            this.connectionString = connectionString;
            this.hubName = hubName;
            this.ServiceStats = new ServiceBusOrchestrationServiceStats();

            this.workerEntityName = string.Format(ServiceBusConstants.WorkerEndpointFormat, this.hubName);
            this.orchestratorEntityName = string.Format(ServiceBusConstants.OrchestratorEndpointFormat, this.hubName);
            this.trackingEntityName = string.Format(ServiceBusConstants.TrackingEndpointFormat, this.hubName);
            this.namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            this.sbConnectionStringBuilder = new ServiceBusConnectionStringBuilder(connectionString);
            this.Settings = settings ?? new ServiceBusOrchestrationServiceSettings();
            this.orchestratorBatchSenderMessagingFactory = ServiceBusUtils.CreateSenderMessagingFactory(
                this.namespaceManager,
                this.sbConnectionStringBuilder,
                this.orchestratorEntityName,
                this.Settings.MessageSenderSettings);
            this.orchestrationBatchMessageSender = ServiceBusUtils.CreateMessageSender(this.orchestratorBatchSenderMessagingFactory, this.orchestratorEntityName);
            this.BlobStore = blobStore;
            if (instanceStore != null)
            {
                this.InstanceStore = instanceStore;
                this.trackingDispatcher = new WorkItemDispatcher<TrackingWorkItem>(
                    "TrackingDispatcher",
                    item => item == null ? string.Empty : item.InstanceId,
                    FetchTrackingWorkItemAsync,
                    ProcessTrackingWorkItemAsync)
                {
                    GetDelayInSecondsAfterOnFetchException = GetDelayInSecondsAfterOnFetchException,
                    GetDelayInSecondsAfterOnProcessException = GetDelayInSecondsAfterOnProcessException,
                    DispatcherCount = this.Settings.TrackingDispatcherSettings.DispatcherCount,
                    MaxConcurrentWorkItems = this.Settings.TrackingDispatcherSettings.MaxConcurrentTrackingSessions
                };

                if (this.Settings.JumpStartSettings.JumpStartEnabled)
                {
                    this.jumpStartManager = new JumpStartManager(this, this.Settings.JumpStartSettings.Interval, this.Settings.JumpStartSettings.IgnoreWindow);
                }
            }
        }

        /// <summary>
        /// Starts the service initializing the required resources
        /// </summary>
        public async Task StartAsync()
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            this.orchestrationSessions = new ConcurrentDictionary<string, ServiceBusOrchestrationSession>();
            this.orchestrationMessages = new ConcurrentDictionary<string, BrokeredMessage>();

            this.orchestratorSenderMessagingFactory = ServiceBusUtils.CreateSenderMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.orchestratorEntityName, this.Settings.MessageSenderSettings);
            this.workerSenderMessagingFactory = ServiceBusUtils.CreateSenderMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.workerEntityName, this.Settings.MessageSenderSettings);
            this.trackingSenderMessagingFactory = ServiceBusUtils.CreateSenderMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.trackingEntityName, this.Settings.MessageSenderSettings);
            this.workerQueueClientMessagingFactory = ServiceBusUtils.CreateReceiverMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.workerEntityName);
            this.orchestratorQueueClientMessagingFactory = ServiceBusUtils.CreateReceiverMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.orchestratorEntityName);
            this.trackingQueueClientMessagingFactory = ServiceBusUtils.CreateReceiverMessagingFactory(this.namespaceManager, this.sbConnectionStringBuilder, this.trackingEntityName);

            this.orchestratorSender = ServiceBusUtils.CreateMessageSender(this.orchestratorSenderMessagingFactory, this.orchestratorEntityName, this.workerEntityName);
            this.workerSender = ServiceBusUtils.CreateMessageSender(this.workerSenderMessagingFactory, this.workerEntityName, this.orchestratorEntityName);
            this.trackingSender = ServiceBusUtils.CreateMessageSender(this.trackingSenderMessagingFactory, this.trackingEntityName, this.orchestratorEntityName);
            this.workerQueueClient = ServiceBusUtils.CreateQueueClient(this.workerQueueClientMessagingFactory, this.workerEntityName);
            this.orchestratorQueueClient = ServiceBusUtils.CreateQueueClient(this.orchestratorQueueClientMessagingFactory, this.orchestratorEntityName);
            this.trackingQueueClient = ServiceBusUtils.CreateQueueClient(this.trackingQueueClientMessagingFactory, this.trackingEntityName);

            if (this.trackingDispatcher != null)
            {
                await this.trackingDispatcher.StartAsync();
            }

            if (this.jumpStartManager != null)
            {
                await this.jumpStartManager.StartAsync();
            }

            await Task.Factory.StartNew(() => ServiceMonitorAsync(this.cancellationTokenSource.Token), this.cancellationTokenSource.Token);
        }

        /// <summary>
        /// Stops the orchestration service gracefully
        /// </summary>
        public async Task StopAsync()
        {
            await StopAsync(false);
        }

        /// <summary>
        /// Stops the orchestration service with optional forced flag
        /// </summary>
        /// <param name="isForced">Flag when true stops resources aggressively, when false stops gracefully</param>
        public async Task StopAsync(bool isForced)
        {
            this.cancellationTokenSource?.Cancel();

            TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-StatsFinal", "Final Service Stats: {0}", this.ServiceStats.ToString());
            // TODO : call shutdown of any remaining orchestrationSessions and orchestrationMessages

            await Task.WhenAll(
                this.workerSender.CloseAsync(),
                this.orchestratorSender.CloseAsync(),
                this.orchestrationBatchMessageSender?.CloseAsync(),
                this.trackingSender.CloseAsync(),
                this.orchestratorQueueClient.CloseAsync(),
                this.trackingQueueClient.CloseAsync(),
                this.workerQueueClient.CloseAsync()
                );
            if (this.trackingDispatcher != null)
            {
                await this.trackingDispatcher.StopAsync(isForced);
            }

            if (this.jumpStartManager != null)
            {
                await this.jumpStartManager.StopAsync();
            }

            // TODO : Move this, we don't open in start so calling close then start yields a broken reference
            Task.WaitAll(
                this.workerSenderMessagingFactory.CloseAsync(),
                this.orchestratorSenderMessagingFactory.CloseAsync(),
                this.orchestratorBatchSenderMessagingFactory.CloseAsync(),
                this.trackingSenderMessagingFactory.CloseAsync(),
                this.workerQueueClientMessagingFactory.CloseAsync(),
                this.orchestratorQueueClientMessagingFactory.CloseAsync(),
                this.trackingQueueClientMessagingFactory.CloseAsync());
        }

        /// <summary>
        /// Deletes and creates the necessary resources for the orchestration service including the instance store
        /// </summary>
        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        /// <summary>
        /// Deletes and creates the necessary resources for the orchestration service
        /// </summary>
        /// <param name="recreateInstanceStore">Flag indicating whether to drop and create instance store</param>
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);

            await Task.WhenAll(
                SafeDeleteAndCreateQueueAsync(manager, this.orchestratorEntityName, true, true, this.Settings.MaxTaskOrchestrationDeliveryCount, this.Settings.MaxQueueSizeInMegabytes),
                SafeDeleteAndCreateQueueAsync(manager, this.workerEntityName, false, false, this.Settings.MaxTaskActivityDeliveryCount, this.Settings.MaxQueueSizeInMegabytes)
                );

            if (this.InstanceStore != null)
            {
                await SafeDeleteAndCreateQueueAsync(manager, this.trackingEntityName, true, false, this.Settings.MaxTrackingDeliveryCount, this.Settings.MaxQueueSizeInMegabytes);
                await this.InstanceStore.InitializeStoreAsync(recreateInstanceStore);
            }
        }

        /// <summary>
        /// Drops and creates the necessary resources for the orchestration service and the instance store
        /// </summary>
        public async Task CreateIfNotExistsAsync()
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);

            await Task.WhenAll(
                SafeCreateQueueAsync(manager, this.orchestratorEntityName, true, true, this.Settings.MaxTaskOrchestrationDeliveryCount, this.Settings.MaxQueueSizeInMegabytes),
                SafeCreateQueueAsync(manager, this.workerEntityName, false, false, this.Settings.MaxTaskActivityDeliveryCount, this.Settings.MaxQueueSizeInMegabytes)
                );
            if (this.InstanceStore != null)
            {
                await SafeCreateQueueAsync(manager, this.trackingEntityName, true, false, this.Settings.MaxTrackingDeliveryCount, this.Settings.MaxQueueSizeInMegabytes);
                await this.InstanceStore.InitializeStoreAsync(false);
            }
        }

        /// <summary>
        /// Deletes the resources for the orchestration service and the instance store
        /// </summary>
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        /// <summary>
        /// Deletes the resources for the orchestration service and optionally the instance store
        /// </summary>
        /// <param name="deleteInstanceStore">Flag indicating whether to drop instance store</param>
        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);

            await Task.WhenAll(
                SafeDeleteQueueAsync(manager, this.orchestratorEntityName),
                SafeDeleteQueueAsync(manager, this.workerEntityName)
                );
            if (this.InstanceStore != null)
            {
                await SafeDeleteQueueAsync(manager, this.trackingEntityName);
                if (deleteInstanceStore)
                {
                    await this.InstanceStore.DeleteStoreAsync();
                }
            }

            if (this.BlobStore != null)
            {
                await this.BlobStore.DeleteStoreAsync();
            }
        }

        // Service Bus Utility methods

        /// <summary>
        /// Utility method to check if the needed resources are available for the orchestration service
        /// </summary>
        /// <returns>True if all needed queues are present, false otherwise</returns>
        public async Task<bool> HubExistsAsync()
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);

            IEnumerable<QueueDescription> queueDescriptions =
                // ReSharper disable once StringLiteralTypo
                (await manager.GetQueuesAsync("startswith(path, '" + this.hubName + "') eq TRUE")).ToList();

            return queueDescriptions.Any(q => string.Equals(q.Path, this.orchestratorEntityName))
                && queueDescriptions.Any(q => string.Equals(q.Path, this.workerEntityName))
                && (this.InstanceStore == null || queueDescriptions.Any(q => string.Equals(q.Path, this.trackingEntityName)));
        }

        /// <summary>
        ///     Get the count of pending orchestrations
        /// </summary>
        /// <returns>Count of pending orchestrations</returns>
        public long GetPendingOrchestrationsCount()
        {
            return GetQueueCount(this.orchestratorEntityName);
        }

        /// <summary>
        ///     Get the count of pending work items (activities)
        /// </summary>
        /// <returns>Count of pending activities</returns>
        public long GetPendingWorkItemsCount()
        {
            return GetQueueCount(this.workerEntityName);
        }

        /// <summary>
        ///     Internal method for getting the number of items in a queue
        /// </summary>
        long GetQueueCount(string entityName)
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);
            QueueDescription queueDescription = manager.GetQueue(entityName);
            if (queueDescription == null)
            {
                throw TraceHelper.TraceException(
                    TraceEventType.Error,
                    "ServiceBusOrchestrationService-QueueNotFound",
                    new ArgumentException($"Queue {entityName} does not exist"));
            }

            return queueDescription.MessageCount;
        }

        /// <summary>
        ///     Internal method for getting the max delivery counts for each queue
        /// </summary>
        internal async Task<Dictionary<string, int>> GetHubQueueMaxDeliveryCountsAsync()
        {
            NamespaceManager manager = NamespaceManager.CreateFromConnectionString(this.connectionString);

            var result = new Dictionary<string, int>(3);

            IEnumerable<QueueDescription> queues =
                // ReSharper disable once StringLiteralTypo
                (await manager.GetQueuesAsync("startswith(path, '" + this.hubName + "') eq TRUE")).ToList();

            result.Add("TaskOrchestration", queues.Single(q => string.Equals(q.Path, this.orchestratorEntityName))?.MaxDeliveryCount ?? -1);
            result.Add("TaskActivity", queues.Single(q => string.Equals(q.Path, this.workerEntityName))?.MaxDeliveryCount ?? -1);
            result.Add("Tracking", queues.Single(q => string.Equals(q.Path, this.trackingEntityName))?.MaxDeliveryCount ?? -1);

            return result;
        }

        /// <summary>
        ///     Checks the message count against the threshold to see if a limit is being exceeded
        /// </summary>
        /// <param name="currentMessageCount">The current message count to check</param>
        /// <param name="runtimeState">The Orchestration runtime state this message count is associated with</param>
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return currentMessageCount
                + ((this.InstanceStore != null) ? runtimeState.NewEvents.Count + 1 : 0) // one history message per new message + 1 for the orchestration
                > MaxMessageCount;
        }

        /// <summary>
        /// Inspects an exception to get a custom delay based on the exception (e.g. transient) properties for a process exception
        /// </summary>
        /// <param name="exception">The exception to inspect</param>
        /// <returns>Delay in seconds</returns>
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (IsTransientException(exception))
            {
                return this.Settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }

            return 0;
        }

        /// <summary>
        /// Inspects an exception to get a custom delay based on the exception (e.g. transient) properties for a fetch exception
        /// </summary>
        /// <param name="exception">The exception to inspect</param>
        /// <returns>Delay in seconds</returns>
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            if (exception is TimeoutException)
            {
                return 0;
            }

            int delay = this.Settings.TaskOrchestrationDispatcherSettings.NonTransientErrorBackOffSecs;
            if (IsTransientException(exception))
            {
                delay = this.Settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }

            return delay;
        }

        /// <summary>
        /// Gets the the number of task orchestration dispatchers
        /// </summary>
        public int TaskOrchestrationDispatcherCount => this.Settings.TaskOrchestrationDispatcherSettings.DispatcherCount;

        /// <summary>
        /// Gets the maximum number of concurrent task orchestration items
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems => this.Settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;

        /// <summary>
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => this.Settings.TaskOrchestrationDispatcherSettings.EventBehaviourForContinueAsNew;

        /// <summary>
        ///     Wait for the next orchestration work item and return the orchestration work item
        /// </summary>
        /// <param name="receiveTimeout">The timespan to wait for new messages before timing out</param>
        /// <param name="cancellationToken">The cancellation token to cancel execution of the task</param>
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            MessageSession session = await this.orchestratorQueueClient.AcceptMessageSessionAsync(receiveTimeout);
            if (session == null)
            {
                return null;
            }

            this.ServiceStats.OrchestrationDispatcherStats.SessionsReceived.Increment();

            // TODO : Here and elsewhere, consider standard retry block instead of our own hand rolled version
            IList<BrokeredMessage> newMessages =
                (await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(this.Settings.PrefetchCount),
                    session.SessionId, "Receive Session Message Batch", this.Settings.MaxRetries, this.Settings.IntervalBetweenRetriesSecs)).ToList();

            this.ServiceStats.OrchestrationDispatcherStats.MessagesReceived.Increment(newMessages.Count);
            TraceHelper.TraceSession(
                TraceEventType.Information,
                "ServiceBusOrchestrationService-LockNextTaskOrchestrationWorkItem-MessageToProcess",
                session.SessionId,
                GetFormattedLog($@"{newMessages.Count} new messages to process: {
                    string.Join(",", newMessages.Select(m => m.MessageId))}, max latency: {
                    newMessages.Max(message => message.DeliveryLatency())}ms"));

            ServiceBusUtils.CheckAndLogDeliveryCount(session.SessionId, newMessages, this.Settings.MaxTaskOrchestrationDeliveryCount);

            IList<TaskMessage> newTaskMessages = await Task.WhenAll(
                newMessages.Select(async message => await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message, this.BlobStore)));

            OrchestrationRuntimeState runtimeState = await GetSessionStateAsync(session, this.BlobStore);

            long maxSequenceNumber = newMessages.Max(message => message.SequenceNumber);

            Dictionary<Guid, BrokeredMessage> lockTokens = newMessages.ToDictionary(m => m.LockToken, m => m);
            var sessionState = new ServiceBusOrchestrationSession
            {
                Session = session,
                LockTokens = lockTokens,
                SequenceNumber = maxSequenceNumber
            };

            if (!this.orchestrationSessions.TryAdd(session.SessionId, sessionState))
            {
                string error = $"Duplicate orchestration session id '{session.SessionId}', id already exists in session list.";
                TraceHelper.Trace(TraceEventType.Error, "ServiceBusOrchestrationService-LockNextTaskOrchestrationWorkItem-DuplicateSessionId", error);
                throw new OrchestrationFrameworkException(error);
            }

            if (this.InstanceStore != null)
            {
                try
                {
                    TaskMessage executionStartedMessage = newTaskMessages.FirstOrDefault(m => m.Event is ExecutionStartedEvent);

                    if (executionStartedMessage != null)
                    {
                        await UpdateInstanceStoreAsync(executionStartedMessage.Event as ExecutionStartedEvent, maxSequenceNumber);
                    }
                }
                catch (Exception exception)
                {
                    this.orchestrationSessions.TryRemove(session.SessionId, out ServiceBusOrchestrationSession _);

                    string error = $"Exception while updating instance store. Session id: {session.SessionId}";
                    TraceHelper.TraceException(TraceEventType.Error, "ServiceBusOrchestrationService-LockNextTaskOrchestrationWorkItem-ErrorUpdatingInstanceStore", exception, error);

                    throw;
                }
            }

            return new TaskOrchestrationWorkItem
            {
                InstanceId = session.SessionId,
                LockedUntilUtc = session.LockedUntilUtc,
                NewMessages = newTaskMessages.ToList(),
                OrchestrationRuntimeState = runtimeState
            };
        }

        Task UpdateInstanceStoreAsync(ExecutionStartedEvent executionStartedEvent, long sequenceNumber)
        {
            // TODO: Duplicate detection: Check if the orchestration already finished

            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = executionStartedEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = executionStartedEvent.Timestamp,
                LastUpdatedTime = DateTime.UtcNow,
                CompletedTime = DateTimeUtils.MinDateTime,
                ParentInstance = executionStartedEvent.ParentInstance
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity
            {
                State = orchestrationState,
                SequenceNumber = sequenceNumber
            };

            return this.InstanceStore.WriteEntitiesAsync(new[] { orchestrationStateEntity });
        }

        ServiceBusOrchestrationSession GetSessionInstanceForWorkItem(TaskOrchestrationWorkItem workItem)
        {
            if (string.IsNullOrWhiteSpace(workItem?.InstanceId))
            {
                return null;
            }

            return this.orchestrationSessions[workItem.InstanceId];
        }

        ServiceBusOrchestrationSession GetAndDeleteSessionInstanceForWorkItem(TaskOrchestrationWorkItem workItem)
        {
            if (string.IsNullOrWhiteSpace(workItem?.InstanceId))
            {
                return null;
            }

            this.orchestrationSessions.TryRemove(workItem.InstanceId, out ServiceBusOrchestrationSession sessionInstance);
            return sessionInstance;
        }

        /// <summary>
        ///     Renew the lock on an orchestration
        /// </summary>
        /// <param name="workItem">The task orchestration to renew the lock on</param>
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            ServiceBusOrchestrationSession sessionState = GetSessionInstanceForWorkItem(workItem);
            if (sessionState?.Session == null)
            {
                return;
            }

            TraceHelper.TraceSession(TraceEventType.Information, "ServiceBusOrchestrationService-RenewTaskOrchestrationWorkItem", workItem.InstanceId, "Renew lock on orchestration session");
            await sessionState.Session.RenewLockAsync();
            this.ServiceStats.OrchestrationDispatcherStats.SessionsRenewed.Increment();
            workItem.LockedUntilUtc = sessionState.Session.LockedUntilUtc;
        }

        /// <summary>
        ///     Complete an orchestration, this atomically sends any outbound messages and completes the session for all current messages
        /// </summary>
        /// <param name="workItem">The task orchestration to renew the lock on</param>
        /// <param name="newOrchestrationRuntimeState">New state of the orchestration to be persisted. Could be null if the orchestration is in completion.</param>
        /// <param name="outboundMessages">New work item messages to be processed</param>
        /// <param name="orchestratorMessages">New orchestration messages to be scheduled</param>
        /// <param name="timerMessages">Delayed execution messages to be scheduled for the orchestration</param>
        /// <param name="continuedAsNewMessage">Task Message to send to orchestrator queue to treat as new in order to rebuild state</param>
        /// <param name="orchestrationState">The prior orchestration state</param>
        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;
            ServiceBusOrchestrationSession sessionState = GetSessionInstanceForWorkItem(workItem);
            if (sessionState == null)
            {
                // ReSharper disable once NotResolvedInText
                throw new ArgumentNullException("SessionInstance");
            }

            MessageSession session = sessionState.Session;

            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                Transaction.Current.TransactionCompleted += (o, e) =>
                    TraceHelper.TraceInstance(
                    e.Transaction.TransactionInformation.Status == TransactionStatus.Committed ? TraceEventType.Information : TraceEventType.Error,
                    "ServiceBusOrchestrationService-CompleteTaskOrchestrationWorkItem-TransactionComplete",
                    runtimeState.OrchestrationInstance,
                    () => $@"Orchestration Transaction Completed {
                        e.Transaction.TransactionInformation.LocalIdentifier
                        } status: {
                        e.Transaction.TransactionInformation.Status}");

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "ServiceBusOrchestrationService-CompleteTaskOrchestrationWorkItem-CreateTransaction",
                    runtimeState.OrchestrationInstance,
                    () => $@"Created new Orchestration Transaction - txnid: {
                        Transaction.Current.TransactionInformation.LocalIdentifier
                        }");

                if (await TrySetSessionStateAsync(workItem, newOrchestrationRuntimeState, runtimeState, session))
                {
                    if (outboundMessages?.Count > 0)
                    {
                        MessageContainer[] outboundBrokeredMessages = await Task.WhenAll(outboundMessages.Select(async m =>
                        {
                            BrokeredMessage message = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                                m,
                                this.Settings.MessageCompressionSettings,
                                this.Settings.MessageSettings,
                                null,
                                "Worker outbound message",
                                this.BlobStore,
                                DateTimeUtils.MinDateTime);
                            return new MessageContainer(message, m);
                        }));
                        await this.workerSender.SendBatchAsync(outboundBrokeredMessages.Select(m => m.Message));
                        LogSentMessages(session, "Worker outbound", outboundBrokeredMessages);
                        this.ServiceStats.ActivityDispatcherStats.MessageBatchesSent.Increment();
                        this.ServiceStats.ActivityDispatcherStats.MessagesSent.Increment(outboundMessages.Count);
                    }

                    if (timerMessages?.Count > 0 && newOrchestrationRuntimeState != null)
                    {
                        MessageContainer[] timerBrokeredMessages = await Task.WhenAll(timerMessages.Select(async m =>
                        {
                            DateTime messageFireTime = ((TimerFiredEvent)m.Event).FireAt;
                            BrokeredMessage message = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                                m,
                                this.Settings.MessageCompressionSettings,
                                this.Settings.MessageSettings,
                                newOrchestrationRuntimeState.OrchestrationInstance,
                                "Timer Message",
                                this.BlobStore,
                                messageFireTime);
                            message.ScheduledEnqueueTimeUtc = messageFireTime;
                            return new MessageContainer(message, m);
                        }));

                        await this.orchestratorQueueClient.SendBatchAsync(timerBrokeredMessages.Select(m => m.Message));
                        LogSentMessages(session, "Timer Message", timerBrokeredMessages);
                        this.ServiceStats.OrchestrationDispatcherStats.MessageBatchesSent.Increment();
                        this.ServiceStats.OrchestrationDispatcherStats.MessagesSent.Increment(timerMessages.Count);
                    }

                    if (orchestratorMessages?.Count > 0)
                    {
                        MessageContainer[] orchestrationBrokeredMessages = await Task.WhenAll(orchestratorMessages.Select(async m =>
                        {
                            BrokeredMessage message = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                                m,
                                this.Settings.MessageCompressionSettings,
                                this.Settings.MessageSettings,
                                m.OrchestrationInstance,
                                "Sub Orchestration",
                                this.BlobStore,
                                DateTimeUtils.MinDateTime);
                            return new MessageContainer(message, m);
                        }));
                        await this.orchestratorQueueClient.SendBatchAsync(orchestrationBrokeredMessages.Select(m => m.Message));

                        LogSentMessages(session, "Sub Orchestration", orchestrationBrokeredMessages);
                        this.ServiceStats.OrchestrationDispatcherStats.MessageBatchesSent.Increment();
                        this.ServiceStats.OrchestrationDispatcherStats.MessagesSent.Increment(orchestratorMessages.Count);
                    }

                    if (continuedAsNewMessage != null)
                    {
                        BrokeredMessage continuedAsNewBrokeredMessage = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                            continuedAsNewMessage,
                            this.Settings.MessageCompressionSettings,
                            this.Settings.MessageSettings,
                            newOrchestrationRuntimeState?.OrchestrationInstance,
                            "Continue as new",
                            this.BlobStore,
                            DateTimeUtils.MinDateTime);
                        await this.orchestratorQueueClient.SendAsync(continuedAsNewBrokeredMessage);
                        LogSentMessages(session, "Continue as new", new List<MessageContainer> { new MessageContainer(continuedAsNewBrokeredMessage, null) });
                        this.ServiceStats.OrchestrationDispatcherStats.MessageBatchesSent.Increment();
                        this.ServiceStats.OrchestrationDispatcherStats.MessagesSent.Increment();
                    }

                    if (this.InstanceStore != null)
                    {
                        List<MessageContainer> trackingMessages = await CreateTrackingMessagesAsync(runtimeState, sessionState.SequenceNumber);
                        TraceHelper.TraceInstance(
                            TraceEventType.Information,
                            "ServiceBusOrchestrationService-CompleteTaskOrchestrationWorkItem-TrackingMessages",
                            runtimeState.OrchestrationInstance,
                            "Created {0} tracking messages", trackingMessages.Count);

                        if (trackingMessages.Count > 0)
                        {
                            await this.trackingSender.SendBatchAsync(trackingMessages.Select(m => m.Message));
                            LogSentMessages(session, "Tracking messages", trackingMessages);
                            this.ServiceStats.TrackingDispatcherStats.MessageBatchesSent.Increment();
                            this.ServiceStats.TrackingDispatcherStats.MessagesSent.Increment(trackingMessages.Count);
                        }

                        if (newOrchestrationRuntimeState != null && runtimeState != newOrchestrationRuntimeState)
                        {
                            trackingMessages = await CreateTrackingMessagesAsync(newOrchestrationRuntimeState, sessionState.SequenceNumber);
                            TraceHelper.TraceInstance(
                                TraceEventType.Information,
                                "ServiceBusOrchestrationService-CompleteTaskOrchestrationWorkItem-TrackingMessages",
                                newOrchestrationRuntimeState.OrchestrationInstance,
                                "Created {0} tracking messages", trackingMessages.Count);

                            if (trackingMessages.Count > 0)
                            {
                                await this.trackingSender.SendBatchAsync(trackingMessages.Select(m => m.Message));
                                LogSentMessages(session, "Tracking messages", trackingMessages);
                                this.ServiceStats.TrackingDispatcherStats.MessageBatchesSent.Increment();
                                this.ServiceStats.TrackingDispatcherStats.MessagesSent.Increment(trackingMessages.Count);
                            }
                        }
                    }
                }

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "ServiceBusOrchestrationService-CompleteTaskOrchestrationWorkItemMessages",
                    runtimeState.OrchestrationInstance,
                    () =>
                    {
                        string allIds = string.Join(" ", sessionState.LockTokens.Values.Select(m => $"[SEQ: {m.SequenceNumber} LT: {m.LockToken}]"));
                        return $"Completing orchestration messages sequence and lock tokens: {allIds}";
                    });

                await session.CompleteBatchAsync(sessionState.LockTokens.Keys);
                this.ServiceStats.OrchestrationDispatcherStats.SessionBatchesCompleted.Increment();
                ts.Complete();
            }
        }

        /// <summary>
        ///     Release the lock on an orchestration, releases the session, decoupled from CompleteTaskOrchestrationWorkItemAsync to handle nested orchestrations
        /// </summary>
        /// <param name="workItem">The task orchestration to abandon</param>
        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ServiceBusOrchestrationSession sessionState = GetAndDeleteSessionInstanceForWorkItem(workItem);
            // This is Ok, if we abandoned the message it will already be gone
            if (sessionState == null)
            {
                TraceHelper.TraceSession(
                    TraceEventType.Warning,
                    "ServiceBusOrchestrationService-ReleaseTaskOrchestrationWorkItemFailed",
                    workItem?.InstanceId,
                    "DeleteSessionInstance failed, could already be aborted");
                return;
            }

            await sessionState.Session.CloseAsync();
        }

        /// <summary>
        ///     Abandon an orchestration, this abandons ownership/locking of all messages for an orchestration and it's session
        /// </summary>
        /// <param name="workItem">The task orchestration to abandon</param>
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ServiceBusOrchestrationSession sessionState = GetAndDeleteSessionInstanceForWorkItem(workItem);
            if (sessionState?.Session == null)
            {
                return;
            }

            TraceHelper.TraceSession(TraceEventType.Error, "ServiceBusOrchestrationService-AbandonTaskOrchestrationWorkItem", workItem.InstanceId, "Abandoning {0} messages due to work item abort", sessionState.LockTokens.Keys.Count());
            foreach (Guid lockToken in sessionState.LockTokens.Keys)
            {
                await sessionState.Session.AbandonAsync(lockToken);
            }

            try
            {
                sessionState.Session.Abort();
            }
            catch (Exception ex) when (!Utils.IsFatal(ex))
            {
                TraceHelper.TraceExceptionSession(TraceEventType.Warning, "ServiceBusOrchestrationService-AbandonTaskOrchestrationWorkItemError", workItem.InstanceId, ex, "Error while aborting session");
            }
        }

        /// <summary>
        /// Gets the the number of task activity dispatchers
        /// </summary>
        public int TaskActivityDispatcherCount => this.Settings.TaskActivityDispatcherSettings.DispatcherCount;

        /// <summary>
        /// Gets the maximum number of concurrent task activity items
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems => this.Settings.TaskActivityDispatcherSettings.MaxConcurrentActivities;

        /// <summary>
        ///    Wait for an lock the next task activity to be processed 
        /// </summary>
        /// <param name="receiveTimeout">The timespan to wait for new messages before timing out</param>
        /// <param name="cancellationToken">The cancellation token to cancel execution of the task</param>
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            BrokeredMessage receivedMessage = await this.workerQueueClient.ReceiveAsync(receiveTimeout);
            if (receivedMessage == null)
            {
                return null;
            }

            this.ServiceStats.ActivityDispatcherStats.MessagesReceived.Increment();

            TraceHelper.TraceSession(
                TraceEventType.Information,
                "ServiceBusOrchestrationService-LockNextTaskActivityWorkItem-Messages",
                receivedMessage.SessionId,
                GetFormattedLog($"New message to process: {receivedMessage.MessageId} [{receivedMessage.SequenceNumber}], latency: {receivedMessage.DeliveryLatency()}ms"));

            TaskMessage taskMessage = await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(receivedMessage, this.BlobStore);

            ServiceBusUtils.CheckAndLogDeliveryCount(receivedMessage, this.Settings.MaxTaskActivityDeliveryCount);

            if (!this.orchestrationMessages.TryAdd(receivedMessage.MessageId, receivedMessage))
            {
                string error = $"Duplicate orchestration message id '{receivedMessage.MessageId}', id already exists in message list.";
                TraceHelper.Trace(TraceEventType.Error, "ServiceBusOrchestrationService-DuplicateOrchestration", error);
                throw new OrchestrationFrameworkException(error);
            }

            return new TaskActivityWorkItem
            {
                Id = receivedMessage.MessageId,
                LockedUntilUtc = receivedMessage.LockedUntilUtc,
                TaskMessage = taskMessage
            };
        }

        BrokeredMessage GetBrokeredMessageForWorkItem(TaskActivityWorkItem workItem)
        {
            if (string.IsNullOrWhiteSpace(workItem?.Id))
            {
                return null;
            }

            this.orchestrationMessages.TryGetValue(workItem.Id, out BrokeredMessage message);
            return message;
        }

        BrokeredMessage GetAndDeleteBrokeredMessageForWorkItem(TaskActivityWorkItem workItem)
        {
            if (string.IsNullOrWhiteSpace(workItem?.Id))
            {
                return null;
            }

            this.orchestrationMessages.TryRemove(workItem.Id, out BrokeredMessage existingMessage);
            return existingMessage;
        }

        /// <summary>
        ///    Renew the lock on a still processing work item
        /// </summary>
        /// <param name="workItem">Work item to renew the lock on</param>
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            BrokeredMessage message = GetBrokeredMessageForWorkItem(workItem);
            if (message != null)
            {
                await message.RenewLockAsync();
                workItem.LockedUntilUtc = message.LockedUntilUtc;
                this.ServiceStats.ActivityDispatcherStats.SessionsRenewed.Increment();
            }

            return workItem;
        }

        /// <summary>
        ///    Atomically complete a work item and send the response messages
        /// </summary>
        /// <param name="workItem">Work item to complete</param>
        /// <param name="responseMessage">The response message to send</param>
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            BrokeredMessage brokeredResponseMessage = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                responseMessage,
                this.Settings.MessageCompressionSettings,
                this.Settings.MessageSettings,
                workItem.TaskMessage.OrchestrationInstance,
                $"Response for {workItem.TaskMessage.OrchestrationInstance.InstanceId}",
                this.BlobStore,
                DateTimeUtils.MinDateTime);

            BrokeredMessage originalMessage = GetAndDeleteBrokeredMessageForWorkItem(workItem);
            if (originalMessage == null)
            {
                // ReSharper disable once NotResolvedInText
                throw new ArgumentNullException("originalMessage");
            }

            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                Transaction.Current.TransactionCompleted += (o, e) =>
                    TraceHelper.TraceInstance(
                    e.Transaction.TransactionInformation.Status == TransactionStatus.Committed ? TraceEventType.Information : TraceEventType.Error,
                    "ServiceBusOrchestrationService-CompleteTaskActivityWorkItem-TransactionComplete",
                    workItem.TaskMessage.OrchestrationInstance,
                    () => $@"TaskActivity Transaction Completed {
                        e.Transaction.TransactionInformation.LocalIdentifier
                        } status: {
                        e.Transaction.TransactionInformation.Status}");

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "ServiceBusOrchestrationService-CompleteTaskActivityWorkItem-CreateTransaction",
                    workItem.TaskMessage.OrchestrationInstance,
                    () => $@"Created new TaskActivity Transaction - txnid: {
                        Transaction.Current.TransactionInformation.LocalIdentifier
                        } - message sequence and lock token: [SEQ: {originalMessage.SequenceNumber} LT: {originalMessage.LockToken}]");

                await Task.WhenAll(
                    this.workerQueueClient.CompleteAsync(originalMessage.LockToken),
                    this.orchestratorSender.SendAsync(brokeredResponseMessage));
                ts.Complete();
                this.ServiceStats.ActivityDispatcherStats.SessionBatchesCompleted.Increment();
                this.ServiceStats.OrchestrationDispatcherStats.MessagesSent.Increment();
                this.ServiceStats.OrchestrationDispatcherStats.MessageBatchesSent.Increment();
            }
        }

        /// <summary>
        ///    Abandons a single work item and releases the lock on it
        /// </summary>
        /// <param name="workItem">The work item to abandon</param>
        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            BrokeredMessage message = GetAndDeleteBrokeredMessageForWorkItem(workItem);
            TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-AbandonTaskActivityWorkItem", $"Abandoning message {workItem?.Id}");

            return message == null
                ? Task.FromResult<object>(null)
                : message.AbandonAsync();
        }

        /// <summary>
        /// Creates a new orchestration
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <exception cref="OrchestrationAlreadyExistsException">Will throw exception If any orchestration with the same instance Id exists in the instance store.</exception>
        /// <returns></returns>
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return CreateTaskOrchestrationAsync(creationMessage, null);
        }

        /// <summary>
        /// Creates a new orchestration and specifies a subset of states which should be de duplicated on in the client side
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <param name="dedupeStatuses">States of previous orchestration executions to be considered while de-duping new orchestrations on the client</param>
        /// <exception cref="OrchestrationAlreadyExistsException">Will throw an OrchestrationAlreadyExistsException exception If any orchestration with the same instance Id exists in the instance store and it has a status specified in dedupeStatuses.</exception>
        /// <returns></returns>
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            // First, lets push the orchestration state (Pending) into JumpStart table
            var jumpStartEnabled = false;
            if (this.InstanceStore != null)
            {
                // TODO: GetOrchestrationState is still flaky as we are fetching from 2 tables while messages are being deleted and added
                // to JumpStart table by JumpStart manager
                // not thread safe in case multiple clients attempt to create an orchestration instance with the same id at the same time

                OrchestrationState latestState = (await GetOrchestrationStateAsync(creationMessage.OrchestrationInstance.InstanceId, false)).FirstOrDefault();
                if (latestState != null && (dedupeStatuses == null || dedupeStatuses.Contains(latestState.OrchestrationStatus)))
                {
                    // An orchestration with same instance id is already running
                    throw new OrchestrationAlreadyExistsException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' already exists. It is in state {latestState.OrchestrationStatus}");
                }

                await UpdateJumpStartStoreAsync(creationMessage);
                jumpStartEnabled = true;
            }

            try
            {
                // Second, lets queue orchestration
                await SendTaskOrchestrationMessageAsync(creationMessage);
            }
            catch (Exception ex) when (!Utils.IsFatal(ex) && jumpStartEnabled)
            {
                // Ignore exception
                TraceHelper.Trace(TraceEventType.Warning, "ServiceBusOrchestrationService-CreateTaskOrchestration-ServiceBusError", $"Error while adding message to ServiceBus: {ex.ToString()}");
            }
        }

        /// <summary>
        /// Writes an execution started event to the jump start table in the instance store
        /// </summary>
        /// <param name="creationMessage">Orchestration started message</param>
        public async Task UpdateJumpStartStoreAsync(TaskMessage creationMessage)
        {
            var executionStartedEvent = creationMessage.Event as ExecutionStartedEvent;
            DateTime createTime = DateTime.UtcNow;
            var orchestrationState = new OrchestrationState
            {
                Name = executionStartedEvent?.Name,
                Version = executionStartedEvent?.Version,
                OrchestrationInstance = creationMessage.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent?.Input,
                Tags = executionStartedEvent?.Tags,
                CreatedTime = createTime,
                LastUpdatedTime = createTime,
                CompletedTime = DateTimeUtils.MinDateTime
            };

            var jumpStartEntity = new OrchestrationJumpStartInstanceEntity
            {
                State = orchestrationState,
                JumpStartTime = DateTimeUtils.MinDateTime
            };

            await this.InstanceStore.WriteJumpStartEntitiesAsync(new[] { jumpStartEntity });
        }

        /// <summary>
        ///    Sends an orchestration message
        /// </summary>
        /// <param name="message">The task message to be sent for the orchestration</param>
        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <summary>
        ///    Sends a set of orchestration messages
        /// </summary>
        /// <param name="messages">The task messages to be sent for the orchestration</param>
        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            if (messages.Length == 0)
            {
                return;
            }

            var tasks = new Task<BrokeredMessage>[messages.Length];
            for (var i = 0; i < messages.Length; i++)
            {
                tasks[i] = GetBrokeredMessageAsync(messages[i]);
            }

            BrokeredMessage[] brokeredMessages = await Task.WhenAll(tasks);
            await this.orchestrationBatchMessageSender.SendBatchAsync(brokeredMessages).ConfigureAwait(false);
        }

        async Task<BrokeredMessage> GetBrokeredMessageAsync(TaskMessage message)
        {
            BrokeredMessage brokeredMessage = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                message,
                this.Settings.MessageCompressionSettings,
                this.Settings.MessageSettings,
                message.OrchestrationInstance,
                "SendTaskOrchestrationMessage",
                this.BlobStore,
                DateTimeUtils.MinDateTime);

            // Use duplicate detection of ExecutionStartedEvent by adding messageId
            if (message.Event is ExecutionStartedEvent executionStartedEvent)
            {
                brokeredMessage.MessageId = $"{executionStartedEvent.OrchestrationInstance.InstanceId}_{executionStartedEvent.OrchestrationInstance.ExecutionId}";
            }

            return brokeredMessage;
        }

        /// <summary>
        ///    Force terminates an orchestration by sending a execution terminated event
        /// </summary>
        /// <param name="instanceId">The instance id to terminate</param>
        /// <param name="reason">The string reason for terminating</param>
        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            await SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <summary>
        ///     Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="executionId">The execution id of the orchestration</param>
        /// <param name="instanceId">Instance to wait for</param>
        /// <param name="timeout">Max timeout to wait</param>
        /// <param name="cancellationToken">Task cancellation token</param>
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            ThrowIfInstanceStoreNotConfigured();

            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException("instanceId");
            }

            double timeoutSeconds = timeout.TotalSeconds;

            while (!cancellationToken.IsCancellationRequested && timeoutSeconds > 0)
            {
                OrchestrationState state = (await GetOrchestrationStateAsync(instanceId, false))?.FirstOrDefault();
                if (state == null
                    || (state.OrchestrationStatus == OrchestrationStatus.Running)
                    || (state.OrchestrationStatus == OrchestrationStatus.Pending))
                {
                    await Task.Delay(StatusPollingIntervalInSeconds * 1000, cancellationToken);
                    timeoutSeconds -= StatusPollingIntervalInSeconds;
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="allExecutions">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>List of OrchestrationState objects that represents the list of orchestrations in the instance store</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationStateInstanceEntity> states = await this.InstanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            return states?.Select(s => s.State).ToList() ?? new List<OrchestrationState>();
        }

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Execution id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            OrchestrationStateInstanceEntity state = await this.InstanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return state?.State;
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified orchestration instance specified execution (generation) of the specified instance
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Execution id</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationWorkItemInstanceEntity> historyEvents =
                await this.InstanceStore.GetOrchestrationHistoryEventsAsync(instanceId, executionId);

            return DataConverter.Serialize(historyEvents.Select(historyEventEntity => historyEventEntity.HistoryEvent));
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// Also purges the blob storage.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        public async Task PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-PurgeOrchestrationHistory-Start", $"Purging orchestration instances before: {thresholdDateTimeUtc}, Type: {timeRangeFilterType}");

            if (this.BlobStore != null)
            {
                await this.BlobStore.PurgeExpiredBlobsAsync(thresholdDateTimeUtc);
                TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-BlobsPurged", "Blob storage is purged.");
            }

            if (this.InstanceStore != null)
            {
                int purgedEvents = await this.InstanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);
                TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-HistoryPurged", $"Purged {purgedEvents} orchestration histories");
            }
        }

        /// <summary>
        ///     Inspect an exception to see if it's a transient exception that may apply different delays
        /// </summary>
        /// <param name="exception">Exception to inspect</param>
        static bool IsTransientException(Exception exception)
        {
            // TODO : Once we change the exception model, check for inner exception
            return (exception as MessagingException)?.IsTransient ?? false;
        }

        /// <summary>
        ///     Wait for the next orchestration work item and return the orchestration work item
        /// </summary>
        /// <param name="receiveTimeout">The timespan to wait for new messages before timing out</param>
        /// <param name="cancellationToken">A cancellation token which signals a host shutdown</param>
        async Task<TrackingWorkItem> FetchTrackingWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            MessageSession session = await this.trackingQueueClient.AcceptMessageSessionAsync(receiveTimeout);
            if (session == null)
            {
                return null;
            }

            this.ServiceStats.TrackingDispatcherStats.SessionsReceived.Increment();

            IList<BrokeredMessage> newMessages =
                (await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(this.Settings.PrefetchCount),
                    session.SessionId, "Receive Tracking Session Message Batch", this.Settings.MaxRetries, this.Settings.IntervalBetweenRetriesSecs)).ToList();
            this.ServiceStats.TrackingDispatcherStats.MessagesReceived.Increment(newMessages.Count);

            TraceHelper.TraceSession(
                TraceEventType.Information,
                "ServiceBusOrchestrationService-FetchTrackingWorkItem-Messages",
                session.SessionId,
                GetFormattedLog($"{newMessages.Count} new tracking messages to process: {string.Join(",", newMessages.Select(m => m.MessageId))}"));

            ServiceBusUtils.CheckAndLogDeliveryCount(newMessages, this.Settings.MaxTrackingDeliveryCount);

            IList<TaskMessage> newTaskMessages = await Task.WhenAll(
                newMessages.Select(async message => await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message, this.BlobStore)));

            Dictionary<Guid, BrokeredMessage> lockTokens = newMessages.ToDictionary(m => m.LockToken, m => m);
            var sessionState = new ServiceBusOrchestrationSession
            {
                Session = session,
                LockTokens = lockTokens
            };

            return new TrackingWorkItem
            {
                InstanceId = session.SessionId,
                LockedUntilUtc = session.LockedUntilUtc,
                NewMessages = newTaskMessages.ToList(),
                SessionInstance = sessionState
            };
        }

        /// <summary>
        ///     Creates a list of tracking message for the supplied orchestration state
        /// </summary>
        /// <param name="runtimeState">The orchestration runtime state</param>
        /// <param name="sequenceNumber">Sequence number for the created tracking messages</param>
        async Task<List<MessageContainer>> CreateTrackingMessagesAsync(OrchestrationRuntimeState runtimeState, long sequenceNumber)
        {
            var trackingMessages = new List<MessageContainer>();

            // We cannot create tracking messages if runtime state does not have Orchestration InstanceId
            // This situation can happen due to corruption of service bus session state or if somehow first message of orchestration is not execution started
            if (string.IsNullOrWhiteSpace(runtimeState?.OrchestrationInstance?.InstanceId))
            {
                return trackingMessages;
            }

            if (this.Settings.TrackingDispatcherSettings.TrackHistoryEvents)
            {
                // this is to stamp the tracking events with a sequence number so they can be ordered even if
                // writing to a store like azure table
                int historyEventIndex = runtimeState.Events.Count - runtimeState.NewEvents.Count;
                foreach (HistoryEvent he in runtimeState.NewEvents)
                {
                    var taskMessage = new TaskMessage
                    {
                        Event = he,
                        SequenceNumber = historyEventIndex++,
                        OrchestrationInstance = runtimeState.OrchestrationInstance
                    };

                    BrokeredMessage trackingMessage = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                        taskMessage,
                        this.Settings.MessageCompressionSettings,
                        this.Settings.MessageSettings,
                        runtimeState.OrchestrationInstance,
                        "History Tracking Message",
                        this.BlobStore,
                        DateTimeUtils.MinDateTime);
                    trackingMessages.Add(new MessageContainer(trackingMessage, null));
                }
            }

            var stateMessage = new TaskMessage
            {
                Event = new HistoryStateEvent(-1, Utils.BuildOrchestrationState(runtimeState)),
                SequenceNumber = sequenceNumber,
                OrchestrationInstance = runtimeState.OrchestrationInstance
            };

            BrokeredMessage brokeredStateMessage = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                stateMessage,
                this.Settings.MessageCompressionSettings,
                this.Settings.MessageSettings,
                runtimeState.OrchestrationInstance,
                "State Tracking Message",
                this.BlobStore,
                DateTimeUtils.MinDateTime);
            trackingMessages.Add(new MessageContainer(brokeredStateMessage, null));

            return trackingMessages;
        }

        /// <summary>
        ///     Process a tracking work item, sending to storage and releasing
        /// </summary>
        /// <param name="workItem">The tracking work item to process</param>
        async Task ProcessTrackingWorkItemAsync(TrackingWorkItem workItem)
        {
            var sessionState = workItem.SessionInstance as ServiceBusOrchestrationSession;
            if (sessionState == null)
            {
                throw new ArgumentNullException(nameof(workItem.SessionInstance));
            }

            var historyEntities = new List<OrchestrationWorkItemInstanceEntity>();
            var stateEntities = new List<OrchestrationStateInstanceEntity>();

            foreach (TaskMessage taskMessage in workItem.NewMessages)
            {
                if (taskMessage.Event.EventType == EventType.HistoryState)
                {
                    stateEntities.Add(new OrchestrationStateInstanceEntity
                    {
                        State = (taskMessage.Event as HistoryStateEvent)?.State,
                        SequenceNumber = taskMessage.SequenceNumber
                    });
                }
                else
                {
                    historyEntities.Add(new OrchestrationWorkItemInstanceEntity
                    {
                        InstanceId = taskMessage.OrchestrationInstance.InstanceId,
                        ExecutionId = taskMessage.OrchestrationInstance.ExecutionId,
                        SequenceNumber = taskMessage.SequenceNumber,
                        EventTimestamp = DateTime.UtcNow,
                        HistoryEvent = taskMessage.Event
                    });
                }
            }

            TraceEntities(TraceEventType.Verbose, "Writing tracking history event", historyEntities, GetNormalizedWorkItemEvent);
            TraceEntities(TraceEventType.Verbose, "Writing tracking state event", stateEntities, GetNormalizedStateEvent);

            try
            {
                await this.InstanceStore.WriteEntitiesAsync(historyEntities);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, $"Failed to write history entity: {e}", historyEntities, GetNormalizedWorkItemEvent);
                throw;
            }

            try
            {
                // TODO : send batch to instance store, it can write it as individual if it chooses
                foreach (OrchestrationStateInstanceEntity stateEntity in stateEntities)
                {
                    await this.InstanceStore.WriteEntitiesAsync(new List<OrchestrationStateInstanceEntity> { stateEntity });
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, $"Failed to write state entity: {e}", stateEntities, GetNormalizedStateEvent);
                throw;
            }

            // Cleanup our session
            await sessionState.Session.CompleteBatchAsync(sessionState.LockTokens.Keys);
            await sessionState.Session.CloseAsync();
        }

        void TraceEntities<T>(
            TraceEventType eventType,
            string message,
            IEnumerable<T> entities,
            Func<int, string, T, string> traceGenerator)
        {
            var index = 0;
            foreach (T entry in entities)
            {
                int idx = index;
                TraceHelper.Trace(eventType, "ServiceBusOrchestrationService-Entities", () => traceGenerator(idx, message, entry));
                index++;
            }
        }

        string GetNormalizedStateEvent(int index, string message, OrchestrationStateInstanceEntity stateEntity)
        {
            string serializedHistoryEvent = Utils.EscapeJson(DataConverter.Serialize(stateEntity.State));
            int historyEventLength = serializedHistoryEvent.Length;

            int maxLen = this.InstanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

            if (historyEventLength > maxLen)
            {
                serializedHistoryEvent = serializedHistoryEvent.Substring(0, maxLen) + " ....(truncated)..]";
            }

            return GetFormattedLog(
                $"{message} - #{index} - Instance Id: {stateEntity.State?.OrchestrationInstance?.InstanceId},"
                + $" Execution Id: {stateEntity.State?.OrchestrationInstance?.ExecutionId},"
                + $" State Length: {historyEventLength}\n{serializedHistoryEvent}");
        }

        string GetNormalizedWorkItemEvent(int index, string message, OrchestrationWorkItemInstanceEntity entity)
        {
            string serializedHistoryEvent = Utils.EscapeJson(DataConverter.Serialize(entity.HistoryEvent));
            int historyEventLength = serializedHistoryEvent.Length;
            int maxLen = this.InstanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

            if (historyEventLength > maxLen)
            {
                serializedHistoryEvent = serializedHistoryEvent.Substring(0, maxLen) + " ....(truncated)..]";
            }

            return GetFormattedLog(
                $"{message} - #{index} - Instance Id: {entity.InstanceId}, Execution Id: {entity.ExecutionId}, HistoryEvent Length: {historyEventLength}\n{serializedHistoryEvent}");
        }

        string GetFormattedLog(string input)
        {
            // TODO : take context from the dispatcher and use that to format logs
            return input;
        }

        void LogSentMessages(MessageSession session, string messageType, IList<MessageContainer> messages)
        {
            TraceHelper.TraceSession(
                TraceEventType.Information,
                "ServiceBusOrchestrationService-SentMessageLog",
                session.SessionId,
                GetFormattedLog($@"{messages.Count.ToString()} messages queued for {messageType}: {
                    string.Join(",", messages.Select(m => $"{m.Message.MessageId} <{m.Action?.Event.EventId.ToString()}>"))}"));
        }

        async Task<OrchestrationRuntimeState> GetSessionStateAsync(MessageSession session, IOrchestrationServiceBlobStore orchestrationServiceBlobStore)
        {
            using (Stream rawSessionStream = await session.GetStateAsync())
            {
                this.ServiceStats.OrchestrationDispatcherStats.SessionGets.Increment();
                return await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawSessionStream, session.SessionId, orchestrationServiceBlobStore, DataConverter);
            }
        }

        async Task<bool> TrySetSessionStateAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            OrchestrationRuntimeState runtimeState,
            MessageSession session)
        {
            if (runtimeState.CompressedSize > SessionStreamWarningSizeInBytes && runtimeState.CompressedSize < this.Settings.SessionSettings.SessionOverflowThresholdInBytes)
            {
                TraceHelper.TraceSession(
                    TraceEventType.Error,
                    "ServiceBusOrchestrationService-SessionStateThresholdApproaching",
                    workItem.InstanceId,
                    $"Size of session state ({runtimeState.CompressedSize}B) is nearing session size limit of {this.Settings.SessionSettings.SessionOverflowThresholdInBytes}B");
            }

            var isSessionSizeThresholdExceeded = false;

            if (newOrchestrationRuntimeState == null || newOrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running)
            {
                await session.SetStateAsync(null);
                return true;
            }

            try
            {
                Stream rawStream = await
                    RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                        newOrchestrationRuntimeState,
                        runtimeState,
                        DataConverter,
                        this.Settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState,
                        this.Settings.SessionSettings,
                        this.BlobStore,
                        session.SessionId);

                await session.SetStateAsync(rawStream);
                this.ServiceStats.OrchestrationDispatcherStats.SessionSets.Increment();
            }
            catch (OrchestrationException exception)
            {
                // basic idea is to simply enqueue a terminate message just like how we do it from TaskHubClient
                // it is possible to have other messages in front of the queue and those will get processed before
                // the terminate message gets processed. but that is ok since in the worst case scenario we will
                // simply land in this if-block again and end up queuing up another terminate message.
                //
                // the interesting scenario is when the second time we *don't* land in this if-block because e.g.
                // the new messages that we processed caused a new generation to be created. in that case
                // it is still ok because the worst case scenario is that we will terminate a newly created generation
                // which shouldn't have been created at all in the first place

                isSessionSizeThresholdExceeded = true;

                string reason = $"Session state size of {runtimeState.CompressedSize} exceeded the termination threshold of {this.Settings.SessionSettings.SessionMaxSizeInBytes} bytes. More info: {exception.StackTrace}";
                TraceHelper.TraceSession(TraceEventType.Critical, "ServiceBusOrchestrationService-SessionSizeExceeded", workItem.InstanceId, reason);

                BrokeredMessage forcedTerminateMessage = await CreateForcedTerminateMessageAsync(runtimeState.OrchestrationInstance.InstanceId, reason);
                await this.orchestratorQueueClient.SendAsync(forcedTerminateMessage);

                this.ServiceStats.OrchestrationDispatcherStats.MessagesSent.Increment();
                this.ServiceStats.OrchestrationDispatcherStats.MessageBatchesSent.Increment();
            }

            return !isSessionSizeThresholdExceeded;
        }

        async Task ServiceMonitorAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);

                TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-ServiceStats", "Service Stats: {0}", this.ServiceStats.ToString());
                TraceHelper.Trace(TraceEventType.Information, "ServiceBusOrchestrationService-ServiceStats-Active", "Active Session and Message Stats: Messages: {0}, Sessions: {1}", this.orchestrationMessages.Count, this.orchestrationSessions.Count);
            }
        }

        static async Task SafeDeleteQueueAsync(NamespaceManager namespaceManager, string path)
        {
            await Utils.ExecuteWithRetries(async () =>
            {
                try
                {
                    await namespaceManager.DeleteQueueAsync(path);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    await Task.FromResult(0);
                }
            }, null, "SafeDeleteQueueAsync", 3, 5);
        }

        async Task SafeCreateQueueAsync(
            NamespaceManager manager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount,
            long maxSizeInMegabytes)
        {
            await Utils.ExecuteWithRetries(async () =>
            {
                try
                {
                    await CreateQueueAsync(manager, path, requiresSessions, requiresDuplicateDetection, maxDeliveryCount, maxSizeInMegabytes);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    await Task.FromResult(0);
                }
            }, null, "SafeCreateQueueAsync", 3, 5);
        }

        async Task SafeDeleteAndCreateQueueAsync(
            NamespaceManager manager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount,
            long maxSizeInMegabytes)
        {
            await SafeDeleteQueueAsync(manager, path);
            await SafeCreateQueueAsync(manager, path, requiresSessions, requiresDuplicateDetection, maxDeliveryCount, maxSizeInMegabytes);
        }

        static readonly long[] ValidQueueSizes = { 1024L, 2048L, 3072L, 4096L, 5120L };

        async Task CreateQueueAsync(
            NamespaceManager manager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount,
            long maxSizeInMegabytes)
        {
            if (!ValidQueueSizes.Contains(maxSizeInMegabytes))
            {
                throw new ArgumentException($"The specified value {maxSizeInMegabytes} is invalid for the maximum queue size in megabytes.\r\nIt must be one of the following values:\r\n{string.Join(";", ValidQueueSizes)}", nameof(maxSizeInMegabytes));
            }

            var description = new QueueDescription(path)
            {
                RequiresSession = requiresSessions,
                MaxDeliveryCount = maxDeliveryCount,
                RequiresDuplicateDetection = requiresDuplicateDetection,
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromHours(DuplicateDetectionWindowInHours),
                MaxSizeInMegabytes = maxSizeInMegabytes
            };

            await manager.CreateQueueAsync(description);
        }

        Task<BrokeredMessage> CreateForcedTerminateMessageAsync(string instanceId, string reason)
        {
            var newOrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId };
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = newOrchestrationInstance,
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            return ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                taskMessage,
                this.Settings.MessageCompressionSettings,
                this.Settings.MessageSettings,
                newOrchestrationInstance,
                "Forced Terminate",
                this.BlobStore,
                DateTimeUtils.MinDateTime);
        }

        void ThrowIfInstanceStoreNotConfigured()
        {
            if (this.InstanceStore == null)
            {
                throw new InvalidOperationException("Instance store is not configured");
            }
        }

        internal class MessageContainer
        {
            internal BrokeredMessage Message { get; set; }

            internal TaskMessage Action { get; set; }

            internal MessageContainer(BrokeredMessage message, TaskMessage action)
            {
                Message = message;
                Action = action;
            }
        }
    }
}