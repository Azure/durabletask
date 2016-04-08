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

namespace DurableTask
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

    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.History;
    using DurableTask.Tracing;
    using DurableTask.Tracking;
    using DurableTask.Serializing;
    using DurableTask.Settings;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using System.Globalization;
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
        const int SessionStreamWarningSizeInBytes = 200 * 1024;
        const int SessionStreamTerminationThresholdInBytes = 230 * 1024;
        const int StatusPollingIntervalInSeconds = 2;
        const int DuplicateDetectionWindowInHours = 4;

        public readonly ServiceBusOrchestrationServiceSettings Settings;
        public readonly IOrchestrationServiceInstanceStore InstanceStore;

        static readonly DataConverter DataConverter = new JsonDataConverter();
        readonly string connectionString;
        readonly string hubName;
        readonly MessagingFactory messagingFactory;
        QueueClient workerQueueClient;
        MessageSender orchestratorSender;
        QueueClient orchestratorQueueClient;
        MessageSender workerSender;
        QueueClient trackingQueueClient;
        MessageSender trackingSender;
        readonly string workerEntityName;
        readonly string orchestratorEntityName;
        readonly string trackingEntityName;
        readonly WorkItemDispatcher<TrackingWorkItem> trackingDispatcher;
        readonly JumpStartManager jumpStartManager;

        // TODO : Make user of these instead of passing around the object references.
        ConcurrentDictionary<string, ServiceBusOrchestrationSession> orchestrationSessions;
        ConcurrentDictionary<string, BrokeredMessage> orchestrationMessages;

        /// <summary>
        ///     Create a new ServiceBusOrchestrationService to the given service bus connection string and hubname
        /// </summary>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="hubName">Hubname to use with the connection string</param>
        /// <param name="instanceStore">Instance store Provider, where state and history messages will be stored</param>
        /// <param name="settings">Settings object for service and client</param>
        /// <param name="jumpStartAttemptInterval">Time to wait before jumpstarting an unscheduled orchestration</param>
        public ServiceBusOrchestrationService(
            string connectionString,
            string hubName,
            IOrchestrationServiceInstanceStore instanceStore,
            ServiceBusOrchestrationServiceSettings settings,
            TimeSpan jumpStartAttemptInterval)
        {
            this.connectionString = connectionString;
            this.hubName = hubName;
            messagingFactory = ServiceBusUtils.CreateMessagingFactory(connectionString);
            workerEntityName = string.Format(FrameworkConstants.WorkerEndpointFormat, this.hubName);
            orchestratorEntityName = string.Format(FrameworkConstants.OrchestratorEndpointFormat, this.hubName);
            trackingEntityName = string.Format(FrameworkConstants.TrackingEndpointFormat, this.hubName);
            this.Settings = settings ?? new ServiceBusOrchestrationServiceSettings();
            if (instanceStore != null)
            {
                this.InstanceStore = instanceStore;
                trackingDispatcher = new WorkItemDispatcher<TrackingWorkItem>(
                    "TrackingDispatcher",
                    item => item == null ? string.Empty : item.InstanceId,
                    this.FetchTrackingWorkItemAsync,
                    this.ProcessTrackingWorkItemAsync)
                {
                    GetDelayInSecondsAfterOnFetchException = GetDelayInSecondsAfterOnFetchException,
                    GetDelayInSecondsAfterOnProcessException = GetDelayInSecondsAfterOnProcessException
                };

                jumpStartManager = new JumpStartManager(this, jumpStartAttemptInterval);
            }
        }

        /// <summary>
        /// Starts the service initializing the required resources
        /// </summary>
        public async Task StartAsync()
        {
            orchestrationSessions = new ConcurrentDictionary<string, ServiceBusOrchestrationSession>();
            orchestrationMessages = new ConcurrentDictionary<string, BrokeredMessage>();

            orchestratorSender = await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName, workerEntityName);
            workerSender = await messagingFactory.CreateMessageSenderAsync(workerEntityName, orchestratorEntityName);
            trackingSender = await messagingFactory.CreateMessageSenderAsync(trackingEntityName, orchestratorEntityName);
            workerQueueClient = messagingFactory.CreateQueueClient(workerEntityName);
            orchestratorQueueClient = messagingFactory.CreateQueueClient(orchestratorEntityName);
            trackingQueueClient = messagingFactory.CreateQueueClient(trackingEntityName);
            if (trackingDispatcher != null)
            {
                await trackingDispatcher.StartAsync();
            }

            if (this.jumpStartManager != null)
            {
                await jumpStartManager.StartAsync();
            }
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
        /// <param name="isForced">Flag when true stops resources agresssively, when false stops gracefully</param>
        public async Task StopAsync(bool isForced)
        {
            // TODO : call shutdown of any remaining orchestrationSessions and orchestrationMessages

            await Task.WhenAll(
                workerSender.CloseAsync(),
                orchestratorSender.CloseAsync(),
                trackingSender.CloseAsync(),
                orchestratorQueueClient.CloseAsync(),
                trackingQueueClient.CloseAsync(),
                workerQueueClient.CloseAsync()
                );
            if (trackingDispatcher != null)
            {
                await trackingDispatcher.StopAsync(isForced);
            }

            if (jumpStartManager != null)
            {
                await jumpStartManager.StopAsync();
            }

            // TODO : Move this, we don't open in start so calling close then start yields a broken reference
            messagingFactory.CloseAsync().Wait();
        }

        /// <summary>
        /// Deletes and creates the neccesary resources for the orchestration service including the instance store
        /// </summary>
        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        /// <summary>
        /// Deletes and creates the neccesary resources for the orchestration service
        /// </summary>
        /// <param name="recreateInstanceStore">Flag indicating whether to drop and create instance store</param>
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            await Task.WhenAll(
                this.SafeDeleteAndCreateQueueAsync(namespaceManager, orchestratorEntityName, true, true, Settings.MaxTaskOrchestrationDeliveryCount),
                this.SafeDeleteAndCreateQueueAsync(namespaceManager, workerEntityName, false, false, Settings.MaxTaskActivityDeliveryCount)
                );

            if (InstanceStore != null)
            {
                await this.SafeDeleteAndCreateQueueAsync(namespaceManager, trackingEntityName, true, false, Settings.MaxTrackingDeliveryCount);
                await InstanceStore.InitializeStoreAsync(recreateInstanceStore);
            }
        }

        /// <summary>
        /// Drops and creates the neccesary resources for the orchestration service and the instance store
        /// </summary>
        public async Task CreateIfNotExistsAsync()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            await Task.WhenAll(
                SafeCreateQueueAsync(namespaceManager, orchestratorEntityName, true, true, Settings.MaxTaskOrchestrationDeliveryCount),
                SafeCreateQueueAsync(namespaceManager, workerEntityName, false, false, Settings.MaxTaskActivityDeliveryCount)
                );
            if (InstanceStore != null)
            {
                await SafeCreateQueueAsync(namespaceManager, trackingEntityName, true, false, Settings.MaxTrackingDeliveryCount);
                await InstanceStore.InitializeStoreAsync(false);
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
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            await Task.WhenAll(
                SafeDeleteQueueAsync(namespaceManager, orchestratorEntityName),
                SafeDeleteQueueAsync(namespaceManager, workerEntityName)
                );
            if (InstanceStore != null)
            {
                await SafeDeleteQueueAsync(namespaceManager, trackingEntityName);
                if (deleteInstanceStore)
                {
                    await InstanceStore.DeleteStoreAsync();
                }
            }
        }

        // Service Bus Utility methods

        /// <summary>
        /// Utility method to check if the needed resources are available for the orchestration service
        /// </summary>
        /// <returns>True if all needed queues are present, false otherwise</returns>
        public async Task<bool> HubExistsAsync()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            IEnumerable<QueueDescription> queueDescriptions =
                (await namespaceManager.GetQueuesAsync("startswith(path, '" + hubName + "') eq TRUE")).ToList();

            return queueDescriptions.Any(q => string.Equals(q.Path, orchestratorEntityName))
                && queueDescriptions.Any(q => string.Equals(q.Path, workerEntityName))
                && (InstanceStore == null || queueDescriptions.Any(q => string.Equals(q.Path, trackingEntityName)));
        }

        /// <summary>
        ///     Get the count of pending orchestrationsb
        /// </summary>
        /// <returns>Count of pending orchestrations</returns>
        public long GetPendingOrchestrationsCount()
        {
            return GetQueueCount(orchestratorEntityName);
        }

        /// <summary>
        ///     Get the count of pending work items (activities)
        /// </summary>
        /// <returns>Count of pending activities</returns>
        public long GetPendingWorkItemsCount()
        {
            return GetQueueCount(workerEntityName);
        }

        /// <summary>
        ///     Internal method for getting the number of items in a queue
        /// </summary>
        long GetQueueCount(string entityName)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            QueueDescription queueDescription = namespaceManager.GetQueue(entityName);
            if (queueDescription == null)
            {
                throw TraceHelper.TraceException(TraceEventType.Error,
                    new ArgumentException($"Queue {entityName} does not exist"));
            }
            return queueDescription.MessageCount;
        }

        /// <summary>
        ///     Internal method for getting the max delivery counts for each queue
        /// </summary>
        internal async Task<Dictionary<string, int>> GetHubQueueMaxDeliveryCountsAsync()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            var result = new Dictionary<string, int>(3);

            IEnumerable<QueueDescription> queues =
                (await namespaceManager.GetQueuesAsync("startswith(path, '" + hubName + "') eq TRUE")).ToList();

            result.Add("TaskOrchestration", queues.Single(q => string.Equals(q.Path, orchestratorEntityName))?.MaxDeliveryCount ?? -1);
            result.Add("TaskActivity", queues.Single(q => string.Equals(q.Path, workerEntityName))?.MaxDeliveryCount ?? -1);
            result.Add("Tracking", queues.Single(q => string.Equals(q.Path, trackingEntityName))?.MaxDeliveryCount ?? -1);

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
                + ((InstanceStore != null) ? runtimeState.NewEvents.Count + 1 : 0) // one history message per new message + 1 for the orchestration
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
                return Settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
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

            int delay = Settings.TaskOrchestrationDispatcherSettings.NonTransientErrorBackOffSecs;
            if (IsTransientException(exception))
            {
                delay = Settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }

            return delay;
        }

        /// <summary>
        /// Gets the maximum number of concurrent task orchestration items
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems => this.Settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;

        /// <summary>
        ///     Wait for the next orchestration work item and return the orchestration work item
        /// </summary>
        /// <param name="receiveTimeout">The timespan to wait for new messages before timing out</param>
        /// <param name="cancellationToken">The cancellation token to cancel execution of the task</param>
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            MessageSession session = await orchestratorQueueClient.AcceptMessageSessionAsync(receiveTimeout);

            if (session == null)
            {
                return null;
            }

            // TODO : Here and elsewhere, consider standard retry block instead of our own hand rolled version
            IList<BrokeredMessage> newMessages =
                (await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(Settings.PrefetchCount),
                    session.SessionId, "Receive Session Message Batch", Settings.MaxRetries, Settings.IntervalBetweenRetriesSecs)).ToList();

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                GetFormattedLog($"{newMessages.Count()} new messages to process: {string.Join(",", newMessages.Select(m => m.MessageId))}"));

            ServiceBusUtils.CheckAndLogDeliveryCount(session.SessionId, newMessages, Settings.MaxTaskOrchestrationDeliveryCount);

            IList<TaskMessage> newTaskMessages = await Task.WhenAll(
                newMessages.Select(async message => await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message)));

            OrchestrationRuntimeState runtimeState = await GetSessionState(session);

            var lockTokens = newMessages.ToDictionary(m => m.LockToken, m => m);
            var sessionState = new ServiceBusOrchestrationSession
            {
                Session = session,
                LockTokens = lockTokens
            };

            if (!orchestrationSessions.TryAdd(session.SessionId, sessionState))
            {
                var error = $"Duplicate orchestration session id '{session.SessionId}', id already exists in session list.";
                TraceHelper.Trace(TraceEventType.Error, error);
                throw new OrchestrationFrameworkException(error);
            }

            if (this.InstanceStore != null)
            {
                TaskMessage executionStartedMessage = newTaskMessages.Where(m => m.Event is ExecutionStartedEvent).FirstOrDefault();

                if (executionStartedMessage != null)
                {
                    await UpdateInstanceStoreAsync(executionStartedMessage.Event as ExecutionStartedEvent);
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

        Task UpdateInstanceStoreAsync(ExecutionStartedEvent executionStartedEvent)
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
                CompletedTime = DateTime.MinValue
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity()
            {
                State = orchestrationState,
            };

            return this.InstanceStore.WriteEntitesAsync(new[] { orchestrationStateEntity });
        }

        ServiceBusOrchestrationSession GetSessionInstanceForWorkItem(TaskOrchestrationWorkItem workItem)
        {
            if (string.IsNullOrEmpty(workItem?.InstanceId))
            {
                return null;
            }

            return orchestrationSessions[workItem.InstanceId];
        }

        ServiceBusOrchestrationSession GetAndDeleteSessionInstanceForWorkItem(TaskOrchestrationWorkItem workItem)
        {
            if (string.IsNullOrEmpty(workItem?.InstanceId))
            {
                return null;
            }

            ServiceBusOrchestrationSession sessionInstance;
            orchestrationSessions.TryRemove(workItem.InstanceId, out sessionInstance);
            return sessionInstance;
        }

        /// <summary>
        ///     Renew the lock on an orchestration
        /// </summary>
        /// <param name="workItem">The task orchestration to renew the lock on</param>
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            var sessionState = GetSessionInstanceForWorkItem(workItem);
            if (sessionState?.Session == null)
            {
                return;
            }

            TraceHelper.TraceSession(TraceEventType.Error, workItem.InstanceId, "Renew lock on orchestration session");
            await sessionState.Session.RenewLockAsync();
            workItem.LockedUntilUtc = sessionState.Session.LockedUntilUtc;
        }

        /// <summary>
        ///     Complete an orchestation, this atomically sends any outbound messages and completes the session for all current messages
        /// </summary>
        /// <param name="workItem">The task orchestration to renew the lock on</param>
        /// <param name="newOrchestrationRuntimeState">New state of the orchestration to be persisted</param>
        /// <param name="outboundMessages">New work item messages to be processed</param>
        /// <param name="orchestratorMessages">New orchestration messages to be scheduled</param>
        /// <param name="timerMessages">Delayed exection messages to be scheduled for the orchestration</param>
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
            var runtimeState = workItem.OrchestrationRuntimeState;
            var sessionState = GetSessionInstanceForWorkItem(workItem);
            if (sessionState == null)
            {
                throw new ArgumentNullException("SessionInstance");
            }

            var session = sessionState.Session;

            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                if (await TrySetSessionState(workItem, newOrchestrationRuntimeState, runtimeState, session))
                {
                    if (runtimeState.CompressedSize > SessionStreamWarningSizeInBytes)
                    {
                        TraceHelper.TraceSession(TraceEventType.Error, workItem.InstanceId, "Size of session state is nearing session size limit of 256KB");
                    }

                    // We need to .ToList() the IEnumerable otherwise GetBrokeredMessageFromObject gets called 5 times per message due to Service Bus doing multiple enumeration
                    if (outboundMessages?.Count > 0)
                    {
                        await workerSender.SendBatchAsync(
                            outboundMessages.Select(m =>
                            ServiceBusUtils.GetBrokeredMessageFromObject(
                                m,
                                Settings.MessageCompressionSettings,
                                null,
                                "Worker outbound message"))
                            .ToList()
                            );
                    }

                    if (timerMessages?.Count > 0)
                    {
                        await orchestratorQueueClient.SendBatchAsync(
                            timerMessages.Select(m =>
                            {
                                BrokeredMessage message = ServiceBusUtils.GetBrokeredMessageFromObject(
                                m,
                                Settings.MessageCompressionSettings,
                                newOrchestrationRuntimeState.OrchestrationInstance,
                                "Timer Message");
                                message.ScheduledEnqueueTimeUtc = ((TimerFiredEvent)m.Event).FireAt;
                                return message;
                            })
                            .ToList()
                            );
                    }

                    if (orchestratorMessages?.Count > 0)
                    {
                        await orchestratorQueueClient.SendBatchAsync(
                            orchestratorMessages.Select(m =>
                            ServiceBusUtils.GetBrokeredMessageFromObject(
                                m,
                                Settings.MessageCompressionSettings,
                                m.OrchestrationInstance,
                                "Sub Orchestration"))
                            .ToList()
                            );
                    }

                    if (continuedAsNewMessage != null)
                    {
                        await orchestratorQueueClient.SendAsync(
                            ServiceBusUtils.GetBrokeredMessageFromObject(
                                continuedAsNewMessage,
                                Settings.MessageCompressionSettings,
                                newOrchestrationRuntimeState.OrchestrationInstance,
                                "Continue as new")
                            );
                    }

                    if (InstanceStore != null)
                    {
                        List<BrokeredMessage> trackingMessages = CreateTrackingMessages(runtimeState);

                        TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                            "Created {0} tracking messages", trackingMessages.Count);

                        if (trackingMessages.Count > 0)
                        {
                            await trackingSender.SendBatchAsync(trackingMessages);
                        }
                    }
                }

                await session.CompleteBatchAsync(sessionState.LockTokens.Keys);
                ts.Complete();
            }
        }

        /// <summary>
        ///     Release the lock on an orchestration, releases the session, decoupled from CompleteTaskOrchestrationWorkItemAsync to handle nested orchestrations
        /// </summary>
        /// <param name="workItem">The task orchestration to abandon</param>
        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            var sessionState = GetAndDeleteSessionInstanceForWorkItem(workItem);
            // This is Ok, if we abandoned the message it will already be gone
            if (sessionState == null)
            {
                TraceHelper.TraceSession(TraceEventType.Warning, workItem?.InstanceId,
                    $"DeleteSessionInstance failed, could already be aborted");
                return;
            }

            await sessionState.Session.CloseAsync();
        }

        /// <summary>
        ///     Abandon an orchestation, this abandons ownership/locking of all messages for an orchestation and it's session
        /// </summary>
        /// <param name="workItem">The task orchestration to abandon</param>
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            var sessionState = GetAndDeleteSessionInstanceForWorkItem(workItem);
            if (sessionState?.Session == null)
            {
                return;
            }

            TraceHelper.TraceSession(TraceEventType.Error, workItem.InstanceId, "Abandoning {0} messages due to workitem abort", sessionState.LockTokens.Keys.Count());
            foreach (var lockToken in sessionState.LockTokens.Keys)
            {
                await sessionState.Session.AbandonAsync(lockToken);
            }

            try
            {
                sessionState.Session.Abort();
            }
            catch (Exception ex) when (!Utils.IsFatal(ex))
            {
                TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.InstanceId, ex, "Error while aborting session");
            }
        }

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
            BrokeredMessage receivedMessage = await workerQueueClient.ReceiveAsync(receiveTimeout);

            if (receivedMessage == null)
            {
                return null;
            }

            TraceHelper.TraceSession(TraceEventType.Information,
                receivedMessage.SessionId,
                GetFormattedLog($"New message to process: {receivedMessage.MessageId} [{receivedMessage.SequenceNumber}]"));

            TaskMessage taskMessage = await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(receivedMessage);

            ServiceBusUtils.CheckAndLogDeliveryCount(receivedMessage, Settings.MaxTaskActivityDeliveryCount);

            if (!orchestrationMessages.TryAdd(receivedMessage.MessageId, receivedMessage))
            {
                var error = $"Duplicate orchestration message id '{receivedMessage.MessageId}', id already exists in message list.";
                TraceHelper.Trace(TraceEventType.Error, error);
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
            if (string.IsNullOrEmpty(workItem?.Id))
            {
                return null;
            }

            return orchestrationMessages[workItem.Id];
        }

        BrokeredMessage GetAndDeleteBrokeredMessageForWorkItem(TaskActivityWorkItem workItem)
        {
            if (string.IsNullOrEmpty(workItem?.Id))
            {
                return null;
            }

            BrokeredMessage existingMessage;
            orchestrationMessages.TryRemove(workItem.Id, out existingMessage);
            return existingMessage;
        }

        /// <summary>
        ///    Renew the lock on a still processing work item
        /// </summary>
        /// <param name="workItem">Work item to renew the lock on</param>
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            var message = GetBrokeredMessageForWorkItem(workItem);
            if (message != null)
            {
                await message.RenewLockAsync();
                workItem.LockedUntilUtc = message.LockedUntilUtc;
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
            BrokeredMessage brokeredResponseMessage = ServiceBusUtils.GetBrokeredMessageFromObject(
                responseMessage,
                Settings.MessageCompressionSettings,
                workItem.TaskMessage.OrchestrationInstance,
                $"Response for {workItem.TaskMessage.OrchestrationInstance.InstanceId}");

            var originalMessage = GetAndDeleteBrokeredMessageForWorkItem(workItem);
            if (originalMessage == null)
            {
                throw new ArgumentNullException("originalMessage");
            }

            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await Task.WhenAll(
                    workerQueueClient.CompleteAsync(originalMessage.LockToken),
                    orchestratorSender.SendAsync(brokeredResponseMessage));
                ts.Complete();
            }
        }

        /// <summary>
        ///    Abandons a single work item and releases the lock on it
        /// </summary>
        /// <param name="workItem">The work item to abandon</param>
        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            var message = GetAndDeleteBrokeredMessageForWorkItem(workItem);
            TraceHelper.Trace(TraceEventType.Information, $"Abandoning message {workItem?.Id}");
            return message?.AbandonAsync();
        }

        /// <summary>
        ///    Create/start a new Orchestration
        /// </summary>
        /// <param name="creationMessage">The task message for the new Orchestration</param>
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            // First, lets push the orchestration state (Pending) into JumpStart table
            bool jumpStartEnabled = false;
            if (this.InstanceStore != null)
            {
                // TODO: GetOrchestrationState is still flaky as we are fetching from 2 tables while messages are being deleted and added
                // to JumpStart table by JumpStart manager
                if ((await this.GetOrchestrationStateAsync(creationMessage.OrchestrationInstance.InstanceId, true)).Count != 0)
                {
                    // An orchestratoion with same instance id is already running
                    throw new InvalidOperationException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' already exists");
                }

                await this.UpdateJumpStartStoreAsync(creationMessage);
                jumpStartEnabled = true;
            }

            try
            {
                // Second, lets queue orchestration
                await SendTaskOrchestrationMessageAsync(creationMessage);
            }
            catch (Exception ex) when (!Utils.IsFatal(ex) && jumpStartEnabled)
            {
                // Ingore exception
                TraceHelper.Trace(TraceEventType.Warning, $"Error while adding message to ServiceBus: {ex.ToString()}");
            }
        }

        public async Task UpdateJumpStartStoreAsync(TaskMessage creationMessage)
        {
            var executionStartedEvent = creationMessage.Event as ExecutionStartedEvent;
            var createTime = DateTime.UtcNow;
            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = creationMessage.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = createTime,
                LastUpdatedTime = createTime
            };

            var jumpStartEntity = new OrchestrationJumpStartInstanceEntity()
            {
                State = orchestrationState,
                JumpStartTime = DateTime.MinValue
            };

            await this.InstanceStore.WriteJumpStartEntitesAsync(new[] { jumpStartEntity });
        }

        /// <summary>
        ///    Send an orchestration message
        /// </summary>
        /// <param name="message">The task message to be sent for the orchestration</param>
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            BrokeredMessage brokeredMessage = ServiceBusUtils.GetBrokeredMessageFromObject(
                message,
                Settings.MessageCompressionSettings,
                message.OrchestrationInstance,
                "SendTaskOrchestrationMessage");

            // Use duplicate detection of ExecutionStartedEvent by addin messageId
            var executionStartedEvent = message.Event as ExecutionStartedEvent;
            if (executionStartedEvent != null)
            {
                brokeredMessage.MessageId = string.Format(CultureInfo.InvariantCulture, $"{executionStartedEvent.OrchestrationInstance.InstanceId}_{executionStartedEvent.OrchestrationInstance.ExecutionId}");
            }

            MessageSender sender = await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName).ConfigureAwait(false);
            await sender.SendAsync(brokeredMessage).ConfigureAwait(false);
            await sender.CloseAsync().ConfigureAwait(false);
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

            var timeoutSeconds = timeout.TotalSeconds;

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
            IEnumerable<OrchestrationStateInstanceEntity> states = await InstanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            return states?.Select(s => s.State).ToList() ?? new List<OrchestrationState>();
        }

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            OrchestrationStateInstanceEntity state = await InstanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return state?.State;
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified orchestration instance specified execution (generation) of the specified instance
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            IEnumerable<OrchestrationWorkItemInstanceEntity> historyEvents =
                await InstanceStore.GetOrchestrationHistoryEventsAsync(instanceId, executionId);

            return DataConverter.Serialize(historyEvents.Select(historyEventEntity => historyEventEntity.HistoryEvent));
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        public async Task PurgeOrchestrationInstanceHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            ThrowIfInstanceStoreNotConfigured();

            TraceHelper.Trace(TraceEventType.Information, $"Purging orchestration instances before: {thresholdDateTimeUtc}, Type: {timeRangeFilterType}");

            int purgedEvents = await InstanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);

            TraceHelper.Trace(TraceEventType.Information, $"Purged {purgedEvents} orchestration histories");
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
        async Task<TrackingWorkItem> FetchTrackingWorkItemAsync(TimeSpan receiveTimeout)
        {
            MessageSession session = await trackingQueueClient.AcceptMessageSessionAsync(receiveTimeout);

            if (session == null)
            {
                return null;
            }

            IList<BrokeredMessage> newMessages =
                (await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(Settings.PrefetchCount),
                    session.SessionId, "Receive Tracking Session Message Batch", Settings.MaxRetries, Settings.IntervalBetweenRetriesSecs)).ToList();

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                GetFormattedLog($"{newMessages.Count()} new tracking messages to process: {string.Join(",", newMessages.Select(m => m.MessageId))}"));

            ServiceBusUtils.CheckAndLogDeliveryCount(newMessages, Settings.MaxTrackingDeliveryCount);

            IList<TaskMessage> newTaskMessages = await Task.WhenAll(
                newMessages.Select(async message => await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message)));

            var lockTokens = newMessages.ToDictionary(m => m.LockToken, m => m);
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
        /// <param name="runtimeState">The orchestation runtime state</param>
        List<BrokeredMessage> CreateTrackingMessages(OrchestrationRuntimeState runtimeState)
        {
            var trackingMessages = new List<BrokeredMessage>();

            // We cannot create tracking messages if runtime state does not have Orchestration InstanceId
            // This situation can happen due to corruption of service bus session state or if somehow first message of orchestration is not execution started
            if (string.IsNullOrWhiteSpace(runtimeState?.OrchestrationInstance?.InstanceId))
            {
                return trackingMessages;
            }

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

                BrokeredMessage trackingMessage = ServiceBusUtils.GetBrokeredMessageFromObject(
                    taskMessage,
                    Settings.MessageCompressionSettings,
                    runtimeState.OrchestrationInstance,
                    "History Tracking Message");
                trackingMessages.Add(trackingMessage);
            }

            var stateMessage = new TaskMessage
            {
                Event = new HistoryStateEvent(-1, Utils.BuildOrchestrationState(runtimeState)),
                OrchestrationInstance = runtimeState.OrchestrationInstance
            };

            BrokeredMessage brokeredStateMessage = ServiceBusUtils.GetBrokeredMessageFromObject(
                stateMessage,
                Settings.MessageCompressionSettings,
                runtimeState.OrchestrationInstance,
                "State Tracking Message");
            trackingMessages.Add(brokeredStateMessage);

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
                throw new ArgumentNullException("SessionInstance");
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
                await InstanceStore.WriteEntitesAsync(historyEntities);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, "Failed to write history entity", historyEntities, GetNormalizedWorkItemEvent);
                throw;
            }

            try
            {
                // TODO : send batch to instance store, it can write it as individual if it chooses
                foreach (OrchestrationStateInstanceEntity stateEntity in stateEntities)
                {
                    await InstanceStore.WriteEntitesAsync(new List<OrchestrationStateInstanceEntity> { stateEntity });
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                TraceEntities(TraceEventType.Critical, "Failed to write state entity", stateEntities, GetNormalizedStateEvent);
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
            int index = 0;
            foreach (T entry in entities)
            {
                var idx = index;
                TraceHelper.Trace(eventType, () => traceGenerator(idx, message, entry));
                index++;
            }
        }

        string GetNormalizedStateEvent(int index, string message, OrchestrationStateInstanceEntity stateEntity)
        {
            string serializedHistoryEvent = Utils.EscapeJson(DataConverter.Serialize(stateEntity.State));
            int historyEventLength = serializedHistoryEvent.Length;

            int maxLen = InstanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

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
            int maxLen = InstanceStore?.MaxHistoryEntryLength ?? int.MaxValue;

            if (historyEventLength > maxLen)
            {
                serializedHistoryEvent = serializedHistoryEvent.Substring(0, maxLen) + " ....(truncated)..]";
            }

            return GetFormattedLog(
                $"{message} - #{index} - Instance Id: {entity.InstanceId}, Execution Id: {entity.ExecutionId}, HistoryEvent Length: {historyEventLength}\n{serializedHistoryEvent}");
        }

        string GetFormattedLog(string input)
        {
            // todo : fill this method with the correct workitemdispatcher prefix
            return input;
        }

        static async Task<OrchestrationRuntimeState> GetSessionState(MessageSession session)
        {
            long rawSessionStateSize;
            long newSessionStateSize;
            OrchestrationRuntimeState runtimeState;
            bool isEmptySession;

            using (Stream rawSessionStream = await session.GetStateAsync())
            using (Stream sessionStream = await Utils.GetDecompressedStreamAsync(rawSessionStream))
            {
                isEmptySession = sessionStream == null;
                rawSessionStateSize = isEmptySession ? 0 : rawSessionStream.Length;
                newSessionStateSize = isEmptySession ? 0 : sessionStream.Length;

                runtimeState = GetOrCreateInstanceState(sessionStream, session.SessionId);
            }

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                $"Size of session state is {newSessionStateSize}, compressed {rawSessionStateSize}");

            return runtimeState;
        }

        async Task<bool> TrySetSessionState(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            OrchestrationRuntimeState runtimeState,
            MessageSession session)
        {
            bool isSessionSizeThresholdExceeded = false;

            if (newOrchestrationRuntimeState == null)
            {
                session.SetState(null);
                return true;
            }

            string serializedState = DataConverter.Serialize(newOrchestrationRuntimeState);

            long originalStreamSize = 0;
            using (
                Stream compressedState = Utils.WriteStringToStream(
                    serializedState,
                    Settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState,
                    out originalStreamSize))
            {
                runtimeState.Size = originalStreamSize;
                runtimeState.CompressedSize = compressedState.Length;
                if (runtimeState.CompressedSize > SessionStreamTerminationThresholdInBytes)
                {
                    // basic idea is to simply enqueue a terminate message just like how we do it from taskhubclient
                    // it is possible to have other messages in front of the queue and those will get processed before
                    // the terminate message gets processed. but that is ok since in the worst case scenario we will 
                    // simply land in this if-block again and end up queuing up another terminate message.
                    //
                    // the interesting scenario is when the second time we *dont* land in this if-block because e.g.
                    // the new messages that we processed caused a new generation to be created. in that case
                    // it is still ok because the worst case scenario is that we will terminate a newly created generation
                    // which shouldn't have been created at all in the first place

                    isSessionSizeThresholdExceeded = true;

                    string reason = $"Session state size of {runtimeState.CompressedSize} exceeded the termination threshold of {SessionStreamTerminationThresholdInBytes} bytes";
                    TraceHelper.TraceSession(TraceEventType.Critical, workItem.InstanceId, reason);

                    BrokeredMessage forcedTerminateMessage = CreateForcedTerminateMessage(runtimeState.OrchestrationInstance.InstanceId, reason);

                    await orchestratorQueueClient.SendAsync(forcedTerminateMessage);
                }
                else
                {
                    session.SetState(compressedState);
                }
            }

            return !isSessionSizeThresholdExceeded;
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
            NamespaceManager namespaceManager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount)
        {
            await Utils.ExecuteWithRetries(async () =>
            {
                try
                {
                await CreateQueueAsync(namespaceManager, path, requiresSessions, requiresDuplicateDetection, maxDeliveryCount);
                }
                catch (MessagingEntityAlreadyExistsException)
                {
                    await Task.FromResult(0);
                }
            }, null, "SafeCreateQueueAsync", 3, 5);
        }

        async Task SafeDeleteAndCreateQueueAsync(
            NamespaceManager namespaceManager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount)
        {
            await SafeDeleteQueueAsync(namespaceManager, path);
            await SafeCreateQueueAsync(namespaceManager, path, requiresSessions, requiresDuplicateDetection, maxDeliveryCount);
        }

        async Task CreateQueueAsync(
            NamespaceManager namespaceManager,
            string path,
            bool requiresSessions,
            bool requiresDuplicateDetection,
            int maxDeliveryCount)
        {
            var description = new QueueDescription(path)
            {
                RequiresSession = requiresSessions,
                MaxDeliveryCount = maxDeliveryCount,
                RequiresDuplicateDetection = requiresDuplicateDetection,
                DuplicateDetectionHistoryTimeWindow = TimeSpan.FromHours(DuplicateDetectionWindowInHours)
            };

            await namespaceManager.CreateQueueAsync(description);
        }

        BrokeredMessage CreateForcedTerminateMessage(string instanceId, string reason)
        {
            var newOrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId };
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = newOrchestrationInstance,
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            BrokeredMessage message = ServiceBusUtils.GetBrokeredMessageFromObject(
                taskMessage,
                Settings.MessageCompressionSettings,
                newOrchestrationInstance,
                "Forced Terminate");

            return message;
        }

        static OrchestrationRuntimeState GetOrCreateInstanceState(Stream stateStream, string sessionId)
        {
            OrchestrationRuntimeState runtimeState;
            if (stateStream == null)
            {
                TraceHelper.TraceSession(TraceEventType.Information, sessionId,
                    "No session state exists, creating new session state.");
                runtimeState = new OrchestrationRuntimeState();
            }
            else
            {
                if (stateStream.Position != 0)
                {
                    throw TraceHelper.TraceExceptionSession(TraceEventType.Error, sessionId,
                        new ArgumentException("Stream is partially consumed"));
                }

                string serializedState = null;
                using (var reader = new StreamReader(stateStream))
                {
                    serializedState = reader.ReadToEnd();
                }

                OrchestrationRuntimeState restoredState = DataConverter.Deserialize<OrchestrationRuntimeState>(serializedState);
                // Create a new Object with just the events, we don't want the rest
                runtimeState = new OrchestrationRuntimeState(restoredState.Events);
            }

            return runtimeState;
        }

        void ThrowIfInstanceStoreNotConfigured()
        {
            if (InstanceStore == null)
            {
                throw new InvalidOperationException("Instance store is not configured");
            }
        }
    }
}