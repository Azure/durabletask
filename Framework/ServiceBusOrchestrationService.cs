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
    using System.Diagnostics;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;

    using DurableTask.Common;
    using DurableTask.History;
    using DurableTask.Tracing;
    using DurableTask.Tracking;
    using DurableTask.Serializing;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public class ServiceBusOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        // todo : move to settings
        const int PrefetchCount = 50;
        const int IntervalBetweenRetriesSecs = 5;
        const int MaxRetries = 5;
        // This is the Max number of messages which can be processed in a single transaction.
        // Current ServiceBus limit is 100 so it has to be lower than that.  
        // This also has an impact on prefetch count as PrefetchCount cannot be greater than this value
        // as every fetched message also creates a tracking message which counts towards this limit.
        const int MaxMessageCount = 80;
        const int SessionStreamWarningSizeInBytes = 200 * 1024;
        const int SessionStreamTerminationThresholdInBytes = 230 * 1024;


        private static readonly DataConverter DataConverter = new JsonDataConverter();
        private readonly string connectionString;
        private readonly string hubName;
        private readonly TaskHubWorkerSettings settings;
        private readonly MessagingFactory messagingFactory;
        private QueueClient workerQueueClient;
        private MessageSender deciderSender;
        private QueueClient orchestratorQueueClient;
        private MessageSender workerSender;
        private readonly string workerEntityName;
        private readonly string orchestratorEntityName;

        /// <summary>
        ///     Create a new ServiceBusOrchestrationService to the given service bus connection string and hubname
        /// </summary>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="hubName">Hubname to use with the connection string</param>
        /// <param name="settings">Settings object for service and client</param>
        public ServiceBusOrchestrationService(string connectionString, string hubName, TaskHubWorkerSettings settings)
        {
            this.connectionString = connectionString;
            this.hubName = hubName;
            messagingFactory = ServiceBusUtils.CreateMessagingFactory(connectionString);
            workerEntityName = string.Format(FrameworkConstants.WorkerEndpointFormat, this.hubName);
            orchestratorEntityName = string.Format(FrameworkConstants.OrchestratorEndpointFormat, this.hubName);
            // TODO : siport : figure out who should own the settings, we have this here and in hubclient plus we need to worker settings
            this.settings = settings ?? new TaskHubWorkerSettings();
        }

        public async Task StartAsync()
        {
            workerQueueClient = messagingFactory.CreateQueueClient(workerEntityName);
            deciderSender = await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName, workerEntityName);
            workerSender = messagingFactory.CreateMessageSender(workerEntityName, orchestratorEntityName);
            orchestratorQueueClient = messagingFactory.CreateQueueClient(orchestratorEntityName);
        }

        public async Task StopAsync()
        {
            await this.messagingFactory.CloseAsync();
            await this.orchestratorQueueClient.CloseAsync();
            await this.workerSender.CloseAsync();
            await this.deciderSender.CloseAsync();
            await this.workerQueueClient.CloseAsync();
        }

        public Task CreateAsync()
        {
            throw new NotImplementedException();
        }

        public async Task CreateIfNotExistsAsync()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            await SafeDeleteQueueAsync(namespaceManager, orchestratorEntityName);
            await SafeDeleteQueueAsync(namespaceManager, workerEntityName);
            // await SafeDeleteQueueAsync(namespaceManager, trackingEntityName);

            // TODO : siport : pull these from settings
            await CreateQueueAsync(namespaceManager, orchestratorEntityName, true, FrameworkConstants.MaxDeliveryCount);
            await CreateQueueAsync(namespaceManager, workerEntityName, false, FrameworkConstants.MaxDeliveryCount);
            // await CreateQueueAsync(namespaceManager, trackingEntityName, true, description.MaxTrackingDeliveryCount);
        }

        public Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        ///     Inspect an exception to see if it's a transient exception that may apply different delays
        /// </summary>
        /// <param name="exception">Exception to inspect</param>
        public bool IsTransientException(Exception exception)
        {
            return (exception as MessagingException)?.IsTransient ?? false;
        }

        /// <summary>
        ///     Checks the message count against the threshold to see if a limit is being exceeded
        /// </summary>
        /// <param name="currentMessageCount">The current message count to check</param>
        /// <param name="runtimeState">The Orchestration runtime state this message count is associated with</param>
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return currentMessageCount > MaxMessageCount;
        }

        /// <summary>
        ///     Wait for the next orchestration work item and return the orchestration work item
        /// </summary>
        /// <param name="receiveTimeout">The timespan to wait for new messages before timing out</param>
        /// <param name="cancellationToken">The cancellation token to cancel execution of the task</param>
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            //todo: should timeout be a param or shout if belong to the implementation
            MessageSession session = await orchestratorQueueClient.AcceptMessageSessionAsync(receiveTimeout);

            if (session == null)
            {
                return null;
            }

            IList<BrokeredMessage> newMessages =
                (await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(PrefetchCount),
                    session.SessionId, "Receive Session Message Batch", MaxRetries, IntervalBetweenRetriesSecs)).ToList();

            // TODO: use getformattedlog equiv / standardize log format
            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                $"{newMessages.Count()} new messages to process: {string.Join(",", newMessages.Select(m => m.MessageId))}");

            IList<TaskMessage> newTaskMessages =
                newMessages.Select(message => ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message).Result).ToList();

            // todo : is this correct here or does it belong in processworkitem or even in this class?????
            long rawSessionStateSize;
            long newSessionStateSize;
            OrchestrationRuntimeState runtimeState;

            using (Stream rawSessionStream = await session.GetStateAsync())
            using (Stream sessionStream = await Utils.GetDecompressedStreamAsync(rawSessionStream))
            {
                bool isEmptySession = sessionStream == null;
                rawSessionStateSize = isEmptySession ? 0 : rawSessionStream.Length;
                newSessionStateSize = isEmptySession ? 0 : sessionStream.Length;

                runtimeState = GetOrCreateInstanceState(sessionStream, session.SessionId);
            }

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                $"Size of session state is {newSessionStateSize}, compressed {rawSessionStateSize}");

            var lockTokens = newMessages.ToDictionary(m => m.LockToken, m => true);
            var sessionState = new ServiceBusOrchestrationSession
            {
                Session = session,
                LockTokens = lockTokens
            };

            return new TaskOrchestrationWorkItem
            {
                InstanceId = session.SessionId,
                LockedUntilUtc = session.LockedUntilUtc,
                NewMessages = newTaskMessages,
                OrchestrationRuntimeState = runtimeState,
                SessionInstance = sessionState
            };
        }

        /// <summary>
        ///     Renew the lock on an orchestration
        /// </summary>
        /// <param name="workItem">The task orchestration to renew the lock on</param>
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            var sessionState = workItem?.SessionInstance as ServiceBusOrchestrationSession;
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
            var sessionState = workItem.SessionInstance as ServiceBusOrchestrationSession;
            if (sessionState == null)
            {
                // todo : figure out what this means, its a terminal error for the orchestration
                throw new ArgumentNullException("SessionInstance");
            }

            var session = sessionState.Session;

            // todo : validate updating to .NET 4.5.1 as required for TransactionScopeAsyncFlowOption.Enabled)
            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                if (await TrySetSessionState(workItem, newOrchestrationRuntimeState, runtimeState, session))
                {
                    if (runtimeState.CompressedSize > SessionStreamWarningSizeInBytes)
                    {
                        TraceHelper.TraceSession(TraceEventType.Error, workItem.InstanceId, "Size of session state is nearing session size limit of 256KB");
                    }

                    // todo : in CR please validate use of 'SendBatchAsync', previously was 'Send'
                    if (outboundMessages?.Count > 0)
                    {
                        await workerSender.SendBatchAsync(outboundMessages.Select(m => ServiceBusUtils.GetBrokeredMessageFromObject(m, settings.MessageCompressionSettings)));
                    }

                    if (timerMessages?.Count > 0)
                    {
                        await orchestratorQueueClient.SendBatchAsync(timerMessages.Select(m => ServiceBusUtils.GetBrokeredMessageFromObject(m, settings.MessageCompressionSettings)));
                    }

                    if (orchestratorMessages?.Count > 0)
                    {
                        await orchestratorQueueClient.SendBatchAsync(orchestratorMessages.Select(m => ServiceBusUtils.GetBrokeredMessageFromObject(m, settings.MessageCompressionSettings)));
                    }

                    if (continuedAsNewMessage != null)
                    {
                        await orchestratorQueueClient.SendAsync(ServiceBusUtils.GetBrokeredMessageFromObject(continuedAsNewMessage, settings.MessageCompressionSettings));
                    }

                    // todo : add tracking here
                }

                await session.CompleteBatchAsync(sessionState.LockTokens.Keys);

                ts.Complete();
            }
        }

        private async Task<bool> TrySetSessionState(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            OrchestrationRuntimeState runtimeState,
            MessageSession session)
        {
            bool isSessionSizeThresholdExceeded = false;

            string serializedState = DataConverter.Serialize(newOrchestrationRuntimeState);
            long originalStreamSize = 0;
            using (
                Stream compressedState = Utils.WriteStringToStream(
                    serializedState,
                    settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState,
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

        /// <summary>
        ///     Abandon an orchestation, this abandons ownership/locking of all messages for an orchestation and it's session
        /// </summary>
        /// <param name="workItem">The task orchestration to abandon</param>
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            var sessionState = workItem?.SessionInstance as ServiceBusOrchestrationSession;
            if (sessionState?.Session == null)
            {
                return;
            }

            // todo: old code does not have this in a transaction, should this?
            TraceHelper.TraceSession(TraceEventType.Error, workItem.InstanceId, "Abandoning {0} messages due to workitem abort", sessionState.LockTokens.Keys.Count());
            foreach (var lockToken in sessionState.LockTokens.Keys)
            {
                await sessionState.Session.AbandonAsync(lockToken);
            }

            try
            {
                sessionState.Session.Abort();
            }
            catch (Exception ex)
            {
                TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.InstanceId, ex, "Error while aborting session");
            }
        }

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

            // TODO: use getformattedlog
            TraceHelper.TraceSession(TraceEventType.Information,
                receivedMessage.SessionId,
                $"New message to process: {receivedMessage.MessageId} [{receivedMessage.SequenceNumber}]");

            TaskMessage taskMessage = await ServiceBusUtils.GetObjectFromBrokeredMessageAsync<TaskMessage>(receivedMessage);

            return new TaskActivityWorkItem
            {
                Id = receivedMessage.MessageId,
                LockedUntilUtc = receivedMessage.LockedUntilUtc,
                TaskMessage = taskMessage,
                MessageState = receivedMessage
            };
        }

        /// <summary>
        ///    Renew the lock on a still processing work item
        /// </summary>
        /// <param name="workItem">Work item to renew the lock on</param>
        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // message.renewLock();
            throw new NotImplementedException();
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
                settings.MessageCompressionSettings,
                workItem.TaskMessage.OrchestrationInstance, 
                $"Response for {workItem.TaskMessage.OrchestrationInstance.InstanceId}");

            brokeredResponseMessage.SessionId = responseMessage.OrchestrationInstance.InstanceId;

            var originalMessage = workItem.MessageState as BrokeredMessage;
            if (originalMessage == null)
            {
                //todo : figure out the correct action here, this is bad
                throw new ArgumentNullException("originalMessage");
            }
            
            using (var ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                // todo: see if we can combine these to a task.whenall or do they need to be sequential?
                await workerQueueClient.CompleteAsync(originalMessage.LockToken);
                await deciderSender.SendAsync(brokeredResponseMessage);
                ts.Complete();
            }
        }

        /// <summary>
        ///    Abandons a single work item and releases the lock on it
        /// </summary>
        /// <param name="workItem">The work item to abandon</param>
        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // if (workItem != null)
            // {
            //     TraceHelper.Trace(TraceEventType.Information, "Abandoing message " + workItem.MessageId);
            //     await workItem.AbandonAsync();
            // }

            throw new NotImplementedException();
        }

        /// <summary>
        ///    Create/start a new Orchestration
        /// </summary>
        /// <param name="creationMessage">The task message for the new Orchestration</param>
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            // Keep this as its own message so create specific logic can run here like adding tracking information
            await SendTaskOrchestrationMessage(creationMessage);
        }

        /// <summary>
        ///    Send an orchestration message
        /// </summary>
        /// <param name="message">The task message to be sent for the orchestration</param>
        public async Task SendTaskOrchestrationMessage(TaskMessage message)
        {
            BrokeredMessage brokeredMessage = ServiceBusUtils.GetBrokeredMessageFromObject(
                message,
                this.settings.MessageCompressionSettings);
            brokeredMessage.SessionId = message.OrchestrationInstance.InstanceId;

            MessageSender sender = await messagingFactory.CreateMessageSenderAsync(orchestratorEntityName).ConfigureAwait(false);
            await sender.SendAsync(brokeredMessage).ConfigureAwait(false);
            await sender.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        ///     Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">Instance to terminate</param>
        /// <param name="executionId">The execution id of the orchestration</param>
        /// <param name="timeout">Max timeout to wait</param>
        /// <param name="cancellationToken">Task cancellation token</param>
        public Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId, 
            string executionId, 
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ForceTerminateTaskOrchestrationAsync(string instanceId)
        {
            throw new NotImplementedException();
        }

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            throw new NotImplementedException();
        }

        public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationInstanceHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }
        
        private static Task SafeDeleteQueueAsync(NamespaceManager namespaceManager, string path)
        {
            try
            {
                return namespaceManager.DeleteQueueAsync(path);
            }
            catch (MessagingEntityNotFoundException)
            {
                return Task.FromResult(0);
            }
        }

        private Task SafeCreateQueueAsync(NamespaceManager namespaceManager, string path, bool requiresSessions,
            int maxDeliveryCount)
        {
            try
            {
                return CreateQueueAsync(namespaceManager, path, requiresSessions, maxDeliveryCount);
            }
            catch (MessagingEntityAlreadyExistsException)
            {
                return Task.FromResult(0);
            }
        }

        private Task CreateQueueAsync(
            NamespaceManager namespaceManager, 
            string path, 
            bool requiresSessions,
            int maxDeliveryCount)
        {
            var description = new QueueDescription(path)
            {
                RequiresSession = requiresSessions,
                MaxDeliveryCount = maxDeliveryCount
            };
            return namespaceManager.CreateQueueAsync(description);
        }

        private BrokeredMessage CreateForcedTerminateMessage(string instanceId, string reason)
        {
            //todo : validate we want this method 
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            BrokeredMessage message = ServiceBusUtils.GetBrokeredMessageFromObject(
                taskMessage, settings.MessageCompressionSettings, new OrchestrationInstance { InstanceId = instanceId },
                "Forced Terminate");
            message.SessionId = instanceId;
            return message;
        }

        private static OrchestrationRuntimeState GetOrCreateInstanceState(Stream stateStream, string sessionId)
        {
            // todo : find the correct home for this method
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
    }
}