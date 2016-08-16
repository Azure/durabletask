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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Command;
    using History;
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using Tracing;

    public class TaskOrchestrationDispatcher : DispatcherBase<SessionWorkItem>
    {
        const int PrefetchCount = 50;
        const int SessionStreamWarningSizeInBytes = 200*1024;
        const int SessionStreamTerminationThresholdInBytes = 230*1024;

        // This is the Max number of messages which can be processed in a single transaction.
        // Current ServiceBus limit is 100 so it has to be lower than that.  
        // This also has an impact on prefetch count as PrefetchCount cannot be greater than this value
        // as every fetched message also creates a tracking message which counts towards this limit.
        const int MaxMessageCount = 80;

        const int MaxRetries = 5;
        const int IntervalBetweenRetriesSecs = 5;
        readonly bool isTrackingEnabled;

        protected readonly MessagingFactory messagingFactory;

        readonly NameVersionObjectManager<TaskOrchestration> objectManager;
        protected readonly string orchestratorEntityName;
        readonly TaskHubWorkerSettings settings;
        readonly TaskHubDescription taskHubDescription;
        readonly TrackingDispatcher trackingDipatcher;
        protected readonly string trackingEntityName;
        protected readonly MessageSender trackingSender;
        protected readonly string workerEntityName;
        protected readonly MessageSender workerSender;
        protected QueueClient orchestratorQueueClient;

        internal TaskOrchestrationDispatcher(MessagingFactory messagingFactory,
            TrackingDispatcher trackingDispatcher,
            TaskHubDescription taskHubDescription,
            TaskHubWorkerSettings workerSettings,
            string orchestratorEntityName,
            string workerEntityName,
            string trackingEntityName,
            NameVersionObjectManager<TaskOrchestration> objectManager)
            : base("TaskOrchestration Dispatcher", item => item.Session == null ? string.Empty : item.Session.SessionId)
        {
            this.taskHubDescription = taskHubDescription;
            settings = workerSettings.Clone();
            this.orchestratorEntityName = orchestratorEntityName;
            this.workerEntityName = workerEntityName;
            this.trackingEntityName = trackingEntityName;
            this.messagingFactory = messagingFactory;
            this.messagingFactory.PrefetchCount = PrefetchCount;
            this.objectManager = objectManager;
            orchestratorQueueClient = this.messagingFactory.CreateQueueClient(this.orchestratorEntityName);
            workerSender = this.messagingFactory.CreateMessageSender(this.workerEntityName, this.orchestratorEntityName);

            trackingDipatcher = trackingDispatcher;
            if (trackingDipatcher != null)
            {
                isTrackingEnabled = true;
                trackingSender = this.messagingFactory.CreateMessageSender(this.trackingEntityName,
                    this.orchestratorEntityName);
            }
            maxConcurrentWorkItems = settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;
        }

        public bool IncludeDetails { get; set; }
        public bool IncludeParameters { get; set; }

        protected override void OnStart()
        {
            if (isTrackingEnabled)
            {
                trackingDipatcher.Start();
            }
        }

        protected override void OnStopping(bool isForced)
        {
        }

        protected override void OnStopped(bool isForced)
        {
            if (isTrackingEnabled)
            {
                trackingDipatcher.Stop(isForced);
            }
        }

        protected override async Task<SessionWorkItem> OnFetchWorkItem(TimeSpan receiveTimeout)
        {
            MessageSession session = await orchestratorQueueClient.AcceptMessageSessionAsync(receiveTimeout);

            if (session == null)
            {
                return null;
            }

            IEnumerable<BrokeredMessage> newMessages =
                await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(PrefetchCount),
                    session.SessionId, "Receive Session Message Batch", MaxRetries, IntervalBetweenRetriesSecs);

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                GetFormattedLog(
                    $@"{newMessages.Count()} new messages to process: {
                        string.Join(",", newMessages.Select(m => m.MessageId))
                        }, max latency: {
                        newMessages.Max(message => message.DeliveryLatency())}ms"
                        ));

            return new SessionWorkItem {Session = session, Messages = newMessages};
        }

        protected override async Task OnProcessWorkItem(SessionWorkItem sessionWorkItem)
        {
            var messagesToSend = new List<MessageContainer>();
            var timerMessages = new List<MessageContainer>();
            var subOrchestrationMessages = new List<MessageContainer>();
            bool isCompleted = false;
            bool continuedAsNew = false;

            BrokeredMessage continuedAsNewMessage = null;
            ExecutionStartedEvent continueAsNewExecutionStarted = null;

            MessageSession session = sessionWorkItem.Session;
            IEnumerable<BrokeredMessage> newMessages = sessionWorkItem.Messages ?? new List<BrokeredMessage>();

            long rawSessionStateSize;
            long newSessionStateSize;
            bool isEmptySession = false;
            OrchestrationRuntimeState runtimeState;

            using (Stream rawSessionStream = await session.GetStateAsync())
            using (Stream sessionStream = await Utils.GetDecompressedStreamAsync(rawSessionStream))
            {
                isEmptySession = sessionStream == null;
                rawSessionStateSize = isEmptySession ? 0 : rawSessionStream.Length;
                newSessionStateSize = isEmptySession ? 0 : sessionStream.Length;

                runtimeState = GetOrCreateInstanceState(sessionStream, session.SessionId);
            }

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                "Size of session state is {0}, compressed {1}", newSessionStateSize, rawSessionStateSize);
            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

            if (!(await ReconcileMessagesWithStateAsync(session.SessionId, runtimeState, newMessages)))
            {
                // TODO : mark an orchestration as faulted if there is data corruption
                TraceHelper.TraceSession(TraceEventType.Error, session.SessionId,
                    "Received result for a deleted orchestration");
                isCompleted = true;
            }
            else
            {
                TraceHelper.TraceInstance(
                    TraceEventType.Verbose,
                    runtimeState.OrchestrationInstance,
                    "Executing user orchestration: {0}",
                    JsonConvert.SerializeObject(runtimeState.GetOrchestrationRuntimeStateDump(),
                        new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.Auto,
                            Formatting = Formatting.Indented
                        }));

                IEnumerable<OrchestratorAction> decisions = ExecuteOrchestration(runtimeState);

                TraceHelper.TraceInstance(TraceEventType.Information,
                    runtimeState.OrchestrationInstance,
                    "Executed user orchestration. Received {0} orchestrator actions: {1}",
                    decisions.Count(),
                    string.Join(", ", decisions.Select(d => d.Id + ":" + d.OrchestratorActionType)));

                foreach (OrchestratorAction decision in decisions)
                {
                    TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                        "Processing orchestrator action of type {0}, id {1}", decision.OrchestratorActionType, decision.Id.ToString());
                    switch (decision.OrchestratorActionType)
                    {
                        case OrchestratorActionType.ScheduleOrchestrator:
                            TaskMessage taskMessage =
                                ProcessScheduleTaskDecision((ScheduleTaskOrchestratorAction) decision, runtimeState,
                                    IncludeParameters);
                            BrokeredMessage brokeredMessage = Utils.GetBrokeredMessageFromObject(
                                taskMessage, settings.MessageCompressionSettings, runtimeState.OrchestrationInstance,
                                "ScheduleTask");
                            brokeredMessage.SessionId = session.SessionId;
                            messagesToSend.Add(new MessageContainer(brokeredMessage, decision));
                            break;
                        case OrchestratorActionType.CreateTimer:
                            var timerOrchestratorAction = (CreateTimerOrchestratorAction) decision;
                            TaskMessage timerMessage = ProcessCreateTimerDecision(timerOrchestratorAction, runtimeState);
                            BrokeredMessage brokeredTimerMessage = Utils.GetBrokeredMessageFromObject(
                                timerMessage, settings.MessageCompressionSettings, runtimeState.OrchestrationInstance,
                                "Timer");
                            brokeredTimerMessage.ScheduledEnqueueTimeUtc = timerOrchestratorAction.FireAt;
                            brokeredTimerMessage.SessionId = session.SessionId;
                            timerMessages.Add(new MessageContainer(brokeredTimerMessage, decision));
                            break;
                        case OrchestratorActionType.CreateSubOrchestration:
                            var createSubOrchestrationAction = (CreateSubOrchestrationAction) decision;
                            TaskMessage createSubOrchestrationInstanceMessage =
                                ProcessCreateSubOrchestrationInstanceDecision(createSubOrchestrationAction,
                                    runtimeState, IncludeParameters);
                            BrokeredMessage createSubOrchestrationMessage = Utils.GetBrokeredMessageFromObject(
                                createSubOrchestrationInstanceMessage, settings.MessageCompressionSettings,
                                runtimeState.OrchestrationInstance, "Schedule Suborchestration");
                            createSubOrchestrationMessage.SessionId =
                                createSubOrchestrationInstanceMessage.OrchestrationInstance.InstanceId;
                            subOrchestrationMessages.Add(new MessageContainer(createSubOrchestrationMessage, decision));
                            break;
                        case OrchestratorActionType.OrchestrationComplete:
                            TaskMessage workflowInstanceCompletedMessage =
                                ProcessWorkflowCompletedTaskDecision((OrchestrationCompleteOrchestratorAction) decision,
                                    runtimeState, IncludeDetails, out continuedAsNew);
                            if (workflowInstanceCompletedMessage != null)
                            {
                                // Send complete message to parent workflow or to itself to start a new execution
                                BrokeredMessage workflowCompletedBrokeredMessage = Utils.GetBrokeredMessageFromObject(
                                    workflowInstanceCompletedMessage, settings.MessageCompressionSettings,
                                    runtimeState.OrchestrationInstance, "Complete Suborchestration");
                                workflowCompletedBrokeredMessage.SessionId =
                                    workflowInstanceCompletedMessage.OrchestrationInstance.InstanceId;

                                // Store the event so we can rebuild the state
                                if (continuedAsNew)
                                {
                                    continuedAsNewMessage = workflowCompletedBrokeredMessage;
                                    continueAsNewExecutionStarted =
                                        workflowInstanceCompletedMessage.Event as ExecutionStartedEvent;
                                }
                                else
                                {
                                    subOrchestrationMessages.Add(new MessageContainer(workflowCompletedBrokeredMessage, decision));
                                }
                            }
                            isCompleted = !continuedAsNew;
                            break;
                        default:
                            throw TraceHelper.TraceExceptionInstance(TraceEventType.Error,
                                runtimeState.OrchestrationInstance,
                                new NotSupportedException("decision type not supported"));
                    }

                    // We cannot send more than 100 messages within a transaction, to avoid the situation
                    // we keep on checking the message count and stop processing the new decisions.
                    // We also put in a fake timer to force next orchestration task for remaining messages
                    int totalMessages = messagesToSend.Count + subOrchestrationMessages.Count + timerMessages.Count;
                    // Also add tracking messages as they contribute to total messages within transaction
                    if (isTrackingEnabled)
                    {
                        totalMessages += runtimeState.NewEvents.Count;
                    }
                    if (totalMessages > MaxMessageCount)
                    {
                        TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                            "MaxMessageCount reached.  Adding timer to process remaining events in next attempt.");
                        if (isCompleted || continuedAsNew)
                        {
                            TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                                "Orchestration already completed.  Skip adding timer for splitting messages.");
                            break;
                        }

                        var dummyTimer = new CreateTimerOrchestratorAction
                        {
                            Id = FrameworkConstants.FakeTimerIdToSplitDecision,
                            FireAt = DateTime.UtcNow
                        };
                        TaskMessage timerMessage = ProcessCreateTimerDecision(dummyTimer, runtimeState);
                        BrokeredMessage brokeredTimerMessage = Utils.GetBrokeredMessageFromObject(
                            timerMessage, settings.MessageCompressionSettings, runtimeState.OrchestrationInstance,
                            "MaxMessageCount Timer");
                        brokeredTimerMessage.ScheduledEnqueueTimeUtc = dummyTimer.FireAt;
                        brokeredTimerMessage.SessionId = session.SessionId;
                        timerMessages.Add(new MessageContainer(brokeredTimerMessage, null));
                        break;
                    }
                }
            }

            // TODO : make async, transactions are a bit tricky
            using (var ts = new TransactionScope())
            {
                Transaction.Current.TransactionCompleted += (o, e) =>
                {
                    TraceHelper.TraceInstance(
                     e.Transaction.TransactionInformation.Status == TransactionStatus.Committed ? TraceEventType.Information : TraceEventType.Error,
                         runtimeState.OrchestrationInstance,
                         () => "Transaction " + e.Transaction.TransactionInformation.LocalIdentifier + " status: " + e.Transaction.TransactionInformation.Status);
                };

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    runtimeState.OrchestrationInstance,
                    () => "Created new transaction - txnid: " +
                                    Transaction.Current.TransactionInformation.LocalIdentifier);

                bool isSessionSizeThresholdExceeded = false;
                if (!continuedAsNew)
                {
                    runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                }

                using (var ms = new MemoryStream())
                {
                    if (isCompleted)
                    {
                        TraceHelper.TraceSession(TraceEventType.Information, session.SessionId, "Deleting session state");

                        // if session was already null and we finished the orchestration then no change is required
                        if (!isEmptySession)
                        {
                            session.SetState(null);
                        }

                        runtimeState.Size = 0;
                        runtimeState.CompressedSize = 0;
                    }
                    else
                    {
                        if (ms.Position != 0)
                        {
                            throw TraceHelper.TraceExceptionInstance(TraceEventType.Error,
                                runtimeState.OrchestrationInstance,
                                new ArgumentException("Instance state stream is partially consumed"));
                        }

                        IList<HistoryEvent> newState = runtimeState.Events;
                        if (continuedAsNew)
                        {
                            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                                "Updating state for continuation");
                            newState = new List<HistoryEvent>();
                            newState.Add(new OrchestratorStartedEvent(-1));
                            newState.Add(continueAsNewExecutionStarted);
                            newState.Add(new OrchestratorCompletedEvent(-1));
                        }

                        string serializedState = JsonConvert.SerializeObject(newState,
                            new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Auto});
                        var writer = new StreamWriter(ms);
                        writer.Write(serializedState);
                        writer.Flush();

                        ms.Position = 0;
                        using (Stream compressedState =
                            settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState
                                ? Utils.GetCompressedStream(ms)
                                : ms)
                        {
                            runtimeState.Size = ms.Length;
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

                                TraceHelper.TraceSession(TraceEventType.Critical, session.SessionId,
                                    "Size of session state " + runtimeState.CompressedSize +
                                    " has exceeded termination threshold of "
                                    + SessionStreamTerminationThresholdInBytes);

                                string reason =
                                    string.Format(
                                        "Session state size of {0} exceeded the termination threshold of {1} bytes",
                                        runtimeState.CompressedSize, SessionStreamTerminationThresholdInBytes);

                                BrokeredMessage forcedTerminateMessage = CreateForcedTerminateMessage(
                                    runtimeState.OrchestrationInstance.InstanceId, reason);

                                orchestratorQueueClient.Send(forcedTerminateMessage);
                            }
                            else
                            {
                                session.SetState(compressedState);
                            }
                        }
                        writer.Close();
                    }
                }

                if (!isSessionSizeThresholdExceeded)
                {
                    if (runtimeState.CompressedSize > SessionStreamWarningSizeInBytes)
                    {
                        TraceHelper.TraceSession(TraceEventType.Error, session.SessionId,
                            "Size of session state is nearing session size limit of 256KB");
                    }

                    if (!continuedAsNew)
                    {
                        if (messagesToSend.Count > 0)
                        {
                            messagesToSend.ForEach(m => workerSender.Send(m.Message));
                            this.LogSentMessages(session, "Worker outbound", messagesToSend);
                        }

                        if (timerMessages.Count > 0)
                        {
                            timerMessages.ForEach(m => orchestratorQueueClient.Send(m.Message));
                            this.LogSentMessages(session, "Timer Message", timerMessages);
                        }
                    }

                    if (subOrchestrationMessages.Count > 0)
                    {
                        subOrchestrationMessages.ForEach(m => orchestratorQueueClient.Send(m.Message));
                        this.LogSentMessages(session, "Sub Orchestration", subOrchestrationMessages);
                    }

                    if (continuedAsNewMessage != null)
                    {
                        orchestratorQueueClient.Send(continuedAsNewMessage);
                        this.LogSentMessages(session, "Continue as new", new List<MessageContainer> () { new MessageContainer(continuedAsNewMessage, null) });
                    }

                    if (isTrackingEnabled)
                    {
                        List<MessageContainer> trackingMessages = CreateTrackingMessages(runtimeState);

                        TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                            "Created {0} tracking messages", trackingMessages.Count);

                        if (trackingMessages.Count > 0)
                        {
                            trackingMessages.ForEach(m => trackingSender.Send(m.Message));
                            this.LogSentMessages(session, "Tracking messages", trackingMessages);
                        }
                    }
                }

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    runtimeState.OrchestrationInstance,
                    () =>
                    {
                        string allIds = string.Join(" ", newMessages.Select(m => $"[SEQ: {m.SequenceNumber} LT: {m.LockToken}]"));
                        return "Completing msgs seq and locktokens: " + allIds;
                    });

                IEnumerable<Guid> lockTokens = newMessages.Select(m => m.LockToken);
                session.CompleteBatch(lockTokens);

                ts.Complete();
            }
        }

        void LogSentMessages(MessageSession session, string messageType, IList<MessageContainer> messages)
        {
            TraceHelper.TraceSession(
                TraceEventType.Information,
                session.SessionId,
                this.GetFormattedLog($@"{messages.Count().ToString()} messages queued for {messageType}: {
                    string.Join(",", messages.Select(m => $"{m.Message.MessageId} <{m.Action?.Id.ToString()}>"))}"));
        }

        BrokeredMessage CreateForcedTerminateMessage(string instanceId, string reason)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance {InstanceId = instanceId},
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            BrokeredMessage message = Utils.GetBrokeredMessageFromObject(
                taskMessage, settings.MessageCompressionSettings, new OrchestrationInstance {InstanceId = instanceId},
                "Forced Terminate");
            message.SessionId = instanceId;
            return message;
        }

        List<MessageContainer> CreateTrackingMessages(OrchestrationRuntimeState runtimeState)
        {
            var trackingMessages = new List<MessageContainer> ();

            // We cannot create tracking messages if runtime state does not have Orchestration InstanceId
            // This situation can happen due to corruption of service bus session state or if somehow first message of orchestration is not execution started
            if (runtimeState == null || runtimeState.OrchestrationInstance == null ||
                string.IsNullOrWhiteSpace(runtimeState.OrchestrationInstance.InstanceId))
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
                    OrchestrationInstance = runtimeState.OrchestrationInstance
                };

                BrokeredMessage trackingMessage = Utils.GetBrokeredMessageFromObject(
                    taskMessage, settings.MessageCompressionSettings, runtimeState.OrchestrationInstance,
                    "History Tracking Message");
                trackingMessage.ContentType = FrameworkConstants.TaskMessageContentType;
                trackingMessage.SessionId = runtimeState.OrchestrationInstance.InstanceId;
                trackingMessage.Properties[FrameworkConstants.HistoryEventIndexPropertyName] = historyEventIndex++;
                trackingMessages.Add(new MessageContainer(trackingMessage, null));
            }

            var stateMessage = new StateMessage {State = BuildOrchestrationState(runtimeState)};

            BrokeredMessage brokeredStateMessage = Utils.GetBrokeredMessageFromObject(
                stateMessage, settings.MessageCompressionSettings, runtimeState.OrchestrationInstance,
                "State Tracking Message");
            brokeredStateMessage.SessionId = runtimeState.OrchestrationInstance.InstanceId;
            brokeredStateMessage.ContentType = FrameworkConstants.StateMessageContentType;
            trackingMessages.Add(new MessageContainer(brokeredStateMessage, null));
            return trackingMessages;
        }

        static OrchestrationState BuildOrchestrationState(OrchestrationRuntimeState runtimeState)
        {
            return new OrchestrationState
            {
                OrchestrationInstance = runtimeState.OrchestrationInstance,
                ParentInstance = runtimeState.ParentInstance,
                Name = runtimeState.Name,
                Version = runtimeState.Version,
                Status = runtimeState.Status,
                Tags = runtimeState.Tags,
                OrchestrationStatus = runtimeState.OrchestrationStatus,
                CreatedTime = runtimeState.CreatedTime,
                CompletedTime = runtimeState.CompletedTime,
                LastUpdatedTime = DateTime.UtcNow,
                Size = runtimeState.Size,
                CompressedSize = runtimeState.CompressedSize,
                Input = runtimeState.Input,
                Output = runtimeState.Output
            };
        }

        internal virtual IEnumerable<OrchestratorAction> ExecuteOrchestration(OrchestrationRuntimeState runtimeState)
        {
            TaskOrchestration taskOrchestration = objectManager.GetObject(runtimeState.Name, runtimeState.Version);
            if (taskOrchestration == null)
            {
                throw TraceHelper.TraceExceptionInstance(TraceEventType.Error, runtimeState.OrchestrationInstance,
                    new TypeMissingException(string.Format("Orchestration not found: ({0}, {1})", runtimeState.Name,
                        runtimeState.Version)));
            }

            var taskOrchestrationExecutor = new TaskOrchestrationExecutor(runtimeState, taskOrchestration);
            IEnumerable<OrchestratorAction> decisions = taskOrchestrationExecutor.Execute();
            return decisions;
        }

        async Task<bool> ReconcileMessagesWithStateAsync(string sessionId,
            OrchestrationRuntimeState runtimeState, IEnumerable<BrokeredMessage> messages)
        {
            foreach (BrokeredMessage message in messages)
            {
                Utils.CheckAndLogDeliveryCount(sessionId, message,
                    taskHubDescription.MaxTaskOrchestrationDeliveryCount);

                TaskMessage taskMessage = await Utils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message);
                OrchestrationInstance orchestrationInstance = taskMessage.OrchestrationInstance;
                if (orchestrationInstance == null || string.IsNullOrWhiteSpace(orchestrationInstance.InstanceId))
                {
                    throw TraceHelper.TraceException(TraceEventType.Error,
                        new InvalidOperationException("Message does not contain any OrchestrationInstance information"));
                }

                if (runtimeState.Events.Count == 1 && taskMessage.Event.EventType != EventType.ExecutionStarted)
                {
                    // we get here because of:
                    //      i) responses for scheduled tasks after the orchestrations have been completed
                    //      ii) responses for explicitly deleted orchestrations
                    return false;
                }

                TraceHelper.TraceInstance(TraceEventType.Information, orchestrationInstance,
                    "Processing new event with Id {0} and type {1}", taskMessage.Event.EventId,
                    taskMessage.Event.EventType);

                if (taskMessage.Event.EventType == EventType.ExecutionStarted)
                {
                    if (runtimeState.Events.Count > 1)
                    {
                        // this was caused due to a dupe execution started event, swallow this one
                        TraceHelper.TraceInstance(TraceEventType.Warning, orchestrationInstance,
                            "Duplicate start event.  Ignoring event with Id {0} and type {1} ",
                            taskMessage.Event.EventId, taskMessage.Event.EventType);
                        continue;
                    }
                }
                else if (!string.IsNullOrWhiteSpace(orchestrationInstance.ExecutionId)
                         &&
                         !string.Equals(orchestrationInstance.ExecutionId,
                             runtimeState.OrchestrationInstance.ExecutionId))
                {
                    // eat up any events for previous executions
                    TraceHelper.TraceInstance(TraceEventType.Warning, orchestrationInstance,
                        "ExecutionId of event does not match current executionId.  Ignoring event with Id {0} and type {1} ",
                        taskMessage.Event.EventId, taskMessage.Event.EventType);
                    continue;
                }

                runtimeState.AddEvent(taskMessage.Event);
            }
            return true;
        }

        OrchestrationRuntimeState GetOrCreateInstanceState(Stream stateStream, string sessionId)
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

                var events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedState,
                    new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Auto});
                runtimeState = new OrchestrationRuntimeState(events);
            }
            return runtimeState;
        }

        static TaskMessage ProcessWorkflowCompletedTaskDecision(
            OrchestrationCompleteOrchestratorAction completeOrchestratorAction, OrchestrationRuntimeState runtimeState,
            bool includeDetails, out bool continuedAsNew)
        {
            ExecutionCompletedEvent executionCompletedEvent;
            continuedAsNew = (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);
            if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
            {
                executionCompletedEvent = new ContinueAsNewEvent(completeOrchestratorAction.Id,
                    completeOrchestratorAction.Result);
            }
            else
            {
                executionCompletedEvent = new ExecutionCompletedEvent(completeOrchestratorAction.Id,
                    completeOrchestratorAction.Result,
                    completeOrchestratorAction.OrchestrationStatus);
            }

            runtimeState.AddEvent(executionCompletedEvent);

            TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                "Instance Id '{0}' completed in state {1} with result: {2}",
                runtimeState.OrchestrationInstance, runtimeState.OrchestrationStatus, completeOrchestratorAction.Result);
            string history = JsonConvert.SerializeObject(runtimeState.Events, Formatting.Indented,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects});
            TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                () => Utils.EscapeJson(history));

            // Check to see if we need to start a new execution
            if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
            {
                var taskMessage = new TaskMessage();
                var startedEvent = new ExecutionStartedEvent(-1, completeOrchestratorAction.Result);
                startedEvent.OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                    ExecutionId = Guid.NewGuid().ToString("N")
                };
                startedEvent.Tags = runtimeState.Tags;
                startedEvent.ParentInstance = runtimeState.ParentInstance;
                startedEvent.Name = runtimeState.Name;
                startedEvent.Version = completeOrchestratorAction.NewVersion ?? runtimeState.Version;

                taskMessage.OrchestrationInstance = startedEvent.OrchestrationInstance;
                taskMessage.Event = startedEvent;

                return taskMessage;
            }

            // If this is a Sub Orchestration than notify the parent by sending a complete message
            if (runtimeState.ParentInstance != null)
            {
                var taskMessage = new TaskMessage();
                if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.Completed)
                {
                    var subOrchestrationCompletedEvent =
                        new SubOrchestrationInstanceCompletedEvent(-1, runtimeState.ParentInstance.TaskScheduleId,
                            completeOrchestratorAction.Result);

                    taskMessage.Event = subOrchestrationCompletedEvent;
                }
                else if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.Failed ||
                         completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.Terminated)
                {
                    var subOrchestrationFailedEvent =
                        new SubOrchestrationInstanceFailedEvent(-1, runtimeState.ParentInstance.TaskScheduleId,
                            completeOrchestratorAction.Result,
                            includeDetails ? completeOrchestratorAction.Details : null);

                    taskMessage.Event = subOrchestrationFailedEvent;
                }

                if (taskMessage.Event != null)
                {
                    taskMessage.OrchestrationInstance = runtimeState.ParentInstance.OrchestrationInstance;
                    return taskMessage;
                }
            }

            return null;
        }

        static TaskMessage ProcessScheduleTaskDecision(
            ScheduleTaskOrchestratorAction scheduleTaskOrchestratorAction,
            OrchestrationRuntimeState runtimeState, bool includeParameters)
        {
            var taskMessage = new TaskMessage();

            var scheduledEvent = new TaskScheduledEvent(scheduleTaskOrchestratorAction.Id);
            scheduledEvent.Name = scheduleTaskOrchestratorAction.Name;
            scheduledEvent.Version = scheduleTaskOrchestratorAction.Version;
            scheduledEvent.Input = scheduleTaskOrchestratorAction.Input;

            taskMessage.Event = scheduledEvent;
            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;

            if (!includeParameters)
            {
                scheduledEvent = new TaskScheduledEvent(scheduleTaskOrchestratorAction.Id);
                scheduledEvent.Name = scheduleTaskOrchestratorAction.Name;
                scheduledEvent.Version = scheduleTaskOrchestratorAction.Version;
            }
            runtimeState.AddEvent(scheduledEvent);
            return taskMessage;
        }

        static TaskMessage ProcessCreateTimerDecision(
            CreateTimerOrchestratorAction createTimerOrchestratorAction, OrchestrationRuntimeState runtimeState)
        {
            var taskMessage = new TaskMessage();

            var timerCreatedEvent = new TimerCreatedEvent(createTimerOrchestratorAction.Id);
            timerCreatedEvent.FireAt = createTimerOrchestratorAction.FireAt;
            runtimeState.AddEvent(timerCreatedEvent);

            var timerFiredEvent = new TimerFiredEvent(-1);
            timerFiredEvent.TimerId = createTimerOrchestratorAction.Id;

            taskMessage.Event = timerFiredEvent;
            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;

            return taskMessage;
        }

        static TaskMessage ProcessCreateSubOrchestrationInstanceDecision(
            CreateSubOrchestrationAction createSubOrchestrationAction,
            OrchestrationRuntimeState runtimeState, bool includeParameters)
        {
            var historyEvent = new SubOrchestrationInstanceCreatedEvent(createSubOrchestrationAction.Id);
            historyEvent.Name = createSubOrchestrationAction.Name;
            historyEvent.Version = createSubOrchestrationAction.Version;
            historyEvent.InstanceId = createSubOrchestrationAction.InstanceId;
            if (includeParameters)
            {
                historyEvent.Input = createSubOrchestrationAction.Input;
            }
            runtimeState.AddEvent(historyEvent);

            var taskMessage = new TaskMessage();
            var startedEvent = new ExecutionStartedEvent(-1, createSubOrchestrationAction.Input);
            startedEvent.Tags = runtimeState.Tags;
            startedEvent.OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = createSubOrchestrationAction.InstanceId,
                ExecutionId = Guid.NewGuid().ToString("N")
            };
            startedEvent.ParentInstance = new ParentInstance
            {
                OrchestrationInstance = runtimeState.OrchestrationInstance,
                Name = runtimeState.Name,
                Version = runtimeState.Version,
                TaskScheduleId = createSubOrchestrationAction.Id
            };
            startedEvent.Name = createSubOrchestrationAction.Name;
            startedEvent.Version = createSubOrchestrationAction.Version;

            taskMessage.OrchestrationInstance = startedEvent.OrchestrationInstance;
            taskMessage.Event = startedEvent;

            return taskMessage;
        }

        protected override async Task SafeReleaseWorkItem(SessionWorkItem workItem)
        {
            if (workItem != null && workItem.Session != null)
            {
                try
                {
                    await workItem.Session.CloseAsync();
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.Session.SessionId, ex,
                        "Error while closing session");
                }
            }
        }

        protected override async Task AbortWorkItem(SessionWorkItem workItem)
        {
            if (workItem != null && workItem.Session != null)
            {
                if (workItem.Messages != null && workItem.Messages.Any())
                {
                    TraceHelper.TraceSession(TraceEventType.Error, workItem.Session.SessionId,
                        "Abandoning {0} messages due to workitem abort", workItem.Messages.Count());

                    foreach (BrokeredMessage message in workItem.Messages)
                    {
                        await workItem.Session.AbandonAsync(message.LockToken);
                    }
                }

                try
                {
                    workItem.Session.Abort();
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.Session.SessionId, ex,
                        "Error while aborting session");
                }
            }
        }

        protected override int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (exception is MessagingException)
            {
                return settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }

            return 0;
        }

        protected override int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            if (exception is TimeoutException)
            {
                return 0;
            }

            int delay = settings.TaskOrchestrationDispatcherSettings.NonTransientErrorBackOffSecs;
            if (exception is MessagingException && (exception as MessagingException).IsTransient)
            {
                delay = settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }
            return delay;
        }

        internal class MessageContainer
        {
            internal BrokeredMessage Message { get; set; }
            internal OrchestratorAction Action { get; set; }

            internal MessageContainer(BrokeredMessage message, OrchestratorAction action)
            {
                this.Message = message;
                this.Action = action;
            }
        }
    }
}