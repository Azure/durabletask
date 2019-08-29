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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Dispatcher for orchestrations to handle processing and renewing, completion of orchestration events
    /// </summary>
    public class TaskOrchestrationDispatcher
    {
        static readonly DataConverter DataConverter = new JsonDataConverter();
        static readonly Task CompletedTask = Task.FromResult(0);

        readonly INameVersionObjectManager<TaskOrchestration> objectManager;
        readonly IOrchestrationService orchestrationService;
        readonly WorkItemDispatcher<TaskOrchestrationWorkItem> dispatcher;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        readonly NonBlockingCountdownLock concurrentSessionLock;

        internal TaskOrchestrationDispatcher(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskOrchestration> objectManager,
            DispatchMiddlewarePipeline dispatchPipeline)
        {
            this.objectManager = objectManager ?? throw new ArgumentNullException(nameof(objectManager));
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.dispatchPipeline = dispatchPipeline ?? throw new ArgumentNullException(nameof(dispatchPipeline));

            this.dispatcher = new WorkItemDispatcher<TaskOrchestrationWorkItem>(
                "TaskOrchestrationDispatcher",
                item => item == null ? string.Empty : item.InstanceId,
                OnFetchWorkItemAsync,
                OnProcessWorkItemSessionAsync)
            {
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                SafeReleaseWorkItem = orchestrationService.ReleaseTaskOrchestrationWorkItemAsync,
                AbortWorkItem = orchestrationService.AbandonTaskOrchestrationWorkItemAsync,
                DispatcherCount = orchestrationService.TaskOrchestrationDispatcherCount,
                MaxConcurrentWorkItems = orchestrationService.MaxConcurrentTaskOrchestrationWorkItems
            };

            // To avoid starvation, we only allow half of all concurrently execution orchestrations to
            // leverage extended sessions.
            var maxConcurrentSessions = (int)Math.Ceiling(this.dispatcher.MaxConcurrentWorkItems / 2.0);
            this.concurrentSessionLock = new NonBlockingCountdownLock(maxConcurrentSessions);
        }

        /// <summary>
        /// Starts the dispatcher to start getting and processing orchestration events
        /// </summary>
        public async Task StartAsync()
        {
            await this.dispatcher.StartAsync();
        }

        /// <summary>
        /// Stops the dispatcher to stop getting and processing orchestration events
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully or immediately</param>
        public async Task StopAsync(bool forced)
        {
            await this.dispatcher.StopAsync(forced);
        }

        /// <summary>
        /// Gets or sets flag whether to include additional details in error messages
        /// </summary>
        public bool IncludeDetails { get; set; }

        /// <summary>
        /// Gets or sets flag whether to pass orchestration input parameters to sub orchestrations
        /// </summary>
        public bool IncludeParameters { get; set; }

        /// <summary>
        /// Method to get the next work item to process within supplied timeout
        /// </summary>
        /// <param name="receiveTimeout">The max timeout to wait</param>
        /// <param name="cancellationToken">A cancellation token used to cancel a fetch operation.</param>
        /// <returns>A new TaskOrchestrationWorkItem</returns>
        protected Task<TaskOrchestrationWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.orchestrationService.LockNextTaskOrchestrationWorkItemAsync(receiveTimeout, cancellationToken);
        }

        async Task OnProcessWorkItemSessionAsync(TaskOrchestrationWorkItem workItem)
        {
            if (workItem.Session == null)
            {
                // Legacy behavior
                await OnProcessWorkItemAsync(workItem);
                return;
            }

            var isExtendedSession = false;
            var processCount = 0;
            try
            {
                while (true)
                {
                    // If the provider provided work items, execute them.
                    if (workItem.NewMessages?.Count > 0)
                    {
                        bool isCompletedOrInterrupted = await OnProcessWorkItemAsync(workItem);
                        if (isCompletedOrInterrupted)
                        {
                            break;
                        }

                        processCount++;
                    }

                    // Fetches beyond the first require getting an extended session lock, used to prevent starvation.
                    if (processCount > 0 && !isExtendedSession)
                    {
                        isExtendedSession = this.concurrentSessionLock.Acquire();
                        if (!isExtendedSession)
                        {
                            TraceHelper.Trace(TraceEventType.Verbose, "OnProcessWorkItemSession-MaxOperations", "Failed to acquire concurrent session lock.");
                            break;
                        }
                    }

                    TraceHelper.Trace(TraceEventType.Verbose, "OnProcessWorkItemSession-StartFetch", "Starting fetch of existing session.");
                    Stopwatch timer = Stopwatch.StartNew();

                    // Wait for new messages to arrive for the session. This call is expected to block (asynchronously)
                    // until either new messages are available or until a provider-specific timeout has expired.
                    workItem.NewMessages = await workItem.Session.FetchNewOrchestrationMessagesAsync(workItem);
                    if (workItem.NewMessages == null)
                    {
                        break;
                    }

                    TraceHelper.Trace(
                        TraceEventType.Verbose,
                        "OnProcessWorkItemSession-EndFetch",
                        $"Fetched {workItem.NewMessages.Count} new message(s) after {timer.ElapsedMilliseconds} ms from existing session.");
                    workItem.OrchestrationRuntimeState.NewEvents.Clear();
                }
            }
            finally
            {
                if (isExtendedSession)
                {
                    TraceHelper.Trace(
                        TraceEventType.Verbose,
                        "OnProcessWorkItemSession-Release",
                        $"Releasing extended session after {processCount} batch(es).");
                    this.concurrentSessionLock.Release();
                }
            }
        }

        /// <summary>
        /// Method to process a new work item
        /// </summary>
        /// <param name="workItem">The work item to process</param>
        protected async Task<bool> OnProcessWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            var messagesToSend = new List<TaskMessage>();
            var timerMessages = new List<TaskMessage>();
            var orchestratorMessages = new List<TaskMessage>();
            var isCompleted = false;
            var continuedAsNew = false;
            var isInterrupted = false;

            ExecutionStartedEvent continueAsNewExecutionStarted = null;
            TaskMessage continuedAsNewMessage = null;
            IList<HistoryEvent> carryOverEvents = null;
            string carryOverStatus = null;

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

            OrchestrationRuntimeState originalOrchestrationRuntimeState = runtimeState;

            OrchestrationState instanceState = null;

            if (!ReconcileMessagesWithState(workItem))
            {
                // TODO : mark an orchestration as faulted if there is data corruption
                TraceHelper.TraceSession(
                    TraceEventType.Error,
                    "TaskOrchestrationDispatcher-DeletedOrchestration",
                    runtimeState.OrchestrationInstance?.InstanceId,
                    "Received result for a deleted orchestration");
                isCompleted = true;
            }
            else
            {
                do
                {
                    continuedAsNew = false;
                    continuedAsNewMessage = null;

                    TraceHelper.TraceInstance(
                        TraceEventType.Verbose,
                        "TaskOrchestrationDispatcher-ExecuteUserOrchestration-Begin",
                        runtimeState.OrchestrationInstance,
                        "Executing user orchestration: {0}",
                        DataConverter.Serialize(runtimeState.GetOrchestrationRuntimeStateDump(), true));

                    if (workItem.Cursor == null)
                    {
                        workItem.Cursor = await ExecuteOrchestrationAsync(runtimeState);
                    }
                    else
                    {
                        await ResumeOrchestrationAsync(workItem.Cursor);
                    }

                    IReadOnlyList<OrchestratorAction> decisions = workItem.Cursor.LatestDecisions.ToList();

                    TraceHelper.TraceInstance(
                        TraceEventType.Information,
                        "TaskOrchestrationDispatcher-ExecuteUserOrchestration-End",
                        runtimeState.OrchestrationInstance,
                        "Executed user orchestration. Received {0} orchestrator actions: {1}",
                        decisions.Count,
                        string.Join(", ", decisions.Select(d => d.Id + ":" + d.OrchestratorActionType)));

                    foreach (OrchestratorAction decision in decisions)
                    {
                        TraceHelper.TraceInstance(
                            TraceEventType.Information,
                            "TaskOrchestrationDispatcher-ProcessOrchestratorAction",
                            runtimeState.OrchestrationInstance,
                            "Processing orchestrator action of type {0}",
                            decision.OrchestratorActionType);
                        switch (decision.OrchestratorActionType)
                        {
                            case OrchestratorActionType.ScheduleOrchestrator:
                                messagesToSend.Add(
                                    ProcessScheduleTaskDecision((ScheduleTaskOrchestratorAction)decision, runtimeState,
                                        IncludeParameters));
                                break;
                            case OrchestratorActionType.CreateTimer:
                                var timerOrchestratorAction = (CreateTimerOrchestratorAction)decision;
                                timerMessages.Add(ProcessCreateTimerDecision(timerOrchestratorAction, runtimeState));
                                break;
                            case OrchestratorActionType.CreateSubOrchestration:
                                var createSubOrchestrationAction = (CreateSubOrchestrationAction)decision;
                                orchestratorMessages.Add(
                                    ProcessCreateSubOrchestrationInstanceDecision(createSubOrchestrationAction,
                                        runtimeState, IncludeParameters));
                                break;
                            case OrchestratorActionType.SendEvent:
                                var sendEventAction = (SendEventOrchestratorAction)decision;
                                orchestratorMessages.Add(
                                   ProcessSendEventDecision(sendEventAction, runtimeState));
                                break;
                            case OrchestratorActionType.OrchestrationComplete:
                                OrchestrationCompleteOrchestratorAction completeDecision = (OrchestrationCompleteOrchestratorAction)decision;
                                TaskMessage workflowInstanceCompletedMessage =
                                    ProcessWorkflowCompletedTaskDecision(completeDecision, runtimeState, IncludeDetails, out continuedAsNew);
                                if (workflowInstanceCompletedMessage != null)
                                {
                                    // Send complete message to parent workflow or to itself to start a new execution
                                    // Store the event so we can rebuild the state
                                    carryOverEvents = null;
                                    if (continuedAsNew)
                                    {
                                        continuedAsNewMessage = workflowInstanceCompletedMessage;
                                        continueAsNewExecutionStarted = workflowInstanceCompletedMessage.Event as ExecutionStartedEvent;
                                        if (completeDecision.CarryoverEvents.Any())
                                        {
                                            carryOverEvents = completeDecision.CarryoverEvents.ToList();
                                            completeDecision.CarryoverEvents.Clear();
                                        }
                                    }
                                    else
                                    {
                                        orchestratorMessages.Add(workflowInstanceCompletedMessage);
                                    }
                                }

                                isCompleted = !continuedAsNew;
                                break;
                            default:
                                throw TraceHelper.TraceExceptionInstance(
                                    TraceEventType.Error,
                                    "TaskOrchestrationDispatcher-UnsupportedDecisionType",
                                    runtimeState.OrchestrationInstance,
                                    new NotSupportedException("decision type not supported"));
                        }

                        // Underlying orchestration service provider may have a limit of messages per call, to avoid the situation
                        // we keep on asking the provider if message count is ok and stop processing new decisions if not.
                        //
                        // We also put in a fake timer to force next orchestration task for remaining messages
                        int totalMessages = messagesToSend.Count + orchestratorMessages.Count + timerMessages.Count;
                        if (this.orchestrationService.IsMaxMessageCountExceeded(totalMessages, runtimeState))
                        {
                            TraceHelper.TraceInstance(
                                TraceEventType.Information,
                                "TaskOrchestrationDispatcher-MaxMessageCountReached",
                                runtimeState.OrchestrationInstance,
                                "MaxMessageCount reached.  Adding timer to process remaining events in next attempt.");

                            if (isCompleted || continuedAsNew)
                            {
                                TraceHelper.TraceInstance(
                                    TraceEventType.Information,
                                    "TaskOrchestrationDispatcher-OrchestrationAlreadyCompleted",
                                    runtimeState.OrchestrationInstance,
                                    "Orchestration already completed.  Skip adding timer for splitting messages.");
                                break;
                            }

                            var dummyTimer = new CreateTimerOrchestratorAction
                            {
                                Id = FrameworkConstants.FakeTimerIdToSplitDecision,
                                FireAt = DateTime.UtcNow
                            };

                            timerMessages.Add(ProcessCreateTimerDecision(dummyTimer, runtimeState));
                            isInterrupted = true;
                            break;
                        }
                    }

                    // finish up processing of the work item
                    if (!continuedAsNew && runtimeState.Events.Last().EventType != EventType.OrchestratorCompleted)
                    {
                        runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                    }

                    if (isCompleted)
                    {
                        TraceHelper.TraceSession(TraceEventType.Information, "TaskOrchestrationDispatcher-DeletingSessionState", workItem.InstanceId, "Deleting session state");
                        if (runtimeState.ExecutionStartedEvent != null)
                        {
                            instanceState = Utils.BuildOrchestrationState(runtimeState);
                        }
                    }
                    else
                    {
                        if (continuedAsNew)
                        {
                            TraceHelper.TraceSession(
                                TraceEventType.Information,
                                "TaskOrchestrationDispatcher-UpdatingStateForContinuation",
                                workItem.InstanceId,
                                "Updating state for continuation");

                            runtimeState = new OrchestrationRuntimeState();
                            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                            runtimeState.AddEvent(continueAsNewExecutionStarted);
                            runtimeState.Status = workItem.OrchestrationRuntimeState.Status ?? carryOverStatus;
                            carryOverStatus = workItem.OrchestrationRuntimeState.Status;

                            if (carryOverEvents != null)
                            {
                                foreach (var historyEvent in carryOverEvents)
                                {
                                    runtimeState.AddEvent(historyEvent);
                                }
                            }

                            runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                            workItem.OrchestrationRuntimeState = runtimeState;

                            TaskOrchestration newOrchestration = this.objectManager.GetObject(
                                runtimeState.Name,
                                continueAsNewExecutionStarted.Version);

                            workItem.Cursor = new OrchestrationExecutionCursor(
                                runtimeState,
                                newOrchestration,
                                new TaskOrchestrationExecutor(
                                    runtimeState,
                                    newOrchestration,
                                    orchestrationService.EventBehaviourForContinueAsNew),
                                latestDecisions: null);

                            if (workItem.LockedUntilUtc < DateTime.UtcNow.AddMinutes(1))
                            {
                                await orchestrationService.RenewTaskOrchestrationWorkItemLockAsync(workItem);
                            }
                        }

                        instanceState = Utils.BuildOrchestrationState(runtimeState);
                    }
                } while (continuedAsNew);
            }

            workItem.OrchestrationRuntimeState = originalOrchestrationRuntimeState;

            runtimeState.Status = runtimeState.Status ?? carryOverStatus;

            await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                workItem,
                runtimeState,
                continuedAsNew ? null : messagesToSend,
                orchestratorMessages,
                continuedAsNew ? null : timerMessages,
                continuedAsNewMessage,
                instanceState);

            workItem.OrchestrationRuntimeState = runtimeState;

            workItem.Session?.UpdateRuntimeState(runtimeState);

            return isCompleted || continuedAsNew || isInterrupted;
        }

        async Task<OrchestrationExecutionCursor> ExecuteOrchestrationAsync(OrchestrationRuntimeState runtimeState)
        {
            TaskOrchestration taskOrchestration = this.objectManager.GetObject(runtimeState.Name, runtimeState.Version);
            if (taskOrchestration == null)
            {
                throw TraceHelper.TraceExceptionInstance(
                    TraceEventType.Error,
                    "TaskOrchestrationDispatcher-TypeMissing",
                    runtimeState.OrchestrationInstance,
                    new TypeMissingException($"Orchestration not found: ({runtimeState.Name}, {runtimeState.Version})"));
            }

            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(runtimeState.OrchestrationInstance);
            dispatchContext.SetProperty(taskOrchestration);
            dispatchContext.SetProperty(runtimeState);

            var executor = new TaskOrchestrationExecutor(runtimeState, taskOrchestration, orchestrationService.EventBehaviourForContinueAsNew);

            IEnumerable<OrchestratorAction> decisions = Enumerable.Empty<OrchestratorAction>();
            await this.dispatchPipeline.RunAsync(dispatchContext, _ =>
            {
                decisions = executor.Execute();
                return CompletedTask;
            });

            return new OrchestrationExecutionCursor(runtimeState, taskOrchestration, executor, decisions);
        }

        Task ResumeOrchestrationAsync(OrchestrationExecutionCursor cursor)
        {
            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(cursor.RuntimeState.OrchestrationInstance);
            dispatchContext.SetProperty(cursor.TaskOrchestration);
            dispatchContext.SetProperty(cursor.RuntimeState);

            cursor.LatestDecisions = Enumerable.Empty<OrchestratorAction>();
            return this.dispatchPipeline.RunAsync(dispatchContext, _ =>
            {
                cursor.LatestDecisions = cursor.OrchestrationExecutor.ExecuteNewEvents();
                return CompletedTask;
            });
        }

        static bool ReconcileMessagesWithState(TaskOrchestrationWorkItem workItem)
        {
            foreach (TaskMessage message in workItem.NewMessages)
            {
                OrchestrationInstance orchestrationInstance = message.OrchestrationInstance;
                if (string.IsNullOrWhiteSpace(orchestrationInstance?.InstanceId))
                {
                    throw TraceHelper.TraceException(
                        TraceEventType.Error,
                        "TaskOrchestrationDispatcher-OrchestrationInstanceMissing",
                        new InvalidOperationException("Message does not contain any OrchestrationInstance information"));
                }

                if (workItem.OrchestrationRuntimeState.Events.Count == 1 && message.Event.EventType != EventType.ExecutionStarted)
                {
                    // we get here because of:
                    //      i) responses for scheduled tasks after the orchestrations have been completed
                    //      ii) responses for explicitly deleted orchestrations
                    return false;
                }

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "TaskOrchestrationDispatcher-ProcessEvent",
                    orchestrationInstance,
                    "Processing new event with Id {0} and type {1}",
                    message.Event.EventId,
                    message.Event.EventType);

                if (message.Event.EventType == EventType.ExecutionStarted)
                {
                    if (workItem.OrchestrationRuntimeState.Events.Count > 1)
                    {
                        // this was caused due to a dupe execution started event, swallow this one
                        TraceHelper.TraceInstance(
                            TraceEventType.Warning,
                            "TaskOrchestrationDispatcher-DuplicateStartEvent",
                            orchestrationInstance,
                            "Duplicate start event.  Ignoring event with Id {0} and type {1} ",
                            message.Event.EventId,
                            message.Event.EventType);
                        continue;
                    }
                }
                else if (!string.IsNullOrWhiteSpace(orchestrationInstance.ExecutionId)
                         &&
                         !string.Equals(orchestrationInstance.ExecutionId,
                             workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId))
                {
                    // eat up any events for previous executions
                    TraceHelper.TraceInstance(
                        TraceEventType.Warning,
                        "TaskOrchestrationDispatcher-ExecutionIdMismatch",
                        orchestrationInstance,
                        "ExecutionId of event does not match current executionId.  Ignoring event with Id {0} and type {1} ",
                        message.Event.EventId,
                        message.Event.EventType);
                    continue;
                }

                workItem.OrchestrationRuntimeState.AddEvent(message.Event);
            }

            return true;
        }

        static TaskMessage ProcessWorkflowCompletedTaskDecision(
            OrchestrationCompleteOrchestratorAction completeOrchestratorAction,
            OrchestrationRuntimeState runtimeState,
            bool includeDetails,
            out bool continuedAsNew)
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

            TraceHelper.TraceInstance(
                runtimeState.OrchestrationStatus == OrchestrationStatus.Failed ? TraceEventType.Warning : TraceEventType.Information,
                "TaskOrchestrationDispatcher-InstanceCompleted",
                runtimeState.OrchestrationInstance,
                "Instance Id '{0}' completed in state {1} with result: {2}",
                runtimeState.OrchestrationInstance,
                runtimeState.OrchestrationStatus,
                completeOrchestratorAction.Result);
            TraceHelper.TraceInstance(
                TraceEventType.Information,
                "TaskOrchestrationDispatcher-InstanceCompletionEvents",
                runtimeState.OrchestrationInstance,
                () => Utils.EscapeJson(DataConverter.Serialize(runtimeState.Events, true)));

            // Check to see if we need to start a new execution
            if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
            {
                var taskMessage = new TaskMessage();
                var startedEvent = new ExecutionStartedEvent(-1, completeOrchestratorAction.Result)
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                        ExecutionId = Guid.NewGuid().ToString("N")
                    },
                    Tags = runtimeState.Tags,
                    ParentInstance = runtimeState.ParentInstance,
                    Name = runtimeState.Name,
                    Version = completeOrchestratorAction.NewVersion ?? runtimeState.Version
                };

                taskMessage.OrchestrationInstance = startedEvent.OrchestrationInstance;
                taskMessage.Event = startedEvent;

                return taskMessage;
            }

            // If this is a Sub Orchestration, and not tagged as fire-and-forget, 
            // then notify the parent by sending a complete message
            if (runtimeState.ParentInstance != null
                && !OrchestrationTags.IsTaggedAsFireAndForget(runtimeState.Tags))
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
            OrchestrationRuntimeState runtimeState,
            bool includeParameters)
        {
            var taskMessage = new TaskMessage();

            var scheduledEvent = new TaskScheduledEvent(scheduleTaskOrchestratorAction.Id)
            {
                Name = scheduleTaskOrchestratorAction.Name,
                Version = scheduleTaskOrchestratorAction.Version,
                Input = scheduleTaskOrchestratorAction.Input
            };

            taskMessage.Event = scheduledEvent;
            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;

            if (!includeParameters)
            {
                scheduledEvent = new TaskScheduledEvent(scheduleTaskOrchestratorAction.Id)
                {
                    Name = scheduleTaskOrchestratorAction.Name,
                    Version = scheduleTaskOrchestratorAction.Version
                };
            }

            runtimeState.AddEvent(scheduledEvent);
            return taskMessage;
        }

        static TaskMessage ProcessCreateTimerDecision(
            CreateTimerOrchestratorAction createTimerOrchestratorAction,
            OrchestrationRuntimeState runtimeState)
        {
            var taskMessage = new TaskMessage();

            runtimeState.AddEvent(new TimerCreatedEvent(createTimerOrchestratorAction.Id)
            {
                FireAt = createTimerOrchestratorAction.FireAt
            });

            taskMessage.Event = new TimerFiredEvent(-1)
            {
                TimerId = createTimerOrchestratorAction.Id,
                FireAt = createTimerOrchestratorAction.FireAt
            };

            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;

            return taskMessage;
        }

        static TaskMessage ProcessCreateSubOrchestrationInstanceDecision(
            CreateSubOrchestrationAction createSubOrchestrationAction,
            OrchestrationRuntimeState runtimeState,
            bool includeParameters)
        {
            var historyEvent = new SubOrchestrationInstanceCreatedEvent(createSubOrchestrationAction.Id)
            {
                Name = createSubOrchestrationAction.Name,
                Version = createSubOrchestrationAction.Version,
                InstanceId = createSubOrchestrationAction.InstanceId
            };
            if (includeParameters)
            {
                historyEvent.Input = createSubOrchestrationAction.Input;
            }

            runtimeState.AddEvent(historyEvent);

            var taskMessage = new TaskMessage();

            var startedEvent = new ExecutionStartedEvent(-1, createSubOrchestrationAction.Input)
            {
                Tags = OrchestrationTags.MergeTags(createSubOrchestrationAction.Tags, runtimeState.Tags),
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = createSubOrchestrationAction.InstanceId,
                    ExecutionId = Guid.NewGuid().ToString("N")
                },
                ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = runtimeState.OrchestrationInstance,
                    Name = runtimeState.Name,
                    Version = runtimeState.Version,
                    TaskScheduleId = createSubOrchestrationAction.Id
                },
                Name = createSubOrchestrationAction.Name,
                Version = createSubOrchestrationAction.Version
            };

            taskMessage.OrchestrationInstance = startedEvent.OrchestrationInstance;
            taskMessage.Event = startedEvent;

            return taskMessage;
        }

        static TaskMessage ProcessSendEventDecision(
            SendEventOrchestratorAction sendEventAction,
            OrchestrationRuntimeState runtimeState)
        {
            var historyEvent = new EventSentEvent(sendEventAction.Id)
            {
                 InstanceId = sendEventAction.Instance.InstanceId,
                 Name = sendEventAction.EventName,
                 Input = sendEventAction.EventData
            };
            
            runtimeState.AddEvent(historyEvent);

            return new TaskMessage
            {
                OrchestrationInstance = sendEventAction.Instance,
                Event = new EventRaisedEvent(-1, sendEventAction.EventData)
                {
                    Name = sendEventAction.EventName
                }
            };
        }

 
        class NonBlockingCountdownLock
        {
            int available;

            public NonBlockingCountdownLock(int available)
            {
                if (available <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(available));
                }

                this.available = available;
            }

            public bool Acquire()
            {
                if (this.available <= 0)
                {
                    return false;
                }

                if (Interlocked.Decrement(ref this.available) >= 0)
                {
                    return true;
                }

                // the counter went negative - fix it
                Interlocked.Increment(ref this.available);
                return false;
            }

            public void Release()
            {
                Interlocked.Increment(ref this.available);
            }
        }
    }
}