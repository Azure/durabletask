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
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Dispatcher for orchestrations to handle processing and renewing, completion of orchestration events
    /// </summary>
    public class TaskOrchestrationDispatcher
    {
        static readonly Task CompletedTask = Task.FromResult(0);

        readonly INameVersionObjectManager<TaskOrchestration> objectManager;
        readonly IOrchestrationService orchestrationService;
        readonly WorkItemDispatcher<TaskOrchestrationWorkItem> dispatcher;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        readonly LogHelper logHelper;
        ErrorPropagationMode errorPropagationMode;
        readonly NonBlockingCountdownLock concurrentSessionLock;

        internal TaskOrchestrationDispatcher(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskOrchestration> objectManager,
            DispatchMiddlewarePipeline dispatchPipeline,
            LogHelper logHelper,
            ErrorPropagationMode errorPropagationMode)
        {
            this.objectManager = objectManager ?? throw new ArgumentNullException(nameof(objectManager));
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.dispatchPipeline = dispatchPipeline ?? throw new ArgumentNullException(nameof(dispatchPipeline));
            this.logHelper = logHelper ?? throw new ArgumentNullException(nameof(logHelper));
            this.errorPropagationMode = errorPropagationMode;

            this.dispatcher = new WorkItemDispatcher<TaskOrchestrationWorkItem>(
                "TaskOrchestrationDispatcher",
                item => item == null ? string.Empty : item.InstanceId,
                this.OnFetchWorkItemAsync,
                this.OnProcessWorkItemSessionAsync)
            {
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                SafeReleaseWorkItem = orchestrationService.ReleaseTaskOrchestrationWorkItemAsync,
                AbortWorkItem = orchestrationService.AbandonTaskOrchestrationWorkItemAsync,
                DispatcherCount = orchestrationService.TaskOrchestrationDispatcherCount,
                MaxConcurrentWorkItems = orchestrationService.MaxConcurrentTaskOrchestrationWorkItems,
                LogHelper = logHelper,
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


        /// <summary>
        /// Ensures the first ExecutionStarted event in the batch (if any) appears at the beginning
        /// of its executionID history.
        /// If this is not already the case, we move the first ExecutionStarted event "backwards"
        /// until it either reaches the beginning of the list or reaches a different, non-null, executionID.
        ///
        /// Note that this method modifies its input in-place.
        /// </summary>
        /// <param name="batch">The batch of workitems to potentially re-order in-place</param>
        void EnsureExecutionStartedIsFirst(IList<TaskMessage> batch)
        {
            // We look for *the first* instance of an ExecutionStarted event in the batch, if any.
            int index = 0;
            string previousExecutionId = "";
            int targetPosition = 0; // new position of ExecutionStarted in case of a re-ordering
            TaskMessage? executionStartedEvent = null;
            foreach (TaskMessage message in batch)
            {
                // Keep track of orchestrator generation changes, maybe update target position
                string executionId = message.OrchestrationInstance.ExecutionId;
                if(previousExecutionId != executionId)
                {
                    // We want to re-position the ExecutionStarted event after the "right-most"
                    // event with a non-null executionID that came before it.
                    // So, only update target position if the executionID changed
                    // and the previous executionId was not null.
                    if (previousExecutionId != null)
                    {
                        targetPosition = index;
                    }

                    previousExecutionId = executionId;
                }

                // Find the first ExecutionStarted event.
                if (message.Event.EventType == EventType.ExecutionStarted)
                {
                    // ParentInstance needs to be null to avoid re-ordering
                    // ContinueAsNew events
                    if ((message.Event is ExecutionStartedEvent eventData) &&
                        (eventData.ParentInstance == null))
                    {
                        executionStartedEvent = message;
                    }
                    // We only consider the first ExecutionStarted event in the
                    // list, so we always break.
                    break;
                }
                index++;
            }

            // If we found an ExecutionStartedEvent, we place it either
            // (A) in the beginning or
            // (B) after the "right-most" event with non-null executionID that came before it.
            int executionStartedIndex = index;
            if ((executionStartedEvent != null) && (executionStartedIndex != targetPosition))
            {
                batch.RemoveAt(executionStartedIndex);
                batch.Insert(targetPosition, executionStartedEvent);
            }
        }

        async Task OnProcessWorkItemSessionAsync(TaskOrchestrationWorkItem workItem)
        {
            // DTFx history replay expects that ExecutionStarted comes before other events.
            // If this is not already the case, due to a race-condition, we re-order the
            // messages to enforce this expectation.
            EnsureExecutionStartedIsFirst(workItem.NewMessages);

            try
            {
                if (workItem.Session == null)
                {
                    // Legacy behavior
                    await this.OnProcessWorkItemAsync(workItem);
                    return;
                }

                var isExtendedSession = false;

                CorrelationTraceClient.Propagate(
                    () =>
                    {                
                        // Check if it is extended session.
                        isExtendedSession = this.concurrentSessionLock.Acquire();
                        this.concurrentSessionLock.Release();
                        workItem.IsExtendedSession = isExtendedSession;
                    });

                var processCount = 0;
                try
                {
                    while (true)
                    {
                        // If the provider provided work items, execute them.
                        if (workItem.NewMessages?.Count > 0)
                        {
                            bool isCompletedOrInterrupted = await this.OnProcessWorkItemAsync(workItem);
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
            catch (SessionAbortedException e)
            {
                // Either the orchestration or the orchestration service explicitly abandoned the session.
                OrchestrationInstance instance = workItem.OrchestrationRuntimeState?.OrchestrationInstance ?? new OrchestrationInstance { InstanceId = workItem.InstanceId };
                this.logHelper.OrchestrationAborted(instance, e.Message);
                TraceHelper.TraceInstance(TraceEventType.Warning, "TaskOrchestrationDispatcher-ExecutionAborted", instance, "{0}", e.Message);
                await this.orchestrationService.AbandonTaskOrchestrationWorkItemAsync(workItem);
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
            
            // correlation
            CorrelationTraceClient.Propagate(() => CorrelationTraceContext.Current = workItem.TraceContext);

            ExecutionStartedEvent? continueAsNewExecutionStarted = null;
            TaskMessage? continuedAsNewMessage = null;
            IList<HistoryEvent>? carryOverEvents = null;
            string? carryOverStatus = null;

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

            OrchestrationRuntimeState originalOrchestrationRuntimeState = runtimeState;

            OrchestrationState? instanceState = null;

            Task? renewTask = null;
            using var renewCancellationTokenSource = new CancellationTokenSource();
            if (workItem.LockedUntilUtc < DateTime.MaxValue)
            {
                // start a task to run RenewUntil
                renewTask = Task.Factory.StartNew(
                    () => this.RenewUntil(workItem, renewCancellationTokenSource.Token),
                    renewCancellationTokenSource.Token);
            }

            try
            {
                // Assumes that: if the batch contains a new "ExecutionStarted" event, it is the first message in the batch.
                if (!this.ReconcileMessagesWithState(workItem))
                {
                    // TODO : mark an orchestration as faulted if there is data corruption
                    this.logHelper.DroppingOrchestrationWorkItem(workItem, "Received work-item for an invalid orchestration");
                    TraceHelper.TraceSession(
                        TraceEventType.Error,
                        "TaskOrchestrationDispatcher-DeletedOrchestration",
                        runtimeState.OrchestrationInstance?.InstanceId,
                        "Received work-item for an invalid orchestration");
                    isCompleted = true;
                }
                else
                {
                    do
                    {
                        continuedAsNew = false;
                        continuedAsNewMessage = null;

                        this.logHelper.OrchestrationExecuting(runtimeState.OrchestrationInstance!, runtimeState.Name);
                        TraceHelper.TraceInstance(
                            TraceEventType.Verbose,
                            "TaskOrchestrationDispatcher-ExecuteUserOrchestration-Begin",
                            runtimeState.OrchestrationInstance,
                            "Executing user orchestration: {0}",
                            JsonDataConverter.Default.Serialize(runtimeState.GetOrchestrationRuntimeStateDump(), true));

                        if (workItem.Cursor == null)
                        {
                            workItem.Cursor = await this.ExecuteOrchestrationAsync(runtimeState, workItem);
                        }
                        else
                        {
                            await this.ResumeOrchestrationAsync(workItem);
                        }

                        IReadOnlyList<OrchestratorAction> decisions = workItem.Cursor.LatestDecisions.ToList();

                        this.logHelper.OrchestrationExecuted(
                            runtimeState.OrchestrationInstance!,
                            runtimeState.Name,
                            decisions);
                        TraceHelper.TraceInstance(
                            TraceEventType.Information,
                            "TaskOrchestrationDispatcher-ExecuteUserOrchestration-End",
                            runtimeState.OrchestrationInstance,
                            "Executed user orchestration. Received {0} orchestrator actions: {1}",
                            decisions.Count,
                            string.Join(", ", decisions.Select(d => d.Id + ":" + d.OrchestratorActionType)));

                        // TODO: Exception handling for invalid decisions, which is increasingly likely
                        //       when custom middleware is involved (e.g. out-of-process scenarios).
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
                                    var scheduleTaskAction = (ScheduleTaskOrchestratorAction)decision;
                                    var message = this.ProcessScheduleTaskDecision(
                                        scheduleTaskAction,
                                        runtimeState,
                                        this.IncludeParameters);
                                    messagesToSend.Add(message);
                                    break;
                                case OrchestratorActionType.CreateTimer:
                                    var timerOrchestratorAction = (CreateTimerOrchestratorAction)decision;
                                    timerMessages.Add(this.ProcessCreateTimerDecision(
                                        timerOrchestratorAction,
                                        runtimeState,
                                        isInternal: false));
                                    break;
                                case OrchestratorActionType.CreateSubOrchestration:
                                    var createSubOrchestrationAction = (CreateSubOrchestrationAction)decision;
                                    orchestratorMessages.Add(
                                        this.ProcessCreateSubOrchestrationInstanceDecision(
                                            createSubOrchestrationAction,
                                            runtimeState,
                                            this.IncludeParameters));
                                    break;
                                case OrchestratorActionType.SendEvent:
                                    var sendEventAction = (SendEventOrchestratorAction)decision;
                                    orchestratorMessages.Add(
                                        this.ProcessSendEventDecision(sendEventAction, runtimeState));
                                    break;
                                case OrchestratorActionType.OrchestrationComplete:
                                    OrchestrationCompleteOrchestratorAction completeDecision = (OrchestrationCompleteOrchestratorAction)decision;
                                    TaskMessage? workflowInstanceCompletedMessage =
                                        this.ProcessWorkflowCompletedTaskDecision(completeDecision, runtimeState, this.IncludeDetails, out continuedAsNew);
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
                                        new NotSupportedException($"Decision type '{decision.OrchestratorActionType}' not supported"));
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

                                timerMessages.Add(this.ProcessCreateTimerDecision(
                                    dummyTimer,
                                    runtimeState,
                                    isInternal: true));
                                isInterrupted = true;
                                break;
                            }
                        }

                        // correlation
                        CorrelationTraceClient.Propagate(() =>
                        {
                            if (runtimeState.ExecutionStartedEvent != null)
                                runtimeState.ExecutionStartedEvent.Correlation = CorrelationTraceContext.Current.SerializableTraceContext;
                        });


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

                                // correlation
                                CorrelationTraceClient.Propagate(() =>
                                {
                                    continueAsNewExecutionStarted!.Correlation = CorrelationTraceContext.Current.SerializableTraceContext;
                                });

                                runtimeState = new OrchestrationRuntimeState();
                                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                                runtimeState.AddEvent(continueAsNewExecutionStarted!);
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

                                workItem.Cursor = null;
                            }

                            instanceState = Utils.BuildOrchestrationState(runtimeState);
                        }
                    } while (continuedAsNew);
                }
            }
            finally
            {
                if (renewTask != null)
                {
                    try
                    {
                        renewCancellationTokenSource.Cancel();
                        await renewTask;
                    }
                    catch (ObjectDisposedException)
                    {
                        // ignore
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                }
            }

            if (workItem.RestoreOriginalRuntimeStateDuringCompletion)
            {
                // some backends expect the original runtime state object
                workItem.OrchestrationRuntimeState = originalOrchestrationRuntimeState;
            }

            runtimeState.Status = runtimeState.Status ?? carryOverStatus;

            if (instanceState != null)
            {
                instanceState.Status = runtimeState.Status;
            }


            await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                workItem,
                runtimeState,
                continuedAsNew ? null : messagesToSend,
                orchestratorMessages,
                continuedAsNew ? null : timerMessages,
                continuedAsNewMessage,
                instanceState);
            
            if (workItem.RestoreOriginalRuntimeStateDuringCompletion)
            {
                workItem.OrchestrationRuntimeState = runtimeState;
            }

            return isCompleted || continuedAsNew || isInterrupted;
        }

        static OrchestrationExecutionContext GetOrchestrationExecutionContext(OrchestrationRuntimeState runtimeState)
        {
            return new OrchestrationExecutionContext { OrchestrationTags = runtimeState.Tags ?? new Dictionary<string, string>(capacity: 0) };
        }

        TimeSpan MinRenewalInterval = TimeSpan.FromSeconds(5); // prevents excessive retries if clocks are off
        TimeSpan MaxRenewalInterval = TimeSpan.FromSeconds(30);

        async Task RenewUntil(TaskOrchestrationWorkItem workItem, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TimeSpan delay = workItem.LockedUntilUtc - DateTime.UtcNow - TimeSpan.FromSeconds(30);
                if (delay < MinRenewalInterval)
                {
                    delay = MinRenewalInterval;
                }
                else if (delay > MaxRenewalInterval)
                {
                    delay = MaxRenewalInterval;
                }

                await Utils.DelayWithCancellation(delay, cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                try
                {
                    this.logHelper.RenewOrchestrationWorkItemStarting(workItem);
                    TraceHelper.Trace(TraceEventType.Information, "TaskOrchestrationDispatcher-RenewWorkItemStarting", "Renewing work item for instance {0}", workItem.InstanceId);
                    await this.orchestrationService.RenewTaskOrchestrationWorkItemLockAsync(workItem);
                    this.logHelper.RenewOrchestrationWorkItemCompleted(workItem);
                    TraceHelper.Trace(TraceEventType.Information, "TaskOrchestrationDispatcher-RenewWorkItemCompleted", "Successfully renewed work item for instance {0}", workItem.InstanceId);
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    this.logHelper.RenewOrchestrationWorkItemFailed(workItem, exception);
                    TraceHelper.TraceException(TraceEventType.Warning, "TaskOrchestrationDispatcher-RenewWorkItemFailed", exception, "Failed to renew work item for instance {0}", workItem.InstanceId);
                }
            }
        }

        async Task<OrchestrationExecutionCursor> ExecuteOrchestrationAsync(OrchestrationRuntimeState runtimeState, TaskOrchestrationWorkItem workItem)
        {
            // Get the TaskOrchestration implementation. If it's not found, it either means that the developer never
            // registered it (which is an error, and we'll throw for this further down) or it could be that some custom
            // middleware (e.g. out-of-process execution middleware) is intended to implement the orchestration logic.
            TaskOrchestration? taskOrchestration = this.objectManager.GetObject(runtimeState.Name, runtimeState.Version!);

            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(runtimeState.OrchestrationInstance);
            dispatchContext.SetProperty(taskOrchestration);
            dispatchContext.SetProperty(runtimeState);
            dispatchContext.SetProperty(workItem);
            dispatchContext.SetProperty(GetOrchestrationExecutionContext(runtimeState));

            TaskOrchestrationExecutor? executor = null;

            await this.dispatchPipeline.RunAsync(dispatchContext, _ =>
            {
                // Check to see if the custom middleware intercepted and substituted the orchestration execution
                // with its own execution behavior, providing us with the end results. If so, we can terminate
                // the dispatch pipeline here.
                var resultFromMiddleware = dispatchContext.GetProperty<OrchestratorExecutionResult>();
                if (resultFromMiddleware != null)
                {
                    return CompletedTask;
                }

                if (taskOrchestration == null)
                {
                    throw TraceHelper.TraceExceptionInstance(
                        TraceEventType.Error,
                        "TaskOrchestrationDispatcher-TypeMissing",
                        runtimeState.OrchestrationInstance,
                        new TypeMissingException($"Orchestration not found: ({runtimeState.Name}, {runtimeState.Version})"));
                }

                executor = new TaskOrchestrationExecutor(
                    runtimeState,
                    taskOrchestration,
                    this.orchestrationService.EventBehaviourForContinueAsNew,
                    this.errorPropagationMode);
                OrchestratorExecutionResult resultFromOrchestrator = executor.Execute();
                dispatchContext.SetProperty(resultFromOrchestrator);
                return CompletedTask;
            });

            var result = dispatchContext.GetProperty<OrchestratorExecutionResult>();
            IEnumerable<OrchestratorAction> decisions = result?.Actions ?? Enumerable.Empty<OrchestratorAction>();
            runtimeState.Status = result?.CustomStatus;

            return new OrchestrationExecutionCursor(runtimeState, taskOrchestration, executor, decisions);
        }

        async Task ResumeOrchestrationAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationExecutionCursor cursor = workItem.Cursor;

            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(cursor.RuntimeState.OrchestrationInstance);
            dispatchContext.SetProperty(cursor.TaskOrchestration);
            dispatchContext.SetProperty(cursor.RuntimeState);
            dispatchContext.SetProperty(workItem);

            cursor.LatestDecisions = Enumerable.Empty<OrchestratorAction>();
            await this.dispatchPipeline.RunAsync(dispatchContext, _ =>
            {
                OrchestratorExecutionResult result = cursor.OrchestrationExecutor.ExecuteNewEvents();
                dispatchContext.SetProperty(result);
                return CompletedTask;
            });

            var result = dispatchContext.GetProperty<OrchestratorExecutionResult>();
            cursor.LatestDecisions = result?.Actions ?? Enumerable.Empty<OrchestratorAction>();
            cursor.RuntimeState.Status = result?.CustomStatus;
        }

        /// <summary>
        /// Converts new messages into history events that get appended to the existing orchestration state.
        /// Returns False if the workItem should be discarded. True if it should be processed further.
        /// Assumes that: if the batch contains a new "ExecutionStarted" event, it is the first message in the batch.
        /// </summary>
        /// <param name="workItem">A batch of work item messages.</param>
        /// <returns>True if workItem should be processed further. False otherwise.</returns>
        bool ReconcileMessagesWithState(TaskOrchestrationWorkItem workItem)
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

                if (!workItem.OrchestrationRuntimeState.IsValid)
                {
                    // we get here if the orchestration history is somehow corrupted (partially deleted, etc.)
                    return false;
                }

                if (workItem.OrchestrationRuntimeState.Events.Count == 1 && message.Event.EventType != EventType.ExecutionStarted)
                {
                    // we get here because of:
                    //      i) responses for scheduled tasks after the orchestrations have been completed
                    //      ii) responses for explicitly deleted orchestrations
                    return false;
                }

                this.logHelper.ProcessingOrchestrationMessage(workItem, message);
                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "TaskOrchestrationDispatcher-ProcessEvent",
                    orchestrationInstance,
                    "Processing new event with Id {0} and type {1}",
                    message.Event.EventId,
                    message.Event.EventType);

                if (message.Event.EventType == EventType.ExecutionStarted)
                {
                    if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent != null)
                    {
                        // this was caused due to a dupe execution started event, swallow this one
                        this.logHelper.DroppingOrchestrationMessage(workItem, message, "Duplicate start event");
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
                else if (!string.IsNullOrWhiteSpace(orchestrationInstance?.ExecutionId)
                         &&
                         !string.Equals(orchestrationInstance!.ExecutionId,
                             workItem.OrchestrationRuntimeState.OrchestrationInstance?.ExecutionId))
                {
                    // eat up any events for previous executions
                    this.logHelper.DroppingOrchestrationMessage(
                        workItem,
                        message,
                        $"ExecutionId of event ({orchestrationInstance.ExecutionId}) does not match current executionId");
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

        TaskMessage? ProcessWorkflowCompletedTaskDecision(
            OrchestrationCompleteOrchestratorAction completeOrchestratorAction,
            OrchestrationRuntimeState runtimeState,
            bool includeDetails,
            out bool continuedAsNew)
        {
            ExecutionCompletedEvent executionCompletedEvent;
            continuedAsNew = (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew) && (runtimeState.OrchestrationStatus != OrchestrationStatus.Terminated);
            if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
            {
                executionCompletedEvent = new ContinueAsNewEvent(completeOrchestratorAction.Id,
                    completeOrchestratorAction.Result);
            }
            else
            {
                executionCompletedEvent = new ExecutionCompletedEvent(completeOrchestratorAction.Id,
                    completeOrchestratorAction.Result,
                    completeOrchestratorAction.OrchestrationStatus,
                    completeOrchestratorAction.FailureDetails);
            }

            if (runtimeState.OrchestrationStatus != OrchestrationStatus.Terminated)
            {
                runtimeState.AddEvent(executionCompletedEvent);
            }

            this.logHelper.OrchestrationCompleted(runtimeState, completeOrchestratorAction);
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
                () => Utils.EscapeJson(JsonDataConverter.Default.Serialize(runtimeState.GetOrchestrationRuntimeStateDump(), true)));

            // Check to see if we need to start a new execution
            if (completeOrchestratorAction.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
            {
                var taskMessage = new TaskMessage();
                var startedEvent = new ExecutionStartedEvent(-1, completeOrchestratorAction.Result)
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = runtimeState.OrchestrationInstance!.InstanceId,
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
                    subOrchestrationFailedEvent.FailureDetails = completeOrchestratorAction.FailureDetails;

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

        TaskMessage ProcessScheduleTaskDecision(
            ScheduleTaskOrchestratorAction scheduleTaskOrchestratorAction,
            OrchestrationRuntimeState runtimeState,
            bool includeParameters)
        {
            if (scheduleTaskOrchestratorAction.Name == null)
            {
                throw new ArgumentException("No name was given for the task activity to schedule!", nameof(scheduleTaskOrchestratorAction));
            }

            var taskMessage = new TaskMessage();

            var scheduledEvent = new TaskScheduledEvent(
                eventId: scheduleTaskOrchestratorAction.Id,
                name: scheduleTaskOrchestratorAction.Name,
                version: scheduleTaskOrchestratorAction.Version,
                input: scheduleTaskOrchestratorAction.Input);

            taskMessage.Event = scheduledEvent;
            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;
            taskMessage.OrchestrationExecutionContext = GetOrchestrationExecutionContext(runtimeState);

            if (!includeParameters)
            {
                scheduledEvent = new TaskScheduledEvent(
                    eventId: scheduleTaskOrchestratorAction.Id,
                    name: scheduleTaskOrchestratorAction.Name,
                    version: scheduleTaskOrchestratorAction.Version);
            }

            this.logHelper.SchedulingActivity(
                runtimeState.OrchestrationInstance!,
                scheduledEvent);

            runtimeState.AddEvent(scheduledEvent);
            return taskMessage;
        }

        TaskMessage ProcessCreateTimerDecision(
            CreateTimerOrchestratorAction createTimerOrchestratorAction,
            OrchestrationRuntimeState runtimeState,
            bool isInternal)
        {
            var taskMessage = new TaskMessage();

            var timerCreatedEvent = new TimerCreatedEvent(createTimerOrchestratorAction.Id)
            {
                FireAt = createTimerOrchestratorAction.FireAt
            };

            runtimeState.AddEvent(timerCreatedEvent);

            taskMessage.Event = new TimerFiredEvent(-1)
            {
                TimerId = createTimerOrchestratorAction.Id,
                FireAt = createTimerOrchestratorAction.FireAt
            };

            this.logHelper.CreatingTimer(
                runtimeState.OrchestrationInstance!,
                timerCreatedEvent,
                isInternal);

            taskMessage.OrchestrationInstance = runtimeState.OrchestrationInstance;

            return taskMessage;
        }

        TaskMessage ProcessCreateSubOrchestrationInstanceDecision(
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

            this.logHelper.SchedulingOrchestration(startedEvent);

            taskMessage.OrchestrationInstance = startedEvent.OrchestrationInstance;
            taskMessage.Event = startedEvent;
            taskMessage.OrchestrationExecutionContext = GetOrchestrationExecutionContext(runtimeState);

            return taskMessage;
        }

        TaskMessage ProcessSendEventDecision(
            SendEventOrchestratorAction sendEventAction,
            OrchestrationRuntimeState runtimeState)
        {
            var historyEvent = new EventSentEvent(sendEventAction.Id)
            {
                 InstanceId = sendEventAction.Instance?.InstanceId,
                 Name = sendEventAction.EventName,
                 Input = sendEventAction.EventData
            };
            
            runtimeState.AddEvent(historyEvent);

            this.logHelper.RaisingEvent(runtimeState.OrchestrationInstance!, historyEvent);

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
                this.Capacity = available;
            }

            public int Capacity { get; }

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
