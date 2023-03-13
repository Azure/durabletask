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
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Dispatcher for orchestrations and entities to handle processing and renewing, completion of orchestration events.
    /// </summary>
    public class TaskOrchestrationDispatcher
    {
        readonly INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager;
        readonly INameVersionObjectManager<TaskEntity> entityObjectManager;
        readonly IOrchestrationService orchestrationService;
        readonly WorkItemDispatcher<TaskOrchestrationWorkItem> dispatcher;
        readonly DispatchMiddlewarePipeline orchestrationDispatchPipeline;
        readonly DispatchMiddlewarePipeline entityDispatchPipeline;
        readonly EntityBackendInformation? entityBackendInformation;
        readonly LogHelper logHelper;
        ErrorPropagationMode errorPropagationMode;
        readonly NonBlockingCountdownLock concurrentSessionLock;

        internal TaskOrchestrationDispatcher(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager,
            INameVersionObjectManager<TaskEntity> entityObjectManager,
            DispatchMiddlewarePipeline orchestrationDispatchPipeline,
            DispatchMiddlewarePipeline entityDispatchPipeline,
            LogHelper logHelper,
            ErrorPropagationMode errorPropagationMode)
        {
            this.orchestrationObjectManager = orchestrationObjectManager ?? throw new ArgumentNullException(nameof(orchestrationObjectManager));
            this.entityObjectManager = entityObjectManager ?? throw new ArgumentNullException(nameof(entityObjectManager));
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.orchestrationDispatchPipeline = orchestrationDispatchPipeline ?? throw new ArgumentNullException(nameof(orchestrationDispatchPipeline));
            this.entityDispatchPipeline = entityDispatchPipeline ?? throw new ArgumentNullException(nameof(entityDispatchPipeline));
            this.logHelper = logHelper ?? throw new ArgumentNullException(nameof(logHelper));
            this.errorPropagationMode = errorPropagationMode;
            
            if (EntityBackendInformation.BackendSupportsEntities(orchestrationService, out var options))
            {
                this.entityBackendInformation = options;
            }

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
        /// The entity options configured, or null if the backend does not support entities.
        /// </summary>
        public EntityBackendInformation? EntityBackendInformation => this.entityBackendInformation;

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
            // correlation
            CorrelationTraceClient.Propagate(() => CorrelationTraceContext.Current = workItem.TraceContext);

            OrchestrationRuntimeState originalOrchestrationRuntimeState = workItem.OrchestrationRuntimeState;

            bool isEntity = Common.Entities.IsEntityInstance(workItem.InstanceId);

            WorkItemProcessor specializedDispatcher = isEntity 
                ? new EntityWorkItemProcessor(
                    this, 
                    workItem, 
                    this.logHelper, 
                    this.entityObjectManager, 
                    this.entityDispatchPipeline, 
                    this.entityBackendInformation,
                    this.errorPropagationMode) 
                : new OrchestrationWorkItemProcessor(
                    this, 
                    workItem, 
                    this.orchestrationService, 
                    this.errorPropagationMode, 
                    this.logHelper, 
                    this.orchestrationObjectManager, 
                    this.orchestrationDispatchPipeline);

            specializedDispatcher.workItem = workItem;
            specializedDispatcher.runtimeState = workItem.OrchestrationRuntimeState;
            specializedDispatcher.runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

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
                        specializedDispatcher.runtimeState.OrchestrationInstance?.InstanceId,
                        "Received work-item for an invalid orchestration");

                    specializedDispatcher.isCompleted = true;
                }
                else
                {
                    // now, do the actual processing of the work item
                    await specializedDispatcher.ProcessWorkItemAsync();
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

            await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                workItem,
                specializedDispatcher.runtimeState,
                specializedDispatcher.continuedAsNew ? null : specializedDispatcher.messagesToSend,
                specializedDispatcher.orchestratorMessages,
                specializedDispatcher.continuedAsNew ? null : specializedDispatcher.timerMessages,
                specializedDispatcher.continuedAsNewMessage,
                specializedDispatcher.instanceState);
            
            if (workItem.RestoreOriginalRuntimeStateDuringCompletion)
            {
                workItem.OrchestrationRuntimeState = specializedDispatcher.runtimeState;
            }

            return specializedDispatcher.isCompleted || specializedDispatcher.continuedAsNew || specializedDispatcher.isInterrupted;
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

        /// <summary>
        /// Base class for the specialized work item processing for entity and orchestration work items, respectively
        /// </summary>
        internal abstract class WorkItemProcessor
        {
            public TaskOrchestrationDispatcher dispatcher;
            public TaskOrchestrationWorkItem workItem;
            public OrchestrationRuntimeState runtimeState;

            public List<TaskMessage> messagesToSend = new List<TaskMessage>();
            public List<TaskMessage> timerMessages = new List<TaskMessage>();
            public List<TaskMessage> orchestratorMessages = new List<TaskMessage>();
            public bool isCompleted;
            public bool continuedAsNew;
            public bool isInterrupted;
            public OrchestrationState? instanceState;
            public TaskMessage? continuedAsNewMessage;

            public WorkItemProcessor(TaskOrchestrationDispatcher dispatcher, TaskOrchestrationWorkItem workItem)
            {
                this.dispatcher = dispatcher;
                this.workItem = workItem;
                this.runtimeState = workItem.OrchestrationRuntimeState;
            }

            public abstract Task ProcessWorkItemAsync();
        }
    }
}