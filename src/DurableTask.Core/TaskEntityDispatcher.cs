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
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.EventFormat;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Tracing;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Dispatcher for orchestrations and entities to handle processing and renewing, completion of orchestration events.
    /// </summary>
    public class TaskEntityDispatcher
    {
        readonly INameVersionObjectManager<TaskEntity> objectManager;
        readonly IOrchestrationService orchestrationService;
        readonly IEntityOrchestrationService entityOrchestrationService;
        readonly WorkItemDispatcher<TaskOrchestrationWorkItem> dispatcher;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        readonly EntityBackendProperties entityBackendProperties;
        readonly LogHelper logHelper;
        readonly ErrorPropagationMode errorPropagationMode;
        readonly TaskOrchestrationDispatcher.NonBlockingCountdownLock concurrentSessionLock;

        internal TaskEntityDispatcher(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskEntity> entityObjectManager,
            DispatchMiddlewarePipeline entityDispatchPipeline,
            LogHelper logHelper,
            ErrorPropagationMode errorPropagationMode)
        {
            this.objectManager = entityObjectManager ?? throw new ArgumentNullException(nameof(entityObjectManager));
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.dispatchPipeline = entityDispatchPipeline ?? throw new ArgumentNullException(nameof(entityDispatchPipeline));
            this.logHelper = logHelper ?? throw new ArgumentNullException(nameof(logHelper));
            this.errorPropagationMode = errorPropagationMode;
            this.entityOrchestrationService = (orchestrationService as IEntityOrchestrationService)!;
            this.entityBackendProperties = entityOrchestrationService.EntityBackendProperties;
           
            this.dispatcher = new WorkItemDispatcher<TaskOrchestrationWorkItem>(
                "TaskEntityDispatcher",
                item => item == null ? string.Empty : item.InstanceId,
                this.OnFetchWorkItemAsync,
                this.OnProcessWorkItemSessionAsync)
            {
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                SafeReleaseWorkItem = orchestrationService.ReleaseTaskOrchestrationWorkItemAsync,
                AbortWorkItem = orchestrationService.AbandonTaskOrchestrationWorkItemAsync,
                DispatcherCount = orchestrationService.TaskOrchestrationDispatcherCount,
                MaxConcurrentWorkItems = this.entityBackendProperties.MaxConcurrentTaskEntityWorkItems,
                LogHelper = logHelper,
            };

            // To avoid starvation, we only allow half of all concurrently executing entities to
            // leverage extended sessions.
            var maxConcurrentSessions = (int)Math.Ceiling(this.dispatcher.MaxConcurrentWorkItems / 2.0);
            this.concurrentSessionLock = new TaskOrchestrationDispatcher.NonBlockingCountdownLock(maxConcurrentSessions);
        }

        /// <summary>
        /// The entity options configured, or null if the backend does not support entities.
        /// </summary>
        public EntityBackendProperties EntityBackendProperties => this.entityBackendProperties;

        /// <summary>
        /// Starts the dispatcher to start getting and processing entity message batches
        /// </summary>
        public async Task StartAsync()
        {
            await this.dispatcher.StartAsync();
        }

        /// <summary>
        /// Stops the dispatcher to stop getting and processing entity message batches
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully or immediately</param>
        public async Task StopAsync(bool forced)
        {
            await this.dispatcher.StopAsync(forced);
        }

        /// <summary>
        /// Method to get the next work item to process within supplied timeout
        /// </summary>
        /// <param name="receiveTimeout">The max timeout to wait</param>
        /// <param name="cancellationToken">A cancellation token used to cancel a fetch operation.</param>
        /// <returns>A new TaskOrchestrationWorkItem</returns>
        protected Task<TaskOrchestrationWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.entityOrchestrationService.LockNextEntityWorkItemAsync(receiveTimeout, cancellationToken);
        }

        async Task OnProcessWorkItemSessionAsync(TaskOrchestrationWorkItem workItem)
        {
            try
            {
                if (workItem.Session == null)
                {
                    // Legacy behavior
                    await this.OnProcessWorkItemAsync(workItem);
                    return;
                }

                var isExtendedSession = false;

                var processCount = 0;
                try
                {
                    while (true)
                    {
                        // While the work item contains messages that need to be processed, execute them.
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
                                break;
                            }
                        }

                        Stopwatch timer = Stopwatch.StartNew();

                        // Wait for new messages to arrive for the session. This call is expected to block (asynchronously)
                        // until either new messages are available or until a provider-specific timeout has expired.
                        workItem.NewMessages = await workItem.Session.FetchNewOrchestrationMessagesAsync(workItem);
                        if (workItem.NewMessages == null)
                        {
                            break;
                        }

                        workItem.OrchestrationRuntimeState.NewEvents.Clear();
                    }
                }
                finally
                {
                    if (isExtendedSession)
                    {
                        this.concurrentSessionLock.Release();
                    }
                }
            }
            catch (SessionAbortedException e)
            {
                // Either the orchestration or the orchestration service explicitly abandoned the session.
                OrchestrationInstance instance = workItem.OrchestrationRuntimeState?.OrchestrationInstance ?? new OrchestrationInstance { InstanceId = workItem.InstanceId };
                this.logHelper.OrchestrationAborted(instance, e.Message);
                await this.orchestrationService.AbandonTaskOrchestrationWorkItemAsync(workItem);
            }
        }

        internal class WorkItemEffects
        {
            public List<TaskMessage> ActivityMessages;
            public List<TaskMessage> TimerMessages;
            public List<TaskMessage> InstanceMessages;
            public int taskIdCounter;
            public string InstanceId;
            public OrchestrationRuntimeState RuntimeState;
        }

        /// <summary>
        /// Method to process a new work item
        /// </summary>
        /// <param name="workItem">The work item to process</param>
        protected async Task<bool> OnProcessWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationRuntimeState originalOrchestrationRuntimeState = workItem.OrchestrationRuntimeState;

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;
            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

            Task renewTask = null;
            using var renewCancellationTokenSource = new CancellationTokenSource();
            if (workItem.LockedUntilUtc < DateTime.MaxValue)
            {
                // start a task to run RenewUntil
                renewTask = Task.Factory.StartNew(
                    () => TaskOrchestrationDispatcher.RenewUntil(workItem, this.orchestrationService, this.logHelper, nameof(TaskEntityDispatcher), renewCancellationTokenSource.Token),
                    renewCancellationTokenSource.Token);
            }

            WorkItemEffects effects = new WorkItemEffects()
            {
                ActivityMessages = new List<TaskMessage>(),
                TimerMessages = new List<TaskMessage>(),
                InstanceMessages = new List<TaskMessage>(),
                taskIdCounter = 0,
                InstanceId = workItem.InstanceId,
                RuntimeState = runtimeState,
            };

            try
            {
                // Assumes that: if the batch contains a new "ExecutionStarted" event, it is the first message in the batch.
                if (!TaskOrchestrationDispatcher.ReconcileMessagesWithState(workItem, nameof(TaskEntityDispatcher), this.errorPropagationMode, this.logHelper))
                {
                    // TODO : mark an orchestration as faulted if there is data corruption
                    this.logHelper.DroppingOrchestrationWorkItem(workItem, "Received work-item for an invalid orchestration");
                }
                else
                {

                    // we start with processing all the requests and figuring out which ones to execute now
                    // results can depend on whether the entity is locked, what the maximum batch size is,
                    // and whether the messages arrived out of order

                    this.DetermineWork(workItem.OrchestrationRuntimeState,
                         out SchedulerState schedulerState,
                         out Work workToDoNow);

                    if (workToDoNow.OperationCount > 0)
                    {
                        // execute the user-defined operations on this entity, via the middleware
                        var result = await this.ExecuteViaMiddlewareAsync(workToDoNow, runtimeState.OrchestrationInstance, schedulerState.EntityState);
                        var operationResults = result.Results!;

                        // if we encountered an error, record it as the result of the operations
                        // so that callers are notified that the operation did not succeed.
                        if (result.FailureDetails != null)
                        {
                            OperationResult errorResult = new OperationResult()
                            {
                                // for older SDKs only
                                Result = result.FailureDetails.ErrorMessage,
                                ErrorMessage = "entity dispatch failed",

                                // for newer SDKs only
                                FailureDetails = result.FailureDetails,
                            };

                            for (int i = operationResults.Count; i < workToDoNow.OperationCount; i++)
                            {
                                operationResults.Add(errorResult);
                            }
                        }

                        // go through all results
                        // for each operation that is not a signal, send a result message back to the calling orchestrator
                        for (int i = 0; i < result.Results!.Count; i++)
                        {
                            var req = workToDoNow.Operations[i];
                            if (!req.IsSignal)
                            {
                                this.SendResultMessage(effects, req, result.Results[i]);
                            }
                        }

                        if (result.Results.Count < workToDoNow.OperationCount)
                        {
                            // some requests were not processed (e.g. due to shutdown or timeout)
                            // in this case we just defer the work so it can be retried
                            var deferred = workToDoNow.RemoveDeferredWork(result.Results.Count);
                            schedulerState.PutBack(deferred);
                            workToDoNow.ToBeContinued(schedulerState);
                        }

                        // update the entity state based on the result
                        schedulerState.EntityState = result.EntityState;

                        // perform the actions
                        foreach (var action in result.Actions!)
                        {
                            switch (action)
                            {
                                case (SendSignalOperationAction sendSignalAction):
                                    this.SendSignalMessage(effects, schedulerState, sendSignalAction);
                                    break;
                                case (StartNewOrchestrationOperationAction startAction):
                                    this.ProcessSendStartMessage(effects, runtimeState, startAction);
                                    break;
                            }
                        }
                    }

                    // process the lock request, if any
                    if (workToDoNow.LockRequest != null)
                    {
                        this.ProcessLockRequest(effects, schedulerState, workToDoNow.LockRequest);
                    }

                    if (workToDoNow.ToBeRescheduled != null)
                    {
                        foreach (var request in workToDoNow.ToBeRescheduled)
                        {
                            // Reschedule all signals that were received before their time
                            this.SendScheduledSelfMessage(effects, request);
                        }
                    }

                    if (workToDoNow.SuspendAndContinue)
                    {
                        this.SendContinueSelfMessage(effects);
                    }

                    // this batch is complete. Since this is an entity, we now
                    // (always) start a new execution, as in continue-as-new

                    var serializedSchedulerState = this.SerializeSchedulerStateForNextExecution(schedulerState);
                    var nextExecutionStartedEvent = new ExecutionStartedEvent(-1, serializedSchedulerState)
                    {
                        OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = workItem.InstanceId,
                            ExecutionId = Guid.NewGuid().ToString("N")
                        },
                        Tags = runtimeState.Tags,
                        ParentInstance = runtimeState.ParentInstance,
                        Name = runtimeState.Name,
                        Version = runtimeState.Version
                    };
                    var entityStatus = new EntityStatus()
                    {
                        EntityExists = schedulerState.EntityExists,
                        BacklogQueueSize = schedulerState.Queue?.Count ?? 0,
                        LockedBy = schedulerState.LockedBy,
                    };
                    var serializedEntityStatus = JsonConvert.SerializeObject(entityStatus, Serializer.InternalSerializerSettings);

                    // create the new runtime state for the next execution
                    runtimeState = new OrchestrationRuntimeState();
                    runtimeState.Status = serializedEntityStatus;
                    runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                    runtimeState.AddEvent(nextExecutionStartedEvent);
                    runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
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

            OrchestrationState instanceState = (runtimeState.ExecutionStartedEvent != null) ?
                instanceState = Utils.BuildOrchestrationState(runtimeState) : null;      

            if (workItem.RestoreOriginalRuntimeStateDuringCompletion)
            {
                // some backends expect the original runtime state object
                workItem.OrchestrationRuntimeState = originalOrchestrationRuntimeState;
            }
            else
            {
                workItem.OrchestrationRuntimeState = runtimeState;
            }

            await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                workItem,
                runtimeState,
                effects.ActivityMessages,
                effects.InstanceMessages,
                effects.TimerMessages,
                null,
                instanceState);

            if (workItem.RestoreOriginalRuntimeStateDuringCompletion)
            {
                workItem.OrchestrationRuntimeState = runtimeState;
            }

            return true;
        }

        void ProcessLockRequest(WorkItemEffects effects, SchedulerState schedulerState, RequestMessage request)
        {
            this.logHelper.EntityLockAcquired(effects.InstanceId, request);

            // mark the entity state as locked
            schedulerState.LockedBy = request.ParentInstanceId;

            request.Position++;

            if (request.Position < request.LockSet.Length)
            {
                // send lock request to next entity in the lock set
                var target = new OrchestrationInstance() { InstanceId = request.LockSet[request.Position].ToString() };
                this.SendLockRequestMessage(effects, schedulerState, target, request);
            }
            else
            {
                // send lock acquisition completed response back to originating orchestration instance
                var target = new OrchestrationInstance() { InstanceId = request.ParentInstanceId, ExecutionId = request.ParentExecutionId };
                this.SendLockResponseMessage(effects, target, request.Id);
            }
        }

        string SerializeSchedulerStateForNextExecution(SchedulerState schedulerState)
        {
            if (this.entityBackendProperties.SupportsImplicitEntityDeletion && schedulerState.IsEmpty && !schedulerState.Suspended)
            {
                // this entity scheduler is idle and the entity is deleted, so the instance and history can be removed from storage
                // we convey this to the durability provider by issuing a continue-as-new with null input
                return null;
            }
            else
            {
                // we persist the state of the entity scheduler and entity
                return JsonConvert.SerializeObject(schedulerState, typeof(SchedulerState), Serializer.InternalSerializerSettings);
            }
        }

        #region Preprocess to determine work

        void DetermineWork(OrchestrationRuntimeState runtimeState, out SchedulerState schedulerState, out Work batch)
        {
            string instanceId = runtimeState.OrchestrationInstance.InstanceId;
            schedulerState = new SchedulerState();
            batch = new Work();

            Queue<RequestMessage> lockHolderMessages = null;

            foreach (HistoryEvent e in runtimeState.Events)
            {
                switch (e.EventType)
                {
                    case EventType.ExecutionStarted:


                        if (runtimeState.Input != null)
                        {
                            try
                            {
                                // restore the scheduler state from the input
                                JsonConvert.PopulateObject(runtimeState.Input, schedulerState, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize entity scheduler state - may be corrupted or wrong version.", exception);
                            }
                        }
                        break;

                    case EventType.EventRaised:
                        EventRaisedEvent eventRaisedEvent = (EventRaisedEvent)e;

                        if (EntityMessageEventNames.IsRequestMessage(eventRaisedEvent.Name))
                        {
                            // we are receiving an operation request or a lock request
                            var requestMessage = new RequestMessage();

                            try
                            {
                                JsonConvert.PopulateObject(eventRaisedEvent.Input, requestMessage, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize incoming request message - may be corrupted or wrong version.", exception);
                            }

                            IEnumerable<RequestMessage> deliverNow;

                            if (requestMessage.ScheduledTime.HasValue)
                            {
                                if ((requestMessage.ScheduledTime.Value - DateTime.UtcNow) > TimeSpan.FromMilliseconds(100))
                                {
                                    // message was delivered too early. This can happen e.g. if the orchestration service has limits on the delay times for messages.
                                    // We handle this by rescheduling the message instead of processing it.
                                    deliverNow = Array.Empty<RequestMessage>();
                                    batch.AddMessageToBeRescheduled(requestMessage);

                                    // We do not want to create the Activity for the request yet since it will be redelivered again later. In the case that the parent trace context was attached
                                    // to the EventRaisedEvent and not the RequestMessage, we want to attach it to the RequestMessage such that when it is redelivered the parent trace context can be used
                                    // to create the Activity for the request then.
                                    if (requestMessage.ParentTraceContext == null && eventRaisedEvent.ParentTraceContext != null)
                                    {
                                        requestMessage.ParentTraceContext = eventRaisedEvent.ParentTraceContext;
                                    }
                                }
                                else
                                {
                                    // the message is scheduled to be delivered immediately.
                                    // There are no FIFO guarantees for scheduled messages, so we skip the message sorter.
                                    deliverNow = new RequestMessage[] { requestMessage };
                                    StartTraceActivityForSignalingEntity(requestMessage, eventRaisedEvent, instanceId);
                                }
                            }
                            else
                            {
                                // run this through the message sorter to help with reordering and duplicate filtering
                                deliverNow = schedulerState.MessageSorter.ReceiveInOrder(requestMessage, this.entityBackendProperties.EntityMessageReorderWindow);
                                StartTraceActivityForSignalingEntity(requestMessage, eventRaisedEvent, instanceId);
                            }

                            foreach (var message in deliverNow)
                            {
                                if (schedulerState.LockedBy != null && schedulerState.LockedBy == message.ParentInstanceId)
                                {
                                    if (lockHolderMessages == null)
                                    {
                                        lockHolderMessages = new Queue<RequestMessage>();
                                    }

                                    lockHolderMessages.Enqueue(message);
                                }
                                else
                                {
                                    schedulerState.Enqueue(message);
                                }
                            }
                        }
                        else if (EntityMessageEventNames.IsReleaseMessage(eventRaisedEvent.Name))
                        {
                            // we are receiving a lock release
                            var message = new ReleaseMessage();
                            try
                            {
                                // restore the scheduler state from the input
                                JsonConvert.PopulateObject(eventRaisedEvent.Input, message, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize lock release message - may be corrupted or wrong version.", exception);
                            }

                            if (schedulerState.LockedBy == message.ParentInstanceId)
                            {
                                this.logHelper.EntityLockReleased(instanceId, message);
                                schedulerState.LockedBy = null;
                            }
                        }
                        else
                        {
                            // this is a continue message.
                            // Resumes processing of previously queued operations, if any.
                            schedulerState.Suspended = false;
                        }

                        break;

                }
            }

            // lock holder messages go to the front of the queue
            if (lockHolderMessages != null)
            {
                schedulerState.PutBack(lockHolderMessages);
            }

            if (!schedulerState.Suspended)
            {
                // 2. We add as many requests from the queue to the batch as possible,
                // stopping at lock requests or when the maximum batch size is reached
                while (schedulerState.MayDequeue())
                {
                    if (batch.OperationCount == this.entityBackendProperties.MaxEntityOperationBatchSize)
                    {
                        // we have reached the maximum batch size already
                        // insert a delay after this batch to ensure write back
                        batch.ToBeContinued(schedulerState);
                        break;
                    }

                    var request = schedulerState.Dequeue();

                    if (request.IsLockRequest)
                    {
                        batch.AddLockRequest(request);
                        break;
                    }
                    else
                    {
                        batch.AddOperation(request);
                    }
                }
            }
        }

        class Work
        {
            List<RequestMessage> operationBatch; // a (possibly empty) sequence of operations to be executed on the entity
            RequestMessage lockRequest = null; // zero or one lock request to be executed after all the operations
            List<RequestMessage> toBeRescheduled; // a (possibly empty) list of timed messages that were delivered too early and should be rescheduled
            bool suspendAndContinue; // a flag telling as to send ourselves a continue signal

            public int OperationCount => this.operationBatch?.Count ?? 0;
            public IReadOnlyList<RequestMessage> Operations => this.operationBatch;
            public IReadOnlyList<RequestMessage> ToBeRescheduled => this.toBeRescheduled;
            public RequestMessage LockRequest => this.lockRequest;
            public bool SuspendAndContinue => this.suspendAndContinue;

            public void AddOperation(RequestMessage operationMessage)
            {
                if (this.operationBatch == null)
                {
                    this.operationBatch = new List<RequestMessage>();
                }
                this.operationBatch.Add(operationMessage);
            }

            public void AddLockRequest(RequestMessage lockRequest)
            {
                Debug.Assert(this.lockRequest == null);
                this.lockRequest = lockRequest;
            }

            public void AddMessageToBeRescheduled(RequestMessage requestMessage)
            {
                if (this.toBeRescheduled == null)
                {
                    this.toBeRescheduled = new List<RequestMessage>();
                }
                this.toBeRescheduled.Add(requestMessage);
            }

            public void ToBeContinued(SchedulerState schedulerState)
            {
                if (!schedulerState.Suspended)
                {
                    this.suspendAndContinue = true;
                }
            }

            public (List<OperationRequest>, List<Activity>) GetOperationRequestsAndTraceActivities(string instanceId)
            {
                var operations = new List<OperationRequest>(this.operationBatch.Count);
                var traceActivities = new List<Activity>(this.operationBatch.Count);
                for (int i = 0; i < this.operationBatch.Count; i++)
                {
                    var request = this.operationBatch[i];

                    Activity traceActivity = null;
                    // We only want to create a trace activity for processing the entity invocation in the case that we can successfully parse the trace context of the request that led to this entity invocation.
                    // Otherwise, we will create an unlinked trace activity with no parent 
                    if (ActivityContext.TryParse(request.ParentTraceContext?.TraceParent, request.ParentTraceContext?.TraceState, out ActivityContext parentTraceContext))
                    {
                        if (!request.IsSignal)
                        {
                            var clientSpanId = ActivitySpanId.CreateRandom();

                            // In that case that we are processing a call request as a server, we want to generate a new span ID that will also be used by the Activity we create at the end corresponding to the client call request
                            // That way, this server Activity corresponding to processing the call request will be correctly linked as the child of the Activity for the client call request.
                            parentTraceContext = new ActivityContext(parentTraceContext.TraceId, clientSpanId, parentTraceContext.TraceFlags, parentTraceContext.TraceState);
                            request.ClientSpanId = clientSpanId.ToString();
                        }
                        traceActivity = TraceHelper.StartActivityForProcessingEntityInvocation(
                            instanceId,
                            EntityId.FromString(instanceId).Name,
                            request.Operation,
                            request.IsSignal,
                            parentTraceContext);
                    }

                    // We still want to add the trace activity to the list even if it was not successfully created and is null. This is because otherwise we have no easy way of mapping OperationResults to Activities otherwise if the lists
                    // do not have the same length in TraceHelper.EndActivitiesForProcessingEntityInvocation. We will simply skip ending the Activity if it is null in this method
                    traceActivities.Add(traceActivity);

                    // The trace context of the operation request will be the Activity just created - this can become the parent of future operations started by the entity once it processes the OperationRequest
                    operations.Add(new OperationRequest()
                    {
                        Operation = request.Operation,
                        Id = request.Id,
                        Input = request.Input,
                        TraceContext = traceActivity != null ? new DistributedTraceContext(traceActivity.Id, traceActivity.TraceStateString) : null,
                    });
                }
                return (operations, traceActivities);
            }

            public Queue<RequestMessage> RemoveDeferredWork(int index)
            {
                var deferred = new Queue<RequestMessage>();
                for (int i = index; i < this.operationBatch.Count; i++)
                {
                    deferred.Enqueue(this.operationBatch[i]);
                }
                this.operationBatch.RemoveRange(index, this.operationBatch.Count - index);
                if (this.lockRequest != null)
                {
                    deferred.Enqueue(this.lockRequest);
                    this.lockRequest = null;
                }
                return deferred;
            }
        }

        #endregion

        #region Send Messages

        void SendResultMessage(WorkItemEffects effects, RequestMessage request, OperationResult result)
        {
            // We only want to create a trace activity for calling an entity in the case that we can successfully get the parent trace context of the request.
            // Otherwise, we will create an unlinked trace activity with no parent.
            // Note that we create the Activity once the result has been sent to capture the full length of calling the entity and receiving its response.
            if (ActivityContext.TryParse(request.ParentTraceContext?.TraceParent, request.ParentTraceContext?.TraceState, out ActivityContext parentTraceContext))
            {
                using var traceActivity = TraceHelper.StartActivityForCallingOrSignalingEntity(
                    effects.InstanceId,
                    EntityId.FromString(effects.InstanceId).Name,
                    request.Operation,
                    request.IsSignal,
                    request.ScheduledTime,
                    parentTraceContext,
                    request.RequestTime);
                traceActivity?.SetSpanId(request.ClientSpanId);
            }

            var destination = new OrchestrationInstance()
            {
                InstanceId = request.ParentInstanceId,
                ExecutionId = request.ParentExecutionId,
            };
            var responseMessage = new ResponseMessage()
            {
                Result = result.Result,
                ErrorMessage = result.ErrorMessage,
                FailureDetails = result.FailureDetails,
            };
            this.ProcessSendEventMessage(effects, destination, EntityMessageEventNames.ResponseMessageEventName(request.Id), responseMessage);
        }

        void SendSignalMessage(WorkItemEffects effects, SchedulerState schedulerState, SendSignalOperationAction action)
        {
            OrchestrationInstance destination = new OrchestrationInstance()
            {
                InstanceId = action.InstanceId
            };
            RequestMessage message = new RequestMessage()
            {
                ParentInstanceId = effects.InstanceId,
                ParentExecutionId = null, // for entities, message sorter persists across executions
                Id = Guid.NewGuid(),
                IsSignal = true,
                Operation = action.Name,
                Input = action.Input,
                ScheduledTime = action.ScheduledTime,
            };
            string eventName;
            if (action.ScheduledTime.HasValue)
            {
                DateTime original = action.ScheduledTime.Value;
                DateTime capped = this.entityBackendProperties.GetCappedScheduledTime(DateTime.UtcNow, original);
                eventName = EntityMessageEventNames.ScheduledRequestMessageEventName(capped);
            }
            else
            {
                eventName = EntityMessageEventNames.RequestMessageEventName;
                schedulerState.MessageSorter.LabelOutgoingMessage(message, action.InstanceId, DateTime.UtcNow, this.entityBackendProperties.EntityMessageReorderWindow);
            }

            // We only want to create a trace activity for signaling the entity in the case that we can successfully parse the parent trace context of the signal request.
            // Otherwise, we will create an unlinked trace activity with no parent 
            if (ActivityContext.TryParse(action.ParentTraceContext?.TraceParent, action.ParentTraceContext?.TraceState, out ActivityContext parentTraceContext))
            {
                using var traceActivity = TraceHelper.StartActivityForCallingOrSignalingEntity(
                    destination.InstanceId,
                    EntityId.FromString(destination.InstanceId).Name,
                    action.Name,
                    signalEntity: true,
                    action.ScheduledTime,
                    parentTraceContext,
                    action.RequestTime,
                    entityId: effects.InstanceId);
                if (traceActivity != null)
                {
                    message.ParentTraceContext = new DistributedTraceContext(traceActivity.Id, traceActivity.TraceStateString);
                }
            }
            this.ProcessSendEventMessage(effects, destination, eventName, message);
        }

        void SendLockRequestMessage(WorkItemEffects effects, SchedulerState schedulerState, OrchestrationInstance target, RequestMessage message)
        {
            schedulerState.MessageSorter.LabelOutgoingMessage(message, target.InstanceId, DateTime.UtcNow, this.entityBackendProperties.EntityMessageReorderWindow);
            this.ProcessSendEventMessage(effects, target, EntityMessageEventNames.RequestMessageEventName, message);
        }

        void SendLockResponseMessage(WorkItemEffects effects, OrchestrationInstance target, Guid requestId)
        {
            var message = new ResponseMessage()
            {
                // content is ignored by receiver but helps with tracing
                Result = ResponseMessage.LockAcquisitionCompletion, 
            };
            this.ProcessSendEventMessage(effects, target, EntityMessageEventNames.ResponseMessageEventName(requestId), message);
        }

        void SendScheduledSelfMessage(WorkItemEffects effects, RequestMessage request)
        {
            var self = new OrchestrationInstance()
            {
                InstanceId = effects.InstanceId,
            };
            this.ProcessSendEventMessage(effects, self, EntityMessageEventNames.ScheduledRequestMessageEventName(request.ScheduledTime.Value), request);
        }

        void SendContinueSelfMessage(WorkItemEffects effects)
        {
            var self = new OrchestrationInstance()
            {
                InstanceId = effects.InstanceId,
            };
            this.ProcessSendEventMessage(effects, self, EntityMessageEventNames.ContinueMessageEventName, null);
        }

        void ProcessSendEventMessage(WorkItemEffects effects, OrchestrationInstance destination, string eventName, object eventContent)
        {
            string serializedContent = null;
            if (eventContent != null)
            {
                serializedContent = JsonConvert.SerializeObject(eventContent, Serializer.InternalSerializerSettings);
            }

            var eventSentEvent = new EventSentEvent(effects.taskIdCounter++)
            {
                InstanceId = destination.InstanceId,
                Name = eventName,
                Input = serializedContent,
            };
            this.logHelper.RaisingEvent(effects.RuntimeState.OrchestrationInstance, eventSentEvent);

            effects.InstanceMessages.Add(new TaskMessage
            {
                OrchestrationInstance = destination,
                Event = new EventRaisedEvent(-1, serializedContent)
                {
                    Name = eventName,
                    Input = serializedContent,
                },
            });
        }

        internal void ProcessSendStartMessage(WorkItemEffects effects, OrchestrationRuntimeState runtimeState, StartNewOrchestrationOperationAction action)
        {
            OrchestrationInstance destination = new OrchestrationInstance()
            {
                InstanceId = action.InstanceId,
                ExecutionId = Guid.NewGuid().ToString("N"),
            };
            var executionStartedEvent = new ExecutionStartedEvent(-1, action.Input)
            {
                Tags = OrchestrationTags.MergeTags(
                    newTags: new Dictionary<string, string>() { { OrchestrationTags.FireAndForget, "" } },
                    existingTags: runtimeState.Tags),
                OrchestrationInstance = destination,
                ScheduledStartTime = action.ScheduledStartTime,
                ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = runtimeState.OrchestrationInstance,
                    Name = runtimeState.Name,
                    Version = runtimeState.Version,
                    TaskScheduleId = effects.taskIdCounter++,
                },
                Name = action.Name,
                Version = action.Version,
            };

            // We only want to create a trace activity for an entity starting an orchestration in the case that we can successfully parse the parent trace context of the start orchestration request.
            // Otherwise, we will create an unlinked trace activity with no parent
            if (ActivityContext.TryParse(action.ParentTraceContext?.TraceParent, action.ParentTraceContext?.TraceState, out ActivityContext parentTraceContext))
            {
                using var traceActivity = TraceHelper.StartActivityForEntityStartingAnOrchestration(
                    runtimeState.OrchestrationInstance.InstanceId,
                    EntityId.FromString(runtimeState.OrchestrationInstance.InstanceId).Name,
                    destination.InstanceId,
                    parentTraceContext,
                    action.RequestTime,
                    scheduledTime: action.ScheduledStartTime);
                if (traceActivity != null)
                {
                    executionStartedEvent.ParentTraceContext = new DistributedTraceContext(traceActivity.Id, traceActivity.TraceStateString);
                }
            }
            this.logHelper.SchedulingOrchestration(executionStartedEvent);

            effects.InstanceMessages.Add(new TaskMessage
            {
                OrchestrationInstance = destination,
                Event = executionStartedEvent,
            });
        }

        #endregion

        async Task<EntityBatchResult> ExecuteViaMiddlewareAsync(Work workToDoNow, OrchestrationInstance instance, string serializedEntityState)
        {
            var (operations, traceActivities) = workToDoNow.GetOperationRequestsAndTraceActivities(instance.InstanceId);
            // the request object that will be passed to the worker
            var request = new EntityBatchRequest()
            {
                InstanceId = instance.InstanceId,
                EntityState = serializedEntityState,
                Operations = operations,
            };

            this.logHelper.EntityBatchExecuting(request);

            var entityId = EntityId.FromString(instance.InstanceId);
            string entityName = entityId.Name;

            // Get the TaskEntity implementation. If it's not found, it either means that the developer never
            // registered it (which is an error, and we'll throw for this further down) or it could be that some custom
            // middleware (e.g. out-of-process execution middleware) is intended to implement the entity logic.
            TaskEntity taskEntity = this.objectManager.GetObject(entityName, version: null);

            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(request);

            await this.dispatchPipeline.RunAsync(dispatchContext, async _ =>
            {
                // Check to see if the custom middleware intercepted and substituted the orchestration execution
                // with its own execution behavior, providing us with the end results. If so, we can terminate
                // the dispatch pipeline here.
                var resultFromMiddleware = dispatchContext.GetProperty<EntityBatchResult>();
                if (resultFromMiddleware != null)
                {
                    return;
                }

                if (taskEntity == null)
                {
                    throw TraceHelper.TraceExceptionInstance(
                        TraceEventType.Error,
                        "TaskOrchestrationDispatcher-EntityTypeMissing",
                        instance,
                        new TypeMissingException($"Entity not found: {entityName}"));
                }

                var result = await taskEntity.ExecuteOperationBatchAsync(request);
                
                dispatchContext.SetProperty(result);
            });

            var result = dispatchContext.GetProperty<EntityBatchResult>();
            TraceHelper.EndActivitiesForProcessingEntityInvocation(traceActivities, result.Results, result.FailureDetails);

            this.logHelper.EntityBatchExecuted(request, result);

            return result;
        }

        private static void StartTraceActivityForSignalingEntity(RequestMessage requestMessage, EventRaisedEvent eventRaisedEvent, string instanceId)
        {
            if (requestMessage.CreateTrace)
            {
                // In the case that we are calling an entity, we want to create the Activity once the result for the call is returned and so we do not create now
                if (requestMessage.IsSignal)
                {
                    var successfullyParsed = false;
                    ActivityContext parentTraceContext;
                    if (requestMessage.ParentTraceContext is { } parentContext)
                    {
                        // If a parent trace context was provided but we fail to successfully parse it, we should not create the Activity even if the EventRaisedEvent has a parent trace context attached. 
                        // Otherwise we will incorrectly link the created Activity to a context that is not truly its parent.
                        if (ActivityContext.TryParse(parentContext.TraceParent, parentContext.TraceState, out parentTraceContext))
                        {
                            successfullyParsed = true;
                        }
                    }
                    else if (eventRaisedEvent.TryGetParentTraceContext(out parentTraceContext))
                    {
                        successfullyParsed = true;
                    }
                    if (successfullyParsed)
                    {
                        using var traceActivity = TraceHelper.StartActivityForCallingOrSignalingEntity(
                            instanceId,
                            EntityId.FromString(instanceId).Name,
                            requestMessage.Operation,
                            requestMessage.IsSignal,
                            requestMessage.ScheduledTime,
                            parentTraceContext,
                            requestMessage.RequestTime);
                        if (traceActivity != null)
                        {
                            requestMessage.ParentTraceContext = new DistributedTraceContext(traceActivity.Id, traceActivity.TraceStateString);
                        }
                    }
                }
                // We still want to attach a parent trace context to the request in the case of a call to an entity so that when we create the Activity for the call this information is available.
                else if (requestMessage.ParentTraceContext == null && eventRaisedEvent.ParentTraceContext != null)
                {
                    requestMessage.ParentTraceContext = eventRaisedEvent.ParentTraceContext;
                }
            }
        }
    }
}
