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
using DurableTask.Core.Command;
using DurableTask.Core.Common;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;
using DurableTask.Core.Logging;
using DurableTask.Core.Middleware;
using DurableTask.Core.Serializing;
using DurableTask.Core.Tracing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.Core
{
    /// <summary>
    /// Handles orchestration work items.
    /// </summary>
    class OrchestrationWorkItemProcessor : TaskOrchestrationDispatcher.WorkItemProcessor
    {
        static readonly Task CompletedTask = Task.FromResult(0);

        readonly IOrchestrationService orchestrationService;
        readonly LogHelper logHelper;
        ErrorPropagationMode errorPropagationMode;
        readonly INameVersionObjectManager<TaskOrchestration> objectManager;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        
        public OrchestrationWorkItemProcessor(
            TaskOrchestrationDispatcher dispatcher,
            TaskOrchestrationWorkItem workItem,
            IOrchestrationService orchestrationService,
            ErrorPropagationMode errorPropagationMode,
            LogHelper logHelper,
            INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager,
            DispatchMiddlewarePipeline dispatchPipeline) : base(dispatcher, workItem)
        {
            this.orchestrationService = orchestrationService;
            this.errorPropagationMode = errorPropagationMode;
            this.logHelper = logHelper;
            this.objectManager = orchestrationObjectManager;
            this.dispatchPipeline = dispatchPipeline;
        }
         
        public override async Task ProcessWorkItemAsync()
        {
            ExecutionStartedEvent? continueAsNewExecutionStarted = null;
            IList<HistoryEvent>? carryOverEvents = null;
            string? carryOverStatus = null;

            do
            {
                this.continuedAsNew = false;
                this.continuedAsNewMessage = null;

                this.logHelper.OrchestrationExecuting(this.runtimeState.OrchestrationInstance!, this.runtimeState.Name);
                TraceHelper.TraceInstance(
                    TraceEventType.Verbose,
                    "TaskOrchestrationDispatcher-ExecuteUserOrchestration-Begin",
                    this.runtimeState.OrchestrationInstance,
                    "Executing user orchestration: {0}",
                    JsonDataConverter.Default.Serialize(this.runtimeState.GetOrchestrationRuntimeStateDump(), true));

                if (this.workItem.Cursor == null)
                {
                    this.workItem.Cursor = await this.ExecuteOrchestrationAsync(this.runtimeState, this.workItem);
                }
                else
                {
                    await this.ResumeOrchestrationAsync(this.workItem);
                }

                IReadOnlyList<OrchestratorAction> decisions = this.workItem.Cursor.LatestDecisions.ToList();

                this.logHelper.OrchestrationExecuted(
                    this.runtimeState.OrchestrationInstance!,
                    this.runtimeState.Name,
                    decisions);
                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "TaskOrchestrationDispatcher-ExecuteUserOrchestration-End",
                    this.runtimeState.OrchestrationInstance,
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
                        this.runtimeState.OrchestrationInstance,
                        "Processing orchestrator action of type {0}",
                        decision.OrchestratorActionType);
                    switch (decision.OrchestratorActionType)
                    {
                        case OrchestratorActionType.ScheduleOrchestrator:
                            var scheduleTaskAction = (ScheduleTaskOrchestratorAction)decision;
                            var message = this.ProcessScheduleTaskDecision(
                                scheduleTaskAction,
                                this.runtimeState,
                                this.dispatcher.IncludeParameters);
                            this.messagesToSend.Add(message);
                            break;
                        case OrchestratorActionType.CreateTimer:
                            var timerOrchestratorAction = (CreateTimerOrchestratorAction)decision;
                            this.timerMessages.Add(this.ProcessCreateTimerDecision(
                                timerOrchestratorAction,
                                this.runtimeState,
                                isInternal: false));
                            break;
                        case OrchestratorActionType.CreateSubOrchestration:
                            var createSubOrchestrationAction = (CreateSubOrchestrationAction)decision;
                            this.orchestratorMessages.Add(
                                this.ProcessCreateSubOrchestrationInstanceDecision(
                                    createSubOrchestrationAction,
                                    this.runtimeState,
                                    this.dispatcher.IncludeParameters));
                            break;
                        case OrchestratorActionType.SendEvent:
                            var sendEventAction = (SendEventOrchestratorAction)decision;
                            this.orchestratorMessages.Add(
                                this.ProcessSendEventDecision(sendEventAction, this.runtimeState));
                            break;
                        case OrchestratorActionType.OrchestrationComplete:
                            OrchestrationCompleteOrchestratorAction completeDecision = (OrchestrationCompleteOrchestratorAction)decision;
                            TaskMessage? workflowInstanceCompletedMessage =
                                this.ProcessWorkflowCompletedTaskDecision(completeDecision, this.runtimeState, this.dispatcher.IncludeDetails, out this.continuedAsNew);
                            if (workflowInstanceCompletedMessage != null)
                            {
                                // Send complete message to parent workflow or to itself to start a new execution
                                // Store the event so we can rebuild the state
                                carryOverEvents = null;
                                if (this.continuedAsNew)
                                {
                                    this.continuedAsNewMessage = workflowInstanceCompletedMessage;
                                    continueAsNewExecutionStarted = workflowInstanceCompletedMessage.Event as ExecutionStartedEvent;
                                    if (completeDecision.CarryoverEvents.Any())
                                    {
                                        carryOverEvents = completeDecision.CarryoverEvents.ToList();
                                        completeDecision.CarryoverEvents.Clear();
                                    }
                                }
                                else
                                {
                                    this.orchestratorMessages.Add(workflowInstanceCompletedMessage);
                                }
                            }

                            this.isCompleted = !this.continuedAsNew;
                            break;
                        default:
                            throw TraceHelper.TraceExceptionInstance(
                                TraceEventType.Error,
                                "TaskOrchestrationDispatcher-UnsupportedDecisionType",
                                this.runtimeState.OrchestrationInstance,
                                new NotSupportedException($"Decision type '{decision.OrchestratorActionType}' not supported"));
                    }

                    // Underlying orchestration service provider may have a limit of messages per call, to avoid the situation
                    // we keep on asking the provider if message count is ok and stop processing new decisions if not.
                    //
                    // We also put in a fake timer to force next orchestration task for remaining messages
                    int totalMessages = this.messagesToSend.Count + this.orchestratorMessages.Count + this.timerMessages.Count;
                    if (this.orchestrationService.IsMaxMessageCountExceeded(totalMessages, this.runtimeState))
                    {
                        TraceHelper.TraceInstance(
                            TraceEventType.Information,
                            "TaskOrchestrationDispatcher-MaxMessageCountReached",
                            this.runtimeState.OrchestrationInstance,
                            "MaxMessageCount reached.  Adding timer to process remaining events in next attempt.");

                        if (this.isCompleted || this.continuedAsNew)
                        {
                            TraceHelper.TraceInstance(
                                TraceEventType.Information,
                                "TaskOrchestrationDispatcher-OrchestrationAlreadyCompleted",
                                this.runtimeState.OrchestrationInstance,
                                "Orchestration already completed.  Skip adding timer for splitting messages.");
                            break;
                        }

                        var dummyTimer = new CreateTimerOrchestratorAction
                        {
                            Id = FrameworkConstants.FakeTimerIdToSplitDecision,
                            FireAt = DateTime.UtcNow
                        };

                        this.timerMessages.Add(this.ProcessCreateTimerDecision(
                            dummyTimer,
                            this.runtimeState,
                            isInternal: true));
                        this.isInterrupted = true;
                        break;
                    }
                }

                // correlation
                CorrelationTraceClient.Propagate(() => {
                    if (this.runtimeState.ExecutionStartedEvent != null)
                        this.runtimeState.ExecutionStartedEvent.Correlation = CorrelationTraceContext.Current.SerializableTraceContext;
                });

                // finish up processing of the work item
                if (!this.continuedAsNew && this.runtimeState.Events.Last().EventType != EventType.OrchestratorCompleted)
                {
                    this.runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                }

                if (this.isCompleted)
                {
                    TraceHelper.TraceSession(TraceEventType.Information, "TaskOrchestrationDispatcher-DeletingSessionState", this.workItem.InstanceId, "Deleting session state");
                    if (this.runtimeState.ExecutionStartedEvent != null)
                    {
                        this.instanceState = Utils.BuildOrchestrationState(this.runtimeState);
                    }
                }
                else
                {
                    if (this.continuedAsNew)
                    {
                        TraceHelper.TraceSession(
                            TraceEventType.Information,
                            "TaskOrchestrationDispatcher-UpdatingStateForContinuation",
                            this.workItem.InstanceId,
                            "Updating state for continuation");

                        // correlation
                        CorrelationTraceClient.Propagate(() =>
                        {
                            continueAsNewExecutionStarted!.Correlation = CorrelationTraceContext.Current.SerializableTraceContext;
                        });

                        this.runtimeState = new OrchestrationRuntimeState();
                        this.runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                        this.runtimeState.AddEvent(continueAsNewExecutionStarted!);
                        this.runtimeState.Status = this.workItem.OrchestrationRuntimeState.Status ?? carryOverStatus;
                        carryOverStatus = this.workItem.OrchestrationRuntimeState.Status;

                        if (carryOverEvents != null)
                        {
                            foreach (var historyEvent in carryOverEvents)
                            {
                                this.runtimeState.AddEvent(historyEvent);
                            }
                        }

                        this.runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                        this.workItem.OrchestrationRuntimeState = this.runtimeState;

                        this.workItem.Cursor = null;
                    }

                    this.instanceState = Utils.BuildOrchestrationState(this.runtimeState);
                }
            } while (this.continuedAsNew);

            this.runtimeState.Status = this.runtimeState.Status ?? carryOverStatus;

            if (this.instanceState != null)
            {
                this.instanceState.Status = this.runtimeState.Status;
            }
        }

        static OrchestrationExecutionContext GetOrchestrationExecutionContext(OrchestrationRuntimeState runtimeState)
        {
            return new OrchestrationExecutionContext { OrchestrationTags = runtimeState.Tags ?? new Dictionary<string, string>(capacity: 0) };
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
                    this.dispatcher.EntityBackendInformation,
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


        TaskMessage? ProcessWorkflowCompletedTaskDecision(
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
                    completeOrchestratorAction.OrchestrationStatus,
                    completeOrchestratorAction.FailureDetails);
            }

            runtimeState.AddEvent(executionCompletedEvent);

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
    }
}