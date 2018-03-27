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
    /// Dispatcher for orchestrations to handle processing and renewing, completetion of orchestration events
    /// </summary>
    public class TaskOrchestrationDispatcher 
    {
        readonly INameVersionObjectManager<TaskOrchestration> objectManager;
        readonly IOrchestrationService orchestrationService;
        readonly WorkItemDispatcher<TaskOrchestrationWorkItem> dispatcher;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        static readonly DataConverter DataConverter = new JsonDataConverter();

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
                this.OnFetchWorkItemAsync,
                this.OnProcessWorkItemAsync)
            {
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                SafeReleaseWorkItem = orchestrationService.ReleaseTaskOrchestrationWorkItemAsync,
                AbortWorkItem = orchestrationService.AbandonTaskOrchestrationWorkItemAsync,
                DispatcherCount = orchestrationService.TaskOrchestrationDispatcherCount,
                MaxConcurrentWorkItems = orchestrationService.MaxConcurrentTaskOrchestrationWorkItems
            };
        }

        /// <summary>
        /// Starts the dispatcher to start getting and processing orchestration events
        /// </summary>
        public async Task StartAsync()
        {
            await dispatcher.StartAsync();
        }

        /// <summary>
        /// Stops the dispatcher to stop getting and processing orchestration events
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully or immediately</param>
        public async Task StopAsync(bool forced)
        {
            await dispatcher.StopAsync(forced);
        }

        /// <summary>
        /// Gets or sets flag whether to include additional details in error messages
        /// </summary>
        public bool IncludeDetails { get; set; }

        /// <summary>
        /// Gets or sets flag whether to pass orchestration input parameters to sub orchestations
        /// </summary>
        public bool IncludeParameters { get; set; }

        /// <summary>
        /// Method to get the next work item to process within supplied timeout
        /// </summary>
        /// <param name="receiveTimeout">The max timeout to wait</param>
        /// <returns>A new TaskOrchestrationWorkItem</returns>
        protected async Task<TaskOrchestrationWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout)
        {
            // AFFANDAR : TODO : wire-up cancellation tokens
            return await this.orchestrationService.LockNextTaskOrchestrationWorkItemAsync(receiveTimeout, CancellationToken.None);
        }

        /// <summary>
        /// Method to process a new work item
        /// </summary>
        /// <param name="workItem">The work item to process</param>
        protected async Task OnProcessWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            var messagesToSend = new List<TaskMessage>();
            var timerMessages = new List<TaskMessage>();
            var subOrchestrationMessages = new List<TaskMessage>();
            bool isCompleted = false;
            bool continuedAsNew = false;

            ExecutionStartedEvent continueAsNewExecutionStarted = null;
            TaskMessage continuedAsNewMessage = null;

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));

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
                TraceHelper.TraceInstance(
                    TraceEventType.Verbose,
                    "TaskOrchestrationDispatcher-ExecuteUserOrchestration-Begin",
                    runtimeState.OrchestrationInstance,
                    "Executing user orchestration: {0}",
                    DataConverter.Serialize(runtimeState.GetOrchestrationRuntimeStateDump(), true));

                IList<OrchestratorAction> decisions = (await ExecuteOrchestrationAsync(runtimeState)).ToList();

                TraceHelper.TraceInstance(
                    TraceEventType.Information,
                    "TaskOrchestrationDispatcher-ExecuteUserOrchestration-End",
                    runtimeState.OrchestrationInstance,
                    "Executed user orchestration. Received {0} orchestrator actions: {1}",
                    decisions.Count(),
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
                            var timerOrchestratorAction = (CreateTimerOrchestratorAction) decision;
                            timerMessages.Add(ProcessCreateTimerDecision(timerOrchestratorAction, runtimeState));
                            break;
                        case OrchestratorActionType.CreateSubOrchestration:
                            var createSubOrchestrationAction = (CreateSubOrchestrationAction) decision;
                            subOrchestrationMessages.Add(
                                ProcessCreateSubOrchestrationInstanceDecision(createSubOrchestrationAction,
                                    runtimeState, IncludeParameters));
                            break;
                        case OrchestratorActionType.OrchestrationComplete:
                            TaskMessage workflowInstanceCompletedMessage =
                                ProcessWorkflowCompletedTaskDecision((OrchestrationCompleteOrchestratorAction) decision, runtimeState, IncludeDetails, out continuedAsNew);
                            if (workflowInstanceCompletedMessage != null)
                            {
                                // Send complete message to parent workflow or to itself to start a new execution
                                // Store the event so we can rebuild the state
                                if (continuedAsNew)
                                {
                                    continuedAsNewMessage = workflowInstanceCompletedMessage;
                                    continueAsNewExecutionStarted = workflowInstanceCompletedMessage.Event as ExecutionStartedEvent;
                                }
                                else
                                {
                                    subOrchestrationMessages.Add(workflowInstanceCompletedMessage);
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
                    int totalMessages = messagesToSend.Count + subOrchestrationMessages.Count + timerMessages.Count;
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
                        break;
                    }
                }
            }

            // finish up processing of the work item
            if (!continuedAsNew)
            {
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
            }

            OrchestrationRuntimeState newOrchestrationRuntimeState = workItem.OrchestrationRuntimeState;

            OrchestrationState instanceState = null;

            if (isCompleted)
            {
                TraceHelper.TraceSession(TraceEventType.Information, "TaskOrchestrationDispatcher-DeletingSessionState", workItem.InstanceId, "Deleting session state");
                if (newOrchestrationRuntimeState.ExecutionStartedEvent != null)
                {
                    instanceState = Utils.BuildOrchestrationState(newOrchestrationRuntimeState);
                }

                newOrchestrationRuntimeState = null;
            }
            else
            {
                instanceState = Utils.BuildOrchestrationState(newOrchestrationRuntimeState);

                if (continuedAsNew)
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Information, 
                        "TaskOrchestrationDispatcher-UpdatingStateForContinuation", 
                        workItem.InstanceId,
                        "Updating state for continuation");
                    newOrchestrationRuntimeState = new OrchestrationRuntimeState();
                    newOrchestrationRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                    newOrchestrationRuntimeState.AddEvent(continueAsNewExecutionStarted);
                    newOrchestrationRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                }
            }

            await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                workItem,
                newOrchestrationRuntimeState,
                continuedAsNew ? null : messagesToSend,
                subOrchestrationMessages,
                continuedAsNew ? null : timerMessages,
                continuedAsNewMessage,
                instanceState);
        }

        async Task<IEnumerable<OrchestratorAction>> ExecuteOrchestrationAsync(OrchestrationRuntimeState runtimeState)
        {
            TaskOrchestration taskOrchestration = objectManager.GetObject(runtimeState.Name, runtimeState.Version);
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

            IEnumerable<OrchestratorAction> decisions = null;
            await this.dispatchPipeline.RunAsync(dispatchContext, _ =>
            {
                var taskOrchestrationExecutor = new TaskOrchestrationExecutor(runtimeState, taskOrchestration);
                decisions = taskOrchestrationExecutor.Execute();

                return Task.FromResult(0);
            });

            return decisions;
        }

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
                Tags = MergeTags(createSubOrchestrationAction.Tags, runtimeState.Tags),
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

        static IDictionary<string, string> MergeTags(
            IDictionary<string, string> newTags,
            IDictionary<string, string> existingTags)
        {
            IDictionary<string, string> result;

            // We will merge the two dictionaries of tags, tags in the createSubOrchestrationAction overwrite those in runtimeState
            if (newTags != null && existingTags != null)
            {
                result = newTags.Concat(
                    existingTags.Where(k => !newTags.ContainsKey(k.Key)))
                    .ToDictionary(x => x.Key, y => y.Value);
            }
            else
            {
                result = newTags ?? existingTags;
            }

            return result;
        }
    }
}