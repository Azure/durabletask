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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Command;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.History;
    using DurableTask.Serializing;
    using DurableTask.Tracing;

    public class TaskOrchestrationDispatcher2 : DispatcherBase<TaskOrchestrationWorkItem>
    {
        readonly NameVersionObjectManager<TaskOrchestration> objectManager;
        readonly TaskHubWorkerSettings settings;
        readonly IOrchestrationService orchestrationService;
        private static readonly DataConverter DataConverter = new JsonDataConverter();

        internal TaskOrchestrationDispatcher2(
            TaskHubWorkerSettings workerSettings,
            IOrchestrationService orchestrationService,
            NameVersionObjectManager<TaskOrchestration> objectManager)
            : base("TaskOrchestration Dispatcher", item => item == null  ? string.Empty : item.InstanceId)  // AFFANDAR : TODO : revisit this abstraction
        {
            this.settings = workerSettings.Clone();
            this.objectManager = objectManager;

            this.orchestrationService = orchestrationService;
            maxConcurrentWorkItems = settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;
        }

        public bool IncludeDetails { get; set; }
        public bool IncludeParameters { get; set; }

        protected override void OnStart()
        {
        }

        protected override void OnStopping(bool isForced)
        {
        }

        protected override void OnStopped(bool isForced)
        {
        }

        protected override async Task<TaskOrchestrationWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout)
        {
            // AFFANDAR : TODO : do we really need this abstract method anymore?
            // AFFANDAR : TODO : wire-up cancellation tokens
            return await this.orchestrationService.LockNextTaskOrchestrationWorkItemAsync(receiveTimeout, CancellationToken.None);
        }

        protected override async Task OnProcessWorkItemAsync(TaskOrchestrationWorkItem workItem)
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
                TraceHelper.TraceSession(TraceEventType.Error, runtimeState.OrchestrationInstance?.InstanceId,
                    "Received result for a deleted orchestration");
                isCompleted = true;
            }
            else
            {
                TraceHelper.TraceInstance(
                    TraceEventType.Verbose,
                    runtimeState.OrchestrationInstance,
                    "Executing user orchestration: {0}",
                    DataConverter.Serialize(runtimeState.GetOrchestrationRuntimeStateDump(), false));

                IList<OrchestratorAction> decisions = ExecuteOrchestration(runtimeState).ToList();

                TraceHelper.TraceInstance(TraceEventType.Information,
                    runtimeState.OrchestrationInstance,
                    "Executed user orchestration. Received {0} orchestrator actions: {1}",
                    decisions.Count(),
                    string.Join(", ", decisions.Select(d => d.Id + ":" + d.OrchestratorActionType)));

                foreach (OrchestratorAction decision in decisions)
                {
                    TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                        "Processing orchestrator action of type {0}", decision.OrchestratorActionType);
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
                            // todo : restore logic from original for certain cases
                            TaskMessage workflowInstanceCompletedMessage =
                                ProcessWorkflowCompletedTaskDecision((OrchestrationCompleteOrchestratorAction) decision, runtimeState, IncludeDetails, out continuedAsNew);
                            if (workflowInstanceCompletedMessage != null)
                            {
                                // Send complete message to parent workflow or to itself to start a new execution
                                // moved into else to match old 
                                // subOrchestrationMessages.Add(workflowInstanceCompletedMessage);

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
                            throw TraceHelper.TraceExceptionInstance(TraceEventType.Error,
                                runtimeState.OrchestrationInstance,
                                new NotSupportedException("decision type not supported"));
                    }

                    // Underlying orchestration service provider may have a limit of messages per call, to avoid the situation
                    // we keep on asking the provider if message count is ok and stop processing new decisions if not.
                    //
                    // We also put in a fake timer to force next orchestration task for remaining messages
                    int totalMessages = messagesToSend.Count + subOrchestrationMessages.Count + timerMessages.Count;

                    // AFFANDAR : TODO : this should be moved to the service bus orchestration service
                    if (this.orchestrationService.IsMaxMessageCountExceeded(totalMessages, runtimeState))
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

            // todo : figure out if this is correct, BuildOrchestrationState fails when iscompleted is true
            OrchestrationState instanceState = null;

            if (isCompleted)
            {
                TraceHelper.TraceSession(TraceEventType.Information, workItem.InstanceId, "Deleting session state");
                newOrchestrationRuntimeState = null;
            }
            else
            {
                instanceState = BuildOrchestrationState(newOrchestrationRuntimeState);

                if (continuedAsNew)
                {
                    TraceHelper.TraceSession(TraceEventType.Information, workItem.InstanceId,
                        "Updating state for continuation");
                    newOrchestrationRuntimeState = new OrchestrationRuntimeState();
                    newOrchestrationRuntimeState.AddEvent(new OrchestratorStartedEvent(-1));
                    newOrchestrationRuntimeState.AddEvent(continueAsNewExecutionStarted);
                    newOrchestrationRuntimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                }
            }

            // AFFANDAR : TODO : session state size check, should we let sbus handle it completely?
            try
            {
                await this.orchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                    workItem,
                    newOrchestrationRuntimeState,
                    continuedAsNew ? null : messagesToSend,
                    subOrchestrationMessages,
                    continuedAsNew ? null : timerMessages,
                    continuedAsNewMessage,
                    instanceState);
            }
            catch(Exception)
            {
                // AFFANDAR : TODO : if exception is due to session state size then force terminate message
                throw;
            }
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
                    new TypeMissingException($"Orchestration not found: ({runtimeState.Name}, {runtimeState.Version})"));
            }

            var taskOrchestrationExecutor = new TaskOrchestrationExecutor(runtimeState, taskOrchestration);
            IEnumerable<OrchestratorAction> decisions = taskOrchestrationExecutor.Execute();
            return decisions;
        }

        bool ReconcileMessagesWithState(TaskOrchestrationWorkItem workItem)
        {
            foreach (TaskMessage message in workItem.NewMessages)
            {
                OrchestrationInstance orchestrationInstance = message.OrchestrationInstance;
                if (string.IsNullOrWhiteSpace(orchestrationInstance?.InstanceId))
                {
                    throw TraceHelper.TraceException(TraceEventType.Error,
                        new InvalidOperationException("Message does not contain any OrchestrationInstance information"));
                }

                if (workItem.OrchestrationRuntimeState.Events.Count == 1 && message.Event.EventType != EventType.ExecutionStarted)
                {
                    // we get here because of:
                    //      i) responses for scheduled tasks after the orchestrations have been completed
                    //      ii) responses for explicitly deleted orchestrations
                    return false;
                }

                TraceHelper.TraceInstance(TraceEventType.Information, orchestrationInstance,
                    "Processing new event with Id {0} and type {1}", message.Event.EventId,
                    message.Event.EventType);

                if (message.Event.EventType == EventType.ExecutionStarted)
                {
                    if (workItem.OrchestrationRuntimeState.Events.Count > 1)
                    {
                        // this was caused due to a dupe execution started event, swallow this one
                        TraceHelper.TraceInstance(TraceEventType.Warning, orchestrationInstance,
                            "Duplicate start event.  Ignoring event with Id {0} and type {1} ",
                            message.Event.EventId, message.Event.EventType);
                        continue;
                    }
                }
                else if (!string.IsNullOrWhiteSpace(orchestrationInstance.ExecutionId)
                         &&
                         !string.Equals(orchestrationInstance.ExecutionId,
                             workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId))
                {
                    // eat up any events for previous executions
                    TraceHelper.TraceInstance(TraceEventType.Warning, orchestrationInstance,
                        "ExecutionId of event does not match current executionId.  Ignoring event with Id {0} and type {1} ",
                        message.Event.EventId, message.Event.EventType);
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

            TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
                "Instance Id '{0}' completed in state {1} with result: {2}",
                runtimeState.OrchestrationInstance, runtimeState.OrchestrationStatus, completeOrchestratorAction.Result);
            TraceHelper.TraceInstance(TraceEventType.Information, runtimeState.OrchestrationInstance,
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
            OrchestrationRuntimeState runtimeState, bool includeParameters)
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
                Tags = runtimeState.Tags,
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

        protected override Task AbortWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return this.orchestrationService.AbandonTaskOrchestrationWorkItemAsync(workItem);
        }

        protected override int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (orchestrationService.IsTransientException(exception))  
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
            if (orchestrationService.IsTransientException(exception))
            {
                delay = settings.TaskOrchestrationDispatcherSettings.TransientErrorBackOffSecs;
            }

            return delay;
        }

        protected override Task SafeReleaseWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // no need, 
            return Task.FromResult<object>(null);
        }
    }
}