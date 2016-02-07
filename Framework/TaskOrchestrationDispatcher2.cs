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
    using Command;
    using History;
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Tracing;

    internal class TaskOrchestrationDispatcher2 : DispatcherBase<TaskOrchestrationWorkItem>
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


        readonly NameVersionObjectManager<TaskOrchestration> objectManager;
        readonly TaskHubWorkerSettings settings;
        readonly TaskHubDescription taskHubDescription;
        readonly TrackingDispatcher trackingDipatcher;
        readonly IOrchestrationService orchestrationService;

        internal TaskOrchestrationDispatcher2(MessagingFactory messagingFactory,
            TrackingDispatcher trackingDispatcher,
            TaskHubDescription taskHubDescription,
            TaskHubWorkerSettings workerSettings,
            IOrchestrationService orchestrationService,
            NameVersionObjectManager<TaskOrchestration> objectManager)
            : base("TaskOrchestration Dispatcher", item => item == null  ? string.Empty : item.OrchestrationInstance.InstanceId)  // AFFANDAR : TODO : revisit this abstraction
        {
            this.taskHubDescription = taskHubDescription;
            this.settings = workerSettings.Clone();
            this.objectManager = objectManager;

            this.trackingDipatcher = trackingDispatcher;
            this.orchestrationService = orchestrationService;
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

        protected override async Task<TaskOrchestrationWorkItem> OnFetchWorkItem(TimeSpan receiveTimeout)
        {
            // AFFANDAR : TODO : do we really need this abstract method anymore?
            return await this.orchestrationService.LockNextTaskOrchestrationWorkItemAsync();
        }

        protected override async Task OnProcessWorkItem(TaskOrchestrationWorkItem workItem)
        {
            var messagesToSend = new List<TaskMessage>();
            var timerMessages = new List<TaskMessage>();
            var subOrchestrationMessages = new List<TaskMessage>();
            bool isCompleted = false;
            bool continuedAsNew = false;

            ExecutionStartedEvent continueAsNewExecutionStarted = null;

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            if (!ReconcileMessagesWithState(workItem))
            {
                // TODO : mark an orchestration as faulted if there is data corruption
                TraceHelper.TraceSession(TraceEventType.Error, runtimeState.OrchestrationInstance.InstanceId,
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
                            TaskMessage workflowInstanceCompletedMessage =
                                ProcessWorkflowCompletedTaskDecision((OrchestrationCompleteOrchestratorAction) decision,
                                    runtimeState, IncludeDetails, out continuedAsNew);
                            if (workflowInstanceCompletedMessage != null)
                            {
                                // Send complete message to parent workflow or to itself to start a new execution
                                subOrchestrationMessages.Add(workflowInstanceCompletedMessage);

                                // Store the event so we can rebuild the state
                                if (continuedAsNew)
                                {
                                    continueAsNewExecutionStarted = workflowInstanceCompletedMessage.Event as ExecutionStartedEvent;
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

                    // AFFANDAR : TODO : this should be moved to the service bus orchestration service
                    if (totalMessages > this.orchestrationService.MaxMessageCount)
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

            if (isCompleted)
            {
                TraceHelper.TraceSession(TraceEventType.Information, workItem.OrchestrationInstance.InstanceId, "Deleting session state");
                workItem.OrchestrationRuntimeState = null;
            }
            else
            {
                if (continuedAsNew)
                {
                    TraceHelper.TraceSession(TraceEventType.Information, workItem.OrchestrationInstance.InstanceId,
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
                    continuedAsNew ? null : timerMessages);
            }
            catch(Exception exception)
            {
                // AFFANDAR : TODO : if exception is due to session state size then force terminate message
                throw;
            }
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

        bool ReconcileMessagesWithState(TaskOrchestrationWorkItem workItem)
        {
            foreach (TaskMessage message in workItem.NewMessages)
            {
                OrchestrationInstance orchestrationInstance = message.OrchestrationInstance;
                if (orchestrationInstance == null || string.IsNullOrWhiteSpace(orchestrationInstance.InstanceId))
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

        protected override Task AbortWorkItem(TaskOrchestrationWorkItem workItem)
        {
            return this.orchestrationService.AbandonTaskOrchestrationWorkItemAsync(workItem);
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

        protected override Task SafeReleaseWorkItem(TaskOrchestrationWorkItem workItem)
        {
            // no need, 
            return Task.FromResult<object>(null);
        }
    }
}