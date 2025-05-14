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
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;

    internal class TaskOrchestrationContext : OrchestrationContext
    {
        private readonly IDictionary<int, OpenTaskInfo> openTasks;
        private readonly IDictionary<int, OrchestratorAction> orchestratorActionsMap;
        private OrchestrationCompleteOrchestratorAction continueAsNew;
        private bool executionCompletedOrTerminated;
        private int idCounter;
        private readonly Queue<HistoryEvent> eventsWhileSuspended;
        private readonly IDictionary<int, OrchestratorAction> suspendedActionsMap;

        public bool IsSuspended { get; private set; }

        public bool HasContinueAsNew => continueAsNew != null;

        public void AddEventToNextIteration(HistoryEvent he)
        {
            continueAsNew.CarryoverEvents.Add(he);
        }

        public TaskOrchestrationContext(
            OrchestrationInstance orchestrationInstance,
            TaskScheduler taskScheduler,
            TaskOrchestrationEntityParameters entityParameters = null,
            ErrorPropagationMode errorPropagationMode = ErrorPropagationMode.SerializeExceptions)
        {
            Utils.UnusedParameter(taskScheduler);

            this.openTasks = new Dictionary<int, OpenTaskInfo>();
            this.orchestratorActionsMap = new SortedDictionary<int, OrchestratorAction>();
            this.idCounter = 0;
            this.MessageDataConverter = JsonDataConverter.Default;
            this.ErrorDataConverter = JsonDataConverter.Default;
            OrchestrationInstance = orchestrationInstance;
            IsReplaying = false;
            this.EntityParameters = entityParameters;
            ErrorPropagationMode = errorPropagationMode;
            this.eventsWhileSuspended = new Queue<HistoryEvent>();
            this.suspendedActionsMap = new SortedDictionary<int, OrchestratorAction>();
        }

        public IEnumerable<OrchestratorAction> OrchestratorActions => this.orchestratorActionsMap.Values;

        public bool HasOpenTasks => this.openTasks.Count > 0;

        internal void ClearPendingActions()
        {
            this.orchestratorActionsMap.Clear();
            continueAsNew = null;
        }

        public override async Task<TResult> ScheduleTask<TResult>(string name, string version,
            params object[] parameters)
        {
            TResult result = await ScheduleTaskToWorker<TResult>(name, version, null, parameters);

            return result;
        }

        public override async Task<TResult> ScheduleTask<TResult>(string name, string version,
            ScheduleTaskOptions options, params object[] parameters)
        {
            if (options.RetryOptions != null)
            {
                Task<TResult> RetryCall() => ScheduleTask<TResult>(name, version, ScheduleTaskOptions.CreateBuilder().WithTags(options.Tags).Build(), parameters);
                var retryInterceptor = new RetryInterceptor<TResult>(this, options.RetryOptions, RetryCall);
                return await retryInterceptor.Invoke();
            }
 
            TResult result = await ScheduleTaskToWorker<TResult>(name, version, null, options, parameters);
 
            return result;
        }

        public async Task<TResult> ScheduleTaskToWorker<TResult>(string name, string version, string taskList,
            ScheduleTaskOptions options, params object[] parameters)
        {
            object result = await ScheduleTaskInternal(name, version, taskList, typeof(TResult), options, parameters);

            if (result == null)
            {
                return default(TResult);
            }

            return (TResult)result;
        }

        public async Task<TResult> ScheduleTaskToWorker<TResult>(string name, string version, string taskList,
            params object[] parameters)
        {
            return await ScheduleTaskToWorker<TResult>(name, version, taskList, ScheduleTaskOptions.CreateBuilder().Build(), parameters);
        }

        public async Task<object> ScheduleTaskInternal(string name, string version, string taskList, Type resultType,
            ScheduleTaskOptions options, params object[] parameters)
        {
            int id = this.idCounter++;
            string serializedInput = this.MessageDataConverter.SerializeInternal(parameters);
            var scheduleTaskTaskAction = new ScheduleTaskOrchestratorAction
            {
                Id = id,
                Name = name,
                Version = version,
                Tasklist = taskList,
                Input = serializedInput,
                Tags = options.Tags,
            };

            this.orchestratorActionsMap.Add(id, scheduleTaskTaskAction);

            var tcs = new TaskCompletionSource<string>();
            this.openTasks.Add(id, new OpenTaskInfo { Name = name, Version = version, Result = tcs });

            string serializedResult = await tcs.Task;

            return this.MessageDataConverter.Deserialize(serializedResult, resultType);
        }


        public async Task<object> ScheduleTaskInternal(string name, string version, string taskList, Type resultType,
            params object[] parameters)
        {
            return await ScheduleTaskInternal(name, version, taskList, resultType, ScheduleTaskOptions.CreateBuilder().Build(), parameters);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(
            string name,
            string version,
            string instanceId,
            object input)
        {
            return CreateSubOrchestrationInstanceCore<T>(name, version, instanceId, input, null);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(
            string name,
            string version,
            string instanceId,
            object input,
            IDictionary<string, string> tags)
        {
            return CreateSubOrchestrationInstanceCore<T>(name, version, instanceId, input, tags);
        }

        public override Task<T> CreateSubOrchestrationInstance<T>(
            string name,
            string version,
            object input)
        {
            return CreateSubOrchestrationInstanceCore<T>(name, version, null, input, null);
        }

        async Task<T> CreateSubOrchestrationInstanceCore<T>(
            string name,
            string version,
            string instanceId,
            object input,
            IDictionary<string, string> tags)
        {
            int id = this.idCounter++;
            string serializedInput = this.MessageDataConverter.SerializeInternal(input);

            string actualInstanceId = instanceId;
            if (string.IsNullOrWhiteSpace(actualInstanceId))
            {
                actualInstanceId = OrchestrationInstance.ExecutionId + ":" + id;
            }

            var action = new CreateSubOrchestrationAction
            {
                Id = id,
                InstanceId = actualInstanceId,
                Name = name,
                Version = version,
                Input = serializedInput,
                Tags = tags
            };

            this.orchestratorActionsMap.Add(id, action);

            if (OrchestrationTags.IsTaggedAsFireAndForget(tags))
            {
                // this is a fire-and-forget orchestration, so we do not wait for a result.
                return default(T);
            }
            else
            {
                var tcs = new TaskCompletionSource<string>();
                this.openTasks.Add(id, new OpenTaskInfo { Name = name, Version = version, Result = tcs });

                string serializedResult = await tcs.Task;

                return this.MessageDataConverter.Deserialize<T>(serializedResult);
            }
        }

        public override void SendEvent(OrchestrationInstance orchestrationInstance, string eventName, object eventData)
        {
            if (string.IsNullOrWhiteSpace(orchestrationInstance?.InstanceId))
            {
                throw new ArgumentException(nameof(orchestrationInstance));
            }

            int id = this.idCounter++;

            string serializedEventData = this.MessageDataConverter.SerializeInternal(eventData);

            var action = new SendEventOrchestratorAction
            {
                Id = id,
                Instance = orchestrationInstance,
                EventName = eventName,
                EventData = serializedEventData,
            };

            this.orchestratorActionsMap.Add(id, action);
        }

        public override void ContinueAsNew(object input)
        {
            ContinueAsNew(null, input);
        }

        public override void ContinueAsNew(string newVersion, object input)
        {
            ContinueAsNewCore(newVersion, input);
        }

        void ContinueAsNewCore(string newVersion, object input)
        {
            string serializedInput = this.MessageDataConverter.SerializeInternal(input);

            this.continueAsNew = new OrchestrationCompleteOrchestratorAction
            {
                Result = serializedInput,
                OrchestrationStatus = OrchestrationStatus.ContinuedAsNew,
                NewVersion = newVersion
            };
        }

        public override Task<T> CreateTimer<T>(DateTime fireAt, T state)
        {
            return CreateTimer(fireAt, state, CancellationToken.None);
        }

        public override async Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
        {
            if (state is CancellationToken)
            {
                throw new ArgumentException(
                    "The state parameter cannot be a CancellationToken. Did you mean to use a different overload?",
                    paramName: nameof(state));
            }

            int id = this.idCounter++;
            var createTimerOrchestratorAction = new CreateTimerOrchestratorAction
            {
                Id = id,
                FireAt = fireAt,
            };

            this.orchestratorActionsMap.Add(id, createTimerOrchestratorAction);

            var tcs = new TaskCompletionSource<string>();
            this.openTasks.Add(id, new OpenTaskInfo { Name = null, Version = null, Result = tcs });

            if (cancelToken != CancellationToken.None)
            {
                cancelToken.Register(s =>
                {
                    if (tcs.TrySetCanceled())
                    {
                        // TODO: Emit a log message that the timer is cancelled.
                        this.openTasks.Remove(id);
                    }
                }, tcs);
            }

            await tcs.Task;

            return state;
        }

        public void HandleTaskScheduledEvent(TaskScheduledEvent scheduledEvent)
        {
            int taskId = scheduledEvent.EventId;
            if (!this.orchestratorActionsMap.ContainsKey(taskId))
            {
                throw new NonDeterministicOrchestrationException(scheduledEvent.EventId,
                    $"A previous execution of this orchestration scheduled an activity task with sequence ID {taskId} and name "
                    + $"'{scheduledEvent.Name}' (version '{scheduledEvent.Version}'), but the current replay execution hasn't "
                    + "(yet?) scheduled this task. Was a change made to the orchestrator code after this instance had already "
                    + "started running?");
            }

            var orchestrationAction = this.orchestratorActionsMap[taskId];
            if (orchestrationAction is not ScheduleTaskOrchestratorAction currentReplayAction)
            {
                throw new NonDeterministicOrchestrationException(scheduledEvent.EventId,
                    $"A previous execution of this orchestration scheduled an activity task with sequence number {taskId} named "
                    + $"'{scheduledEvent.Name}', but the current orchestration replay instead produced a "
                    + $"{orchestrationAction.GetType().Name} action with this sequence number. Was a change made to the "
                    + "orchestrator code after this instance had already started running?");
            }

            if (!string.Equals(scheduledEvent.Name, currentReplayAction.Name, StringComparison.OrdinalIgnoreCase))
            {
                throw new NonDeterministicOrchestrationException(scheduledEvent.EventId,
                    $"A previous execution of this orchestration scheduled an activity task with sequence number {taskId} "
                    + $"named '{scheduledEvent.Name}', but the current orchestration replay instead scheduled an activity "
                    + $"task named '{currentReplayAction.Name}' with this sequence number.  Was a change made to the "
                    + "orchestrator code after this instance had already started running?");
            }

            this.orchestratorActionsMap.Remove(taskId);
        }

        public void HandleTimerCreatedEvent(TimerCreatedEvent timerCreatedEvent)
        {
            int taskId = timerCreatedEvent.EventId;
            if (taskId == FrameworkConstants.FakeTimerIdToSplitDecision)
            {
                // This is our dummy timer to split decision for avoiding 100 messages per transaction service bus limit
                return;
            }

            if (!this.orchestratorActionsMap.ContainsKey(taskId))
            {
                throw new NonDeterministicOrchestrationException(timerCreatedEvent.EventId,
                    $"A previous execution of this orchestration scheduled a timer task with sequence number {taskId} but "
                    + "the current replay execution hasn't (yet?) scheduled this task. Was a change made to the orchestrator "
                    + "code after this instance had already started running?");
            }

            var orchestrationAction = this.orchestratorActionsMap[taskId];
            if (orchestrationAction is not CreateTimerOrchestratorAction)
            {
                throw new NonDeterministicOrchestrationException(timerCreatedEvent.EventId,
                    $"A previous execution of this orchestration scheduled a timer task with sequence number {taskId} named "
                    + $"but the current orchestration replay instead produced a {orchestrationAction.GetType().Name} action with "
                    + "this sequence number. Was a change made to the orchestrator code after this instance had already "
                    + "started running?");
            }

            this.orchestratorActionsMap.Remove(taskId);
        }

        public void HandleSubOrchestrationCreatedEvent(SubOrchestrationInstanceCreatedEvent subOrchestrationCreateEvent)
        {
            int taskId = subOrchestrationCreateEvent.EventId;
            if (!this.orchestratorActionsMap.ContainsKey(taskId))
            {
                throw new NonDeterministicOrchestrationException(subOrchestrationCreateEvent.EventId,
                   $"A previous execution of this orchestration scheduled a sub-orchestration task with sequence ID {taskId} "
                   + $"and name '{subOrchestrationCreateEvent.Name}' (version '{subOrchestrationCreateEvent.Version}', "
                   + $"instance ID '{subOrchestrationCreateEvent.InstanceId}'), but the current replay execution hasn't (yet?) "
                   + "scheduled this task. Was a change made to the orchestrator code after this instance had already started running?");
            }

            var orchestrationAction = this.orchestratorActionsMap[taskId];
            if (orchestrationAction is not CreateSubOrchestrationAction currentReplayAction)
            {
                throw new NonDeterministicOrchestrationException(subOrchestrationCreateEvent.EventId,
                   $"A previous execution of this orchestration scheduled a sub-orchestration task with sequence ID {taskId} "
                   + $"and name '{subOrchestrationCreateEvent.Name}' (version '{subOrchestrationCreateEvent.Version}', "
                   + $"instance ID '{subOrchestrationCreateEvent.InstanceId}'), but the current orchestration replay instead "
                   + $"produced a {orchestrationAction.GetType().Name} action at this sequence number. Was a change made to "
                   + "the orchestrator code after this instance had already started running?");
            }

            if (!string.Equals(subOrchestrationCreateEvent.Name, currentReplayAction.Name, StringComparison.OrdinalIgnoreCase))
            {
                throw new NonDeterministicOrchestrationException(subOrchestrationCreateEvent.EventId,
                   $"A previous execution of this orchestration scheduled a sub-orchestration task with sequence ID {taskId} "
                   + $"and name '{subOrchestrationCreateEvent.Name}' (version '{subOrchestrationCreateEvent.Version}', "
                   + $"instance ID '{subOrchestrationCreateEvent.InstanceId}'), but the current orchestration replay instead "
                   + $"scheduled a sub-orchestration task with name {currentReplayAction.Name} at this sequence number. "
                   + "Was a change made to the orchestrator code after this instance had already started running?");
            }

            this.orchestratorActionsMap.Remove(taskId);
        }

        public void HandleEventSentEvent(EventSentEvent eventSentEvent)
        {
            int taskId = eventSentEvent.EventId;
            if (!this.orchestratorActionsMap.ContainsKey(taskId))
            {
                throw new NonDeterministicOrchestrationException(eventSentEvent.EventId,
                   $"A previous execution of this orchestration scheduled a send event task with sequence ID {taskId}, "
                   + $"type '{eventSentEvent.EventType}' name '{eventSentEvent.Name}', instance ID '{eventSentEvent.InstanceId}', "
                   + $"but the current replay execution hasn't (yet?) scheduled this task. Was a change made to the orchestrator code "
                   + $"after this instance had already started running?");
            }

            var orchestrationAction = this.orchestratorActionsMap[taskId];
            if (!(orchestrationAction is SendEventOrchestratorAction currentReplayAction))
            {
                throw new NonDeterministicOrchestrationException(eventSentEvent.EventId,
                   $"A previous execution of this orchestration scheduled a send event task with sequence ID {taskId}, "
                   + $"type '{eventSentEvent.EventType}', name '{eventSentEvent.Name}', instance ID '{eventSentEvent.InstanceId}', "
                   + $"but the current orchestration replay instead scheduled a {orchestrationAction.GetType().Name} task "
                   + "at this sequence number. Was a change made to the orchestrator code after this instance had already "
                   + "started running?");

            }

            if (!string.Equals(eventSentEvent.Name, currentReplayAction.EventName, StringComparison.OrdinalIgnoreCase))
            {
                throw new NonDeterministicOrchestrationException(eventSentEvent.EventId,
                   $"A previous execution of this orchestration scheduled a send event task with sequence ID {taskId}, "
                   + $"type '{eventSentEvent.EventType}', name '{eventSentEvent.Name}', instance ID '{eventSentEvent.InstanceId}'), "
                   + $"but the current orchestration replay instead scheduled a send event task with name {currentReplayAction.EventName}"
                   + "at this sequence number. Was a change made to the orchestrator code after this instance had already "
                   + "started running?");
            }

            this.orchestratorActionsMap.Remove(taskId);
        }

        public void HandleEventRaisedEvent(EventRaisedEvent eventRaisedEvent, bool skipCarryOverEvents, TaskOrchestration taskOrchestration)
        {
            if (skipCarryOverEvents || !this.HasContinueAsNew)
            {
                taskOrchestration.RaiseEvent(this, eventRaisedEvent.Name, eventRaisedEvent.Input);
            }
            else
            {
                this.AddEventToNextIteration(eventRaisedEvent);
            }
        }

        public void HandleTaskCompletedEvent(TaskCompletedEvent completedEvent)
        {
            int taskId = completedEvent.TaskScheduledId;
            if (this.openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = this.openTasks[taskId];
                info.Result.SetResult(completedEvent.Result);

                this.openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("TaskCompleted", completedEvent, taskId);
            }
        }

        public void HandleTaskFailedEvent(TaskFailedEvent failedEvent)
        {
            int taskId = failedEvent.TaskScheduledId;
            if (this.openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = this.openTasks[taskId];

                // When using ErrorPropagationMode.SerializeExceptions the "cause" is deserialized from history.
                // This isn't fully reliable because not all exception types can be serialized/deserialized.
                // When using ErrorPropagationMode.UseFailureDetails we instead use FailureDetails to convey
                // error information, which doesn't involve any serialization at all.
                Exception cause = this.ErrorPropagationMode == ErrorPropagationMode.SerializeExceptions ?
                    Utils.RetrieveCause(failedEvent.Details, this.ErrorDataConverter) :
                    null;

                var taskFailedException = new TaskFailedException(
                    failedEvent.EventId,
                    taskId,
                    info.Name,
                    info.Version,
                    failedEvent.Reason,
                    cause);

                taskFailedException.FailureDetails = failedEvent.FailureDetails;

                TaskCompletionSource<string> tcs = info.Result;
                tcs.SetException(taskFailedException);

                this.openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("TaskFailed", failedEvent, taskId);
            }
        }

        public void HandleSubOrchestrationInstanceCompletedEvent(SubOrchestrationInstanceCompletedEvent completedEvent)
        {
            int taskId = completedEvent.TaskScheduledId;
            if (this.openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = this.openTasks[taskId];
                info.Result.SetResult(completedEvent.Result);

                this.openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("SubOrchestrationInstanceCompleted", completedEvent, taskId);
            }
        }

        public void HandleSubOrchestrationInstanceFailedEvent(SubOrchestrationInstanceFailedEvent failedEvent)
        {
            int taskId = failedEvent.TaskScheduledId;
            if (this.openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = this.openTasks[taskId];

                // When using ErrorPropagationMode.SerializeExceptions the "cause" is deserialized from history.
                // This isn't fully reliable because not all exception types can be serialized/deserialized.
                // When using ErrorPropagationMode.UseFailureDetails we instead use FailureDetails to convey
                // error information, which doesn't involve any serialization at all.
                Exception cause = this.ErrorPropagationMode == ErrorPropagationMode.SerializeExceptions ?
                    Utils.RetrieveCause(failedEvent.Details, this.ErrorDataConverter)
                    : null;

                var failedException = new SubOrchestrationFailedException(failedEvent.EventId, taskId, info.Name,
                    info.Version,
                    failedEvent.Reason, cause);
                failedException.FailureDetails = failedEvent.FailureDetails;

                TaskCompletionSource<string> tcs = info.Result;
                tcs.SetException(failedException);

                this.openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("SubOrchestrationInstanceFailed", failedEvent, taskId);
            }
        }

        public void HandleTimerFiredEvent(TimerFiredEvent timerFiredEvent)
        {
            int taskId = timerFiredEvent.TimerId;
            if (this.openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = this.openTasks[taskId];
                info.Result.SetResult(timerFiredEvent.TimerId.ToString());
                this.openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("TimerFired", timerFiredEvent, taskId);
            }
        }

        private void LogDuplicateEvent(string source, HistoryEvent historyEvent, int taskId)
        {
            TraceHelper.TraceSession(
                TraceEventType.Warning,
                "TaskOrchestrationContext-DuplicateEvent",
                OrchestrationInstance.InstanceId,
                "Duplicate {0} Event: {1}, type: {2}, ts: {3}",
                source,
                taskId.ToString(),
                historyEvent.EventType,
                historyEvent.Timestamp.ToString(CultureInfo.InvariantCulture));
        }

        public void HandleExecutionTerminatedEvent(ExecutionTerminatedEvent terminatedEvent)
        {
            CompleteOrchestration(terminatedEvent.Input, null, OrchestrationStatus.Terminated);
        }

        public void CompleteOrchestration(string result)
        {
            CompleteOrchestration(result, null, OrchestrationStatus.Completed);
        }

        public void HandleEventWhileSuspended(HistoryEvent historyEvent)
        {
            if (historyEvent.EventType != EventType.ExecutionSuspended)
            {
                this.eventsWhileSuspended.Enqueue(historyEvent);
            }
        }

        public void HandleExecutionSuspendedEvent(ExecutionSuspendedEvent suspendedEvent)
        {
            this.IsSuspended = true;

            // When the orchestrator is suspended, a task could potentially be added to the orchestratorActionsMap.
            // This could lead to the task being executed repeatedly without completion until the orchestrator is resumed.
            // To prevent this scenario, check if orchestratorActionsMap is empty before proceeding.
            if (this.orchestratorActionsMap.Any())
            {
                // If not, store its contents to a temporary dictionary to allow processing of the task later when orchestrator resumes.
                foreach (var pair in this.orchestratorActionsMap)
                {
                    this.suspendedActionsMap.Add(pair.Key, pair.Value);
                }
                this.orchestratorActionsMap.Clear();
            }
        }

        public void HandleExecutionResumedEvent(ExecutionResumedEvent resumedEvent, Action<HistoryEvent> eventProcessor)
        {
            this.IsSuspended = false;

            // Add the actions stored in the suspendedActionsMap before back to orchestratorActionsMap to ensure proper sequencing.
            if (this.suspendedActionsMap.Any())
            {
                foreach (var pair in this.suspendedActionsMap)
                {
                    this.orchestratorActionsMap.Add(pair.Key, pair.Value);
                }
                this.suspendedActionsMap.Clear();
            }

            while (eventsWhileSuspended.Count > 0)
            {
                eventProcessor(eventsWhileSuspended.Dequeue());
            }
        }

        public void FailOrchestration(Exception failure, OrchestrationRuntimeState runtimeState)
        {
            if (failure == null)
            {
                throw new ArgumentNullException(nameof(failure));
            }

            string reason = failure.Message;

            // string details is legacy, FailureDetails is the newer way to share failure information
            string details = null;
            FailureDetails failureDetails = null;

            // correlation 
            CorrelationTraceClient.Propagate(
                () =>
                {
                    CorrelationTraceClient.TrackException(failure);
                });

            if (failure is OrchestrationFailureException orchestrationFailureException)
            {
                if (this.ErrorPropagationMode == ErrorPropagationMode.UseFailureDetails)
                {
                    // When not serializing exceptions, we instead construct FailureDetails objects
                    failureDetails = orchestrationFailureException.FailureDetails;
                }
                else
                {
                    details = orchestrationFailureException.Details;
                }
            }
            else if (failure is TaskFailedException taskFailedException &&
                this.ErrorPropagationMode == ErrorPropagationMode.UseFailureDetails)
            {
                // Propagate the original FailureDetails
                failureDetails = taskFailedException.FailureDetails;
            }
            else
            {
                if (this.ErrorPropagationMode == ErrorPropagationMode.UseFailureDetails)
                {
                    failureDetails = new FailureDetails(failure);
                }
                else
                {
                    details = $"Unhandled exception while executing orchestration: {failure}\n\t{failure.StackTrace}";
                }
            }

            // save exception so it can be retrieved and sanitized during logging. See LogEvents.OrchestrationCompleted for more details
            runtimeState.Exception = failure;
            CompleteOrchestration(reason, details, OrchestrationStatus.Failed, failureDetails);
        }

        public void CompleteOrchestration(string result, string details, OrchestrationStatus orchestrationStatus, FailureDetails failureDetails = null)
        {
            int id = this.idCounter++;
            OrchestrationCompleteOrchestratorAction completedOrchestratorAction;
            if (orchestrationStatus == OrchestrationStatus.Completed && this.continueAsNew != null)
            {
                completedOrchestratorAction = this.continueAsNew;
            }
            else
            {
                if (this.executionCompletedOrTerminated)
                {
                    return;
                }

                this.executionCompletedOrTerminated = true;

                completedOrchestratorAction = new OrchestrationCompleteOrchestratorAction();
                completedOrchestratorAction.Result = result;
                completedOrchestratorAction.Details = details;
                completedOrchestratorAction.OrchestrationStatus = orchestrationStatus;
                completedOrchestratorAction.FailureDetails = failureDetails;
            }

            completedOrchestratorAction.Id = id;
            this.orchestratorActionsMap.Add(id, completedOrchestratorAction);
        }

        class OpenTaskInfo
        {
            public string Name { get; set; }

            public string Version { get; set; }

            public TaskCompletionSource<string> Result { get; set; }
        }
    }
}