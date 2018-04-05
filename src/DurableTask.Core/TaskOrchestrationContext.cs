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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;

    internal class TaskOrchestrationContext : OrchestrationContext
    {
        readonly JsonDataConverter dataConverter;
        readonly IDictionary<int, OpenTaskInfo> openTasks;
        readonly IDictionary<int, OrchestratorAction> orchestratorActionsMap;
        readonly TaskScheduler taskScheduler;
        OrchestrationCompleteOrchestratorAction continueAsNew;
        bool executionTerminated;
        int idCounter;

        public TaskOrchestrationContext(OrchestrationInstance orchestrationInstance, TaskScheduler taskScheduler)
        {
            this.taskScheduler = taskScheduler;
            openTasks = new Dictionary<int, OpenTaskInfo>();
            orchestratorActionsMap = new Dictionary<int, OrchestratorAction>();
            idCounter = 0;
            dataConverter = new JsonDataConverter();
            OrchestrationInstance = orchestrationInstance;
            IsReplaying = false;
        }

        public IEnumerable<OrchestratorAction> OrchestratorActions
        {
            get { return orchestratorActionsMap.Values; }
        }

        public bool HasOpenTasks
        {
            get { return openTasks.Count > 0; }
        }

        public override async Task<TResult> ScheduleTask<TResult>(string name, string version,
            params object[] parameters)
        {
            TResult result = await ScheduleTaskToWorker<TResult>(name, version, null, parameters);

            return result;
        }

        public async Task<TResult> ScheduleTaskToWorker<TResult>(string name, string version, string tasklist,
            params object[] parameters)
        {
            object result = await ScheduleTaskInternal(name, version, tasklist, typeof (TResult), parameters);

            return (TResult) result;
        }

        public async Task<object> ScheduleTaskInternal(string name, string version, string tasklist, Type resultType,
            params object[] parameters)
        {
            int id = idCounter++;
            string serializedInput = dataConverter.Serialize(parameters);
            var scheduleTaskTaskAction = new ScheduleTaskOrchestratorAction
            {
                Id = id,
                Name = name,
                Version = version,
                Tasklist = tasklist,
                Input = serializedInput,
            };

            orchestratorActionsMap.Add(id, scheduleTaskTaskAction);

            var tcs = new TaskCompletionSource<string>();
            openTasks.Add(id, new OpenTaskInfo {Name = name, Version = version, Result = tcs});

            string serializedResult = await tcs.Task;

            return dataConverter.Deserialize(serializedResult, resultType);
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
            int id = idCounter++;
            string serializedInput = dataConverter.Serialize(input);

            string actualInstanceId = instanceId;
            if (string.IsNullOrEmpty(actualInstanceId))
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

            orchestratorActionsMap.Add(id, action);

            var tcs = new TaskCompletionSource<string>();
            openTasks.Add(id, new OpenTaskInfo {Name = name, Version = version, Result = tcs});

            string serializedResult = await tcs.Task;

            return dataConverter.Deserialize<T>(serializedResult);
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
            string serializedInput = dataConverter.Serialize(input);

            continueAsNew = new OrchestrationCompleteOrchestratorAction
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
            int id = idCounter++;
            var createTimerOrchestratorAction = new CreateTimerOrchestratorAction
            {
                Id = id,
                FireAt = fireAt,
            };

            orchestratorActionsMap.Add(id, createTimerOrchestratorAction);

            var tcs = new TaskCompletionSource<string>();
            openTasks.Add(id, new OpenTaskInfo {Name = null, Version = null, Result = tcs});

            if (cancelToken != CancellationToken.None)
            {
                cancelToken.Register(s =>
                {
                    if (tcs.TrySetCanceled())
                    {
                        openTasks.Remove(id);
                    }
                }, tcs);
            }

            await tcs.Task;

            return state;
        }

        public void HandleTaskScheduledEvent(TaskScheduledEvent scheduledEvent)
        {
            int taskId = scheduledEvent.EventId;
            if (orchestratorActionsMap.ContainsKey(taskId))
            {
                orchestratorActionsMap.Remove(taskId);
            }
            else
            {
                throw new NonDeterministicOrchestrationException(scheduledEvent.EventId,
                    string.Format("TaskScheduledEvent: {0} {1} {2} {3}",
                        scheduledEvent.EventId, scheduledEvent.EventType, scheduledEvent.Name, scheduledEvent.Version));
            }
        }

        public void HandleTimerCreatedEvent(TimerCreatedEvent timerCreatedEvent)
        {
            int taskId = timerCreatedEvent.EventId;
            if (taskId == FrameworkConstants.FakeTimerIdToSplitDecision)
            {
                // This is our dummy timer to split decision for avoiding 100 messages per transaction service bus limit
                return;
            }

            if (orchestratorActionsMap.ContainsKey(taskId))
            {
                orchestratorActionsMap.Remove(taskId);
            }
            else
            {
                throw new NonDeterministicOrchestrationException(timerCreatedEvent.EventId,
                    string.Format("TimerCreatedEvent: {0} {1}",
                        timerCreatedEvent.EventId, timerCreatedEvent.EventType));
            }
        }

        public void HandleSubOrchestrationCreatedEvent(SubOrchestrationInstanceCreatedEvent subOrchestrationCreateEvent)
        {
            int taskId = subOrchestrationCreateEvent.EventId;
            if (orchestratorActionsMap.ContainsKey(taskId))
            {
                orchestratorActionsMap.Remove(taskId);
            }
            else
            {
                throw new NonDeterministicOrchestrationException(subOrchestrationCreateEvent.EventId,
                    string.Format("SubOrchestrationInstanceCreatedEvent: {0} {1} {2} {3} {4}",
                        subOrchestrationCreateEvent.EventId,
                        subOrchestrationCreateEvent.EventType,
                        subOrchestrationCreateEvent.Name,
                        subOrchestrationCreateEvent.Version,
                        subOrchestrationCreateEvent.InstanceId));
            }
        }

        public void HandleTaskCompletedEvent(TaskCompletedEvent completedEvent)
        {
            int taskId = completedEvent.TaskScheduledId;
            if (openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = openTasks[taskId];
                info.Result.SetResult(completedEvent.Result);

                openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("TaskCompleted", completedEvent, taskId);
            }
        }

        public void HandleTaskFailedEvent(TaskFailedEvent failedEvent)
        {
            int taskId = failedEvent.TaskScheduledId;
            if (openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = openTasks[taskId];
                Exception cause = Utils.RetrieveCause(failedEvent.Details, dataConverter);

                var taskFailedException = new TaskFailedException(failedEvent.EventId, taskId, info.Name, info.Version,
                    failedEvent.Reason, cause);

                TaskCompletionSource<string> tcs = info.Result;
                tcs.SetException(taskFailedException);

                openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("TaskFailed", failedEvent, taskId);
            }
        }

        public void HandleSubOrchestrationInstanceCompletedEvent(SubOrchestrationInstanceCompletedEvent completedEvent)
        {
            int taskId = completedEvent.TaskScheduledId;
            if (openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = openTasks[taskId];
                info.Result.SetResult(completedEvent.Result);

                openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("SubOrchestrationInstanceCompleted", completedEvent, taskId);
            }
        }

        public void HandleSubOrchestrationInstanceFailedEvent(SubOrchestrationInstanceFailedEvent failedEvent)
        {
            int taskId = failedEvent.TaskScheduledId;
            if (openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = openTasks[taskId];
                Exception cause = Utils.RetrieveCause(failedEvent.Details, dataConverter);
                var failedException = new SubOrchestrationFailedException(failedEvent.EventId, taskId, info.Name,
                    info.Version,
                    failedEvent.Reason, cause);
                TaskCompletionSource<string> tcs = info.Result;
                tcs.SetException(failedException);

                openTasks.Remove(taskId);
            }
            else
            {
                LogDuplicateEvent("SubOrchestrationInstanceFailed", failedEvent, taskId);
            }
        }

        public void HandleTimerFiredEvent(TimerFiredEvent timerFiredEvent)
        {
            int taskId = timerFiredEvent.TimerId;
            if (openTasks.ContainsKey(taskId))
            {
                OpenTaskInfo info = openTasks[taskId];
                info.Result.SetResult(timerFiredEvent.TimerId.ToString());
                openTasks.Remove(taskId);
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
                this.OrchestrationInstance.InstanceId,
                "Duplicate {0} Event: {1}, type: {2}, ts: {3}",
                source,
                taskId.ToString(),
                historyEvent.EventType,
                historyEvent.Timestamp.ToString(CultureInfo.InvariantCulture));
        }

        public void HandleExecutionTerminatedEvent(ExecutionTerminatedEvent terminatedEvent)
        {
            if (!executionTerminated)
            {
                executionTerminated = true;
                CompleteOrchestration(terminatedEvent.Input, null, OrchestrationStatus.Terminated);
            }
        }

        public void CompleteOrchestration(string result)
        {
            if (!executionTerminated)
            {
                CompleteOrchestration(result, null, OrchestrationStatus.Completed);
            }
        }

        public void FailOrchestration(Exception failure)
        {
            if (failure == null)
            {
                throw new ArgumentNullException("failure");
            }

            string reason = failure.Message;
            string details = null;
            var orchestrationFailureException = failure as OrchestrationFailureException;
            if (orchestrationFailureException != null)
            {
                details = orchestrationFailureException.Details;
            }
            else
            {
                details = string.Format("Unhandled exception while executing orchestration: {0}\n\t{1}", failure,
                    failure.StackTrace);
            }

            CompleteOrchestration(reason, details, OrchestrationStatus.Failed);
        }

        public void CompleteOrchestration(string result, string details, OrchestrationStatus orchestrationStatus)
        {
            int id = idCounter++;
            OrchestrationCompleteOrchestratorAction completedOrchestratorAction;
            if (orchestrationStatus == OrchestrationStatus.Completed && continueAsNew != null)
            {
                completedOrchestratorAction = continueAsNew;
            }
            else
            {
                completedOrchestratorAction = new OrchestrationCompleteOrchestratorAction();
                completedOrchestratorAction.Result = result;
                completedOrchestratorAction.Details = details;
                completedOrchestratorAction.OrchestrationStatus = orchestrationStatus;
            }

            completedOrchestratorAction.Id = id;
            orchestratorActionsMap.Add(id, completedOrchestratorAction);
        }

        class OpenTaskInfo
        {
            public string Name { get; set; }
            public string Version { get; set; }
            public TaskCompletionSource<string> Result { get; set; }
        }
    }
}