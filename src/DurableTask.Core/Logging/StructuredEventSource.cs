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

namespace DurableTask.Core.Logging
{
    using System;
    using System.Diagnostics.Tracing;
    using System.Threading;

    // NOTE: This is intended to eventually replace the other DurableTask-Core provider
    [EventSource(Name = "DurableTask-Core")]
    class StructuredEventSource : EventSource
    {
        public static readonly StructuredEventSource Log = new StructuredEventSource();

        static readonly AsyncLocal<Guid> ActivityIdState = new AsyncLocal<Guid>();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
            // We use AsyncLocal to preserve activity IDs across async/await boundaries.
            ActivityIdState.Value = activityId;
            SetCurrentThreadActivityId(activityId);
        }

        [NonEvent]
        static void EnsureLogicalTraceActivityId()
        {
            Guid currentActivityId = ActivityIdState.Value;
            if (currentActivityId != CurrentThreadActivityId)
            {
                SetCurrentThreadActivityId(currentActivityId);
            }
        }

        bool IsEnabled(EventLevel level) => this.IsEnabled(level, EventKeywords.None);

        [Event(EventIds.TaskHubWorkerStarting, Level = EventLevel.Informational, Version = 1)]
        public void TaskHubWorkerStarting()
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.TaskHubWorkerStarting);
        }

        [Event(EventIds.TaskHubWorkerStarted, Level = EventLevel.Informational, Version = 1)]
        public void TaskHubWorkerStarted(long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.TaskHubWorkerStarted, LatencyMs);
        }

        [Event(EventIds.TaskHubWorkerStopping, Level = EventLevel.Informational, Version = 1)]
        public void TaskHubWorkerStopping(bool IsForced)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.TaskHubWorkerStopping, IsForced);
        }

        [Event(EventIds.TaskHubWorkerStopped, Level = EventLevel.Informational, Version = 1)]
        public void TaskHubWorkerStopped(long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.TaskHubWorkerStopped, LatencyMs);
        }

        [Event(EventIds.DispatcherStarting, Level = EventLevel.Verbose, Version = 1)]
        public void DispatcherStarting(string Dispatcher)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.DispatcherStarting, Dispatcher);
        }

        [Event(EventIds.DispatcherStopped, Level = EventLevel.Verbose, Version = 1)]
        public void DispatcherStopped(string Dispatcher)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.DispatcherStopped, Dispatcher);
        }

        [Event(EventIds.DispatchersStopping, Level = EventLevel.Verbose, Version = 1)]
        public void DispatchersStopping(string Dispatcher, int WorkItemCount, int ActiveFetcherCount)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.DispatchersStopping, Dispatcher, WorkItemCount, ActiveFetcherCount);
        }

        [Event(EventIds.FetchingWorkItem, Level = EventLevel.Verbose, Version = 1)]
        internal void FetchingWorkItem(
            string Dispatcher,
            int TimeoutSeconds,
            int WorkItemCount,
            int MaxWorkItemCount)
        {
            if (this.IsEnabled() && ActivityIdState.Value == Guid.Empty)
            {
                SetLogicalTraceActivityId(Guid.NewGuid());
            }

            if (this.IsEnabled(EventLevel.Verbose))
            {
                // CONSIDER: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.FetchingWorkItem,
                    Dispatcher,
                    TimeoutSeconds,
                    WorkItemCount,
                    MaxWorkItemCount);
            }
        }

        [Event(EventIds.FetchedWorkItem, Level = EventLevel.Verbose, Version = 1)]
        internal void FetchedWorkItem(
            string Dispatcher,
            string WorkItemId,
            long LatencyMs,
            int WorkItemCount,
            int MaxWorkItemCount)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                EnsureLogicalTraceActivityId();
                // CONSIDER: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.FetchedWorkItem,
                    Dispatcher,
                    WorkItemId,
                    LatencyMs,
                    WorkItemCount,
                    MaxWorkItemCount);
            }
        }

        [Event(EventIds.FetchWorkItemFailure, Level = EventLevel.Error, Version = 1)]
        internal void FetchWorkItemFailure(string Dispatcher, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.FetchWorkItemFailure, Dispatcher, Details);
        }

        [Event(EventIds.FetchingThrottled, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingThrottled(string Dispatcher, int WorkItemCount, int MaxWorkItemCount)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.FetchingThrottled, Dispatcher, WorkItemCount, MaxWorkItemCount);
        }

        [Event(EventIds.ProcessWorkItemStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessWorkItemStarting(string Dispatcher, string WorkItemId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.ProcessWorkItemStarting, Dispatcher, WorkItemId);
        }

        [Event(EventIds.ProcessWorkItemCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessWorkItemCompleted(string Dispatcher, string WorkItemId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.ProcessWorkItemCompleted, Dispatcher, WorkItemId);
        }

        [Event(EventIds.ProcessWorkItemFailed, Level = EventLevel.Error, Version = 1)]
        public void ProcessWorkItemFailed(string Dispatcher, string WorkItemId, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(EventIds.ProcessWorkItemFailed, Dispatcher, WorkItemId, Details);
        }

        [Event(EventIds.SchedulingOrchestration, Level = EventLevel.Informational, Version = 1)]
        internal void SchedulingOrchestration(
            string InstanceId,
            string ExecutionId,
            string TargetInstanceId,
            string TargetExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.SchedulingOrchestration,
                    InstanceId,
                    ExecutionId,
                    TargetInstanceId,
                    TargetExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes);
            }
        }

        [Event(EventIds.RaisingEvent, Level = EventLevel.Informational, Version = 1)]
        internal void RaisingEvent(
            string InstanceId,
            string ExecutionId,
            string TargetInstanceId,
            string Name,
            int TaskEventId,
            int SizeInBytes)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RaisingEvent,
                    InstanceId ?? string.Empty,
                    ExecutionId ?? string.Empty,
                    TargetInstanceId,
                    Name,
                    TaskEventId,
                    SizeInBytes);
            }
        }

        [Event(EventIds.TerminatingInstance, Level = EventLevel.Informational, Version = 1)]
        internal void TerminatingInstance(string InstanceId, string ExecutionId, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                EventIds.TerminatingInstance,
                InstanceId,
                ExecutionId ?? string.Empty,
                Details ?? string.Empty);
        }

        [Event(EventIds.WaitingForInstance, Level = EventLevel.Informational, Version = 1)]
        internal void WaitingForInstance(
            string InstanceId,
            string ExecutionId,
            int TimeoutSeconds)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                EventIds.WaitingForInstance,
                InstanceId,
                ExecutionId ?? string.Empty,
                TimeoutSeconds);
        }

        [Event(EventIds.FetchingInstanceState, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingInstanceState(string InstanceId, string ExecutionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                EventIds.FetchingInstanceState,
                InstanceId,
                ExecutionId ?? string.Empty);
        }

        [Event(EventIds.FetchingInstanceHistory, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingInstanceHistory(string InstanceId, string ExecutionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                EventIds.FetchingInstanceHistory,
                InstanceId,
                ExecutionId ?? string.Empty);
        }

        [Event(EventIds.ProcessingOrchestrationMessage, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessingOrchestrationMessage(
            string InstanceId,
            string ExecutionId,
            string EventType,
            int TaskEventId)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.ProcessingOrchestrationMessage,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    EventType,
                    TaskEventId);
            }
        }

        [Event(EventIds.OrchestrationExecuting, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationExecuting(
            string InstanceId,
            string ExecutionId,
            string Name)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                EventIds.OrchestrationExecuting,
                InstanceId,
                ExecutionId,
                Name);
        }

        [Event(EventIds.OrchestrationExecuted, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationExecuted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int ActionCount)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.OrchestrationExecuted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    ActionCount);
            }
        }

        [Event(EventIds.SchedulingActivity, Level = EventLevel.Informational, Version = 1)]
        internal void SchedulingActivity(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.SchedulingActivity,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes);
            }
        }

        [Event(EventIds.CreatingTimer, Level = EventLevel.Informational, Version = 1)]
        internal void CreatingTimer(
            string InstanceId,
            string ExecutionId,
            DateTime FireAt,
            int TaskEventId,
            bool IsInternal)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.CreatingTimer,
                    InstanceId,
                    ExecutionId,
                    FireAt,
                    TaskEventId,
                    IsInternal);
            }
        }

        [Event(EventIds.OrchestrationCompleted, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationCompleted(
            string InstanceId,
            string ExecutionId,
            string Status,
            string Details,
            int SizeInBytes)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.OrchestrationCompleted,
                    InstanceId,
                    ExecutionId,
                    Status,
                    Details,
                    SizeInBytes);
            }
        }

        [Event(EventIds.OrchestrationAborted, Level = EventLevel.Warning, Version = 1)]
        internal void OrchestrationAborted(
            string InstanceId,
            string ExecutionId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                EnsureLogicalTraceActivityId();
                this.WriteEvent(
                    EventIds.OrchestrationAborted,
                    InstanceId,
                    ExecutionId,
                    Details);
            }
        }

        [Event(EventIds.DiscardingMessage, Level = EventLevel.Warning, Version = 1)]
        internal void DiscardingMessage(
            string InstanceId,
            string ExecutionId,
            string EventType,
            int TaskEventId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.DiscardingMessage,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    EventType,
                    TaskEventId,
                    Details);
            }
        }

        [Event(EventIds.TaskActivityStarting, Level = EventLevel.Informational, Version = 1)]
        internal void TaskActivityStarting(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityStarting,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId);
            }
        }

        [Event(EventIds.TaskActivityCompleted, Level = EventLevel.Informational, Version = 1)]
        internal void TaskActivityCompleted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityCompleted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes);
            }
        }

        [Event(EventIds.TaskActivityFailure, Level = EventLevel.Warning, Version = 1)]
        internal void TaskActivityFailure(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityFailure,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details);
            }
        }

        [Event(EventIds.TaskActivityAborted, Level = EventLevel.Warning, Version = 1)]
        internal void TaskActivityAborted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityAborted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details);
            }
        }

        [Event(EventIds.TaskActivityDispatcherError, Level = EventLevel.Error, Version = 1)]
        internal void TaskActivityDispatcherError(
            string InstanceId,
            string ExecutionId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                EnsureLogicalTraceActivityId();
                this.WriteEvent(
                    EventIds.TaskActivityDispatcherError,
                    InstanceId,
                    ExecutionId,
                    Details);
            }
        }

        [Event(EventIds.RenewActivityMessageStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewActivityMessageStarting(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewActivityMessageStarting,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId);
            }
        }

        [Event(EventIds.RenewActivityMessageCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewActivityMessageCompleted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            DateTime NextRenewal)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                EnsureLogicalTraceActivityId();
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewActivityMessageCompleted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    NextRenewal);
            }
        }

        [Event(EventIds.RenewActivityMessageFailed, Level = EventLevel.Error, Version = 1)]
        internal void RenewActivityMessageFailed(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                EnsureLogicalTraceActivityId();
                this.WriteEvent(
                    EventIds.RenewActivityMessageFailed,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details);
            }
        }
    }
}
