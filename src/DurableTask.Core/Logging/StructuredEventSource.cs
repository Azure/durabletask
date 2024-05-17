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
    /// <summary>
    /// Event source logger for DurableTask.Core that uses structured events rather than log messages.
    /// </summary>
    [EventSource(Name = "DurableTask-Core")]
    class StructuredEventSource : EventSource
    {
        internal static readonly StructuredEventSource Log = new StructuredEventSource();

        static readonly AsyncLocal<Guid> ActivityIdState = new AsyncLocal<Guid>();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
            // We use AsyncLocal to preserve activity IDs across async/await boundaries.
            ActivityIdState.Value = activityId;
            SetCurrentThreadActivityId(activityId);
        }

        [NonEvent]
        internal static void EnsureLogicalTraceActivityId()
        {
            Guid currentActivityId = ActivityIdState.Value;
            if (currentActivityId != CurrentThreadActivityId)
            {
                SetCurrentThreadActivityId(currentActivityId);
            }
        }

        bool IsEnabled(EventLevel level) => this.IsEnabled(level, EventKeywords.None);

        [Event(EventIds.TaskHubWorkerStarting, Level = EventLevel.Informational, Version = 1)]
        internal void TaskHubWorkerStarting(string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(EventIds.TaskHubWorkerStarting, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.TaskHubWorkerStarted, Level = EventLevel.Informational, Version = 1)]
        internal void TaskHubWorkerStarted(long LatencyMs, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(EventIds.TaskHubWorkerStarted, LatencyMs, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.TaskHubWorkerStopping, Level = EventLevel.Informational, Version = 1)]
        internal void TaskHubWorkerStopping(bool IsForced, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(EventIds.TaskHubWorkerStopping, IsForced, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.TaskHubWorkerStopped, Level = EventLevel.Informational, Version = 1)]
        internal void TaskHubWorkerStopped(long LatencyMs, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(EventIds.TaskHubWorkerStopped, LatencyMs, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.DispatcherStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void DispatcherStarting(string Dispatcher, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(EventIds.DispatcherStarting, Dispatcher, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.DispatcherStopped, Level = EventLevel.Verbose, Version = 1)]
        internal void DispatcherStopped(string Dispatcher, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(EventIds.DispatcherStopped, Dispatcher, AppName, ExtensionVersion);
            }
        }

        [Event(EventIds.DispatchersStopping, Level = EventLevel.Verbose, Version = 1)]
        internal void DispatchersStopping(
            string Dispatcher,
            int WorkItemCount,
            int ActiveFetcherCount,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(
                    EventIds.DispatchersStopping,
                    Dispatcher,
                    WorkItemCount,
                    ActiveFetcherCount,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchWorkItemStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void FetchWorkItemStarting(
            string Dispatcher,
            int TimeoutSeconds,
            int WorkItemCount,
            int MaxWorkItemCount,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled())
            {
                // Fetching a work item is always the start of a new operation
                SetLogicalTraceActivityId(Guid.NewGuid());
            }

            if (this.IsEnabled(EventLevel.Verbose))
            {
                // CONSIDER: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.FetchWorkItemStarting,
                    Dispatcher,
                    TimeoutSeconds,
                    WorkItemCount,
                    MaxWorkItemCount,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchWorkItemCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void FetchWorkItemCompleted(
            string Dispatcher,
            string WorkItemId,
            long LatencyMs,
            int WorkItemCount,
            int MaxWorkItemCount,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // CONSIDER: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.FetchWorkItemCompleted,
                    Dispatcher,
                    WorkItemId,
                    LatencyMs,
                    WorkItemCount,
                    MaxWorkItemCount,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchWorkItemFailure, Level = EventLevel.Error, Version = 1)]
        internal void FetchWorkItemFailure(string Dispatcher, string Details, string AppName, string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                this.WriteEvent(
                    EventIds.FetchWorkItemFailure,
                    Dispatcher,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchingThrottled, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingThrottled(
            string Dispatcher,
            int WorkItemCount,
            int MaxWorkItemCount,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.FetchingThrottled,
                    Dispatcher,
                    WorkItemCount,
                    MaxWorkItemCount,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.ProcessWorkItemStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessWorkItemStarting(
            string Dispatcher,
            string WorkItemId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(
                    EventIds.ProcessWorkItemStarting,
                    Dispatcher,
                    WorkItemId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.ProcessWorkItemCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessWorkItemCompleted(
            string Dispatcher,
            string WorkItemId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(
                    EventIds.ProcessWorkItemCompleted,
                    Dispatcher,
                    WorkItemId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.ProcessWorkItemFailed, Level = EventLevel.Error, Version = 1)]
        internal void ProcessWorkItemFailed(
            string Dispatcher,
            string WorkItemId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                this.WriteEvent(
                    EventIds.ProcessWorkItemFailed,
                    Dispatcher,
                    WorkItemId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.SchedulingOrchestration, Level = EventLevel.Informational, Version = 1)]
        internal void SchedulingOrchestration(
            string InstanceId,
            string ExecutionId,
            string TargetInstanceId,
            string TargetExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.SchedulingOrchestration,
                    InstanceId,
                    ExecutionId,
                    TargetInstanceId,
                    TargetExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RaisingEvent, Level = EventLevel.Informational, Version = 1)]
        internal void RaisingEvent(
            string InstanceId,
            string ExecutionId,
            string TargetInstanceId,
            string Name,
            int TaskEventId,
            int SizeInBytes,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RaisingEvent,
                    InstanceId ?? string.Empty,
                    ExecutionId ?? string.Empty,
                    TargetInstanceId,
                    Name,
                    TaskEventId,
                    SizeInBytes,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TerminatingInstance, Level = EventLevel.Informational, Version = 1)]
        internal void TerminatingInstance(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.TerminatingInstance,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    Details ?? string.Empty,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.SuspendingInstance, Level = EventLevel.Informational, Version = 1)]
        internal void SuspendingInstance(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.SuspendingInstance,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    Details ?? string.Empty,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.ResumingInstance, Level = EventLevel.Informational, Version = 1)]
        internal void ResumingInstance(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.ResumingInstance,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    Details ?? string.Empty,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.WaitingForInstance, Level = EventLevel.Informational, Version = 1)]
        internal void WaitingForInstance(
            string InstanceId,
            string ExecutionId,
            int TimeoutSeconds,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.WaitingForInstance,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    TimeoutSeconds,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchingInstanceState, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingInstanceState(
            string InstanceId,
            string ExecutionId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.FetchingInstanceState,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.FetchingInstanceHistory, Level = EventLevel.Informational, Version = 1)]
        internal void FetchingInstanceHistory(
            string InstanceId,
            string ExecutionId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.FetchingInstanceHistory,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.ProcessingOrchestrationMessage, Level = EventLevel.Verbose, Version = 1)]
        internal void ProcessingOrchestrationMessage(
            string InstanceId,
            string ExecutionId,
            string EventType,
            int TaskEventId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.ProcessingOrchestrationMessage,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    EventType,
                    TaskEventId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.OrchestrationExecuting, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationExecuting(
            string InstanceId,
            string ExecutionId,
            string Name,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                this.WriteEvent(
                    EventIds.OrchestrationExecuting,
                    InstanceId,
                    ExecutionId,
                    Name,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.OrchestrationExecuted, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationExecuted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int ActionCount,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.OrchestrationExecuted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    ActionCount,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.SchedulingActivity, Level = EventLevel.Informational, Version = 1)]
        internal void SchedulingActivity(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.SchedulingActivity,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.CreatingTimer, Level = EventLevel.Informational, Version = 1)]
        internal void CreatingTimer(
            string InstanceId,
            string ExecutionId,
            DateTime FireAt,
            int TaskEventId,
            bool IsInternal,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.CreatingTimer,
                    InstanceId,
                    ExecutionId,
                    FireAt,
                    TaskEventId,
                    IsInternal,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.OrchestrationCompleted, Level = EventLevel.Informational, Version = 1)]
        internal void OrchestrationCompleted(
            string InstanceId,
            string ExecutionId,
            string RuntimeStatus,
            string Details,
            int SizeInBytes,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.OrchestrationCompleted,
                    InstanceId,
                    ExecutionId,
                    RuntimeStatus,
                    Details ?? string.Empty,
                    SizeInBytes,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.OrchestrationAborted, Level = EventLevel.Warning, Version = 1)]
        internal void OrchestrationAborted(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                this.WriteEvent(
                    EventIds.OrchestrationAborted,
                    InstanceId,
                    ExecutionId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.DiscardingMessage, Level = EventLevel.Warning, Version = 1)]
        internal void DiscardingMessage(
            string InstanceId,
            string ExecutionId,
            string EventType,
            int TaskEventId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.DiscardingMessage,
                    InstanceId,
                    ExecutionId ?? string.Empty,
                    EventType,
                    TaskEventId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.EntityBatchExecuting, Level = EventLevel.Informational, Version = 1)]
        internal void EntityBatchExecuting(
            string InstanceId,
            int OperationCount,
            int EntityStateLength,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.EntityBatchExecuting,
                    InstanceId,
                    OperationCount,
                    EntityStateLength,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.EntityBatchExecuted, Level = EventLevel.Informational, Version = 1)]
        internal void EntityBatchExecuted(
            string InstanceId,
            int OperationCount,
            int ResultCount,
            int ErrorCount,
            int ActionCount,
            int EntityStateLength,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.EntityBatchExecuted,
                    InstanceId,
                    OperationCount,
                    ResultCount,
                    ErrorCount,
                    ActionCount,
                    EntityStateLength,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.EntityLockAcquired, Level = EventLevel.Informational, Version = 1)]
        internal void EntityLockAcquired(
            string EntityId,
            string InstanceId,
            string ExecutionId,
            Guid CriticalSectionId,
            string LockSet,
            int Position,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.EntityLockAcquired,
                    EntityId,
                    InstanceId,
                    ExecutionId,
                    CriticalSectionId,
                    LockSet,
                    Position,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.EntityLockReleased, Level = EventLevel.Informational, Version = 1)]
        internal void EntityLockReleased(
            string EntityId,
            string InstanceId,
            string Id,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.EntityLockReleased,
                    EntityId, 
                    InstanceId,
                    Id,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TaskActivityStarting, Level = EventLevel.Informational, Version = 1)]
        internal void TaskActivityStarting(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityStarting,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TaskActivityCompleted, Level = EventLevel.Informational, Version = 1)]
        internal void TaskActivityCompleted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            int SizeInBytes,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Informational))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityCompleted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    SizeInBytes,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TaskActivityFailure, Level = EventLevel.Warning, Version = 1)]
        internal void TaskActivityFailure(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityFailure,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TaskActivityAborted, Level = EventLevel.Warning, Version = 1)]
        internal void TaskActivityAborted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Warning))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.TaskActivityAborted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.TaskActivityDispatcherError, Level = EventLevel.Error, Version = 1)]
        internal void TaskActivityDispatcherError(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                this.WriteEvent(
                    EventIds.TaskActivityDispatcherError,
                    InstanceId,
                    ExecutionId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewActivityMessageStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewActivityMessageStarting(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewActivityMessageStarting,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewActivityMessageCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewActivityMessageCompleted(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            DateTime NextRenewal,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewActivityMessageCompleted,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    NextRenewal,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewActivityMessageFailed, Level = EventLevel.Error, Version = 1)]
        internal void RenewActivityMessageFailed(
            string InstanceId,
            string ExecutionId,
            string Name,
            int TaskEventId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                this.WriteEvent(
                    EventIds.RenewActivityMessageFailed,
                    InstanceId,
                    ExecutionId,
                    Name,
                    TaskEventId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewOrchestrationWorkItemStarting, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewOrchestrationWorkItemStarting(
            string InstanceId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewOrchestrationWorkItemStarting,
                    InstanceId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewOrchestrationWorkItemCompleted, Level = EventLevel.Verbose, Version = 1)]
        internal void RenewOrchestrationWorkItemCompleted(
            string InstanceId,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                // TODO: Use WriteEventCore for better performance
                this.WriteEvent(
                    EventIds.RenewOrchestrationWorkItemCompleted,
                    InstanceId,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.RenewOrchestrationWorkItemFailed, Level = EventLevel.Error, Version = 1)]
        internal void RenewOrchestrationWorkItemFailed(
            string InstanceId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Error))
            {
                this.WriteEvent(
                    EventIds.RenewOrchestrationWorkItemFailed,
                    InstanceId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }

        [Event(EventIds.OrchestrationDebugTrace, Level = EventLevel.Verbose, Version = 1)]
        internal void OrchestrationDebugTrace(
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            if (this.IsEnabled(EventLevel.Verbose))
            {
                this.WriteEvent(
                    EventIds.OrchestrationDebugTrace,
                    InstanceId,
                    ExecutionId,
                    Details,
                    AppName,
                    ExtensionVersion);
            }
        }
    }
}
