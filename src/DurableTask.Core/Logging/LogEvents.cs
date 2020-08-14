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
    using System.Text;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// This class defines all log events supported by DurableTask.Core.
    /// </summary>
    /// <remarks>
    /// Each inner-class represents a single log event that derives from <see cref="StructuredLogEvent"/> and
    /// optionally implements <see cref="IEventSourceEvent"/>.
    /// </remarks>
    static class LogEvents
    {
        internal class TaskHubWorkerStarting : StructuredLogEvent, IEventSourceEvent
        {
            public override EventId EventId => new EventId(
                EventIds.TaskHubWorkerStarted,
                nameof(EventIds.TaskHubWorkerStarted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => "Durable task hub worker is starting";

            void IEventSourceEvent.WriteEventSource() => 
                StructuredEventSource.Log.TaskHubWorkerStarting(Utils.AppName, Utils.PackageVersion);
        }

        internal class TaskHubWorkerStarted : StructuredLogEvent, IEventSourceEvent
        {
            public TaskHubWorkerStarted(TimeSpan latency)
            {
                this.LatencyMs = (long)latency.TotalMilliseconds;
            }

            public override EventId EventId => new EventId(
                EventIds.TaskHubWorkerStarted,
                nameof(EventIds.TaskHubWorkerStarted));

            public override LogLevel Level => LogLevel.Information;

            [StructuredLogField]
            public long LatencyMs { get; }

            protected override string CreateLogMessage() => 
                $"Durable task hub worker started successfully after {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskHubWorkerStarted(this.LatencyMs, Utils.AppName, Utils.PackageVersion);
        }

        internal class TaskHubWorkerStopping : StructuredLogEvent, IEventSourceEvent
        {
            public TaskHubWorkerStopping(bool isForced)
            {
                this.IsForced = isForced;
            }

            [StructuredLogField]
            public bool IsForced { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskHubWorkerStopping,
                nameof(EventIds.TaskHubWorkerStopping));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Durable task hub worker is stopping (isForced = {this.IsForced})";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskHubWorkerStopping(this.IsForced, Utils.AppName, Utils.PackageVersion);
        }

        internal class TaskHubWorkerStopped : StructuredLogEvent, IEventSourceEvent
        {
            public TaskHubWorkerStopped(TimeSpan latency)
            {
                this.LatencyMs = (long)latency.TotalMilliseconds;
            }

            public override EventId EventId => new EventId(
                EventIds.TaskHubWorkerStopped,
                nameof(EventIds.TaskHubWorkerStopped));

            public override LogLevel Level => LogLevel.Information;

            [StructuredLogField]
            public long LatencyMs { get; }

            protected override string CreateLogMessage() => 
                $"Durable task hub worker stopped successfully after {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskHubWorkerStopped(this.LatencyMs, Utils.AppName, Utils.PackageVersion);
        }

        internal class DispatcherStarting : StructuredLogEvent, IEventSourceEvent
        {
            public DispatcherStarting(WorkItemDispatcherContext context)
            {
                this.Dispatcher = context.GetDisplayName();
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            public override EventId EventId => new EventId(
                EventIds.DispatcherStarting,
                nameof(EventIds.DispatcherStarting));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() => $"{this.Dispatcher}: Starting dispatch loop";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.DispatcherStarting(this.Dispatcher, Utils.AppName, Utils.PackageVersion);
        }

        internal class DispatcherStopped : StructuredLogEvent, IEventSourceEvent
        {
            public DispatcherStopped(WorkItemDispatcherContext context)
            {
                this.Dispatcher = context.GetDisplayName();
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            public override EventId EventId => new EventId(
                EventIds.DispatcherStopped,
                nameof(EventIds.DispatcherStopped));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() => $"{this.Dispatcher}: Stopped dispatch loop";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.DispatcherStopped(this.Dispatcher, Utils.AppName, Utils.PackageVersion);
        }

        internal class DispatchersStopping : StructuredLogEvent, IEventSourceEvent
        {
            public DispatchersStopping(string name, string id, int concurrentWorkItemCount, int activeFetchers)
            {
                // Use a fake dispatcher name with the same basic pattern
                this.Dispatcher = new WorkItemDispatcherContext(name, id, "*").GetDisplayName();
                this.WorkItemCount = concurrentWorkItemCount;
                this.ActiveFetcherCount = activeFetchers;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public int WorkItemCount { get; }

            [StructuredLogField]
            public int ActiveFetcherCount { get; }

            public override EventId EventId => new EventId(
                EventIds.DispatchersStopping,
                nameof(EventIds.DispatchersStopping));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Dispatchers are draining. Remaining work items: {this.WorkItemCount}. " +
                $"Remaining work item fetchers: {this.ActiveFetcherCount}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.DispatchersStopping(
                    this.Dispatcher,
                    this.WorkItemCount,
                    this.ActiveFetcherCount,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class FetchWorkItemStarting : StructuredLogEvent, IEventSourceEvent
        {
            public FetchWorkItemStarting(
                WorkItemDispatcherContext context,
                TimeSpan timeout,
                int concurrentWorkItemCount,
                int maxConcurrentWorkItems)
            {
                this.Dispatcher = context.GetDisplayName();
                this.TimeoutSeconds = (int)timeout.TotalSeconds;
                this.WorkItemCount = concurrentWorkItemCount;
                this.MaxWorkItemCount = maxConcurrentWorkItems;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public int TimeoutSeconds { get; }

            [StructuredLogField]
            public int WorkItemCount { get; }

            [StructuredLogField]
            public int MaxWorkItemCount { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchWorkItemStarting,
                nameof(EventIds.FetchWorkItemStarting));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Fetching next work item. Current active work-item count: {this.WorkItemCount}. " +
                $"Maximum active work-item count: {this.MaxWorkItemCount}. Timeout: {this.TimeoutSeconds}s";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchWorkItemStarting(
                    this.Dispatcher,
                    this.TimeoutSeconds,
                    this.WorkItemCount,
                    this.MaxWorkItemCount,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class FetchWorkItemCompleted : StructuredLogEvent, IEventSourceEvent
        {
            public FetchWorkItemCompleted(
                WorkItemDispatcherContext context,
                string workItemId,
                TimeSpan latency,
                int concurrentWorkItemCount,
                int maxConcurrentWorkItems)
            {
                this.Dispatcher = context.GetDisplayName();
                this.WorkItemId = workItemId;
                this.LatencyMs = (long)latency.TotalMilliseconds;
                this.WorkItemCount = concurrentWorkItemCount;
                this.MaxWorkItemCount = maxConcurrentWorkItems;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public string WorkItemId { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public int WorkItemCount { get; }

            [StructuredLogField]
            public int MaxWorkItemCount { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchWorkItemCompleted,
                nameof(EventIds.FetchWorkItemCompleted));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Fetched next work item '{this.WorkItemId}' after {this.LatencyMs}ms. " +
                $"Current active work-item count: {this.WorkItemCount}. Maximum active work-item count: {this.MaxWorkItemCount}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchWorkItemCompleted(
                    this.Dispatcher,
                    this.WorkItemId,
                    this.LatencyMs,
                    this.WorkItemCount,
                    this.MaxWorkItemCount,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class FetchWorkItemFailure : StructuredLogEvent, IEventSourceEvent
        {
            public FetchWorkItemFailure(WorkItemDispatcherContext context, Exception exception)
            {
                this.Dispatcher = context.GetDisplayName();
                this.Details = exception?.ToString() ?? string.Empty;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchWorkItemFailure,
                nameof(EventIds.FetchWorkItemFailure));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Failed to fetch a work-item: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchWorkItemFailure(
                    this.Dispatcher,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class FetchingThrottled : StructuredLogEvent, IEventSourceEvent
        {
            public FetchingThrottled(
                WorkItemDispatcherContext context,
                int concurrentWorkItemCount,
                int maxConcurrentWorkItems)
            {
                this.Dispatcher = context.GetDisplayName();
                this.WorkItemCount = concurrentWorkItemCount;
                this.MaxWorkItemCount = maxConcurrentWorkItems;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public int WorkItemCount { get; }

            [StructuredLogField]
            public int MaxWorkItemCount { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchingThrottled,
                nameof(EventIds.FetchingThrottled));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Delaying work item fetching because the current active work-item count ({this.WorkItemCount}) " +
                $"exceeds the configured maximum active work-item count ({this.MaxWorkItemCount})";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchingThrottled(
                    this.Dispatcher,
                    this.WorkItemCount,
                    this.MaxWorkItemCount,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class ProcessWorkItemStarting : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessWorkItemStarting(WorkItemDispatcherContext context, string workItemId)
            {
                this.Dispatcher = context.GetDisplayName();
                this.WorkItemId = workItemId;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public string WorkItemId { get; }

            public override EventId EventId => new EventId(
                EventIds.ProcessWorkItemStarting,
                nameof(EventIds.ProcessWorkItemStarting));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Processing work-item '{this.WorkItemId}'";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.ProcessWorkItemStarting(
                    this.Dispatcher,
                    this.WorkItemId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class ProcessWorkItemCompleted : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessWorkItemCompleted(WorkItemDispatcherContext context, string workItemId)
            {
                this.Dispatcher = context.GetDisplayName();
                this.WorkItemId = workItemId;
            }

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public string WorkItemId { get; }

            public override EventId EventId => new EventId(
                EventIds.ProcessWorkItemCompleted,
                nameof(EventIds.ProcessWorkItemCompleted));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Finished processing work-item '{this.WorkItemId}'";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.ProcessWorkItemCompleted(
                    this.Dispatcher,
                    this.WorkItemId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class ProcessWorkItemFailed : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessWorkItemFailed(WorkItemDispatcherContext context, string workItemId, string additionalInfo, Exception exception)
            {
                this.Dispatcher = context.GetDisplayName();
                this.WorkItemId = workItemId;
                this.Details = string.Concat(
                    exception.ToString(),
                    Environment.NewLine,
                    Environment.NewLine,
                    additionalInfo);
            }

            public override EventId EventId => new EventId(
                EventIds.ProcessWorkItemFailed,
                nameof(EventIds.ProcessWorkItemFailed));

            [StructuredLogField]
            public string Dispatcher { get; }

            [StructuredLogField]
            public string WorkItemId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() =>
                $"{this.Dispatcher}: Unhandled exception with work item '{this.WorkItemId}': {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.ProcessWorkItemFailed(
                    this.Dispatcher,
                    this.WorkItemId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class SchedulingOrchestration : StructuredLogEvent, IEventSourceEvent
        {
            public SchedulingOrchestration(ExecutionStartedEvent startedEvent)
            {
                this.Name = startedEvent.Name;
                this.TaskEventId = startedEvent.ParentInstance?.TaskScheduleId ?? startedEvent.EventId;
                this.InstanceId = startedEvent.ParentInstance?.OrchestrationInstance.InstanceId ?? string.Empty;
                this.ExecutionId = startedEvent.ParentInstance?.OrchestrationInstance.ExecutionId ?? string.Empty;
                this.TargetInstanceId = startedEvent.OrchestrationInstance.InstanceId;
                this.TargetExecutionId = startedEvent.OrchestrationInstance.ExecutionId ?? string.Empty;
                this.SizeInBytes = Encoding.UTF8.GetByteCount(startedEvent.Input ?? string.Empty);
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string TargetInstanceId { get; }

            [StructuredLogField]
            public string TargetExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            public override EventId EventId => new EventId(
                EventIds.SchedulingOrchestration,
                nameof(EventIds.SchedulingOrchestration));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage()
            {
                string message = $"Scheduling orchestration '{this.Name}' with instance ID = '{this.TargetInstanceId}' and {this.SizeInBytes} bytes of input";
                if (!string.IsNullOrEmpty(this.InstanceId))
                {
                    // This is the case where a parent orchestration is scheduling a child-orchestration
                    message = this.InstanceId + ": " + message;
                }

                return message;
            }

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.SchedulingOrchestration(
                    this.InstanceId,
                    this.ExecutionId,
                    this.TargetInstanceId,
                    this.TargetExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.SizeInBytes,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class RaisingEvent : StructuredLogEvent, IEventSourceEvent
        {
            public RaisingEvent(OrchestrationInstance target, EventRaisedEvent raisedEvent)
            {
                this.Name = raisedEvent.Name;
                this.TaskEventId = raisedEvent.EventId;
                this.InstanceId = string.Empty;
                this.ExecutionId = string.Empty;
                this.TargetInstanceId = target.InstanceId;
                this.SizeInBytes = raisedEvent.Input != null ? Encoding.UTF8.GetByteCount(raisedEvent.Input) : 0;
            }

            public RaisingEvent(OrchestrationInstance source, EventSentEvent sentEvent)
            {
                this.Name = sentEvent.Name;
                this.TaskEventId = sentEvent.EventId;
                this.InstanceId = source.InstanceId;
                this.ExecutionId = source.ExecutionId;
                this.TargetInstanceId = sentEvent.InstanceId;
                this.SizeInBytes = sentEvent.Input != null ? Encoding.UTF8.GetByteCount(sentEvent.Input) : 0;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string TargetInstanceId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            public override EventId EventId => new EventId(
                EventIds.RaisingEvent,
                nameof(EventIds.RaisingEvent));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage()
            {
                string message = $"Raising '{this.Name}' event with {this.SizeInBytes} bytes to '{this.TargetInstanceId}'";
                if (!string.IsNullOrEmpty(this.InstanceId))
                {
                    message = this.InstanceId + ": " + message;
                }

                return message;
            }

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.RaisingEvent(
                    this.InstanceId,
                    this.ExecutionId,
                    this.TargetInstanceId,
                    this.Name,
                    this.TaskEventId,
                    this.SizeInBytes,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class TerminatingInstance : StructuredLogEvent, IEventSourceEvent
        {
            public TerminatingInstance(OrchestrationInstance instance, string reason)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Details = reason;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.TerminatingInstance,
                nameof(EventIds.TerminatingInstance));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Terminating instance '{this.InstanceId}': {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TerminatingInstance(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class WaitingForInstance : StructuredLogEvent, IEventSourceEvent
        {
            public WaitingForInstance(OrchestrationInstance instance, TimeSpan timeout)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.TimeoutSeconds = (int)timeout.TotalSeconds;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int TimeoutSeconds { get; }

            public override EventId EventId => new EventId(
                EventIds.WaitingForInstance,
                nameof(EventIds.WaitingForInstance));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Waiting up to {this.TimeoutSeconds} seconds for instance '{this.InstanceId}' to complete, fail, or be terminated";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.WaitingForInstance(
                    this.InstanceId,
                    this.ExecutionId,
                    this.TimeoutSeconds,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class FetchingInstanceState : StructuredLogEvent, IEventSourceEvent
        {
            public FetchingInstanceState(string instanceId, string executionId = null)
            {
                this.InstanceId = instanceId;
                this.ExecutionId = executionId ?? string.Empty;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchingInstanceState,
                nameof(EventIds.FetchingInstanceState));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Fetching tracking state for instance '{this.InstanceId}'";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchingInstanceState(
                    this.InstanceId,
                    this.ExecutionId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing a task hub client querying for orchestration instance history.
        /// </summary>
        internal class FetchingInstanceHistory : StructuredLogEvent, IEventSourceEvent
        {
            public FetchingInstanceHistory(OrchestrationInstance instance)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchingInstanceHistory,
                nameof(EventIds.FetchingInstanceHistory));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"Fetching history for instance '{this.InstanceId}'";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.FetchingInstanceHistory(
                    this.InstanceId,
                    this.ExecutionId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing a task hub worker processing a new message from a fetched work item.
        /// </summary>
        internal class ProcessingOrchestrationMessage : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessingOrchestrationMessage(TaskOrchestrationWorkItem workItem, TaskMessage message)
            {
                this.InstanceId = workItem.InstanceId;
                this.ExecutionId = message.OrchestrationInstance.ExecutionId ?? string.Empty;
                this.EventType = message.Event.EventType.ToString();
                this.TaskEventId = Utils.GetTaskEventId(message.Event);
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            public override EventId EventId => new EventId(
                EventIds.ProcessingOrchestrationMessage,
                nameof(EventIds.ProcessingOrchestrationMessage));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Preparing to process a {GetEventDescription(this.EventType, this.TaskEventId)} message";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.ProcessingOrchestrationMessage(
                    this.InstanceId,
                    this.ExecutionId,
                    this.EventType,
                    this.TaskEventId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing a task hub worker beginning an orchestration instance execution.
        /// </summary>
        internal class OrchestrationExecuting : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationExecuting(OrchestrationInstance instance, string name)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.Name = name;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationExecuting,
                nameof(EventIds.OrchestrationExecuting));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Executing '{this.Name}' orchestration logic";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.OrchestrationExecuting(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing a task hub worker completed an orchestration instance execution.
        /// </summary>
        internal class OrchestrationExecuted : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationExecuted(OrchestrationInstance instance, string name, int actionCount)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId ?? string.Empty;
                this.Name = name;
                this.ActionCount = actionCount;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }
            
            [StructuredLogField]
            public int ActionCount { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationExecuted,
                nameof(EventIds.OrchestrationExecuted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Orchestration '{this.Name}' awaited and scheduled {this.ActionCount} durable operation(s).";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.OrchestrationExecuted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.ActionCount,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing an orchestration instance scheduling an activity as part of its execution.
        /// </summary>
        internal class SchedulingActivity : StructuredLogEvent, IEventSourceEvent
        {
            public SchedulingActivity(OrchestrationInstance instance, TaskScheduledEvent taskScheduledEvent)
            {
                this.Name = taskScheduledEvent.Name;
                this.TaskEventId = taskScheduledEvent.EventId;
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.SizeInBytes = Encoding.UTF8.GetByteCount(taskScheduledEvent.Input ?? string.Empty);
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            public override EventId EventId => new EventId(
                EventIds.SchedulingActivity,
                nameof(EventIds.SchedulingActivity));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Scheduling activity {GetEventDescription(this.Name, this.TaskEventId)} with {this.SizeInBytes} bytes of input";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.SchedulingActivity(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.SizeInBytes,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing an orchestration instance scheduling an activity as part of its execution.
        /// This could be an explicit timer created by the orchestration or an implicit one created by the dispatcher.
        /// </summary>
        internal class CreatingTimer : StructuredLogEvent, IEventSourceEvent
        {
            public CreatingTimer(
                OrchestrationInstance instance,
                TimerCreatedEvent timerEvent,
                bool isInternal)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.FireAt = timerEvent.FireAt;
                this.TaskEventId = timerEvent.EventId;
                this.IsInternal = isInternal;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public DateTime FireAt { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public bool IsInternal { get; }

            public override EventId EventId => new EventId(
                EventIds.CreatingTimer,
                nameof(EventIds.CreatingTimer));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Scheduling {GetEventDescription(EventType.TimerFired.ToString(), this.TaskEventId)} to fire at {this.FireAt:o}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.CreatingTimer(
                    this.InstanceId,
                    this.ExecutionId,
                    this.FireAt,
                    this.TaskEventId,
                    this.IsInternal,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing an orchestration instance ran to completion.
        /// This could be a success or a failure.
        /// </summary>
        internal class OrchestrationCompleted : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationCompleted(
                OrchestrationRuntimeState runtimeState,
                OrchestrationCompleteOrchestratorAction action)
            {
                this.InstanceId = runtimeState.OrchestrationInstance.InstanceId;
                this.ExecutionId = runtimeState.OrchestrationInstance.ExecutionId;
                this.RuntimeStatus = action.OrchestrationStatus.ToString();
                this.Details = action.Details;
                this.SizeInBytes = Encoding.UTF8.GetByteCount(action.Result ?? string.Empty);
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string RuntimeStatus { get; }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationCompleted,
                nameof(EventIds.OrchestrationCompleted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Orchestration completed with a '{this.RuntimeStatus}' status and {this.SizeInBytes} bytes of output. Details: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.OrchestrationCompleted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.RuntimeStatus,
                    this.Details,
                    this.SizeInBytes,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing an orchestration aborted event, which can happen if the host is shutting down.
        /// </summary>
        internal class OrchestrationAborted : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationAborted(OrchestrationInstance instance, string reason)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Details = reason;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationAborted,
                nameof(EventIds.OrchestrationAborted));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Orchestration execution was aborted: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.OrchestrationAborted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event representing the discarding of an orchestration message that cannot be processed.
        /// </summary>
        internal class DiscardingMessage : StructuredLogEvent, IEventSourceEvent
        {
            public DiscardingMessage(TaskOrchestrationWorkItem workItem, TaskMessage message, string reason)
            {
                this.InstanceId = message.OrchestrationInstance?.InstanceId ?? workItem.InstanceId;
                this.ExecutionId = message.OrchestrationInstance?.ExecutionId;
                this.EventType = message.Event.EventType.ToString();
                this.TaskEventId = Utils.GetTaskEventId(message.Event);
                this.Details = reason;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.DiscardingMessage,
                nameof(EventIds.DiscardingMessage));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Discarding {GetEventDescription(this.EventType, this.TaskEventId)}: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.DiscardingMessage(
                    this.InstanceId,
                    this.ExecutionId,
                    this.EventType,
                    this.TaskEventId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event indicating that an activity execution is starting.
        /// </summary>
        internal class TaskActivityStarting : StructuredLogEvent, IEventSourceEvent
        {
            public TaskActivityStarting(OrchestrationInstance instance, TaskScheduledEvent taskEvent)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Name = taskEvent.Name;
                this.TaskEventId = taskEvent.EventId;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskActivityStarting,
                nameof(EventIds.TaskActivityStarting));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Starting task activity {GetEventDescription(this.Name, this.TaskEventId)}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskActivityStarting(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        /// <summary>
        /// Log event indicating that an activity execution has completed successfully.
        /// </summary>
        internal class TaskActivityCompleted : StructuredLogEvent, IEventSourceEvent
        {
            public TaskActivityCompleted(
                OrchestrationInstance instance,
                string name,
                TaskCompletedEvent taskEvent)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Name = name;
                this.TaskEventId = taskEvent.TaskScheduledId;
                this.SizeInBytes = Encoding.UTF8.GetByteCount(taskEvent.Result ?? string.Empty);
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskActivityCompleted,
                nameof(EventIds.TaskActivityCompleted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Task activity {GetEventDescription(this.Name, this.TaskEventId)} completed successfully";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskActivityCompleted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.SizeInBytes,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class TaskActivityFailure : StructuredLogEvent, IEventSourceEvent
        {
            public TaskActivityFailure(
                OrchestrationInstance instance,
                string name,
                TaskFailedEvent taskEvent,
                Exception exception)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Name = name;
                this.TaskEventId = taskEvent.EventId;
                this.Details = exception.ToString();
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskActivityFailure,
                nameof(EventIds.TaskActivityFailure));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Task activity {GetEventDescription(this.Name, this.TaskEventId)} failed: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskActivityFailure(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class TaskActivityAborted : StructuredLogEvent, IEventSourceEvent
        {
            public TaskActivityAborted(OrchestrationInstance instance, TaskScheduledEvent taskEvent, string details)
            {
                this.InstanceId = instance.InstanceId;
                this.ExecutionId = instance.ExecutionId;
                this.Name = taskEvent.Name;
                this.TaskEventId = taskEvent.EventId;
                this.Details = details;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskActivityAborted,
                nameof(EventIds.TaskActivityAborted));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Task activity {GetEventDescription(this.Name, this.TaskEventId)} was aborted: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskActivityAborted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class TaskActivityDispatcherError : StructuredLogEvent, IEventSourceEvent
        {
            public TaskActivityDispatcherError(TaskActivityWorkItem workItem, string details)
            {
                // There's no guarantee that we received valid work item data
                this.InstanceId = workItem.TaskMessage?.OrchestrationInstance?.InstanceId;
                this.ExecutionId = workItem.TaskMessage?.OrchestrationInstance?.ExecutionId;
                this.Details = details;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.TaskActivityDispatcherError,
                nameof(EventIds.TaskActivityDispatcherError));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() => this.Details;

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.TaskActivityDispatcherError(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class RenewActivityMessageStarting : StructuredLogEvent, IEventSourceEvent
        {
            public RenewActivityMessageStarting(TaskActivityWorkItem workItem)
            {
                this.InstanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                this.ExecutionId = workItem.TaskMessage.OrchestrationInstance.ExecutionId;
                var taskEvent = (TaskScheduledEvent)workItem.TaskMessage.Event;
                this.Name = taskEvent.Name;
                this.TaskEventId = taskEvent.EventId;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            public override EventId EventId => new EventId(
                EventIds.RenewActivityMessageStarting,
                nameof(EventIds.RenewActivityMessageStarting));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Renewing message for task activity {GetEventDescription(this.Name, this.TaskEventId)}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.RenewActivityMessageStarting(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class RenewActivityMessageCompleted : StructuredLogEvent, IEventSourceEvent
        {
            public RenewActivityMessageCompleted(TaskActivityWorkItem workItem, DateTime renewAt)
            {
                this.InstanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                this.ExecutionId = workItem.TaskMessage.OrchestrationInstance.ExecutionId;

                var taskEvent = (TaskScheduledEvent)workItem.TaskMessage.Event;
                this.Name = taskEvent.Name;
                this.TaskEventId = taskEvent.EventId;
                this.NextRenewal = renewAt;
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public DateTime NextRenewal { get; }

            public override EventId EventId => new EventId(
                EventIds.RenewActivityMessageCompleted,
                nameof(EventIds.RenewActivityMessageCompleted));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Renewed message for task activity {GetEventDescription(this.Name, this.TaskEventId)} successfully. " +
                $"Next renewal time is {this.NextRenewal:o}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.RenewActivityMessageCompleted(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.NextRenewal,
                    Utils.AppName,
                    Utils.PackageVersion);
        }

        internal class RenewActivityMessageFailed : StructuredLogEvent, IEventSourceEvent
        {
            public RenewActivityMessageFailed(TaskActivityWorkItem workItem, Exception exception)
            {
                this.InstanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                this.ExecutionId = workItem.TaskMessage.OrchestrationInstance.ExecutionId;

                var taskEvent = (TaskScheduledEvent)workItem.TaskMessage.Event;
                this.Name = taskEvent.Name;
                this.TaskEventId = taskEvent.EventId;
                this.Details = exception.ToString();
            }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Name { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.RenewActivityMessageFailed,
                nameof(EventIds.RenewActivityMessageFailed));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Failed to renew message for task activity {GetEventDescription(this.Name, this.TaskEventId)}: {this.Details}";

            void IEventSourceEvent.WriteEventSource() =>
                StructuredEventSource.Log.RenewActivityMessageFailed(
                    this.InstanceId,
                    this.ExecutionId,
                    this.Name,
                    this.TaskEventId,
                    this.Details,
                    Utils.AppName,
                    Utils.PackageVersion);
        }
    }
}
