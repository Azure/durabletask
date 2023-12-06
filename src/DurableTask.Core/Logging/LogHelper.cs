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
namespace DurableTask.Core.Logging
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DurableTask.Core.Command;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;

    class LogHelper
    {
        readonly ILogger? log;

        public LogHelper(ILogger? log)
        {
            // null is okay
            this.log = log;
        }

        bool IsStructuredLoggingEnabled => this.log != null;

        #region TaskHubWorker
        /// <summary>
        /// Logs that a <see cref="TaskHubWorker"/> is starting.
        /// </summary>
        internal void TaskHubWorkerStarting()
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskHubWorkerStarting());
            }
        }

        /// <summary>
        /// Logs that a <see cref="TaskHubWorker"/> started successfully.
        /// </summary>
        /// <param name="latency">The amount of time it took to start the <see cref="TaskHubWorker"/>.</param>
        internal void TaskHubWorkerStarted(TimeSpan latency)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskHubWorkerStarted(latency));
            }
        }

        /// <summary>
        /// Logs that a <see cref="TaskHubWorker"/> is stopping.
        /// </summary>
        internal void TaskHubWorkerStopping(bool isForced)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskHubWorkerStopping(isForced));
            }
        }

        /// <summary>
        /// Logs that a <see cref="TaskHubWorker"/> stopped successfully.
        /// </summary>
        /// <param name="latency">The amount of time it took to stop the <see cref="TaskHubWorker"/>.</param>
        internal void TaskHubWorkerStopped(TimeSpan latency)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskHubWorkerStopped(latency));
            }
        }
        #endregion

        #region WorkItemDispatcher traces

        /// <summary>
        /// Indicates that the work item dispatcher is starting.
        /// </summary>
        /// <param name="context">The context of the starting dispatcher.</param>
        internal void DispatcherStarting(WorkItemDispatcherContext context)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.DispatcherStarting(context));
            }
        }

        /// <summary>
        /// Logs that a work item dispatcher has stopped running.
        /// </summary>
        /// <param name="context">The context of the starting dispatcher.</param>
        internal void DispatcherStopped(WorkItemDispatcherContext context)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.DispatcherStopped(context));
            }
        }

        /// <summary>
        /// Logs that the work item dispatcher is watching for individual dispatch loops to finish stopping.
        /// </summary>
        /// <param name="name">The name of the dispatcher - e.g. "TaskOrchestrationDispatcher"</param>
        /// <param name="id">The unique ID of the dispatcher.</param>
        /// <param name="concurrentWorkItemCount">The number of active work items.</param>
        /// <param name="activeFetchers">The number of active fetchers.</param>
        internal void DispatchersStopping(string name, string id, int concurrentWorkItemCount, int activeFetchers)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.DispatchersStopping(name, id, concurrentWorkItemCount, activeFetchers));
            }
        }

        /// <summary>
        /// Logs that we're starting to fetch a new work item.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="timeout">The fetch timeout.</param>
        /// <param name="concurrentWorkItemCount">The current number of concurrent work items.</param>
        /// <param name="maxConcurrentWorkItems">The maximum number of concurrent work items.</param>
        internal void FetchWorkItemStarting(
            WorkItemDispatcherContext context,
            TimeSpan timeout,
            int concurrentWorkItemCount,
            int maxConcurrentWorkItems)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.FetchWorkItemStarting(
                        context,
                        timeout,
                        concurrentWorkItemCount,
                        maxConcurrentWorkItems));
            }
        }

        /// <summary>
        /// Logs that a work-item was fetched successfully.
        /// This event does not have enough context to understand the details of the work item.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="workItemId">The ID of the fetched work item.</param>
        /// <param name="latency">The latency of the fetch operation.</param>
        /// <param name="concurrentWorkItemCount">The current number of concurrent work items.</param>
        /// <param name="maxConcurrentWorkItems">The maximum number of concurrent work items.</param>
        internal void FetchWorkItemCompleted(
            WorkItemDispatcherContext context,
            string workItemId,
            TimeSpan latency,
            int concurrentWorkItemCount,
            int maxConcurrentWorkItems)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.FetchWorkItemCompleted(
                        context,
                        workItemId,
                        latency,
                        concurrentWorkItemCount,
                        maxConcurrentWorkItems));
            }
        }

        /// <summary>
        /// Logs that a work-item fetched failed with an unhandled exception.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="exception">The exception associted with the failure.</param>
        internal void FetchWorkItemFailure(WorkItemDispatcherContext context, Exception exception)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.FetchWorkItemFailure(context, exception),
                    exception);
            }
        }

        /// <summary>
        /// Logs that work-item fetching has been throttled due to concurrency constraints.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="concurrentWorkItemCount">The current number of concurrent work items.</param>
        /// <param name="maxConcurrentWorkItems">The maximum number of concurrent work items.</param>
        internal void FetchingThrottled(
            WorkItemDispatcherContext context,
            int concurrentWorkItemCount,
            int maxConcurrentWorkItems)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.FetchingThrottled(
                        context,
                        concurrentWorkItemCount,
                        maxConcurrentWorkItems));
            }
        }

        /// <summary>
        /// Logs that a work item is about to be processed.
        /// This event does not have enough context to understand the details of the work item.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="workItemId">The ID of the work item.</param>
        internal void ProcessWorkItemStarting(WorkItemDispatcherContext context, string workItemId)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.ProcessWorkItemStarting(context, workItemId));
            }
        }

        /// <summary>
        /// Logs that a work item was processed successfully.
        /// This event does not have enough context to understand the details of the work item.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="workItemId">The ID of the work item.</param>
        internal void ProcessWorkItemCompleted(WorkItemDispatcherContext context, string workItemId)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.ProcessWorkItemCompleted(context, workItemId));
            }
        }

        /// <summary>
        /// Logs that a work item was processed successfully.
        /// This event does not have enough context to understand the details of the work item.
        /// </summary>
        /// <param name="context">The dispatcher context.</param>
        /// <param name="workItemId">The ID of the work item.</param>
        /// <param name="additionalInfo">Additional information associated with the failure.</param>
        /// <param name="exception">The exception associated with the failure.</param>
        internal void ProcessWorkItemFailed(
            WorkItemDispatcherContext context,
            string workItemId,
            string additionalInfo,
            Exception exception)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.ProcessWorkItemFailed(context, workItemId, additionalInfo, exception),
                    exception);
            }
        }
        #endregion

        #region TaskHubClient traces

        /// <summary>
        /// Logs that a new orchestration instance has been scheduled.
        /// </summary>
        /// <param name="startedEvent">The start event for the new orchestration.</param>
        internal void SchedulingOrchestration(ExecutionStartedEvent startedEvent)
        {
            // Note that this log event is also used when orchestrations send events
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.SchedulingOrchestration(startedEvent));
            }
        }

        /// <summary>
        /// Logs that an event is being raised to an orchestration.
        /// </summary>
        /// <param name="target">The target orchestration instance.</param>
        /// <param name="raisedEvent">The event-raised event.</param>
        internal void RaisingEvent(OrchestrationInstance target, EventRaisedEvent raisedEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RaisingEvent(target, raisedEvent));
            }
        }

        /// <summary>
        /// Logs that an event is being raised by an orchestration.
        /// </summary>
        /// <param name="source">The sending orchestration instance.</param>
        /// <param name="raisedEvent">The event-sent event.</param>
        internal void RaisingEvent(OrchestrationInstance source, EventSentEvent raisedEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RaisingEvent(source, raisedEvent));
            }
        }

        /// <summary>
        /// Logs that an instance is being scheduled to be terminated.
        /// </summary>
        /// <param name="instance">The instance to be terminated.</param>
        /// <param name="reason">The user-specified reason for the termination.</param>
        internal void TerminatingInstance(OrchestrationInstance instance, string reason)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TerminatingInstance(instance, reason));
            }
        }

        /// <summary>
        /// Logs that an instance is being scheduled to be suspended.
        /// </summary>
        /// <param name="instance">The instance to be suspended.</param>
        /// <param name="reason">The user-specified reason for the suspension.</param>
        internal void SuspendingInstance(OrchestrationInstance instance, string reason)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.SuspendingInstance(instance, reason));
            }
        }

        /// <summary>
        /// Logs that an instance is being scheduled to be resumed.
        /// </summary>
        /// <param name="instance">The instance to be resumed.</param>
        /// <param name="reason">The user-specified reason for the resumption.</param>
        internal void ResumingInstance(OrchestrationInstance instance, string reason)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.ResumingInstance(instance, reason));
            }
        }

        /// <summary>
        /// Logs that the client is waiting for an instance to reach a terminal state.
        /// </summary>
        /// <param name="instance">The instance being awaited.</param>
        /// <param name="timeout">The maximum timeout the client will wait.</param>
        internal void WaitingForInstance(OrchestrationInstance instance, TimeSpan timeout)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.WaitingForInstance(instance, timeout));
            }
        }

        /// <summary>
        /// Logs that the client is attempting to fetch the state of an orchestration instance.
        /// </summary>
        /// <param name="instanceId">The ID of the instance.</param>
        /// <param name="executionId">The execution ID of the instance, if applicable.</param>
        internal void FetchingInstanceState(string instanceId, string? executionId = null)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.FetchingInstanceState(instanceId, executionId));
            }
        }

        /// <summary>
        /// Logs that the client is attempting to fetch the history of an orchestration instance.
        /// </summary>
        /// <param name="instance">The instance to fetch the history of.</param>
        internal void FetchingInstanceHistory(OrchestrationInstance instance)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.FetchingInstanceHistory(instance));
            }
        }

        #endregion

        #region Orchestration dispatcher

        /// <summary>
        /// Logs that an orchestration work item message is being processed.
        /// </summary>
        /// <param name="workItem">The orchestration work item.</param>
        /// <param name="message">The message being processed.</param>
        internal void ProcessingOrchestrationMessage(TaskOrchestrationWorkItem workItem, TaskMessage message)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.ProcessingOrchestrationMessage(workItem, message));
            }
        }

        /// <summary>
        /// Logs that an orchestration is about to start executing.
        /// </summary>
        /// <param name="instance">The orchestration instance that is about to start.</param>
        /// <param name="name">The name of the orchestration.</param>
        internal void OrchestrationExecuting(OrchestrationInstance instance, string name)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.OrchestrationExecuting(instance, name));
            }
        }

        /// <summary>
        /// Logs that an orchestration successfully executed an episode.
        /// </summary>
        /// <param name="instance">The orchestration instance that was executed.</param>
        /// <param name="name">The name of the orchestration.</param>
        /// <param name="actions">The actions taken by the orchestration</param>
        internal void OrchestrationExecuted(
            OrchestrationInstance instance,
            string name,
            IReadOnlyList<OrchestratorAction> actions)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.OrchestrationExecuted(instance, name, actions.Count));
            }
        }

        /// <summary>
        /// Logs that an orchestration is scheduling a task activity for execution.
        /// </summary>
        /// <param name="instance">The orchestration instance scheduling the task activity.</param>
        /// <param name="scheduledEvent">The task scheduled event.</param>
        internal void SchedulingActivity(OrchestrationInstance instance, TaskScheduledEvent scheduledEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.SchedulingActivity(instance, scheduledEvent));
            }
        }

        /// <summary>
        /// Logs that an orchestration is creating a durable timer.
        /// </summary>
        /// <param name="instance">The orchestration instance scheduling the task activity.</param>
        /// <param name="timerEvent">The timer creation event.</param>
        /// <param name="isInternal"><c>true</c> if the timer was created internally by the framework; <c>false</c> if it was created by user code.</param>
        internal void CreatingTimer(OrchestrationInstance instance, TimerCreatedEvent timerEvent, bool isInternal)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.CreatingTimer(instance, timerEvent, isInternal));
            }
        }

        /// <summary>
        /// Logs that an orchestration is sending an event to another orchestration instance.
        /// </summary>
        /// <param name="source">The orchestration instance sending the event.</param>
        /// <param name="eventSentEvent">The event being sent.</param>
        internal void SendingEvent(OrchestrationInstance source, EventSentEvent eventSentEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                // We re-use the existing RaisingEvent log event for this
                this.WriteStructuredLog(new LogEvents.RaisingEvent(source, eventSentEvent));
            }
        }

        /// <summary>
        /// Logs that an orchestration instance has completed.
        /// </summary>
        /// <param name="runtimeState">The final runtime state of the completed orchestration.</param>
        /// <param name="action">The orchestration instance's completion actions.</param>
        internal void OrchestrationCompleted(
            OrchestrationRuntimeState runtimeState,
            OrchestrationCompleteOrchestratorAction action)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.OrchestrationCompleted(runtimeState, action));
            }
        }

        /// <summary>
        /// Logs that an orchestration execution was aborted.
        /// </summary>
        /// <param name="instance">The orchestration instance that aborted execution.</param>
        /// <param name="reason">The reason for aborting the orchestration execution.</param>
        internal void OrchestrationAborted(OrchestrationInstance instance, string reason)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.OrchestrationAborted(instance, reason));
            }
        }

        /// <summary>
        /// Helper method for logging the dropping of all messages associated with the specified work item.
        /// </summary>
        /// <param name="workItem">The work item being dropped.</param>
        /// <param name="reason">The reason for dropping this work item.</param>
        internal void DroppingOrchestrationWorkItem(TaskOrchestrationWorkItem workItem, string reason)
        {
            foreach (TaskMessage message in workItem.NewMessages)
            {
                this.DroppingOrchestrationMessage(workItem, message, reason);
            }
        }

        /// <summary>
        /// Logs that a work item message is being dropped and includes a reason.
        /// </summary>
        /// <param name="workItem">The work item that this message belonged to.</param>
        /// <param name="message">The message being dropped.</param>
        /// <param name="reason">The reason for dropping the message.</param>
        internal void DroppingOrchestrationMessage(TaskOrchestrationWorkItem workItem, TaskMessage message, string reason)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.DiscardingMessage(workItem, message, reason));
            }
        }

        /// <summary>
        /// Logs that an orchestration work item renewal operation is starting.
        /// </summary>
        /// <param name="workItem">The work item to be renewed.</param>
        internal void RenewOrchestrationWorkItemStarting(TaskOrchestrationWorkItem workItem)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewOrchestrationWorkItemStarting(workItem));
            }
        }

        /// <summary>
        /// Logs that an orchestration work item renewal operation succeeded.
        /// </summary>
        /// <param name="workItem">The work item that was renewed.</param>
        internal void RenewOrchestrationWorkItemCompleted(TaskOrchestrationWorkItem workItem)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewOrchestrationWorkItemCompleted(workItem));
            }
        }

        /// <summary>
        /// Logs that an orchestration work item renewal operation failed.
        /// </summary>
        /// <param name="workItem">The work item that was to be renewed.</param>
        /// <param name="exception">The renew failure exception.</param>
        internal void RenewOrchestrationWorkItemFailed(TaskOrchestrationWorkItem workItem, Exception exception)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewOrchestrationWorkItemFailed(workItem, exception), exception);
            }
        }


        /// <summary>
        /// Logs that an entity operation batch is about to start executing.
        /// </summary>
        /// <param name="request">The batch request.</param>
        internal void EntityBatchExecuting(EntityBatchRequest request)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.EntityBatchExecuting(request));
            }
        }

        /// <summary>
        /// Logs that an entity operation batch completed its execution.
        /// </summary>
        /// <param name="request">The batch request.</param>
        /// <param name="result">The batch result.</param>
        internal void EntityBatchExecuted(EntityBatchRequest request, EntityBatchResult result)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.EntityBatchExecuted(request, result));
            }
        }

        /// <summary>
        /// Logs that an entity processed a lock acquire message.
        /// </summary>
        /// <param name="entityId">The entity id.</param>
        /// <param name="message">The message.</param>
        internal void EntityLockAcquired(string entityId, Core.Entities.EventFormat.RequestMessage message)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.EntityLockAcquired(entityId, message));
            }
        }

        /// <summary>
        /// Logs that an entity processed a lock release message.
        /// </summary>
        /// <param name="entityId">The entity id.</param>
        /// <param name="message">The message.</param>
        internal void EntityLockReleased(string entityId, Core.Entities.EventFormat.ReleaseMessage message)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.EntityLockReleased(entityId, message));
            }
        }

        #endregion

        #region Activity dispatcher
        /// <summary>
        /// Logs that a task activity is about to begin execution.
        /// </summary>
        /// <param name="instance">The orchestration instance that scheduled this task activity.</param>
        /// <param name="taskEvent">The history event associated with this activity execution.</param>
        internal void TaskActivityStarting(OrchestrationInstance instance, TaskScheduledEvent taskEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskActivityStarting(instance, taskEvent));
            }
        }

        /// <summary>
        /// Logs that a task activity completed successfully.
        /// </summary>
        /// <param name="instance">The orchestration instance that scheduled this task activity.</param>
        /// <param name="name">The name of the task activity.</param>
        /// <param name="taskEvent">The history event associated with this activity completion.</param>
        internal void TaskActivityCompleted(OrchestrationInstance instance, string name, TaskCompletedEvent taskEvent)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskActivityCompleted(instance, name, taskEvent));
            }
        }

        /// <summary>
        /// Logs that an activity failed with an unhandled exception.
        /// </summary>
        /// <param name="instance">The orchestration instance that scheduled this task activity.</param>
        /// <param name="name">The name of the task activity.</param>
        /// <param name="failedEvent">The history event associated with this activity failure.</param>
        /// <param name="exception">The unhandled exception.</param>
        internal void TaskActivityFailure(
            OrchestrationInstance instance,
            string name,
            TaskFailedEvent failedEvent,
            Exception exception)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(
                    new LogEvents.TaskActivityFailure(instance, name, failedEvent, exception),
                    exception);
            }
        }

        /// <summary>
        /// Logs a warning indicating that the activity execution was aborted.
        /// </summary>
        /// <param name="instance">The orchestration instance that scheduled this task activity.</param>
        /// <param name="taskEvent">The history event associated with this activity execution.</param>
        /// <param name="details">More information about why the execution was aborted.</param>
        internal void TaskActivityAborted(OrchestrationInstance instance, TaskScheduledEvent taskEvent, string details)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskActivityAborted(instance, taskEvent, details));
            }
        }

        /// <summary>
        /// Logs that an error occurred when attempting to dispatch an activity work item.
        /// </summary>
        /// <param name="workItem">The work item that caused the failure.</param>
        /// <param name="details">The details of the failure.</param>
        internal void TaskActivityDispatcherError(TaskActivityWorkItem workItem, string details)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.TaskActivityDispatcherError(workItem, details));
            }
        }

        /// <summary>
        /// Logs that an activity message renewal operation is starting.
        /// </summary>
        /// <param name="workItem">The work item associated with the message to be renewed.</param>
        internal void RenewActivityMessageStarting(TaskActivityWorkItem workItem)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewActivityMessageStarting(workItem));
            }
        }

        /// <summary>
        /// Logs that an activity message renewal operation succeeded.
        /// </summary>
        /// <param name="workItem">The work item associated with the renewed message.</param>
        /// <param name="renewAt">The next scheduled renewal message time.</param>
        internal void RenewActivityMessageCompleted(TaskActivityWorkItem workItem, DateTime renewAt)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewActivityMessageCompleted(workItem, renewAt));
            }
        }

        /// <summary>
        /// Logs that an activity message renewal operation failed.
        /// </summary>
        /// <param name="workItem">The work item associated with the message to be renewed.</param>
        /// <param name="exception">The renew failure exception.</param>
        internal void RenewActivityMessageFailed(TaskActivityWorkItem workItem, Exception exception)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.RenewActivityMessageFailed(workItem, exception), exception);
            }
        }
        #endregion

        internal void OrchestrationDebugTrace(string instanceId, string executionId, string details)
        {
            if (this.IsStructuredLoggingEnabled)
            {
                this.WriteStructuredLog(new LogEvents.OrchestrationDebugTrace(instanceId, executionId, details));
            }
        }

        void WriteStructuredLog(ILogEvent logEvent, Exception? exception = null)
        {
            this.log?.LogDurableEvent(logEvent, exception);
        }

        internal static string GetRedactedExceptionDetails(Exception? exception)
        {
            if (exception == null)
            {
                return string.Empty;
            }

            // Redact the exception message since its possible to contain sensitive information (PII, secrets, etc.)
            // Exception.ToString() code: https://referencesource.microsoft.com/#mscorlib/system/exception.cs,e2e19f4ed8da81aa
            // Example output for a method chain of Foo() --> Bar() --> Baz() --> (exception):
            // System.ApplicationException: [Redacted]
            //     ---> System.Exception: [Redacted]
            //     ---> System.InvalidOperationException: [Redacted]
            //     at UserQuery.<Main>g__Baz|4_3() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 68
            //     at UserQuery.<Main>g__Bar|4_2() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 58
            //     --- End of inner exception stack trace ---
            //     at UserQuery.<Main>g__Bar|4_2() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 62
            //     at UserQuery.<Main>g__Foo|4_1() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 46
            //     --- End of inner exception stack trace ---
            //     at UserQuery.<Main>g__Foo|4_1() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 50
            //     at UserQuery.Main() in C:\Users\xxx\AppData\Local\Temp\LINQPad7\_wrmpjfpn\hjvskp\LINQPadQuery:line 4
            var sb = new StringBuilder(capacity: 1024);
            sb.Append(exception.GetType().FullName).Append(": ").Append("[Redacted]");
            if (exception.InnerException != null)
            {
                // Recursive
                sb.AppendLine().Append(" ---> ").AppendLine(GetRedactedExceptionDetails(exception.InnerException));
                sb.Append("   --- End of inner exception stack trace ---");
            }

            sb.AppendLine().Append(exception.StackTrace);
            return sb.ToString();
        }
    }
}
