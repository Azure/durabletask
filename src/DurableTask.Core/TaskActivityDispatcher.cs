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
    using System.Diagnostics;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Dispatcher for task activities to handle processing and renewing of work items
    /// </summary>
    public sealed class TaskActivityDispatcher
    {
        readonly INameVersionObjectManager<TaskActivity> objectManager;
        readonly WorkItemDispatcher<TaskActivityWorkItem> dispatcher;
        readonly IOrchestrationService orchestrationService;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        readonly LogHelper logHelper;

        internal TaskActivityDispatcher(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskActivity> objectManager,
            DispatchMiddlewarePipeline dispatchPipeline,
            LogHelper logHelper)
        {
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.objectManager = objectManager ?? throw new ArgumentNullException(nameof(objectManager));
            this.dispatchPipeline = dispatchPipeline ?? throw new ArgumentNullException(nameof(dispatchPipeline));
            this.logHelper = logHelper;

            this.dispatcher = new WorkItemDispatcher<TaskActivityWorkItem>(
                "TaskActivityDispatcher",
                item => item.Id,
                this.OnFetchWorkItemAsync,
                this.OnProcessWorkItemAsync)
            {
                AbortWorkItem = orchestrationService.AbandonTaskActivityWorkItemAsync,
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                DispatcherCount = orchestrationService.TaskActivityDispatcherCount,
                MaxConcurrentWorkItems = orchestrationService.MaxConcurrentTaskActivityWorkItems,
                LogHelper = logHelper,
            };
        }

        /// <summary>
        /// Starts the dispatcher to start getting and processing task activities
        /// </summary>
        public async Task StartAsync()
        {
            await this.dispatcher.StartAsync();
        }

        /// <summary>
        /// Stops the dispatcher to stop getting and processing task activities
        /// </summary>
        /// <param name="forced">Flag indicating whether to stop gracefully or immediately</param>
        public async Task StopAsync(bool forced)
        {
            await this.dispatcher.StopAsync(forced);
        }

        /// <summary>
        /// Gets or sets flag whether to include additional details in error messages
        /// </summary>
        public bool IncludeDetails { get; set; }

        Task<TaskActivityWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.orchestrationService.LockNextTaskActivityWorkItem(receiveTimeout, cancellationToken);
        }

        async Task OnProcessWorkItemAsync(TaskActivityWorkItem workItem)
        {
            Task renewTask = null;
            var renewCancellationTokenSource = new CancellationTokenSource();

            TaskMessage taskMessage = workItem.TaskMessage;
            OrchestrationInstance orchestrationInstance = taskMessage.OrchestrationInstance;
            TaskScheduledEvent scheduledEvent = null;
            Activity diagnosticActivity = null;
            try
            {
                if (string.IsNullOrWhiteSpace(orchestrationInstance?.InstanceId))
                {
                    this.logHelper.TaskActivityDispatcherError(
                        workItem,
                        $"The activity worker received a message that does not have any OrchestrationInstance information.");
                    throw TraceHelper.TraceException(
                        TraceEventType.Error,
                        "TaskActivityDispatcher-MissingOrchestrationInstance",
                        new InvalidOperationException("Message does not contain any OrchestrationInstance information"));
                }

                if (taskMessage.Event.EventType != EventType.TaskScheduled)
                {
                    this.logHelper.TaskActivityDispatcherError(
                        workItem, 
                        $"The activity worker received an event of type '{taskMessage.Event.EventType}' but only '{EventType.TaskScheduled}' is supported.");
                    throw TraceHelper.TraceException(
                        TraceEventType.Critical,
                        "TaskActivityDispatcher-UnsupportedEventType",
                        new NotSupportedException("Activity worker does not support event of type: " +
                                                  taskMessage.Event.EventType));
                }

                // call and get return message
                scheduledEvent = (TaskScheduledEvent)taskMessage.Event;
                this.logHelper.TaskActivityStarting(orchestrationInstance, scheduledEvent);
                TaskActivity taskActivity = this.objectManager.GetObject(scheduledEvent.Name, scheduledEvent.Version);
                if (taskActivity == null)
                {
                    throw new TypeMissingException($"TaskActivity {scheduledEvent.Name} version {scheduledEvent.Version} was not found");
                }

                if (workItem.LockedUntilUtc < DateTime.MaxValue)
                {
                    // start a task to run RenewUntil
                    renewTask = Task.Factory.StartNew(
                        () => this.RenewUntil(workItem, renewCancellationTokenSource.Token),
                        renewCancellationTokenSource.Token);
                }

                // TODO : pass workflow instance data
                var context = new TaskContext(taskMessage.OrchestrationInstance);
                HistoryEvent eventToRespond = null;

                var dispatchContext = new DispatchMiddlewareContext();
                dispatchContext.SetProperty(taskMessage.OrchestrationInstance);
                dispatchContext.SetProperty(taskActivity);
                dispatchContext.SetProperty(scheduledEvent);

                // correlation
                CorrelationTraceClient.Propagate(() =>
                {
                    workItem.TraceContextBase?.SetActivityToCurrent();
                    diagnosticActivity = workItem.TraceContextBase?.CurrentActivity;
                });

                await this.dispatchPipeline.RunAsync(dispatchContext, async _ =>
                {
                    try
                    {
                        string output = await taskActivity.RunAsync(context, scheduledEvent.Input);
                        eventToRespond = new TaskCompletedEvent(-1, scheduledEvent.EventId, output);
                    }
                    catch (TaskFailureException e)
                    {
                        TraceHelper.TraceExceptionInstance(TraceEventType.Error, "TaskActivityDispatcher-ProcessTaskFailure", taskMessage.OrchestrationInstance, e);
                        string details = this.IncludeDetails ? e.Details : null;
                        eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                        this.logHelper.TaskActivityFailure(orchestrationInstance, scheduledEvent.Name, (TaskFailedEvent)eventToRespond, e);
                        CorrelationTraceClient.Propagate(() => CorrelationTraceClient.TrackException(e));
                    }
                    catch (Exception e) when (!Utils.IsFatal(e) && !Utils.IsExecutionAborting(e))
                    {
                        TraceHelper.TraceExceptionInstance(TraceEventType.Error, "TaskActivityDispatcher-ProcessException", taskMessage.OrchestrationInstance, e);
                        string details = this.IncludeDetails
                            ? $"Unhandled exception while executing task: {e}\n\t{e.StackTrace}"
                            : null;
                        eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                        this.logHelper.TaskActivityFailure(orchestrationInstance, scheduledEvent.Name, (TaskFailedEvent)eventToRespond, e);
                    }

                    if (eventToRespond is TaskCompletedEvent completedEvent)
                    {
                        this.logHelper.TaskActivityCompleted(orchestrationInstance, scheduledEvent.Name, completedEvent);
                    }
                });

                var responseTaskMessage = new TaskMessage
                {
                    Event = eventToRespond,
                    OrchestrationInstance = orchestrationInstance
                };

                await this.orchestrationService.CompleteTaskActivityWorkItemAsync(workItem, responseTaskMessage);
            }
            catch (SessionAbortedException e)
            {
                // The activity aborted its execution
                this.logHelper.TaskActivityAborted(orchestrationInstance, scheduledEvent, e.Message);
                TraceHelper.TraceInstance(TraceEventType.Warning, "TaskActivityDispatcher-ExecutionAborted", orchestrationInstance, "{0}: {1}", scheduledEvent.Name, e.Message);
                await this.orchestrationService.AbandonTaskActivityWorkItemAsync(workItem);
            }
            finally
            {
                diagnosticActivity?.Stop(); // Ensure the activity is stopped here to prevent it from leaking out.
                if (renewTask != null)
                {
                    renewCancellationTokenSource.Cancel();
                    try
                    {
                        // wait the renewTask finish
                        await renewTask;
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                }
            }
        }

        async Task RenewUntil(TaskActivityWorkItem workItem, CancellationToken cancellationToken)
        {
            try
            {
                if (workItem.LockedUntilUtc < DateTime.UtcNow)
                {
                    return;
                }

                DateTime renewAt = workItem.LockedUntilUtc.Subtract(TimeSpan.FromSeconds(30));

                // service bus clock sku can really mess us up so just always renew every 30 secs regardless of 
                // what the message.LockedUntilUtc says. if the sku is negative then in the worst case we will be
                // renewing every 5 secs
                //
                renewAt = this.AdjustRenewAt(renewAt);

                while (!cancellationToken.IsCancellationRequested)
                {
                    await Utils.DelayWithCancellation(TimeSpan.FromSeconds(5), cancellationToken);

                    if (DateTime.UtcNow < renewAt || cancellationToken.IsCancellationRequested)
                    {
                        continue;
                    }

                    try
                    {
                        this.logHelper.RenewActivityMessageStarting(workItem);
                        TraceHelper.Trace(TraceEventType.Information, "TaskActivityDispatcher-RenewLock", "Renewing lock for work item id {0}", workItem.Id);
                        workItem = await this.orchestrationService.RenewTaskActivityWorkItemLockAsync(workItem);
                        renewAt = workItem.LockedUntilUtc.Subtract(TimeSpan.FromSeconds(30));
                        renewAt = this.AdjustRenewAt(renewAt);
                        this.logHelper.RenewActivityMessageCompleted(workItem, renewAt);
                        TraceHelper.Trace(TraceEventType.Information, "TaskActivityDispatcher-RenewLockAt", "Next renew for work item id '{0}' at '{1}'", workItem.Id, renewAt);
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        // might have been completed
                        this.logHelper.RenewActivityMessageFailed(workItem, exception);
                        TraceHelper.TraceException(TraceEventType.Warning, "TaskActivityDispatcher-RenewLockFailure", exception, "Failed to renew lock for work item {0}", workItem.Id);
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // cancellation was triggered
            }
            catch (ObjectDisposedException)
            {
                // brokered message is already disposed probably through 
                // a complete call in the main dispatcher thread
            }
        }

        DateTime AdjustRenewAt(DateTime renewAt)
        {
            DateTime maxRenewAt = DateTime.UtcNow.Add(TimeSpan.FromSeconds(30));
            return renewAt > maxRenewAt ? maxRenewAt : renewAt;
        }
    }
}