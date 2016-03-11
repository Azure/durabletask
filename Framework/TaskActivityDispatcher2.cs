﻿//  ----------------------------------------------------------------------------------
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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using DurableTask.History;
    using DurableTask.Serializing;
    using DurableTask.Tracing;

    public sealed class TaskActivityDispatcher2 //: DispatcherBase2<TaskActivityWorkItem>
    {
        readonly NameVersionObjectManager<TaskActivity> objectManager;
        //readonly TaskHubWorkerSettings settings;
        private readonly WorkItemDispatcher<TaskActivityWorkItem> dispatcher; 
        readonly IOrchestrationService orchestrationService;
        
        internal TaskActivityDispatcher2(
            //TaskHubWorkerSettings workerSettings,
            IOrchestrationService orchestrationService,
            NameVersionObjectManager<TaskActivity> objectManager)
            //: base("TaskActivityDispatcher", item => item.Id)
        {
            //if (settings == null)
            //{
            //    throw new ArgumentNullException(nameof(settings));
            //}

            if (orchestrationService == null)
            {
                throw new ArgumentNullException(nameof(orchestrationService));
            }

            if (objectManager == null)
            {
                throw new ArgumentNullException(nameof(objectManager));
            }

            //settings = workerSettings.Clone();

            this.orchestrationService = orchestrationService;
            this.objectManager = objectManager;

            this.dispatcher = new WorkItemDispatcher<TaskActivityWorkItem>(
                "TaskActivityDispatcher",
                item => item.Id,
                this.OnFetchWorkItemAsync,
                this.OnProcessWorkItemAsync)
            {
                GetDelayInSecondsAfterOnFetchException = orchestrationService.GetDelayInSecondsAfterOnFetchException,
                GetDelayInSecondsAfterOnProcessException = orchestrationService.GetDelayInSecondsAfterOnProcessException,
                MaxConcurrentWorkItems = orchestrationService.MaxConcurrentTaskOrchestrationWorkItems()
            };
        }

        public async Task StartAsync()
        {
            await dispatcher.StartAsync();
        }

        public async Task StopAsync(bool forced)
        {
            await dispatcher.StopAsync(forced);
        }

        public bool IncludeDetails { get; set;} 

        private Task<TaskActivityWorkItem> OnFetchWorkItemAsync(TimeSpan receiveTimeout)
        {
            return this.orchestrationService.LockNextTaskActivityWorkItem(receiveTimeout, CancellationToken.None);
        }

        private async Task OnProcessWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // AFFANDAR : TODO : add this to the orchestration service impl
            //Utils.CheckAndLogDeliveryCount(message, taskHubDescription.MaxTaskActivityDeliveryCount);

            Task renewTask = null;
            var renewCancellationTokenSource = new CancellationTokenSource();

            try
            {
                TaskMessage taskMessage = workItem.TaskMessage;
                OrchestrationInstance orchestrationInstance = taskMessage.OrchestrationInstance;
                if (string.IsNullOrWhiteSpace(orchestrationInstance?.InstanceId))
                {
                    throw TraceHelper.TraceException(TraceEventType.Error,
                        new InvalidOperationException("Message does not contain any OrchestrationInstance information"));
                }
                if (taskMessage.Event.EventType != EventType.TaskScheduled)
                {
                    throw TraceHelper.TraceException(TraceEventType.Critical,
                        new NotSupportedException("Activity worker does not support event of type: " +
                                                  taskMessage.Event.EventType));
                }

                // call and get return message
                var scheduledEvent = (TaskScheduledEvent) taskMessage.Event;
                TaskActivity taskActivity = objectManager.GetObject(scheduledEvent.Name, scheduledEvent.Version);
                if (taskActivity == null)
                {
                    throw new TypeMissingException($"TaskActivity {scheduledEvent.Name} version {scheduledEvent.Version} was not found");
                }

                renewTask = Task.Factory.StartNew(() => RenewUntil(workItem, renewCancellationTokenSource.Token));

                // TODO : pass workflow instance data
                var context = new TaskContext(taskMessage.OrchestrationInstance);
                HistoryEvent eventToRespond = null;

                try
                {
                    string output = await taskActivity.RunAsync(context, scheduledEvent.Input);
                    eventToRespond = new TaskCompletedEvent(-1, scheduledEvent.EventId, output);
                }
                catch (TaskFailureException e)
                {
                    TraceHelper.TraceExceptionInstance(TraceEventType.Error, taskMessage.OrchestrationInstance, e);
                    string details = IncludeDetails ? e.Details : null;
                    eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    TraceHelper.TraceExceptionInstance(TraceEventType.Error, taskMessage.OrchestrationInstance, e);
                    string details = IncludeDetails
                        ? $"Unhandled exception while executing task: {e}\n\t{e.StackTrace}"
                        : null;
                    eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                }

                var responseTaskMessage = new TaskMessage
                {
                    Event = eventToRespond,
                    OrchestrationInstance = orchestrationInstance
                };

                await this.orchestrationService.CompleteTaskActivityWorkItemAsync(workItem, responseTaskMessage);
            }
            finally
            {
                if (renewTask != null)
                {
                    renewCancellationTokenSource.Cancel();
                    renewTask.Wait();
                }
            }
        }

        async void RenewUntil(TaskActivityWorkItem workItem, CancellationToken cancellationToken)
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
                renewAt = AdjustRenewAt(renewAt);

                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5));

                    if (DateTime.UtcNow < renewAt)
                    {
                        continue;
                    }

                    try
                    {
                        TraceHelper.Trace(TraceEventType.Information, "Renewing lock for workitem id {0}", workItem.Id);
                        workItem = await this.orchestrationService.RenewTaskActivityWorkItemLockAsync(workItem);
                        renewAt = workItem.LockedUntilUtc.Subtract(TimeSpan.FromSeconds(30));
                        renewAt = AdjustRenewAt(renewAt);
                        TraceHelper.Trace(TraceEventType.Information, "Next renew for workitem id '{0}' at '{1}'", workItem.Id, renewAt);
                    }
                    catch (Exception exception) when (!Utils.IsFatal(exception))
                    {
                        // might have been completed
                        TraceHelper.TraceException(TraceEventType.Information, exception, "Failed to renew lock for workitem {0}", workItem.Id);
                        break;
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // brokeredmessage is already disposed probably through 
                // a complete call in the main dispatcher thread
            }
        }

        DateTime AdjustRenewAt(DateTime renewAt)
        {
            DateTime maxRenewAt = DateTime.UtcNow.Add(TimeSpan.FromSeconds(30));
            return renewAt > maxRenewAt ? maxRenewAt : renewAt;
        }
        /*
        private int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (orchestrationService.IsTransientException(exception))
            {
                return settings.TaskActivityDispatcherSettings.TransientErrorBackOffSecs;
            }

            return 0;
        }

        private int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            int delay = settings.TaskActivityDispatcherSettings.NonTransientErrorBackOffSecs;
            if (orchestrationService.IsTransientException(exception))
            {
                delay = settings.TaskActivityDispatcherSettings.TransientErrorBackOffSecs;
            }
            return delay;
        }
        */
    }
}