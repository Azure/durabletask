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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using History;
    using Microsoft.ServiceBus.Messaging;
    using Tracing;
    using System.Collections.Generic;

    public sealed class TaskActivityDispatcher : DispatcherBase<BrokeredMessage>
    {
        readonly MessagingFactory messagingFactory;

        readonly NameVersionObjectManager<TaskActivity> objectManager;
        readonly string orchestratorQueueName;
        readonly TaskHubWorkerSettings settings;
        readonly TaskHubDescription taskHubDescription;
        readonly string workerQueueName;
        MessageSender deciderSender;
        QueueClient workerQueueClient;

        internal TaskActivityDispatcher(MessagingFactory messagingFactory,
            TaskHubDescription taskHubDescription,
            TaskHubWorkerSettings workerSettings,
            string orchestratorQueueName,
            string workerQueueName,
            NameVersionObjectManager<TaskActivity> objectManager)
            : base("TaskActivityDispatcher", item => item.MessageId)
        {
            this.taskHubDescription = taskHubDescription;
            settings = workerSettings.Clone();
            this.orchestratorQueueName = orchestratorQueueName;
            this.workerQueueName = workerQueueName;
            this.messagingFactory = messagingFactory;
            this.objectManager = objectManager;
            maxConcurrentWorkItems = settings.TaskActivityDispatcherSettings.MaxConcurrentActivities;
        }

        public bool IncludeDetails { get; set; }

        protected override async Task<BrokeredMessage> OnFetchWorkItem(TimeSpan receiveTimeout)
        {
            BrokeredMessage receivedMessage = await workerQueueClient.ReceiveAsync(receiveTimeout);

            if (receivedMessage != null)
            {
                TraceHelper.TraceSession(TraceEventType.Information,
                    receivedMessage.SessionId,
                    GetFormattedLog($"New message to process: {receivedMessage.MessageId} [{receivedMessage.SequenceNumber}], latency: {receivedMessage.DeliveryLatency()}ms"));
            }

            return receivedMessage;
        }

        protected override async Task OnProcessWorkItem(BrokeredMessage message)
        {
            Utils.CheckAndLogDeliveryCount(message, taskHubDescription.MaxTaskActivityDeliveryCount);

            Task renewTask = null;
            var renewCancellationTokenSource = new CancellationTokenSource();

            try
            {
                TaskMessage taskMessage = await Utils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message);
                OrchestrationInstance orchestrationInstance = taskMessage.OrchestrationInstance;
                if (orchestrationInstance == null || string.IsNullOrWhiteSpace(orchestrationInstance.InstanceId))
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
                var scheduledEvent = (TaskScheduledEvent)taskMessage.Event;
                TaskActivity taskActivity = objectManager.GetObject(scheduledEvent.Name, scheduledEvent.Version);
                if (taskActivity == null)
                {
                    throw new TypeMissingException("TaskActivity " + scheduledEvent.Name + " version " +
                                                   scheduledEvent.Version + " was not found");
                }

                renewTask = Task.Factory.StartNew(() => RenewUntil(message, renewCancellationTokenSource.Token));

                // TODO : pass workflow instance data
                var context = new TaskContext(taskMessage.OrchestrationInstance);
                HistoryEvent eventToRespond = null;

                try
                {
                    /* string output = null;
                     Task<string> actionTask = taskActivity.RunAsync(context, scheduledEvent.Input);                   
                     Task timeOutTask = Task.Delay(30000);

                     List<Task> waitingTasks = new List<Task>();
                     waitingTasks.Add(actionTask);
                     waitingTasks.Add(Task.Delay(30000));
                     bool isTaskCompleted = false;

                     while (isTaskCompleted == false)
                     {
                         if (await Task.WhenAny(waitingTasks) == actionTask)
                         {
                             output = actionTask.Result;
                             isTaskCompleted = true;
                         }
                         else
                         {
                             waitingTasks = new List<Task>();
                             waitingTasks.Add(actionTask);
                             waitingTasks.Add(Task.Delay(30000));

                             message.RenewLock();
                         }
                     }*/

                    string output = await taskActivity.RunAsync(context, scheduledEvent.Input);

                    eventToRespond = new TaskCompletedEvent(-1, scheduledEvent.EventId, output);
                }
                catch (TaskFailureException e)
                {
                    TraceHelper.TraceExceptionInstance(TraceEventType.Error, taskMessage.OrchestrationInstance, e);
                    string details = IncludeDetails ? e.Details : null;
                    eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                }
                catch (Exception e)
                {
                    TraceHelper.TraceExceptionInstance(TraceEventType.Error, taskMessage.OrchestrationInstance, e);
                    string details = IncludeDetails
                        ? string.Format("Unhandled exception while executing task: {0}\n\t{1}", e, e.StackTrace)
                        : null;
                    eventToRespond = new TaskFailedEvent(-1, scheduledEvent.EventId, e.Message, details);
                }

                var responseTaskMessage = new TaskMessage();
                responseTaskMessage.Event = eventToRespond;
                responseTaskMessage.OrchestrationInstance = orchestrationInstance;

                BrokeredMessage responseMessage = Utils.GetBrokeredMessageFromObject(responseTaskMessage,
                    settings.MessageCompressionSettings,
                    orchestrationInstance, "Response for " + message.MessageId);
                responseMessage.SessionId = orchestrationInstance.InstanceId;

                using (var ts = new TransactionScope())
                {
                    //TraceHelper.Trace(TraceEventType.Information, "1 - workerQueueClient.Complete(message.LockToken)", message.MessageId);
                    workerQueueClient.Complete(message.LockToken);

                    //TraceHelper.Trace(TraceEventType.Information, "2 - deciderSender.Send(responseMessage)", message.MessageId);

                    retry((msg) =>
                    {
                        deciderSender.Send(msg);
                    }, responseMessage);

                    //TraceHelper.Trace(TraceEventType.Information, "3 - deciderSender has sent the message.", message.MessageId);

                    ts.Complete();

                    //TraceHelper.Trace(TraceEventType.Information, "4 - ts.Complete()", message.MessageId);
                }
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


        /// <summary>
        /// The new "retry()" method provides retrying of sending of the new task message to SB.        
        /// In some rare cases Complete of the current message and sending of the new message, which run in one transaction, 
        /// might fail du transaction failure.This exceptional case can be mostly reproduced when host is running On-Prem and Azure Service Bus in remote hosting center is used.
        /// This method is called inside of the transaction, when Complete succeeds and Sending of the new message fails.
        ///  When that happen, without of sending of the new message, system would stay in inconsistent state:
        /// looping orchestration would not be dispatched any more and orchestration instance would be still tracked as running.
        /// </summary>
        /// <param name="action"></param>
        /// <param name="msg"></param>
        private static void retry(Action<BrokeredMessage> action, BrokeredMessage msg)
        {
            // TODO: Consider to make configurable.
            int retries = 5;
            double delay = 1000;

            while (true)
            {
                try
                {
                    bool i = false;
                    if (i)
                        throw new NotImplementedException(":)");

                    action(msg);

                    return;
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceException(TraceEventType.Error, ex, "Failed to send message with id='{0}'. Delay: {1}, Retries: {2}",
                      msg.MessageId, delay, retries);

                    retries--;

                    Thread.Sleep((int)delay);

                    delay = delay * 1.5;

                    if (retries <= 0)
                    {
                        string errMsg = String.Format("Fatal Error! Orchestration might run in inconsostent state. Failed to send message with id='{0}' after multiple retries.",
                        delay, retries);
                        TraceHelper.TraceException(TraceEventType.Error, ex,
                        errMsg);

                        throw new TaskFailedException(errMsg, ex);
                    }
                    else
                        msg = msg.Clone();
                }
            }
        }


        /// <summary>
        /// Ensures that long running tasks are not redispatched if execution takes longer than 
        /// LockDuration, which is usually 1 minute by default.
        /// It would be good to make default renew interval configurable.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        async void RenewUntil(BrokeredMessage message, CancellationToken cancellationToken)
        {
            try
            {
                int renewInterval = 30000;

                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(renewInterval));
                    {
                        if (!cancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                TraceHelper.Trace(TraceEventType.Information, "Renewing lock for message id {0}",
                                    message.MessageId);

                                await message.RenewLockAsync();

                                TraceHelper.Trace(TraceEventType.Information, "Next renew for message id '{0}' at '{1}'",
                                    message.MessageId, DateTime.UtcNow.AddMilliseconds(renewInterval));
                            }
                            catch (Exception exception)
                            {
                                // might have been completed
                                TraceHelper.TraceException(TraceEventType.Information, exception,
                                    "Failed to renew lock for message {0}", message.MessageId);
                                break;
                            }
                        }
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

            if (renewAt > maxRenewAt)
            {
                return maxRenewAt;
            }

            return renewAt;
        }

        protected override void OnStart()
        {
            workerQueueClient = messagingFactory.CreateQueueClient(workerQueueName);
            deciderSender = messagingFactory.CreateMessageSender(orchestratorQueueName, workerQueueName);
        }

        protected override void OnStopping(bool isForced)
        {
        }

        protected override void OnStopped(bool isForced)
        {
            workerQueueClient.Close();
            deciderSender.Close();
            messagingFactory.Close();
        }

        protected override Task SafeReleaseWorkItem(BrokeredMessage workItem)
        {
            if (workItem != null)
            {
                workItem.Dispose();
            }

            return Task.FromResult<Object>(null);
        }

        protected override async Task AbortWorkItem(BrokeredMessage workItem)
        {
            if (workItem != null)
            {
                TraceHelper.Trace(TraceEventType.Information, "Abandoing message " + workItem.MessageId);
                await workItem.AbandonAsync();
            }
        }

        protected override int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (exception is MessagingException)
            {
                return settings.TaskActivityDispatcherSettings.TransientErrorBackOffSecs;
            }

            return 0;
        }

        protected override int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            int delay = settings.TaskActivityDispatcherSettings.NonTransientErrorBackOffSecs;
            if (exception is MessagingException && (exception as MessagingException).IsTransient)
            {
                delay = settings.TaskActivityDispatcherSettings.TransientErrorBackOffSecs;
            }
            return delay;
        }
    }
}