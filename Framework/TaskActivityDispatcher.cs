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
                var scheduledEvent = (TaskScheduledEvent) taskMessage.Event;
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
                    workerQueueClient.Complete(message.LockToken);
                    deciderSender.Send(responseMessage);
                    ts.Complete();
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

        async void RenewUntil(BrokeredMessage message, CancellationToken cancellationToken)
        {
            try
            {
                if (message.LockedUntilUtc < DateTime.UtcNow)
                {
                    return;
                }

                DateTime renewAt = message.LockedUntilUtc.Subtract(TimeSpan.FromSeconds(30));

                // service bus clock sku can really mess us up so just always renew every 30 secs regardless of 
                // what the message.LockedUntilUtc says. if the sku is negative then in the worst case we will be
                // renewing every 5 secs
                //
                renewAt = AdjustRenewAt(renewAt);

                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5));

                    if (DateTime.UtcNow >= renewAt)
                    {
                        try
                        {
                            TraceHelper.Trace(TraceEventType.Information, "Renewing lock for message id {0}",
                                message.MessageId);
                            message.RenewLock();
                            renewAt = message.LockedUntilUtc.Subtract(TimeSpan.FromSeconds(30));
                            renewAt = AdjustRenewAt(renewAt);
                            TraceHelper.Trace(TraceEventType.Information, "Next renew for message id '{0}' at '{1}'",
                                message.MessageId, renewAt);
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