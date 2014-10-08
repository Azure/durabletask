//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using Microsoft.ServiceBus.DurableTask.Tracing;

namespace Microsoft.ServiceBus.DurableTask
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus.DurableTask.Tracking;
    using Microsoft.ServiceBus.Messaging;

    // AFFANDAR : TODO : master list for tracking
    //          * file rename orchestratorstartedaction
    //          * utc time in azure table
    //          * data retention
    //      
    //  DONE:
    //          * store only event list in session state
    //          * unit tests
    //          * test intermediate states
    //          * remove KeepStateAfterCompletion
    //          * do not expose historyevent directly
    //          * test for terminate instance
    //          * management api hookup
    //          * separate management api --- NO

    sealed class TrackingDispatcher : DispatcherBase<MessageSession>
    {
        private const int TransientErrorBackOffSecs = 10;
        private const int NonTransientErrorBackOffSecs = 120;
        private const int PrefetchCount = 50;

        private static TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(30);
        private const int MaxRetries = 5;
        private const int IntervalBetweenRetriesSecs = 5;

        protected readonly MessagingFactory messagingFactory;
        protected QueueClient trackingQueueClient;
        protected readonly string trackingEntityName;
        private TableClient tableClient;

        internal TrackingDispatcher(MessagingFactory messagingFactory, string tableConnectionString, string hubName,
                                  string trackingEntityName)
            : base("Tracking Dispatcher", item => item.SessionId)
        {
            this.trackingEntityName = trackingEntityName;
            this.messagingFactory = messagingFactory;
            this.messagingFactory.PrefetchCount = PrefetchCount;
            this.tableClient = new TableClient(hubName, tableConnectionString);
        }

        protected override void OnStart()
        {
            this.trackingQueueClient = this.messagingFactory.CreateQueueClient(this.trackingEntityName);
        }

        protected override void OnStopping(bool isForced)
        {
        }

        protected override void OnStopped(bool isForced)
        {
            this.trackingQueueClient.Close();
            this.messagingFactory.Close();
        }

        protected override Task<MessageSession> OnFetchWorkItem()
        {
            return this.trackingQueueClient.AcceptMessageSessionAsync(ReceiveTimeout);
        }

        protected override async Task OnProcessWorkItem(MessageSession session)
        {
            IEnumerable<BrokeredMessage> newMessages =
                await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(PrefetchCount),
                                               session.SessionId, "Receive Tracking Message Batch", MaxRetries,
                                               IntervalBetweenRetriesSecs);

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                                     "{0} new tracking messages to process",
                                     newMessages.Count());

            List<OrchestrationHistoryEventEntity> historyEntities = new List<OrchestrationHistoryEventEntity>();
            List<OrchestrationStateEntity> stateEntities = new List<OrchestrationStateEntity>();
            foreach (BrokeredMessage message in newMessages)
            {
                if (message.ContentType.Equals(FrameworkConstants.TaskMessageContentType, StringComparison.OrdinalIgnoreCase))
                {
                    object historyEventIndexObj;
                    
                    if (!message.Properties.TryGetValue(FrameworkConstants.HistoryEventIndexPropertyName, out historyEventIndexObj)
                        || historyEventIndexObj == null)
                    {
                        throw new InvalidOperationException("Could not find a valid history event index property on tracking message " + message.MessageId);
                    }

                    int historyEventIndex = (int) historyEventIndexObj;

                    TaskMessage taskMessage = message.GetBody<TaskMessage>();
                    historyEntities.Add(new OrchestrationHistoryEventEntity(
                                            taskMessage.OrchestrationInstance.InstanceId,
                                            taskMessage.OrchestrationInstance.ExecutionId,
                                            historyEventIndex,
                                            DateTime.UtcNow,
                                            taskMessage.Event));

                }
                else if (message.ContentType.Equals(FrameworkConstants.StateMessageContentType, StringComparison.OrdinalIgnoreCase))
                {
                    StateMessage stateMessage = message.GetBody<StateMessage>();
                    stateEntities.Add(new OrchestrationStateEntity(stateMessage.State));
                }
                else
                {
                    throw new InvalidOperationException("Invalid tracking message content type: " + message.ContentType);
                }
            }

            foreach (var historyEntity in historyEntities)
            {
                TraceHelper.TraceSession(TraceEventType.Verbose, session.SessionId, 
                    () => "Writing tracking history event: " + Utils.EscapeJson(historyEntity.ToString()));
            }
            
            foreach (var stateEntity in stateEntities)
            {
                TraceHelper.TraceSession(TraceEventType.Verbose, session.SessionId, 
                    () => "Writing tracking state event: " + Utils.EscapeJson(stateEntity.ToString()));
            }

            await Utils.ExecuteWithRetries<object>(() => this.tableClient.WriteEntitesAsync(historyEntities), 
                                session.SessionId, string.Format("WriteHistoryEntities:{0}", session.SessionId), MaxRetries,
                                IntervalBetweenRetriesSecs);

            foreach (var stateEntity in stateEntities)
            {
                await Utils.ExecuteWithRetries(
                    () => this.tableClient.WriteEntitesAsync(new List<OrchestrationStateEntity>() { stateEntity }),
                                session.SessionId, string.Format("WriteStateEntities:{0}", session.SessionId), MaxRetries,
                                IntervalBetweenRetriesSecs);
            }

            IEnumerable<Guid> lockTokens = newMessages.Select((m) => m.LockToken);
            Utils.SyncExecuteWithRetries<object>(() =>
                        {
                            session.CompleteBatch(lockTokens);
                            return null;
                        }, session.SessionId, "Complete New Tracking Messages", MaxRetries, IntervalBetweenRetriesSecs);

        }

        protected override async Task SafeReleaseWorkItem(MessageSession workItem)
        {
            if (workItem != null)
            {
                try
                {
                    await Task.Factory.FromAsync(workItem.BeginClose, workItem.EndClose, null);
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.SessionId, ex,
                                                      "Error while closing session");
                }
            }
        }

        protected override async Task AbortWorkItem(MessageSession workItem)
        {
            await this.SafeReleaseWorkItem(workItem);
        }

        protected override int GetDelayInSeconds(Exception exception)
        {
            if (exception is TimeoutException)
            {
                return 0;
            }

            int delay = NonTransientErrorBackOffSecs;
            if (exception is MessagingException && (exception as MessagingException).IsTransient)
            {
                delay = TransientErrorBackOffSecs;
            }
            return delay;
        }
    }
}
