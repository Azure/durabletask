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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using Tracing;
    using Tracking;

    internal sealed class TrackingDispatcher : DispatcherBase<SessionWorkItem>
    {
        const int PrefetchCount = 50;

        const int MaxRetriesServiceBus = 1;
        const int MaxRetriesTableStore = 5;
        const int IntervalBetweenRetriesSecs = 5;
        const int MaxDisplayStringLengthForAzureTableColumn = 1024*24;

        readonly MessagingFactory messagingFactory;
        readonly TrackingDispatcherSettings settings;

        readonly TableClient tableClient;
        readonly TaskHubDescription taskHubDescription;
        readonly string trackingEntityName;
        QueueClient trackingQueueClient;

        internal TrackingDispatcher(MessagingFactory messagingFactory,
            TaskHubDescription taskHubDescription,
            TaskHubWorkerSettings workerSettings,
            string tableConnectionString,
            string hubName,
            string trackingEntityName)
            : base("Tracking Dispatcher", item => item == null ? string.Empty : item.Session.SessionId)
        {
            this.taskHubDescription = taskHubDescription;
            settings = workerSettings.TrackingDispatcherSettings.Clone();
            this.trackingEntityName = trackingEntityName;
            this.messagingFactory = messagingFactory;
            this.messagingFactory.PrefetchCount = PrefetchCount;
            tableClient = new TableClient(hubName, tableConnectionString);
            maxConcurrentWorkItems = settings.MaxConcurrentTrackingSessions;
        }

        protected override void OnStart()
        {
            trackingQueueClient = messagingFactory.CreateQueueClient(trackingEntityName);
        }

        protected override void OnStopping(bool isForced)
        {
        }

        protected override void OnStopped(bool isForced)
        {
            trackingQueueClient.Close();
            messagingFactory.Close();
        }

        protected override async Task<SessionWorkItem> OnFetchWorkItem(TimeSpan receiveTimeout)
        {
            IEnumerable<BrokeredMessage> newMessages = null;

            MessageSession session = await trackingQueueClient.AcceptMessageSessionAsync(receiveTimeout);

            if (session == null)
            {
                return null;
            }

            newMessages = await Utils.ExecuteWithRetries(() => session.ReceiveBatchAsync(PrefetchCount),
                session.SessionId, "Receive Tracking Message Batch",
                MaxRetriesServiceBus,
                IntervalBetweenRetriesSecs);

            TraceHelper.TraceSession(TraceEventType.Information, session.SessionId,
                GetFormattedLog(
                    string.Format("{0} new messages to process: {1}",
                        newMessages.Count(),
                        string.Join(",", newMessages.Select(m => m.MessageId + " [" + m.SequenceNumber + "]")))));

            return new SessionWorkItem {Session = session, Messages = newMessages};
        }

        protected override async Task OnProcessWorkItem(SessionWorkItem sessionWorkItem)
        {
            MessageSession session = sessionWorkItem.Session;
            IEnumerable<BrokeredMessage> newMessages = sessionWorkItem.Messages;

            var historyEntities = new List<OrchestrationHistoryEventEntity>();
            var stateEntities = new List<OrchestrationStateEntity>();
            foreach (BrokeredMessage message in newMessages)
            {
                Utils.CheckAndLogDeliveryCount(message, taskHubDescription.MaxTrackingDeliveryCount);

                if (message.ContentType.Equals(FrameworkConstants.TaskMessageContentType,
                    StringComparison.OrdinalIgnoreCase))
                {
                    object historyEventIndexObj;

                    if (!message.Properties.TryGetValue(FrameworkConstants.HistoryEventIndexPropertyName,
                        out historyEventIndexObj)
                        || historyEventIndexObj == null)
                    {
                        throw new InvalidOperationException(
                            "Could not find a valid history event index property on tracking message " +
                            message.MessageId);
                    }

                    var historyEventIndex = (int) historyEventIndexObj;

                    TaskMessage taskMessage = await Utils.GetObjectFromBrokeredMessageAsync<TaskMessage>(message);
                    historyEntities.Add(new OrchestrationHistoryEventEntity(
                        taskMessage.OrchestrationInstance.InstanceId,
                        taskMessage.OrchestrationInstance.ExecutionId,
                        historyEventIndex,
                        DateTime.UtcNow,
                        taskMessage.Event));
                }
                else if (message.ContentType.Equals(FrameworkConstants.StateMessageContentType,
                    StringComparison.OrdinalIgnoreCase))
                {
                    StateMessage stateMessage = await Utils.GetObjectFromBrokeredMessageAsync<StateMessage>(message);
                    stateEntities.Add(new OrchestrationStateEntity(stateMessage.State));
                }
                else
                {
                    throw new InvalidOperationException("Invalid tracking message content type: " +
                                                        message.ContentType);
                }
            }

            TraceEntities(TraceEventType.Verbose, "Writing tracking history event", historyEntities,
                GetNormalizedHistoryEventEntityTrace);
            TraceEntities(TraceEventType.Verbose, "Writing tracking state event", stateEntities,
                GetNormalizedStateEntityTrace);

            try
            {
                await Utils.ExecuteWithRetries(() => tableClient.WriteEntitesAsync(historyEntities),
                    session.SessionId, string.Format("WriteHistoryEntities:{0}", session.SessionId),
                    MaxRetriesTableStore,
                    IntervalBetweenRetriesSecs);
            }
            catch (Exception)
            {
                TraceEntities(TraceEventType.Critical, "Failed to write history entity", historyEntities,
                    GetNormalizedHistoryEventEntityTrace);
                throw;
            }

            try
            {
                foreach (OrchestrationStateEntity stateEntity in stateEntities)
                {
                    await Utils.ExecuteWithRetries(
                        () => tableClient.WriteEntitesAsync(new List<OrchestrationStateEntity> {stateEntity}),
                        session.SessionId, string.Format("WriteStateEntities:{0}", session.SessionId),
                        MaxRetriesTableStore,
                        IntervalBetweenRetriesSecs);
                }
            }
            catch (Exception)
            {
                TraceEntities(TraceEventType.Critical, "Failed to write state entity", stateEntities,
                    GetNormalizedStateEntityTrace);
                throw;
            }

            IEnumerable<Guid> lockTokens = newMessages.Select(m => m.LockToken);
            Utils.SyncExecuteWithRetries<object>(() =>
            {
                session.CompleteBatch(lockTokens);
                return null;
            },
                session.SessionId,
                "Complete New Tracking Messages",
                MaxRetriesServiceBus,
                IntervalBetweenRetriesSecs);
        }

        void TraceEntities<T>(TraceEventType eventType, string message,
            IEnumerable<T> entities, Func<int, string, T, string> traceGenerator)
        {
            int index = 0;
            foreach (T entry in entities)
            {
                TraceHelper.Trace(eventType, () => traceGenerator(index, message, entry));
                index++;
            }
        }

        string GetNormalizedStateEntityTrace(int index, string message, OrchestrationStateEntity stateEntity)
        {
            string serializedHistoryEvent = Utils.EscapeJson(JsonConvert.SerializeObject(stateEntity.State));
            int historyEventLength = serializedHistoryEvent.Length;

            if (historyEventLength > MaxDisplayStringLengthForAzureTableColumn)
            {
                serializedHistoryEvent =
                    serializedHistoryEvent.Substring(0, MaxDisplayStringLengthForAzureTableColumn) +
                    " ....(truncated)..]";
            }

            return
                GetFormattedLog(
                    string.Format(message + " - #{0} - Instance Id: {1}, Execution Id: {2}, State Length: {3}\n{4}",
                        index,
                        stateEntity.State != null && stateEntity.State.OrchestrationInstance != null
                            ? stateEntity.State.OrchestrationInstance.InstanceId
                            : string.Empty,
                        stateEntity.State != null && stateEntity.State.OrchestrationInstance != null
                            ? stateEntity.State.OrchestrationInstance.ExecutionId
                            : string.Empty,
                        historyEventLength, serializedHistoryEvent));
        }

        string GetNormalizedHistoryEventEntityTrace(int index, string message, OrchestrationHistoryEventEntity entity)
        {
            string serializedHistoryEvent = Utils.EscapeJson(JsonConvert.SerializeObject(entity.HistoryEvent));
            int historyEventLength = serializedHistoryEvent.Length;

            if (historyEventLength > MaxDisplayStringLengthForAzureTableColumn)
            {
                serializedHistoryEvent =
                    serializedHistoryEvent.Substring(0, MaxDisplayStringLengthForAzureTableColumn) +
                    " ....(truncated)..]";
            }

            return
                GetFormattedLog(
                    string.Format(
                        message + " - #{0} - Instance Id: {1}, Execution Id: {2}, HistoryEvent Length: {3}\n{4}",
                        index, entity.InstanceId, entity.ExecutionId, historyEventLength, serializedHistoryEvent));
        }

        protected override async Task SafeReleaseWorkItem(SessionWorkItem workItem)
        {
            if (workItem != null && workItem.Session != null)
            {
                try
                {
                    await workItem.Session.CloseAsync();
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.Session.SessionId, ex,
                        "Error while closing session");
                }
            }
        }

        protected override async Task AbortWorkItem(SessionWorkItem workItem)
        {
            if (workItem != null && workItem.Session != null)
            {
                if (workItem.Messages != null && workItem.Messages.Any())
                {
                    TraceHelper.TraceSession(TraceEventType.Error, workItem.Session.SessionId,
                        "Abandoning {0} messages due to workitem abort", workItem.Messages.Count());

                    foreach (BrokeredMessage message in workItem.Messages)
                    {
                        await workItem.Session.AbandonAsync(message.LockToken);
                    }
                }

                try
                {
                    workItem.Session.Abort();
                }
                catch (Exception ex)
                {
                    TraceHelper.TraceExceptionSession(TraceEventType.Warning, workItem.Session.SessionId, ex,
                        "Error while aborting session");
                }
            }
        }

        protected override int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            if (exception is MessagingException)
            {
                return settings.TransientErrorBackOffSecs;
            }

            return 0;
        }

        protected override int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            if (exception is TimeoutException)
            {
                return 0;
            }

            int delay = settings.NonTransientErrorBackOffSecs;
            if (exception is MessagingException && (exception as MessagingException).IsTransient)
            {
                delay = settings.TransientErrorBackOffSecs;
            }
            return delay;
        }
    }
}