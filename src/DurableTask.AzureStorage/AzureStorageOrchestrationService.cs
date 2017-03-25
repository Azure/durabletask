//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public class AzureStorageOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CloudQueue controlQueue;
        readonly CloudQueue workItemQueue;
        readonly CloudTable historyTable;
        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessages;

        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;

        readonly BackoffPollingHelper controlQueueBackoff;
        readonly BackoffPollingHelper workItemQueueBackoff;

        bool isStarted;

        public AzureStorageOrchestrationService(AzureStorageOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            this.settings = settings;
            this.tableEntityConverter = new TableEntityConverter();
            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);
            CloudQueueClient queueClient = account.CreateCloudQueueClient();
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // TODO: Need to do input validation on the TaskHubName.
            string controlQueueName = $"{settings.TaskHubName.ToLowerInvariant()}-control";
            this.controlQueue = queueClient.GetQueueReference(controlQueueName);

            string workItemQueueName = $"{settings.TaskHubName.ToLowerInvariant()}-workitems";
            this.workItemQueue = queueClient.GetQueueReference(workItemQueueName);

            string historyTableName = $"{settings.TaskHubName}History";
            this.historyTable = tableClient.GetTableReference(historyTableName);

            this.pendingOrchestrationMessages = new LinkedList<PendingMessageBatch>();

            // Use reflection to learn all the different event types supported by DTFx.
            // This could have been hardcoded, but I generally try to avoid hardcoding of point-in-time DTFx knowledge.
            Type historyEventType = typeof(HistoryEvent);
            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));
            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);

            // Queue polling backoff policies
            var maxPollingDelay = TimeSpan.FromSeconds(10);
            var minPollingDelayThreshold = TimeSpan.FromMilliseconds(500);
            this.controlQueueBackoff = new BackoffPollingHelper(maxPollingDelay, minPollingDelayThreshold);
            this.workItemQueueBackoff = new BackoffPollingHelper(maxPollingDelay, minPollingDelayThreshold);
        }

        public event EventHandler<HistoryEventArgs> OnNewEvent;

        #region IOrchestrationService
        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems
        {
            get { return this.settings.MaxConcurrentTaskOrchestrationWorkItems; }
        }

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems
        {
            get { return this.settings.MaxConcurrentTaskActivityWorkItems; }
        }

        // We always leave the dispatcher counts at one unless we can find a customer workload that requires more.
        int IOrchestrationService.TaskActivityDispatcherCount { get; } = 1;
        int IOrchestrationService.TaskOrchestrationDispatcherCount { get; } = 1;

        #region Management Operations (Create/Delete/Start/Stop)
        /// <summary>
        /// Deletes and creates the neccesary Azure Storage resources for the orchestration service.
        /// </summary>
        public async Task CreateAsync()
        {
            await this.DeleteAsync();

            Task createControlQueue = this.controlQueue.CreateAsync();
            Task createWorkItemQueue = this.workItemQueue.CreateAsync();
            Task createHistoryTable = this.historyTable.CreateAsync();
            await Task.WhenAll(createControlQueue, createWorkItemQueue, createHistoryTable);
        }

        /// <summary>
        /// Creates the necessary Azure Storage resources for the orchestration service if they don't already exist.
        /// </summary>
        public async Task CreateIfNotExistsAsync()
        {
            Task createControlQueue = this.controlQueue.CreateIfNotExistsAsync();
            Task createWorkItemQueue = this.workItemQueue.CreateIfNotExistsAsync();
            Task createHistoryTable = this.historyTable.CreateIfNotExistsAsync();
            await Task.WhenAll(createControlQueue, createWorkItemQueue, createHistoryTable);
        }

        /// <summary>
        /// Deletes the Azure Storage resources used by the orchestration service.
        /// </summary>
        public async Task DeleteAsync()
        {
            Task<bool> deleteControlQueue = this.controlQueue.DeleteIfExistsAsync();
            Task<bool> deleteWorkItemQueue = this.workItemQueue.DeleteIfExistsAsync();
            Task<bool> deleteHistoryTable = this.historyTable.DeleteIfExistsAsync();
            await Task.WhenAll(deleteControlQueue, deleteWorkItemQueue, deleteHistoryTable);
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            throw new NotSupportedException("The instance store is not supported by the Azure Storage provider.");
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            throw new NotSupportedException("The instance store is not supported by the Azure Storage provider.");
        }

        public Task StartAsync()
        {
            if (this.isStarted)
            {
                throw new InvalidOperationException("The orchestration service has already started.");
            }

            // Disable nagling to improve storage access latency:
            // https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            // Ad-hoc testing has shown very nice improvements (20%-50% drop in queue message age for simple scenarios).
            ServicePointManager.FindServicePoint(this.historyTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.controlQueue.Uri).UseNagleAlgorithm = false;

            this.isStarted = true;
            return Task.FromResult(0);
        }

        public Task StopAsync()
        {
            return this.StopAsync(isForced: false);
        }

        public Task StopAsync(bool isForced)
        {
            this.isStarted = false;
            return Task.FromResult(0);
        }
        #endregion

        #region Orchestration Work Item Methods
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            PendingMessageBatch nextBatch;
            while (true)
            {
                // Each iteration we fetch a batch of messages and sort them into in-memory buckets according to their instance ID.
                IEnumerable<CloudQueueMessage> queueMessageBatch = await this.controlQueue.GetMessagesAsync(
                    this.settings.ControlQueueBatchSize,
                    this.settings.ControlQueueVisibilityTimeout,
                    this.settings.ControlQueueRequestOptions,
                    null /* operationContext */,
                    cancellationToken);

                nextBatch = StashMessagesAndGetNextBatch(queueMessageBatch);
                if (nextBatch != null)
                {
                    break;
                }

                await this.controlQueueBackoff.WaitAsync(cancellationToken);
            }

            this.controlQueueBackoff.Reset();

            ReceivedMessageContext messageContext = 
                ReceivedMessageContext.CreateFromReceivedMessageBatch(nextBatch.Messages);

            string instanceId = messageContext.InstanceId;
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                messageContext,
                cancellationToken);

            var taskMessages = new List<TaskMessage>(nextBatch.Messages.Count);
            foreach (MessageData messageData in nextBatch.Messages)
            {
                TaskMessage taskMessage = messageData.TaskMessage;
                if (runtimeState.OrchestrationInstance != null)
                {
                    this.OnNewEvent?.Invoke(this, new HistoryEventArgs(taskMessage.Event, runtimeState));
                }
                else
                {
                    this.OnNewEvent?.Invoke(this, new HistoryEventArgs(taskMessage.Event, instanceId));
                }

                taskMessages.Add(taskMessage);
            }

            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = instanceId,
                NewMessages = taskMessages,
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = messageContext.GetNextMessageExpirationTimeUtc()
            };

            // Associate this message context with the work item. We'll restore it back later.
            messageContext.TrySave(orchestrationWorkItem);
            return orchestrationWorkItem;
        }

        PendingMessageBatch StashMessagesAndGetNextBatch(IEnumerable<CloudQueueMessage> queueMessages)
        {
            lock (this.pendingOrchestrationMessages)
            {
                // If the queue is empty, queueMessages will be an empty enumerable and this foreach will be skipped.
                foreach (MessageData data in queueMessages.Select(Utils.DeserializeQueueMessage))
                {
                    PendingMessageBatch targetBatch = null;

                    // Walk backwards through the list of batches until we find one with a matching Instance ID.
                    // This is assumed to be more efficient than walking forward if most messages arrive in the queue in groups.
                    LinkedListNode<PendingMessageBatch> node = this.pendingOrchestrationMessages.Last;
                    while (node != null)
                    {
                        PendingMessageBatch batch = node.Value;
                        if (batch.OrchestrationInstanceId == data.TaskMessage.OrchestrationInstance.InstanceId)
                        {
                            targetBatch = batch;
                            break;
                        }

                        node = node.Previous;
                    }

                    if (targetBatch == null)
                    {
                        targetBatch = new PendingMessageBatch();
                        this.pendingOrchestrationMessages.AddLast(targetBatch);
                    }

                    targetBatch.OrchestrationInstanceId = data.TaskMessage.OrchestrationInstance.InstanceId;
                    targetBatch.Messages.Add(data);
                }

                // Pull batches of messages off the linked-list in FIFO order to ensure fairness.
                if (this.pendingOrchestrationMessages.Count > 0)
                {
                    PendingMessageBatch nextBatch = this.pendingOrchestrationMessages.First.Value;
                    this.pendingOrchestrationMessages.RemoveFirst();
                    return nextBatch;
                }

                return null;
            }
        }

        async Task<OrchestrationRuntimeState> GetOrchestrationRuntimeStateAsync(
            string instanceId,
            ReceivedMessageContext receivedMessageContext,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            TableContinuationToken continuationToken = null;
            TableQuery query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId));

            // TODO: Write-through caching should ensure that we rarely need to make this call?
            var historyEvents = new List<HistoryEvent>(25);

            var stopwatch = new Stopwatch();
            int requestCount = 0;
            while (true)
            {
                requestCount++;
                stopwatch.Start();
                TableQuerySegment<DynamicTableEntity> segment = await this.historyTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    this.settings.HistoryTableRequestOptions,
                    receivedMessageContext?.StorageOperationContext,
                    cancellationToken);
                stopwatch.Stop();

                continuationToken = segment.ContinuationToken;
                historyEvents.AddRange(
                    segment.Select(e => (HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(e, GetTypeForTableEntity)));

                if (continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            AnalyticsEventSource.Log.FetchedInstanceState(
                instanceId,
                historyEvents.Count,
                requestCount,
                stopwatch.ElapsedMilliseconds);

            return new OrchestrationRuntimeState(historyEvents);
        }

        Type GetTypeForTableEntity(DynamicTableEntity tableEntity)
        {
            string propertyName = nameof(HistoryEvent.EventType);

            EntityProperty eventTypeProperty;
            if (!tableEntity.Properties.TryGetValue(propertyName, out eventTypeProperty))
            {
                throw new ArgumentException($"The DynamicTableEntity did not contain a '{propertyName}' property.");
            }

            if (eventTypeProperty.PropertyType != EdmType.String)
            {
                throw new ArgumentException($"The DynamicTableEntity's {propertyName} property type must a String.");
            }

            EventType eventType;
            if (!Enum.TryParse(eventTypeProperty.StringValue, out eventType))
            {
                throw new ArgumentException($"{eventTypeProperty.StringValue} is not a valid EventType value.");
            }
            
            return this.eventTypeMap[eventType];
        }

        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for orchestration work item with InstanceId = {workItem.InstanceId}.");
                return;
            }

            string instanceId = workItem.InstanceId;

            IList<HistoryEvent> newEvents = workItem.OrchestrationRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = workItem.OrchestrationRuntimeState.Events;

            var newEventList = new StringBuilder(newEvents.Count * 40);
            var batchOperation = new TableBatchOperation();
            for (int i = 0; i < newEvents.Count; i++)
            {
                HistoryEvent historyEvent = newEvents[i];
                DynamicTableEntity entity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);

                newEventList.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is a sortable 64-bit hex number string, e.g. 000000000000001B
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                entity.RowKey = $"{sequenceNumber:X16}";
                entity.PartitionKey = instanceId;

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                batchOperation.InsertOrReplace(entity);
            }

            // TODO: Check to see if the orchestration has completed (newOrchestrationRuntimeState == null)
            //       and schedule a time to clean up that state from the history table.

            // TODO: May need some special handling for the continuedAsNewMessage scenario, though SiPort says
            //       that the requirements for this may change in the near future.

            // First persistence step is to commit history to the history table. Messages must come after.
            if (batchOperation.Count > 0)
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                await this.historyTable.ExecuteBatchAsync(
                    batchOperation,
                    this.settings.HistoryTableRequestOptions,
                    context.StorageOperationContext);

                AnalyticsEventSource.Log.AppendedInstanceState(
                    instanceId,
                    newEvents.Count,
                    allEvents.Count,
                    newEventList.ToString(0, newEventList.Length - 1),
                    stopwatch.ElapsedMilliseconds);
            }

            bool addedControlMessages = false;
            bool addedWorkItemMessages = false;

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueTasks = new List<Task>(newEvents.Count);
            if (orchestratorMessages?.Count > 0)
            {
                addedControlMessages = true;
                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    enqueueTasks.Add(this.controlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage),
                        null /* timeToLive */,
                        null /* initialVisibilityDelay */,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (timerMessages?.Count > 0)
            {
                addedControlMessages = true;
                foreach (TaskMessage taskMessage in timerMessages)
                {
                    DateTime messageFireTime = ((TimerFiredEvent)taskMessage.Event).FireAt;
                    TimeSpan initialVisibilityDelay = messageFireTime.Subtract(DateTime.UtcNow);
                    Debug.Assert(initialVisibilityDelay <= TimeSpan.FromDays(7));

                    enqueueTasks.Add(this.controlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage),
                        null /* timeToLive */,
                        initialVisibilityDelay,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (outboundMessages?.Count > 0)
            {
                addedWorkItemMessages = true;
                foreach (TaskMessage taskMessage in outboundMessages)
                {
                    enqueueTasks.Add(this.workItemQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage),
                        null /* timeToLive */,
                        null /* initialVisibilityDelay */,
                        this.settings.WorkItemQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (continuedAsNewMessage != null)
            {
                addedControlMessages = true;
                enqueueTasks.Add(this.controlQueue.AddMessageAsync(
                    context.CreateOutboundQueueMessage(continuedAsNewMessage),
                    null /* timeToLive */,
                    null /* initialVisibilityDelay */,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext));
            }

            await Task.WhenAll(enqueueTasks);

            // Signal queue listeners to start polling immediately to reduce
            // unnecessary wait time between sending and receiving.
            if (addedControlMessages)
            {
                this.controlQueueBackoff.Reset();
            }

            if (addedWorkItemMessages)
            {
                this.workItemQueueBackoff.Reset();
            }
        }

        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for orchestration work item with InstanceId = {workItem.InstanceId}.");
                return;
            }

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            try
            {
                await Task.WhenAll(context.MessageDataBatch.Select(e =>
                    this.controlQueue.UpdateMessageAsync(
                        e.OriginalQueueMessage,
                        this.settings.ControlQueueVisibilityTimeout,
                        MessageUpdateFields.Visibility,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext)));
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message may have been processed and deleted already.
            }
        }

        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                return;
            }

            Task[] updates = new Task[context.MessageDataBatch.Count];

            // We "abandon" the message by settings its visibility timeout to zero.
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;

                AnalyticsEventSource.Log.AbandoningMessage(
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    taskMessage.OrchestrationInstance.InstanceId);

                updates[i] = this.controlQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext);
            }

            try
            {
                await Task.WhenAll(updates);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message may have been processed and deleted already.
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchistration work item is abandoned.
        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for orchestration work item with InstanceId = {workItem.InstanceId}.");
                return;
            }

            Task[] deletes = new Task[context.MessageDataBatch.Count];
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;
                AnalyticsEventSource.Log.DeletingMessage(taskMessage.Event.EventType.ToString(), queueMessage.Id, workItem.InstanceId);
                deletes[i] = this.controlQueue.DeleteMessageAsync(
                    queueMessage,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext);
            }

            try
            {
                await Task.WhenAll(deletes);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message was already deleted
            }

            ReceivedMessageContext.RemoveContext(workItem);
        }
        #endregion

        #region Task Activity Methods
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            CloudQueueMessage queueMessage;
            while (true)
            {
                queueMessage = await this.workItemQueue.GetMessageAsync(
                    this.settings.WorkItemQueueVisibilityTimeout,
                    this.settings.WorkItemQueueRequestOptions,
                    null /* operationContext */,
                    cancellationToken);

                if (queueMessage != null)
                {
                    break;
                }

                await this.workItemQueueBackoff.WaitAsync(cancellationToken);
            }

            this.workItemQueueBackoff.Reset();

            ReceivedMessageContext context = ReceivedMessageContext.CreateFromReceivedMessage(queueMessage);
            if (!context.TrySave(queueMessage.Id))
            {
                // This means we're already processing this message. This is never expected since the message
                // should be kept invisible via background calls to RenewTaskActivityWorkItemLockAsync.
                AnalyticsEventSource.Log.AssertFailure($"Work item queue message with ID = {queueMessage.Id} is being processed multiple times concurrently.");
                return null;
            }

            TaskMessage taskMessage = context.MessageData.TaskMessage;
            this.OnNewEvent?.Invoke(this, new HistoryEventArgs(taskMessage.Event, taskMessage.OrchestrationInstance.InstanceId));

            return new TaskActivityWorkItem
            {
                Id = queueMessage.Id,
                TaskMessage = taskMessage,
                LockedUntilUtc = context.GetNextMessageExpirationTimeUtc(),
            };
        }

        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseTaskMessage)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            // First, send a response message back. If this fails, we'll try again later since we haven't deleted the
            // work item message yet (that happens next).
            await this.controlQueue.AddMessageAsync(
                context.CreateOutboundQueueMessage(responseTaskMessage),
                null /* timeToLive */,
                null /* initialVisibilityDelay */,
                this.settings.WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            // Signal the control queue listener thread to poll immediately 
            // to avoid unnecessary delay between sending and receiving.
            this.controlQueueBackoff.Reset();

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            AnalyticsEventSource.Log.DeletingMessage(
                workItem.TaskMessage.Event.EventType.ToString(),
                context.MessageData.OriginalQueueMessage.Id,
                workItem.TaskMessage.OrchestrationInstance.InstanceId);

            try
            {
                await this.workItemQueue.DeleteMessageAsync(
                    context.MessageData.OriginalQueueMessage,
                    this.settings.WorkItemQueueRequestOptions,
                    context.StorageOperationContext);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message was already deleted
            }

            ReceivedMessageContext.RemoveContext(workItem);
        }

        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for work item with ID = {workItem.Id}.");
                return null;
            }

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            try
            {
                await this.workItemQueue.UpdateMessageAsync(
                    context.MessageData.OriginalQueueMessage,
                    this.settings.WorkItemQueueVisibilityTimeout,
                    MessageUpdateFields.Visibility,
                    this.settings.WorkItemQueueRequestOptions,
                    context.StorageOperationContext);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message was deleted
            }

            return workItem;
        }

        public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure($"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            AnalyticsEventSource.Log.AbandoningMessage(
                workItem.TaskMessage.Event.EventType.ToString(),
                context.MessageData.OriginalQueueMessage.Id,
                workItem.TaskMessage.OrchestrationInstance.InstanceId);

            // We "abandon" the message by settings its visibility timeout to zero.
            try
            {
                await this.workItemQueue.UpdateMessageAsync(
                    context.MessageData.OriginalQueueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    this.settings.WorkItemQueueRequestOptions,
                    context.StorageOperationContext);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message was deleted
            }

            ReceivedMessageContext.RemoveContext(workItem);
        }
        #endregion

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            // Azure Table storage only supports inserting batches of 100 at a time.
            // Azure Queues do not support batches at all, but we don't rely on transactional
            // enqueue operations so we are not affected by this limitation.
            return runtimeState.NewEvents.Count > 100;
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            // TODO: Need to reason about exception delays
            return 10;
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            // TODO: Need to reason about exception delays
            return 10;
        }
        #endregion

        #region IOrchestrationServiceClient
        /// <summary>
        /// Creates and starts a new orchestration.
        /// </summary>
        /// <param name="creationMessage">The message which creates and starts the orchestration.</param>
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return this.SendTaskOrchestrationMessageAsync(creationMessage);
        }

        /// <summary>
        /// Sends a list of messages to an orchestration.
        /// </summary>
        /// <remarks>
        /// Azure Storage does not support batch sending to queues, so there are no transactional guarantees in this method.
        /// </remarks>
        /// <param name="messages">The list of messages to send.</param>
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            return Task.WhenAll(messages.Select(msg => this.SendTaskOrchestrationMessageAsync(msg)));
        }

        /// <summary>
        /// Sends a message to an orchestration.
        /// </summary>
        /// <param name="message">The message to send.</param>
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            var operationContext = new OperationContext();
            operationContext.SendingRequest += ReceivedMessageContext.OnSendingRequest;

            await this.controlQueue.AddMessageAsync(
                ReceivedMessageContext.CreateOutboundQueueMessageInternal(message),
                null /* timeToLive */,
                null /* initialVisibilityDelay */,
                this.settings.ControlQueueRequestOptions,
                operationContext);

            // Notify the control queue poller that there are new messages to process.
            this.controlQueueBackoff.Reset();
        }

        /// <summary>
        /// Get a list of orchestration states from storage for the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="allExecutions">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>List of <see cref="OrchestrationState"/> objects that represent the list of orchestrations.</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            // TODO: Need to consider the execution ID
            return new[] { await this.GetOrchestrationStateAsync(instanceId, executionId: null) };
        }

        /// <summary>
        /// Get a the state of the specified execution (generation) of the specified orchestration instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>The <see cref="OrchestrationState"/> object that represents the orchestration.</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            // TODO: Need to consider the execution ID
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                receivedMessageContext: null);
            if (runtimeState.Events.Count == 0)
            {
                return new OrchestrationState { OrchestrationStatus = OrchestrationStatus.Pending };
            }

            return new OrchestrationState
            {
                OrchestrationInstance = runtimeState.OrchestrationInstance,
                ParentInstance = runtimeState.ParentInstance,
                Name = runtimeState.Name,
                Version = runtimeState.Version,
                Status = runtimeState.Status,
                Tags = runtimeState.Tags,
                OrchestrationStatus = runtimeState.OrchestrationStatus,
                CreatedTime = runtimeState.CreatedTime,
                CompletedTime = runtimeState.CompletedTime,
                LastUpdatedTime = runtimeState.Events.Last().Timestamp,
                Size = runtimeState.Size,
                CompressedSize = runtimeState.CompressedSize,
                Input = runtimeState.Input,
                Output = runtimeState.Output
            };
        }

        /// <summary>
        /// Force terminates an orchestration by sending a execution terminated event
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration to terminate.</param>
        /// <param name="reason">The user-friendly reason for terminating.</param>
        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            return SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">Exectuion ID (generation) of the orchestration instance.</param>
        /// <returns>String with formatted JSON array representing the execution history.</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            // TODO: Need to consider the execution ID
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                receivedMessageContext: null);
            return JsonConvert.SerializeObject(runtimeState.Events);
        }

        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="executionId">The execution ID (generation) of the orchestration instance.</param>
        /// <param name="instanceId">The orchestration instance to wait for.</param>
        /// <param name="timeout">Max timeout to wait.</param>
        /// <param name="cancellationToken">Task cancellation token.</param>
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException("instanceId");
            }

            TimeSpan statusPollingInterval = TimeSpan.FromSeconds(2);
            while (!cancellationToken.IsCancellationRequested && timeout > TimeSpan.Zero)
            {
                OrchestrationState state = (await GetOrchestrationStateAsync(instanceId, false))?.FirstOrDefault();
                if (state == null || 
                    state.OrchestrationStatus == OrchestrationStatus.Running ||
                    state.OrchestrationStatus == OrchestrationStatus.Pending)
                {
                    await Task.Delay(statusPollingInterval, cancellationToken);
                    timeout -= statusPollingInterval;
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException();
        }
        #endregion

        class PendingMessageBatch
        {
            public string OrchestrationInstanceId { get; set; }

            public List<MessageData> Messages { get; set; } = new List<MessageData>();
        }
    }
}
