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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public class AzureStorageOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CloudQueue controlQueue;
        readonly CloudQueue workItemQueue;
        readonly CloudTable historyTable;
        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessages;
        readonly ConcurrentDictionary<string, object> activeOrchestrationInstances;

        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;

        readonly BackoffPollingHelper controlQueueBackoff;
        readonly BackoffPollingHelper workItemQueueBackoff;

        readonly object hubCreationLock;

        Task cachedTaskHubCreationTask;
        bool isStarted;

        public AzureStorageOrchestrationService(AzureStorageOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            ValidateSettings(settings);

            this.settings = settings;
            this.tableEntityConverter = new TableEntityConverter();
            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);
            CloudQueueClient queueClient = account.CreateCloudQueueClient();
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // TODO: Need to do input validation on the TaskHubName.
            this.controlQueue = GetControlQueue(account, settings.TaskHubName);
            this.workItemQueue = GetWorkItemQueue(account, settings.TaskHubName);

            string historyTableName = $"{settings.TaskHubName}History";
            this.historyTable = tableClient.GetTableReference(historyTableName);

            this.pendingOrchestrationMessages = new LinkedList<PendingMessageBatch>();
            this.activeOrchestrationInstances = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

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

            this.hubCreationLock = new object();
        }

        public event EventHandler<HistoryEventArgs> OnNewEvent;

        public static CloudQueue GetControlQueue(CloudStorageAccount account, string taskHub)
        {
            return GetQueueInternal(account, taskHub, "control");
        }

        public static CloudQueue GetWorkItemQueue(CloudStorageAccount account, string taskHub)
        {
            return GetQueueInternal(account, taskHub, "workitems");
        }

        static CloudQueue GetQueueInternal(CloudStorageAccount account, string taskHub, string suffix)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            if (string.IsNullOrEmpty(taskHub))
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            string queueName = $"{taskHub.ToLowerInvariant()}-{suffix}";
            return account.CreateCloudQueueClient().GetQueueReference(queueName);
        }

        static void ValidateSettings(AzureStorageOrchestrationServiceSettings settings)
        {
            if (settings.ControlQueueBatchSize > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(settings), "The control queue batch size must not exceed 32.");
            }

            // TODO: More validation.
        }

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
        public int TaskActivityDispatcherCount { get; } = 1;

        public int TaskOrchestrationDispatcherCount { get; } = 1;

        #region Management Operations (Create/Delete/Start/Stop)
        /// <summary>
        /// Deletes and creates the neccesary Azure Storage resources for the orchestration service.
        /// </summary>
        public async Task CreateAsync()
        {
            await this.DeleteAsync();

            lock (this.hubCreationLock)
            {
                this.cachedTaskHubCreationTask = Task.WhenAll(
                    this.controlQueue.CreateAsync(),
                    this.workItemQueue.CreateAsync(),
                    this.historyTable.CreateAsync());
            }

            await this.cachedTaskHubCreationTask;
        }

        /// <summary>
        /// Creates the necessary Azure Storage resources for the orchestration service if they don't already exist.
        /// </summary>
        public Task CreateIfNotExistsAsync()
        {
            lock (this.hubCreationLock)
            {
                this.cachedTaskHubCreationTask = Task.WhenAll(
                    this.controlQueue.CreateIfNotExistsAsync(),
                    this.workItemQueue.CreateIfNotExistsAsync(),
                    this.historyTable.CreateIfNotExistsAsync());
            }

            return this.cachedTaskHubCreationTask;
        }

        /// <summary>
        /// Deletes the Azure Storage resources used by the orchestration service.
        /// </summary>
        public async Task DeleteAsync()
        {
            await Task.WhenAll(
                this.controlQueue.DeleteIfExistsAsync(),
                this.workItemQueue.DeleteIfExistsAsync(),
                this.historyTable.DeleteIfExistsAsync());
            this.cachedTaskHubCreationTask = null;
        }

        Task EnsuredCreatedIfNotExistsAsync()
        {
            return this.cachedTaskHubCreationTask ?? this.CreateIfNotExistsAsync();
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
            await this.EnsuredCreatedIfNotExistsAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
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

                nextBatch = this.StashMessagesAndGetNextBatch(queueMessageBatch);
                if (nextBatch != null)
                {
                    break;
                }

                if (receiveTimeoutStopwatch.Elapsed > receiveTimeout)
                {
                    return null;
                }

                await this.controlQueueBackoff.WaitAsync(cancellationToken);
            }

            this.controlQueueBackoff.Reset();

            ReceivedMessageContext messageContext = 
                ReceivedMessageContext.CreateFromReceivedMessageBatch(nextBatch.Messages);

            OrchestrationInstance instance = messageContext.Instance;
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instance.InstanceId,
                instance.ExecutionId,
                messageContext.StorageOperationContext,
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
                    this.OnNewEvent?.Invoke(this, new HistoryEventArgs(taskMessage.Event, instance.InstanceId));
                }

                taskMessages.Add(taskMessage);
            }

            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = instance.InstanceId,
                NewMessages = taskMessages,
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = messageContext.GetNextMessageExpirationTimeUtc()
            };

            // Associate this message context with the work item. We'll restore it back later.
            messageContext.TrySave(orchestrationWorkItem);

            if (runtimeState.ExecutionStartedEvent != null &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                // The instance has already completed. Delete this message batch.
                await this.ReleaseTaskOrchestrationWorkItemAsync(orchestrationWorkItem);
                return null;
            }

            return orchestrationWorkItem;
        }

        PendingMessageBatch StashMessagesAndGetNextBatch(IEnumerable<CloudQueueMessage> queueMessages)
        {
            lock (this.pendingOrchestrationMessages)
            {
                LinkedListNode<PendingMessageBatch> node;

                // If the queue is empty, queueMessages will be an empty enumerable and this foreach will be skipped.
                foreach (MessageData data in queueMessages.Select(Utils.DeserializeQueueMessage))
                {
                    PendingMessageBatch targetBatch = null;

                    // Walk backwards through the list of batches until we find one with a matching Instance ID.
                    // This is assumed to be more efficient than walking forward if most messages arrive in the queue in groups.
                    node = this.pendingOrchestrationMessages.Last;
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
                // Skip over instances which are currently being processed.
                node = this.pendingOrchestrationMessages.First;
                while (node != null)
                {
                    PendingMessageBatch nextBatch = node.Value;
                    if (!this.activeOrchestrationInstances.ContainsKey(nextBatch.OrchestrationInstanceId))
                    {
                        this.activeOrchestrationInstances.TryAdd(nextBatch.OrchestrationInstanceId, null);
                        this.pendingOrchestrationMessages.Remove(node);
                        return nextBatch;
                    }

                    node = node.Next;
                }

                return null;
            }
        }

        Task<OrchestrationRuntimeState> GetOrchestrationRuntimeStateAsync(
            string instanceId,
            OperationContext storageOperationContext,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return this.GetOrchestrationRuntimeStateAsync(instanceId, null, storageOperationContext, cancellationToken);
        }

        async Task<OrchestrationRuntimeState> GetOrchestrationRuntimeStateAsync(
            string instanceId,
            string expectedExecutionId,
            OperationContext storageOperationContext,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            filterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
            if (expectedExecutionId != null)
            {
                // Filter down to a specific generation.
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and RowKey gt '85f05ce1494c4a29989f64d3fe0f9089' and ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089'"
                filterCondition.Append(" and RowKey gt ").Append(Quote).Append(expectedExecutionId).Append(Quote);
                filterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(expectedExecutionId).Append(Quote);
            }

            TableQuery query = new TableQuery().Where(filterCondition.ToString());
                    
            // TODO: Write-through caching should ensure that we rarely need to make this call?
            var historyEventEntities = new List<DynamicTableEntity>(100);

            var stopwatch = new Stopwatch();
            int requestCount = 0;

            bool finishedEarly = false;
            TableContinuationToken continuationToken = null;
            while (true)
            {
                requestCount++;
                stopwatch.Start();
                TableQuerySegment<DynamicTableEntity> segment = await this.historyTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    this.settings.HistoryTableRequestOptions,
                    storageOperationContext,
                    cancellationToken);
                stopwatch.Stop();

                historyEventEntities.AddRange(segment);

                continuationToken = segment.ContinuationToken;
                if (finishedEarly || continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            IList<HistoryEvent> historyEvents;
            string executionId;
            if (historyEventEntities.Count > 0)
            {
                // Check to see whether the list of history events might contain multiple generations. If so,
                // consider only the latest generation. The timestamp on the first entry of each generation
                // is used to deterine the "age" of that generation. This assumes that rows within a generation
                // are sorted in chronological order.
                IGrouping<string, DynamicTableEntity> mostRecentGeneration = historyEventEntities
                    .GroupBy(e => e.Properties["ExecutionId"].StringValue)
                    .OrderByDescending(g => g.First().Timestamp)
                    .First();
                executionId = mostRecentGeneration.Key;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(100);
                events.AddRange(
                    mostRecentGeneration.Select(
                        entity => (HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(entity, GetTypeForTableEntity)));
                historyEvents = events;
            }
            else
            {
                historyEvents = EmptyHistoryEventList;
                executionId = expectedExecutionId ?? string.Empty;
            }

            AnalyticsEventSource.Log.FetchedInstanceState(
                instanceId,
                executionId,
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

            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;
            IList<HistoryEvent> newEvents = runtimeState.NewEvents;
            IList<HistoryEvent> allEvents = runtimeState.Events;

            string instanceId = workItem.InstanceId;
            string executionId = runtimeState.OrchestrationInstance.ExecutionId;

            var newEventList = new StringBuilder(newEvents.Count * 40);
            var batchOperation = new TableBatchOperation();
            var tableBatchBatches = new List<TableBatchOperation>();
            tableBatchBatches.Add(batchOperation);

            for (int i = 0; i < newEvents.Count; i++)
            {
                HistoryEvent historyEvent = newEvents[i];
                DynamicTableEntity entity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);

                newEventList.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is in the form "{ExecutionId}.{SequenceNumber}" where {ExecutionId} represents the generation
                // of a particular instance and {SequenceNumber} represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                entity.RowKey = $"{executionId}.{sequenceNumber:X16}";
                entity.PartitionKey = instanceId;
                entity.Properties["ExecutionId"] = new EntityProperty(executionId);

                // Table storage only supports inserts of up to 100 entities at a time.
                if (batchOperation.Count == 100)
                {
                    tableBatchBatches.Add(batchOperation);
                    batchOperation = new TableBatchOperation();
                }

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                batchOperation.InsertOrReplace(entity);
            }

            // TODO: Check to see if the orchestration has completed (newOrchestrationRuntimeState == null)
            //       and schedule a time to clean up that state from the history table.

            // First persistence step is to commit history to the history table. Messages must come after.
            // CONSIDER: If there are a large number of history items and messages, we could potentially
            //           improve overall system throughput by incrementally enqueuing batches of messages
            //           as we write to the table rather than waiting for all table batches to complete
            //           before we enqueue messages.
            foreach (TableBatchOperation operation in tableBatchBatches)
            {
                if (operation.Count > 0)
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    await this.historyTable.ExecuteBatchAsync(
                        operation,
                        this.settings.HistoryTableRequestOptions,
                        context.StorageOperationContext);

                    AnalyticsEventSource.Log.AppendedInstanceState(
                        instanceId,
                        executionId,
                        newEvents.Count,
                        allEvents.Count,
                        newEventList.ToString(0, newEventList.Length - 1),
                        stopwatch.ElapsedMilliseconds);
                }
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
                    if (initialVisibilityDelay < TimeSpan.Zero)
                    {
                        initialVisibilityDelay = TimeSpan.Zero;
                    }

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
                // Message(s) may have been processed and deleted already.
                foreach (MessageData message in context.MessageDataBatch)
                {
                    AnalyticsEventSource.Log.MessageGone(
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(RenewTaskOrchestrationWorkItemLockAsync));
                }
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
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId);

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
                foreach (MessageData message in context.MessageDataBatch)
                {
                    AnalyticsEventSource.Log.MessageGone(
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(AbandonTaskOrchestrationWorkItemAsync));
                }
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
                // Message may have been processed and deleted already.
                foreach (MessageData message in context.MessageDataBatch)
                {
                    AnalyticsEventSource.Log.MessageGone(
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(ReleaseTaskOrchestrationWorkItemAsync));
                }
            }

            ReceivedMessageContext.RemoveContext(workItem);

            object ignored;
            this.activeOrchestrationInstances.TryRemove(workItem.InstanceId, out ignored);
        }
        #endregion

        #region Task Activity Methods
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsuredCreatedIfNotExistsAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
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

                if (receiveTimeoutStopwatch.Elapsed > receiveTimeout)
                {
                    return null;
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
                AnalyticsEventSource.Log.MessageGone(
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(CompleteTaskActivityWorkItemAsync));
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
                AnalyticsEventSource.Log.MessageGone(
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(RenewTaskActivityWorkItemLockAsync));
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
                workItem.TaskMessage.OrchestrationInstance.InstanceId,
                workItem.TaskMessage.OrchestrationInstance.ExecutionId);

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
                AnalyticsEventSource.Log.MessageGone(
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(AbandonTaskActivityWorkItemAsync));
            }

            ReceivedMessageContext.RemoveContext(workItem);
        }
        #endregion

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            // This orchestration service implementation will manage batch sizes by itself.
            // We don't want to rely on the underlying framework's backoff mechanism because
            // it would require us to implement some kind of duplicate message detection.
            return false;
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
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsuredCreatedIfNotExistsAsync();

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
        /// Get the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="allExecutions">This parameter is not used.</param>
        /// <returns>List of <see cref="OrchestrationState"/> objects that represent the list of orchestrations.</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            return new[] { await this.GetOrchestrationStateAsync(instanceId, executionId: null) };
        }

        /// <summary>
        /// Get a the state of the specified execution (generation) of the specified orchestration instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <returns>The <see cref="OrchestrationState"/> object that represents the orchestration.</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsuredCreatedIfNotExistsAsync();

            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                executionId,
                storageOperationContext: null);
            if (runtimeState.Events.Count == 0)
            {
                return null;
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
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <returns>String with formatted JSON array representing the execution history.</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instanceId,
                executionId,
                storageOperationContext: null);
            return JsonConvert.SerializeObject(runtimeState.Events);
        }

        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">The orchestration instance to wait for.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
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
                throw new ArgumentException(nameof(instanceId));
            }

            TimeSpan statusPollingInterval = TimeSpan.FromSeconds(2);
            while (!cancellationToken.IsCancellationRequested && timeout > TimeSpan.Zero)
            {
                OrchestrationState state = await GetOrchestrationStateAsync(instanceId, executionId);
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

        /// <summary>
        /// This method is not supported.
        /// </summary>
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
