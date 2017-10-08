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
    using System.Runtime.ExceptionServices;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public class AzureStorageOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient,
        IPartitionObserver<BlobLease>
    {
        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly string storageAccountName;
        readonly CloudQueueClient queueClient;
        readonly ConcurrentDictionary<string, CloudQueue> ownedControlQueues;
        readonly ConcurrentDictionary<string, CloudQueue> allControlQueues;
        readonly CloudQueue workItemQueue;
        readonly CloudTable historyTable;
        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessageBatches;
        readonly ConcurrentDictionary<string, object> activeOrchestrationInstances;

        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;

        readonly BackoffPollingHelper controlQueueBackoff;
        readonly BackoffPollingHelper workItemQueueBackoff;

        readonly ResettableLazy<Task> taskHubCreator;
        readonly BlobLeaseManager leaseManager;
        readonly PartitionManager<BlobLease> partitionManager;

        readonly object hubCreationLock;

        bool isStarted;
        Task statsLoop;
        CancellationTokenSource shutdownSource;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageOrchestrationService"/> class.
        /// </summary>
        /// <param name="settings">The settings used to configure the orchestration service.</param>
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
            this.storageAccountName = account.Credentials.AccountName;
            this.stats = new AzureStorageOrchestrationServiceStats();
            this.queueClient = account.CreateCloudQueueClient();
            CloudTableClient tableClient = account.CreateCloudTableClient();

            // TODO: Need to do input validation on the TaskHubName.

            this.ownedControlQueues = new ConcurrentDictionary<string, CloudQueue>();
            this.allControlQueues = new ConcurrentDictionary<string, CloudQueue>();
            this.workItemQueue = GetWorkItemQueue(account, settings.TaskHubName);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                CloudQueue queue = GetControlQueue(this.queueClient, this.settings.TaskHubName, i);
                this.allControlQueues.TryAdd(queue.Name, queue);
            }

            string historyTableName = $"{settings.TaskHubName}History";
            NameValidator.ValidateTableName(historyTableName);

            this.historyTable = tableClient.GetTableReference(historyTableName);

            this.pendingOrchestrationMessageBatches = new LinkedList<PendingMessageBatch>();
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
            this.taskHubCreator = new ResettableLazy<Task>(
                this.GetTaskHubCreatorTask,
                LazyThreadSafetyMode.ExecutionAndPublication);

            this.leaseManager = GetBlobLeaseManager(
                settings.TaskHubName,
                settings.WorkerId,
                account,
                settings.LeaseInterval,
                settings.LeaseRenewInterval,
                this.stats);
            this.partitionManager = new PartitionManager<BlobLease>(
                this.storageAccountName,
                this.settings.TaskHubName,
                settings.WorkerId,
                this.leaseManager,
                new PartitionManagerOptions
                {
                    AcquireInterval = settings.LeaseAcquireInterval,
                    RenewInterval = settings.LeaseRenewInterval,
                    LeaseInterval = settings.LeaseInterval,
                });
        }

        internal string WorkerId => this.settings.WorkerId;

        internal IEnumerable<CloudQueue> AllControlQueues => this.allControlQueues.Values;

        internal IEnumerable<CloudQueue> OwnedControlQueues => this.ownedControlQueues.Values;

        internal CloudQueue WorkItemQueue => this.workItemQueue;

        internal CloudTable HistoryTable => this.historyTable;

        internal static CloudQueue GetControlQueue(CloudStorageAccount account, string taskHub, int partitionIndex)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            return GetControlQueue(account.CreateCloudQueueClient(), taskHub, partitionIndex);
        }

        internal static CloudQueue GetControlQueue(CloudQueueClient queueClient, string taskHub, int partitionIndex)
        {
            return GetQueueInternal(queueClient, taskHub, $"control-{partitionIndex:00}");
        }

        internal static CloudQueue GetWorkItemQueue(CloudStorageAccount account, string taskHub)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            return GetQueueInternal(account.CreateCloudQueueClient(), taskHub, "workitems");
        }

        static CloudQueue GetQueueInternal(CloudQueueClient queueClient, string taskHub, string suffix)
        {
            if (queueClient == null)
            {
                throw new ArgumentNullException(nameof(queueClient));
            }

            if (string.IsNullOrEmpty(taskHub))
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            string queueName = $"{taskHub.ToLowerInvariant()}-{suffix}";
            NameValidator.ValidateQueueName(queueName);

            return queueClient.GetQueueReference(queueName);
        }

        static BlobLeaseManager GetBlobLeaseManager(
            string taskHub,
            string workerName,
            CloudStorageAccount account,
            TimeSpan leaseInterval,
            TimeSpan renewalInterval,
            AzureStorageOrchestrationServiceStats stats)
        {
            return new BlobLeaseManager(
                taskHubName: taskHub,
                workerName: workerName,
                leaseContainerName: taskHub.ToLowerInvariant() + "-leases",
                blobPrefix: string.Empty,
                consumerGroupName: "default",
                storageClient: account.CreateCloudBlobClient(),
                leaseInterval: leaseInterval,
                renewInterval: renewalInterval,
                skipBlobContainerCreation: false,
                stats: stats);
        }

        static void ValidateSettings(AzureStorageOrchestrationServiceSettings settings)
        {
            if (settings.ControlQueueBatchSize > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(settings), "The control queue batch size must not exceed 32.");
            }

            if (settings.PartitionCount < 1 || settings.PartitionCount > 16)
            {
                throw new ArgumentOutOfRangeException(nameof(settings), "The number of partitions must be a positive integer and no greater than 16.");
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
        /// <inheritdoc />
        public int TaskActivityDispatcherCount { get; } = 1;

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount { get; } = 1;

        #region Management Operations (Create/Delete/Start/Stop)
        /// <summary>
        /// Deletes and creates the neccesary Azure Storage resources for the orchestration service.
        /// </summary>
        public async Task CreateAsync()
        {
            await this.DeleteAsync();
            await this.taskHubCreator.Value;
        }

        /// <summary>
        /// Creates the necessary Azure Storage resources for the orchestration service if they don't already exist.
        /// </summary>
        public Task CreateIfNotExistsAsync()
        {
            return this.taskHubCreator.Value;
        }

        // Internal logic used by the lazy taskHubCreator
        async Task GetTaskHubCreatorTask()
        {
            TaskHubInfo hubInfo = GetTaskHubInfo(this.settings.TaskHubName, this.settings.PartitionCount);
            await this.leaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo);
            this.stats.StorageRequests.Increment();

            var tasks = new List<Task>();
            tasks.Add(this.workItemQueue.CreateIfNotExistsAsync());
            tasks.Add(this.historyTable.CreateIfNotExistsAsync());

            foreach (CloudQueue controlQueue in this.allControlQueues.Values)
            {
                tasks.Add(controlQueue.CreateIfNotExistsAsync());
                tasks.Add(this.leaseManager.CreateLeaseIfNotExistAsync(controlQueue.Name));
            }

            await Task.WhenAll(tasks.ToArray());
            this.stats.StorageRequests.Increment(tasks.Count);
        }

        /// <summary>
        /// Deletes the Azure Storage resources used by the orchestration service.
        /// </summary>
        public async Task DeleteAsync()
        {
            var tasks = new List<Task>();

            foreach (string partitionId in this.allControlQueues.Keys)
            {
                if (this.allControlQueues.TryGetValue(partitionId, out CloudQueue controlQueue))
                {
                    tasks.Add(controlQueue.DeleteIfExistsAsync());
                }
            }

            tasks.Add(this.workItemQueue.DeleteIfExistsAsync());
            tasks.Add(this.historyTable.DeleteIfExistsAsync());

            // This code will throw if the container doesn't exist.
            tasks.Add(this.leaseManager.DeleteAllAsync().ContinueWith(t =>
            {
                if (t.Exception?.InnerExceptions?.Count > 0)
                {
                    foreach (Exception e in t.Exception.InnerExceptions)
                    {
                        StorageException storageException = e as StorageException;
                        if (storageException == null || storageException.RequestInformation.HttpStatusCode != 404)
                        {
                            ExceptionDispatchInfo.Capture(e).Throw();
                        }
                    }
                }
            }));

            await Task.WhenAll(tasks.ToArray());
            this.stats.StorageRequests.Increment(tasks.Count);
            this.taskHubCreator.Reset();
        }

        Task EnsuredCreatedIfNotExistsAsync()
        {
            return this.CreateIfNotExistsAsync();
        }

        /// <inheritdoc />
        public Task CreateAsync(bool recreateInstanceStore)
        {
            throw new NotSupportedException("The instance store is not supported by the Azure Storage provider.");
        }

        /// <inheritdoc />
        public Task DeleteAsync(bool deleteInstanceStore)
        {
            throw new NotSupportedException("The instance store is not supported by the Azure Storage provider.");
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            if (this.isStarted)
            {
                throw new InvalidOperationException("The orchestration service has already started.");
            }

            // Disable nagling to improve storage access latency:
            // https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            // Ad-hoc testing has shown very nice improvements (20%-50% drop in queue message age for simple scenarios).
            ServicePointManager.FindServicePoint(this.historyTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.workItemQueue.Uri).UseNagleAlgorithm = false;

            this.shutdownSource = new CancellationTokenSource();
            this.statsLoop = Task.Run(() => this.ReportStatsLoop(this.shutdownSource.Token));

            await this.partitionManager.InitializeAsync();
            await this.partitionManager.SubscribeAsync(this);
            await this.partitionManager.StartAsync();

            this.isStarted = true;
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return this.StopAsync(isForced: false);
        }

        /// <inheritdoc />
        public async Task StopAsync(bool isForced)
        {
            this.shutdownSource.Cancel();
            await this.statsLoop;
            await this.partitionManager.StopAsync();
            this.isStarted = false;
        }

        async Task ReportStatsLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
                    this.ReportStats();
                }
                catch (TaskCanceledException)
                {
                    // shutting down
                    break;
                }
                catch (Exception e)
                {
                    AnalyticsEventSource.Log.GeneralError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        $"Unexpected error in {nameof(ReportStatsLoop)}: {e}");
                }
            }

            // Final reporting of stats
            this.ReportStats();
        }

        void ReportStats()
        {
            // The following stats are reported on a per-interval basis.
            long storageRequests = this.stats.StorageRequests.Reset();
            long messagesSent = this.stats.MessagesSent.Reset();
            long messagesRead = this.stats.MessagesRead.Reset();
            long messagesUpdated = this.stats.MessagesUpdated.Reset();
            long tableEntitiesWritten = this.stats.TableEntitiesWritten.Reset();
            long tableEntitiesRead = this.stats.TableEntitiesRead.Reset();

            // The remaining stats are running numbers
            int pendingOrchestratorInstances;
            long pendingOrchestrationMessages;
            lock (this.pendingOrchestrationMessageBatches)
            {
                pendingOrchestratorInstances = this.pendingOrchestrationMessageBatches.Count;
                pendingOrchestrationMessages = this.stats.PendingOrchestratorMessages.Value;
            }

            AnalyticsEventSource.Log.OrchestrationServiceStats(
                this.storageAccountName,
                this.settings.TaskHubName,
                storageRequests,
                messagesSent,
                messagesRead,
                messagesUpdated,
                tableEntitiesWritten,
                tableEntitiesRead,
                pendingOrchestratorInstances,
                pendingOrchestrationMessages,
                this.activeOrchestrationInstances.Count,
                this.stats.ActiveActivityExecutions.Value);
        }

        async Task IPartitionObserver<BlobLease>.OnPartitionAcquiredAsync(BlobLease lease)
        {
            CloudQueue controlQueue = this.queueClient.GetQueueReference(lease.PartitionId);
            await controlQueue.CreateIfNotExistsAsync();
            this.stats.StorageRequests.Increment();
            this.ownedControlQueues[lease.PartitionId] = controlQueue;
            this.allControlQueues[lease.PartitionId] = controlQueue;
        }

        Task IPartitionObserver<BlobLease>.OnPartitionReleasedAsync(BlobLease lease, CloseReason reason)
        {
            if (!this.ownedControlQueues.TryRemove(lease.PartitionId, out CloudQueue controlQueue))
            {
                AnalyticsEventSource.Log.PartitionManagerWarning(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    $"Worker ${this.settings.WorkerId} lost a lease '{lease.PartitionId}' but didn't own the queue.");
            }

            return Utils.CompletedTask;
        }

        // Used for testing
        internal Task<IEnumerable<BlobLease>> ListBlobLeasesAsync()
        {
            return this.leaseManager.ListLeasesAsync();
        }

        internal static async Task<CloudQueue[]> GetControlQueuesAsync(
            CloudStorageAccount account,
            string taskHub,
            int partitionCount)
        {
            if (account == null)
            {
                throw new ArgumentNullException(nameof(account));
            }

            if (taskHub == null)
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            BlobLeaseManager inactiveLeaseManager = GetBlobLeaseManager(taskHub, "n/a", account, TimeSpan.Zero, TimeSpan.Zero, null);
            TaskHubInfo hubInfo = await inactiveLeaseManager.GetOrCreateTaskHubInfoAsync(
                GetTaskHubInfo(taskHub, partitionCount));

            CloudQueueClient queueClient = account.CreateCloudQueueClient();

            var controlQueues = new CloudQueue[hubInfo.PartitionCount];
            for (int i = 0; i < hubInfo.PartitionCount; i++)
            {
                controlQueues[i] = GetControlQueue(queueClient, taskHub, i);
            }

            return controlQueues;
        }

        static TaskHubInfo GetTaskHubInfo(string taskHub, int partitionCount)
        {
            return new TaskHubInfo(taskHub, DateTime.UtcNow, partitionCount);
        }

        #endregion

        #region Orchestration Work Item Methods
        /// <inheritdoc />
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsuredCreatedIfNotExistsAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
            PendingMessageBatch nextBatch;
            while (true)
            {
                var messages = new List<MessageData>();

                // Stop dequeuing messages if the buffer gets too full.
                if (this.stats.PendingOrchestratorMessages.Value < this.settings.ControlQueueBufferThreshold)
                {
                    await this.ownedControlQueues.Values.ParallelForEachAsync(
                        async delegate (CloudQueue controlQueue)
                        {
                            IEnumerable<CloudQueueMessage> batch = await controlQueue.GetMessagesAsync(
                                this.settings.ControlQueueBatchSize,
                                this.settings.ControlQueueVisibilityTimeout,
                                this.settings.ControlQueueRequestOptions,
                                null /* operationContext */,
                                cancellationToken);
                            this.stats.StorageRequests.Increment();

                            IEnumerable<MessageData> deserializedBatch = 
                                batch.Select(m => Utils.DeserializeQueueMessage(m, controlQueue.Name));
                            lock (messages)
                            {
                                messages.AddRange(deserializedBatch);
                            }
                        });

                    this.stats.MessagesRead.Increment(messages.Count);
                    this.stats.PendingOrchestratorMessages.Increment(messages.Count);
                }

                nextBatch = this.StashMessagesAndGetNextBatch(messages);
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
                ReceivedMessageContext.CreateFromReceivedMessageBatch(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    nextBatch.Messages);

            OrchestrationInstance instance = messageContext.Instance;
            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instance.InstanceId,
                instance.ExecutionId,
                messageContext.StorageOperationContext,
                cancellationToken);

            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = instance.InstanceId,
                NewMessages = nextBatch.Messages.Select(msg => msg.TaskMessage).ToList(),
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

        PendingMessageBatch StashMessagesAndGetNextBatch(IEnumerable<MessageData> queueMessages)
        {
            lock (this.pendingOrchestrationMessageBatches)
            {
                LinkedListNode<PendingMessageBatch> node;

                // If the queue is empty, queueMessages will be an empty enumerable and this foreach will be skipped.
                foreach (MessageData data in queueMessages)
                {
                    PendingMessageBatch targetBatch = null;

                    // Walk backwards through the list of batches until we find one with a matching Instance ID.
                    // This is assumed to be more efficient than walking forward if most messages arrive in the queue in groups.
                    node = this.pendingOrchestrationMessageBatches.Last;
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
                        this.pendingOrchestrationMessageBatches.AddLast(targetBatch);
                    }

                    targetBatch.OrchestrationInstanceId = data.TaskMessage.OrchestrationInstance.InstanceId;

                    // If a message has been sitting in the buffer for too long, the invisibility timeout may expire and 
                    // it may get dequeued a second time. In such cases, we should replace the existing copy of the message
                    // with the newer copy to ensure it can be deleted successfully after being processed.
                    int i;
                    for (i = 0; i < targetBatch.Messages.Count; i++)
                    {
                        CloudQueueMessage existingMessage = targetBatch.Messages[i].OriginalQueueMessage;
                        if (existingMessage.Id == data.OriginalQueueMessage.Id)
                        {
                            AnalyticsEventSource.Log.DuplicateMessageDetected(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                existingMessage.Id,
                                existingMessage.DequeueCount);
                            targetBatch.Messages[i] = data;
                            break;
                        }
                    }

                    if (i >= targetBatch.Messages.Count)
                    {
                        targetBatch.Messages.Add(data);
                    }
                }

                // Pull batches of messages off the linked-list in FIFO order to ensure fairness.
                // Skip over instances which are currently being processed.
                node = this.pendingOrchestrationMessageBatches.First;
                while (node != null)
                {
                    PendingMessageBatch nextBatch = node.Value;
                    if (!this.activeOrchestrationInstances.ContainsKey(nextBatch.OrchestrationInstanceId))
                    {
                        this.activeOrchestrationInstances.TryAdd(nextBatch.OrchestrationInstanceId, null);
                        this.pendingOrchestrationMessageBatches.Remove(node);
                        this.stats.PendingOrchestratorMessages.Increment(-nextBatch.Messages.Count);
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
                /*TableQuerySegment<DynamicTableEntity>*/ var segment = await this.historyTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    this.settings.HistoryTableRequestOptions,
                    storageOperationContext,
                    cancellationToken);
                stopwatch.Stop();

                int previousCount = historyEventEntities.Count;
                historyEventEntities.AddRange(segment);
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(historyEventEntities.Count - previousCount);

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
                this.storageAccountName,
                this.settings.TaskHubName,
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

        /// <inheritdoc />
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
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    $"Could not find context for orchestration work item with InstanceId = {workItem.InstanceId}.");
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
                    this.stats.StorageRequests.Increment();
                    this.stats.TableEntitiesWritten.Increment(operation.Count);

                    AnalyticsEventSource.Log.AppendedInstanceState(
                        this.storageAccountName,
                        this.settings.TaskHubName,
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

            CloudQueue controlQueue = null;
            int totalMessageCount = 0;

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueTasks = new List<Task>(newEvents.Count);
            if (orchestratorMessages?.Count > 0)
            {
                totalMessageCount += orchestratorMessages.Count;
                addedControlMessages = true;
                if (controlQueue == null)
                {
                    controlQueue = await this.GetControlQueueAsync(instanceId);
                }

                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    enqueueTasks.Add(controlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage, controlQueue.Name),
                        null /* timeToLive */,
                        null /* initialVisibilityDelay */,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (timerMessages?.Count > 0)
            {
                totalMessageCount += timerMessages.Count; 
                addedControlMessages = true;
                if (controlQueue == null)
                {
                    controlQueue = await this.GetControlQueueAsync(instanceId);
                }

                foreach (TaskMessage taskMessage in timerMessages)
                {
                    DateTime messageFireTime = ((TimerFiredEvent)taskMessage.Event).FireAt;
                    TimeSpan initialVisibilityDelay = messageFireTime.Subtract(DateTime.UtcNow);
                    Debug.Assert(initialVisibilityDelay <= TimeSpan.FromDays(7));
                    if (initialVisibilityDelay < TimeSpan.Zero)
                    {
                        initialVisibilityDelay = TimeSpan.Zero;
                    }

                    enqueueTasks.Add(controlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage, controlQueue.Name),
                        null /* timeToLive */,
                        initialVisibilityDelay,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (outboundMessages?.Count > 0)
            {
                totalMessageCount += outboundMessages.Count;
                addedWorkItemMessages = true;
                foreach (TaskMessage taskMessage in outboundMessages)
                {
                    enqueueTasks.Add(this.workItemQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage, this.workItemQueue.Name),
                        null /* timeToLive */,
                        null /* initialVisibilityDelay */,
                        this.settings.WorkItemQueueRequestOptions,
                        context.StorageOperationContext));
                }
            }

            if (continuedAsNewMessage != null)
            {
                totalMessageCount++;
                addedControlMessages = true;
                if (controlQueue == null)
                {
                    controlQueue = await this.GetControlQueueAsync(instanceId);
                }

                enqueueTasks.Add(controlQueue.AddMessageAsync(
                    context.CreateOutboundQueueMessage(continuedAsNewMessage, controlQueue.Name),
                    null /* timeToLive */,
                    null /* initialVisibilityDelay */,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext));
            }

            await Task.WhenAll(enqueueTasks);
            this.stats.StorageRequests.Increment(totalMessageCount);
            this.stats.MessagesSent.Increment(totalMessageCount);

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

        /// <inheritdoc />
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                workItem.LockedUntilUtc = DateTime.UtcNow;
                return;
            }


            string instanceId = workItem.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            // REVIEW: The error handling and reporting is not entirely correct. However, there doesn't
            //         seem to be any code which calls this method, so there is little reason to fix it.
            //         https://github.com/Azure/durabletask/issues/112
            try
            {
                await Task.WhenAll(context.MessageDataBatch.Select(e =>
                    controlQueue.UpdateMessageAsync(
                        e.OriginalQueueMessage,
                        this.settings.ControlQueueVisibilityTimeout,
                        MessageUpdateFields.Visibility,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext)));
                workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.ControlQueueVisibilityTimeout);
                this.stats.MessagesUpdated.Increment(context.MessageDataBatch.Count);
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message(s) may have been processed and deleted already.
                foreach (MessageData message in context.MessageDataBatch)
                {
                    AnalyticsEventSource.Log.MessageGone(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(RenewTaskOrchestrationWorkItemLockAsync));
                }
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        /// <inheritdoc />
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                return;
            }

            string instanceId = workItem.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            Task[] updates = new Task[context.MessageDataBatch.Count];

            // We "abandon" the message by settings its visibility timeout to zero.
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;

                AnalyticsEventSource.Log.AbandoningMessage(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    taskMessage.OrchestrationInstance.InstanceId,
                    taskMessage.OrchestrationInstance.ExecutionId);

                updates[i] = controlQueue.UpdateMessageAsync(
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
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(AbandonTaskOrchestrationWorkItemAsync));
                }
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchestration work item is abandoned.
        /// <inheritdoc />
        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            string instanceId = workItem.InstanceId;

            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem, out context))
            {
                // The context doesn't exist - possibly because this is a duplicate message.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"Could not find context for orchestration work item with InstanceId = {instanceId}.");
                return;
            }

            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            Task[] deletes = new Task[context.MessageDataBatch.Count];
            for (int i = 0; i < context.MessageDataBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = context.MessageDataBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = context.MessageDataBatch[i].TaskMessage;
                AnalyticsEventSource.Log.DeletingMessage(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    instanceId);
                deletes[i] = controlQueue.DeleteMessageAsync(
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
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        message.OriginalQueueMessage.Id,
                        workItem.InstanceId,
                        nameof(ReleaseTaskOrchestrationWorkItemAsync));
                }
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }

            ReceivedMessageContext.RemoveContext(workItem);

            this.activeOrchestrationInstances.TryRemove(workItem.InstanceId, out _);
        }
        #endregion

        #region Task Activity Methods
        /// <inheritdoc />
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
                this.stats.StorageRequests.Increment();

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

            this.stats.MessagesRead.Increment();
            this.workItemQueueBackoff.Reset();

            ReceivedMessageContext context = ReceivedMessageContext.CreateFromReceivedMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                queueMessage,
                this.workItemQueue.Name);
            if (!context.TrySave(queueMessage.Id))
            {
                // This means we're already processing this message. This is never expected since the message
                // should be kept invisible via background calls to RenewTaskActivityWorkItemLockAsync.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"Work item queue message with ID = {queueMessage.Id} is being processed multiple times concurrently.");
                return null;
            }

            this.stats.ActiveActivityExecutions.Increment();

            return new TaskActivityWorkItem
            {
                Id = queueMessage.Id,
                TaskMessage = context.MessageData.TaskMessage,
                LockedUntilUtc = context.GetNextMessageExpirationTimeUtc(),
            };
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseTaskMessage)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // First, send a response message back. If this fails, we'll try again later since we haven't deleted the
            // work item message yet (that happens next).
            try
            {
                await controlQueue.AddMessageAsync(
                    context.CreateOutboundQueueMessage(responseTaskMessage, controlQueue.Name),
                    null /* timeToLive */,
                    null /* initialVisibilityDelay */,
                    this.settings.WorkItemQueueRequestOptions,
                    context.StorageOperationContext);
                this.stats.MessagesSent.Increment();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            // Signal the control queue listener thread to poll immediately 
            // to avoid unnecessary delay between sending and receiving.
            this.controlQueueBackoff.Reset();

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            AnalyticsEventSource.Log.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                context.MessageData.OriginalQueueMessage.Id,
                instanceId);

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
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(CompleteTaskActivityWorkItemAsync));
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (ReceivedMessageContext.RemoveContext(workItem.Id))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }

        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                // Expire the work item to prevent subsequent renewal attempts.
                return ExpireWorkItem(workItem);
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
                workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.WorkItemQueueVisibilityTimeout);
                this.stats.MessagesUpdated.Increment();
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // Message was deleted
                AnalyticsEventSource.Log.MessageGone(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(RenewTaskActivityWorkItemLockAsync));
                return ExpireWorkItem(workItem);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            return workItem;
        }

        static TaskActivityWorkItem ExpireWorkItem(TaskActivityWorkItem workItem)
        {
            workItem.LockedUntilUtc = DateTime.UtcNow;
            return workItem;
        }

        /// <inheritdoc />
        public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            ReceivedMessageContext context;
            if (!ReceivedMessageContext.TryRestoreContext(workItem.Id, out context))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            AnalyticsEventSource.Log.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
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
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    context.MessageData.OriginalQueueMessage.Id,
                    workItem.TaskMessage.OrchestrationInstance.InstanceId,
                    nameof(AbandonTaskActivityWorkItemAsync));
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (ReceivedMessageContext.RemoveContext(workItem.Id))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }
        #endregion

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            // This orchestration service implementation will manage batch sizes by itself.
            // We don't want to rely on the underlying framework's backoff mechanism because
            // it would require us to implement some kind of duplicate message detection.
            return false;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            // TODO: Need to reason about exception delays
            return 10;
        }

        /// <inheritdoc />
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

            CloudQueue controlQueue = await this.GetControlQueueAsync(message.OrchestrationInstance.InstanceId);

            await this.SendTaskOrchestrationMessageInternalAsync(controlQueue, message);
        }

        async Task SendTaskOrchestrationMessageInternalAsync(CloudQueue controlQueue, TaskMessage message)
        {
            await controlQueue.AddMessageAsync(
                ReceivedMessageContext.CreateOutboundQueueMessageInternal(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    controlQueue.Name,
                    message),
                null /* timeToLive */,
                null /* initialVisibilityDelay */,
                this.settings.ControlQueueRequestOptions,
                null /* operationContext */);
            this.stats.StorageRequests.Increment();
            this.stats.MessagesSent.Increment();

            // Notify the control queue poller that there are new messages to process.
            // TODO: This should be specific to the one control queue
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

        // TODO: Change this to a sticky assignment so that partition count changes can
        //       be supported: https://github.com/Azure/azure-functions-durable-extension/issues/1
        async Task<CloudQueue> GetControlQueueAsync(string instanceId)
        {
            uint partitionIndex = Fnv1aHashHelper.ComputeHash(instanceId) % (uint)this.settings.PartitionCount;
            CloudQueue controlQueue = GetControlQueue(this.queueClient, this.settings.TaskHubName, (int)partitionIndex);

            CloudQueue cachedQueue;
            if (this.ownedControlQueues.TryGetValue(controlQueue.Name, out cachedQueue) ||
                this.allControlQueues.TryGetValue(controlQueue.Name, out cachedQueue))
            {
                return cachedQueue;
            }
            else
            {
                try
                {
                    await controlQueue.CreateIfNotExistsAsync();
                }
                finally
                {
                    this.stats.StorageRequests.Increment();
                }

                this.allControlQueues.TryAdd(controlQueue.Name, controlQueue);
                return controlQueue;
            }
        }

        class PendingMessageBatch
        {
            public string OrchestrationInstanceId { get; set; }

            public List<MessageData> Messages { get; set; } = new List<MessageData>();
        }

        class ResettableLazy<T>
        {
            readonly Func<T> valueFactory;
            readonly LazyThreadSafetyMode threadSafetyMode;

            Lazy<T> lazy;

            public ResettableLazy(Func<T> valueFactory, LazyThreadSafetyMode mode)
            {
                this.valueFactory = valueFactory;
                this.threadSafetyMode = mode;

                this.Reset();
            }

            public T Value => this.lazy.Value;

            public void Reset()
            {
                this.lazy = new Lazy<T>(this.valueFactory, this.threadSafetyMode);
            }
        }
    }
}
