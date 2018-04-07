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
        readonly CloudTable instancesTable;
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

            string instancesTableName = $"{settings.TaskHubName}Instances";
            NameValidator.ValidateTableName(instancesTableName);

            this.historyTable = tableClient.GetTableReference(historyTableName);

            this.instancesTable = tableClient.GetTableReference(instancesTableName);

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

        internal CloudTable InstancesTable => this.instancesTable;

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
            tasks.Add(this.instancesTable.CreateIfNotExistsAsync());

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
            tasks.Add(this.instancesTable.DeleteIfExistsAsync());

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
            ServicePointManager.FindServicePoint(this.instancesTable.Uri).UseNagleAlgorithm = false;
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
                CloudQueue controlQueue = await this.GetControlQueueAsync(instance.InstanceId);
                await this.DeleteMessageBatchAsync(messageContext, controlQueue);
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
                        if (batch.OrchestrationInstanceId == data.TaskMessage.OrchestrationInstance.InstanceId &&
                            batch.OrchestrationExecutionId == data.TaskMessage.OrchestrationInstance.ExecutionId)
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
                    targetBatch.OrchestrationExecutionId = data.TaskMessage.OrchestrationInstance.ExecutionId;

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
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089'"
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
                var segment = await this.historyTable.ExecuteQuerySegmentedAsync(
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
                // The most recent generation will always be in the first history event.
                executionId = historyEventEntities[0].Properties["ExecutionId"].StringValue;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(historyEventEntities.Count);
                foreach (DynamicTableEntity entity in historyEventEntities)
                {
                    if (entity.Properties["ExecutionId"].StringValue != executionId)
                    {
                        // The remaining entities are from a previous generation and can be discarded.
                        break;
                    }

                    events.Add((HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(entity, GetTypeForTableEntity));
                }

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

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new TableBatchOperation();

            EventType? orchestratorEventType = null;

            DynamicTableEntity orchestrationInstanceUpdate = new DynamicTableEntity(instanceId, "")
            {
                Properties =
                {
                    ["CustomStatus"] = new EntityProperty(runtimeState.Status),
                    ["ExecutionId"] = new EntityProperty(executionId),
                    ["LastUpdatedTime"] = new EntityProperty(newEvents.Last().Timestamp),
                }
            };

            for (int i = 0; i < newEvents.Count; i++)
            {
                HistoryEvent historyEvent = newEvents[i];
                DynamicTableEntity entity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);

                newEventListBuffer.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is the sequence number, which represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                entity.RowKey = sequenceNumber.ToString("X16");
                entity.PartitionKey = instanceId;
                entity.Properties["ExecutionId"] = new EntityProperty(executionId);

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                historyEventBatch.InsertOrReplace(entity);

                // Table storage only supports inserts of up to 100 entities at a time.
                if (historyEventBatch.Count == 100)
                {
                    await this.UploadHistoryBatch(context, historyEventBatch, newEventListBuffer, newEvents.Count);

                    // Reset local state for the next batch
                    newEventListBuffer.Clear();
                    historyEventBatch.Clear();
                }

                // Monitor for orchestration instance events 
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Name"] = new EntityProperty(executionStartedEvent.Name);
                        orchestrationInstanceUpdate.Properties["Version"] = new EntityProperty(executionStartedEvent.Version);
                        orchestrationInstanceUpdate.Properties["Input"] = new EntityProperty(executionStartedEvent.Input);
                        orchestrationInstanceUpdate.Properties["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Running.ToString());
                        break;
                    case EventType.ExecutionCompleted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionCompleted.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(executionCompleted.OrchestrationStatus.ToString());
                        break;
                    case EventType.ExecutionTerminated:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionTerminatedEvent.Input);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Terminated.ToString());
                        break;
                    case EventType.ContinueAsNew:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Output"] = new EntityProperty(executionCompletedEvent.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.ContinuedAsNew.ToString());
                        break;
                }
            }

            // First persistence step is to commit history to the history table. Messages must come after.
            if (historyEventBatch.Count > 0)
            {
                await this.UploadHistoryBatch(context, historyEventBatch, newEventListBuffer, newEvents.Count);
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(TableOperation.InsertOrMerge(orchestrationInstanceUpdate));

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment();

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.settings.TaskHubName,
                instanceId,
                executionId,
                orchestratorEventType?.ToString() ?? string.Empty,
                orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds);

            bool addedControlMessages = false;
            bool addedWorkItemMessages = false;

            CloudQueue currentControlQueue = await this.GetControlQueueAsync(instanceId);
            int totalMessageCount = 0;

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueTasks = new List<Task>(newEvents.Count);
            if (orchestratorMessages?.Count > 0)
            {
                totalMessageCount += orchestratorMessages.Count;
                addedControlMessages = true;

                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    string targetInstanceId = taskMessage.OrchestrationInstance.InstanceId;
                    CloudQueue targetControlQueue = await this.GetControlQueueAsync(targetInstanceId);

                    enqueueTasks.Add(targetControlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage, targetControlQueue.Name),
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

                foreach (TaskMessage taskMessage in timerMessages)
                {
                    DateTime messageFireTime = ((TimerFiredEvent)taskMessage.Event).FireAt;
                    TimeSpan initialVisibilityDelay = messageFireTime.Subtract(DateTime.UtcNow);
                    Debug.Assert(initialVisibilityDelay <= TimeSpan.FromDays(7));
                    if (initialVisibilityDelay < TimeSpan.Zero)
                    {
                        initialVisibilityDelay = TimeSpan.Zero;
                    }

                    enqueueTasks.Add(currentControlQueue.AddMessageAsync(
                        context.CreateOutboundQueueMessage(taskMessage, currentControlQueue.Name),
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

                enqueueTasks.Add(currentControlQueue.AddMessageAsync(
                    context.CreateOutboundQueueMessage(continuedAsNewMessage, currentControlQueue.Name),
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

            await this.DeleteMessageBatchAsync(context, currentControlQueue);
        }

        async Task UploadHistoryBatch(
            ReceivedMessageContext context,
            TableBatchOperation historyEventBatch,
            StringBuilder historyEventNamesBuffer,
            int numberOfTotalEvents)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.historyTable.ExecuteBatchAsync(
                historyEventBatch,
                this.settings.HistoryTableRequestOptions,
                context.StorageOperationContext);
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(historyEventBatch.Count);

            AnalyticsEventSource.Log.AppendedInstanceState(
                this.storageAccountName,
                this.settings.TaskHubName,
                context.Instance.InstanceId,
                context.Instance.ExecutionId,
                historyEventBatch.Count,
                numberOfTotalEvents,
                historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                stopwatch.ElapsedMilliseconds);
        }

        async Task DeleteMessageBatchAsync(ReceivedMessageContext context, CloudQueue controlQueue)
        {
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
                    context.Instance.InstanceId,
                    context.Instance.ExecutionId);
                Task deletetask = controlQueue.DeleteMessageAsync(
                    queueMessage,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext);

                // Handle the case where this message was already deleted.
                deletes[i] = this.HandleNotFoundException(
                    deletetask,
                    queueMessage.Id,
                    context.Instance.InstanceId);
            }

            try
            {
                await Task.WhenAll(deletes);
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        // REVIEW: There doesn't seem to be any code which calls this method.
        //         https://github.com/Azure/durabletask/issues/112
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
            try
            {
                await Task.WhenAll(context.MessageDataBatch.Select(e =>
                {
                    Task updateTask = controlQueue.UpdateMessageAsync(
                        e.OriginalQueueMessage,
                        this.settings.ControlQueueVisibilityTimeout,
                        MessageUpdateFields.Visibility,
                        this.settings.ControlQueueRequestOptions,
                        context.StorageOperationContext);

                    return this.HandleNotFoundException(updateTask, e.OriginalQueueMessage.Id, workItem.InstanceId);
                }));

                workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.ControlQueueVisibilityTimeout);
                this.stats.MessagesUpdated.Increment(context.MessageDataBatch.Count);
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
            // This allows it to be reprocessed on this node or another node.
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

                Task abandonTask = controlQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    this.settings.ControlQueueRequestOptions,
                    context.StorageOperationContext);

                // Message may have been processed and deleted already.
                updates[i] = HandleNotFoundException(abandonTask, queueMessage.Id, instanceId);
            }

            try
            {
                await Task.WhenAll(updates);
            }
            finally
            {
                this.stats.StorageRequests.Increment(context.MessageDataBatch.Count);
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchestration work item is abandoned.
        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // Release is local/in-memory only because instances are affinitized to queues and this
            // node already holds the lease for the target control queue.
            ReceivedMessageContext.RemoveContext(workItem);
            this.activeOrchestrationInstances.TryRemove(workItem.InstanceId, out _);
            return Utils.CompletedTask;
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

            string messageId = context.MessageData.OriginalQueueMessage.Id;

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            AnalyticsEventSource.Log.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instanceId,
                context.Instance.ExecutionId);

            Task deleteTask = this.workItemQueue.DeleteMessageAsync(
                context.MessageData.OriginalQueueMessage,
                this.settings.WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                // Handle the case where the message was already deleted
                await this.HandleNotFoundException(deleteTask, messageId, instanceId);
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

            string messageId = context.MessageData.OriginalQueueMessage.Id;
            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            Task renewTask = this.workItemQueue.UpdateMessageAsync(
                context.MessageData.OriginalQueueMessage,
                this.settings.WorkItemQueueVisibilityTimeout,
                MessageUpdateFields.Visibility,
                this.settings.WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(renewTask, messageId, instanceId);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.WorkItemQueueVisibilityTimeout);
            this.stats.MessagesUpdated.Increment();

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

            string messageId = context.MessageData.OriginalQueueMessage.Id;
            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;

            AnalyticsEventSource.Log.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instanceId,
                workItem.TaskMessage.OrchestrationInstance.ExecutionId);

            // We "abandon" the message by settings its visibility timeout to zero.
            Task abandonTask = this.workItemQueue.UpdateMessageAsync(
                context.MessageData.OriginalQueueMessage,
                TimeSpan.Zero,
                MessageUpdateFields.Visibility,
                this.settings.WorkItemQueueRequestOptions,
                context.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(abandonTask, messageId, instanceId);
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

        Task HandleNotFoundException(Task storagetask, string messageId, string instanceId)
        {
            return storagetask.ContinueWith(t =>
            {
                StorageException e = t.Exception?.InnerException as StorageException;
                if (e?.RequestInformation?.HttpStatusCode == 404)
                {
                    // Message may have been processed and deleted already.
                    AnalyticsEventSource.Log.MessageGone(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        messageId,
                        instanceId,
                        nameof(AbandonTaskOrchestrationWorkItemAsync));
                }
                else if (t.Exception?.InnerException != null)
                {
                    // Rethrow the original exception, preserving the callstack.
                    ExceptionDispatchInfo.Capture(t.Exception.InnerException).Throw();
                }
            });
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

            ExecutionStartedEvent executionStartedEvent = message.Event as ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                return;
            }

            DynamicTableEntity entity = new DynamicTableEntity(message.OrchestrationInstance.InstanceId, "")
            {
                Properties =
                {
                    ["Input"] = new EntityProperty(executionStartedEvent.Input),
                    ["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                    ["Name"] = new EntityProperty(executionStartedEvent.Name),
                    ["Version"] = new EntityProperty(executionStartedEvent.Version),
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                }
            };

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(
                TableOperation.Insert(entity));
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(1);

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.settings.TaskHubName,
                message.OrchestrationInstance.InstanceId,
                message.OrchestrationInstance.ExecutionId,
                message.Event.EventType.ToString(),
                stopwatch.ElapsedMilliseconds);
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
            OrchestrationState orchestrationState = new OrchestrationState();
           
            var stopwatch = new Stopwatch();
            TableResult orchestration = await this.InstancesTable.ExecuteAsync(TableOperation.Retrieve<OrchestrationInstanceStatus>(instanceId, ""));
            stopwatch.Stop();
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesRead.Increment(1);

            AnalyticsEventSource.Log.FetchedInstanceStatus(
                this.storageAccountName,
                this.settings.TaskHubName,
                instanceId,
                executionId,
                stopwatch.ElapsedMilliseconds);

            OrchestrationInstanceStatus orchestrationInstanceStatus = (OrchestrationInstanceStatus)orchestration.Result;
            if (orchestrationInstanceStatus != null)
            {
                if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
                {
                    throw new ArgumentException($"{orchestrationInstanceStatus.RuntimeStatus} is not a valid OrchestrationStatus value.");
                }

                orchestrationState.OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = orchestrationInstanceStatus.ExecutionId,
                };
                orchestrationState.Name = orchestrationInstanceStatus.Name;
                orchestrationState.Version = orchestrationInstanceStatus.Version;
                orchestrationState.Status = orchestrationInstanceStatus.CustomStatus;
                orchestrationState.CreatedTime = orchestrationInstanceStatus.CreatedTime;
                orchestrationState.LastUpdatedTime = orchestrationInstanceStatus.LastUpdatedTime;
                orchestrationState.Input = orchestrationInstanceStatus.Input;
                orchestrationState.Output = orchestrationInstanceStatus.Output;
            }

            return orchestrationState;
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
            public string OrchestrationExecutionId { get; set; }

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

        class OrchestrationInstanceStatus : TableEntity
        {
            public string ExecutionId { get; set; }
            public string  Name { get; set; }
            public string Version { get; set; }
            public string Input { get; set; }
            public string Output { get; set; }
            public string CustomStatus { get; set; }
            public DateTime CreatedTime { get; set; }
            public DateTime LastUpdatedTime { get; set; }
            public string RuntimeStatus { get; set; }
        }
    }
}
