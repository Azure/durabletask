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
    using System.Runtime.ExceptionServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public class AzureStorageOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient,
        IPartitionObserver<BlobLease>
    {
        internal static readonly TimeSpan MaxQueuePollingDelay = TimeSpan.FromSeconds(10);

        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];
        static readonly OrchestrationInstance EmptySourceInstance = new OrchestrationInstance
        {
            InstanceId = string.Empty,
            ExecutionId = string.Empty
        };

        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly string storageAccountName;
        readonly CloudQueueClient queueClient;
        readonly CloudBlobClient blobClient;
        readonly ConcurrentDictionary<string, CloudQueue> ownedControlQueues;
        readonly ConcurrentDictionary<string, CloudQueue> allControlQueues;
        readonly CloudQueue workItemQueue;
        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessageBatches;
        readonly ConcurrentDictionary<string, OrchestrationSession> activeOrchestrationSessions;
        readonly ConcurrentDictionary<string, ActivitySession> activeActivitySessions;
        readonly MessageManager messageManager;

        readonly ITrackingStore trackingStore;

        readonly TableEntityConverter tableEntityConverter;

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
            : this(settings, null)
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageOrchestrationService"/> class with a custom instance store.
        /// </summary>
        /// <param name="settings">The settings used to configure the orchestration service.</param>
        /// <param name="customInstanceStore">Custom UserDefined Instance store to be used with the AzureStorageOrchestrationService</param>
        public AzureStorageOrchestrationService(AzureStorageOrchestrationServiceSettings settings, IOrchestrationServiceInstanceStore customInstanceStore)
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
            this.queueClient.BufferManager = SimpleBufferManager.Shared;
            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.BufferManager = SimpleBufferManager.Shared;

            // TODO: Need to do input validation on the TaskHubName.

            this.ownedControlQueues = new ConcurrentDictionary<string, CloudQueue>();
            this.allControlQueues = new ConcurrentDictionary<string, CloudQueue>();
            this.workItemQueue = GetWorkItemQueue(account, settings.TaskHubName);

            for (int i = 0; i < this.settings.PartitionCount; i++)
            {
                CloudQueue queue = GetControlQueue(this.queueClient, this.settings.TaskHubName, i);
                this.allControlQueues.TryAdd(queue.Name, queue);
            }

            string compressedMessageBlobContainerName = $"{settings.TaskHubName.ToLowerInvariant()}-largemessages";
            NameValidator.ValidateContainerName(compressedMessageBlobContainerName);
            this.messageManager = new MessageManager(this.blobClient, compressedMessageBlobContainerName);

            if (customInstanceStore == null)
            {
                this.trackingStore = new AzureTableTrackingStore(settings, this.messageManager, this.stats);
            }
            else
            {
                this.trackingStore = new InstanceStoreBackedTrackingStore(customInstanceStore);
            }

            this.pendingOrchestrationMessageBatches = new LinkedList<PendingMessageBatch>();
            this.activeOrchestrationSessions = new ConcurrentDictionary<string, OrchestrationSession>(StringComparer.OrdinalIgnoreCase);
            this.activeActivitySessions = new ConcurrentDictionary<string, ActivitySession>(StringComparer.OrdinalIgnoreCase);

            // Queue polling backoff policies
            var minPollingDelayThreshold = TimeSpan.FromMilliseconds(500);
            this.controlQueueBackoff = new BackoffPollingHelper(MaxQueuePollingDelay, minPollingDelayThreshold);
            this.workItemQueueBackoff = new BackoffPollingHelper(MaxQueuePollingDelay, minPollingDelayThreshold);

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

        internal ITrackingStore TrackingStore => this.trackingStore;

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
            await this.EnsureTaskHubAsync();
        }

        /// <summary>
        /// Creates the necessary Azure Storage resources for the orchestration service if they don't already exist.
        /// </summary>
        public Task CreateIfNotExistsAsync()
        {
            return this.EnsureTaskHubAsync();
        }

        async Task EnsureTaskHubAsync()
        {
            try
            {
                await this.taskHubCreator.Value;
            }
            catch (Exception e)
            {
                AnalyticsEventSource.Log.GeneralError(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"Failed to create the task hub: {e}",
                    Utils.ExtensionVersion);

                // Don't want to cache the failed task
                this.taskHubCreator.Reset();
                throw;
            }
        }

        // Internal logic used by the lazy taskHubCreator
        async Task GetTaskHubCreatorTask()
        {
            TaskHubInfo hubInfo = GetTaskHubInfo(this.settings.TaskHubName, this.settings.PartitionCount);
            await this.leaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo);
            this.stats.StorageRequests.Increment();

            var tasks = new List<Task>();

            tasks.Add(this.trackingStore.CreateAsync());

            tasks.Add(this.workItemQueue.CreateIfNotExistsAsync());

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
        public Task DeleteAsync()
        {
            return this.DeleteAsync(deleteInstanceStore: true);
        }

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            if (recreateInstanceStore)
            {
               await DeleteTrackingStore();

               this.taskHubCreator.Reset();
            }

            await this.taskHubCreator.Value;
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore)
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

            if (deleteInstanceStore)
            {
                tasks.Add(DeleteTrackingStore());
            }

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

        private Task DeleteTrackingStore()
        {
            return this.trackingStore.DeleteAsync();
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            if (this.isStarted)
            {
                throw new InvalidOperationException("The orchestration service has already started.");
            }

            await this.trackingStore.StartAsync();

            // Disable nagling to improve storage access latency:
            // https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            // Ad-hoc testing has shown very nice improvements (20%-50% drop in queue message age for simple scenarios).
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
                        $"Unexpected error in {nameof(ReportStatsLoop)}: {e}",
                        Utils.ExtensionVersion);
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
                this.activeOrchestrationSessions.Count,
                this.stats.ActiveActivityExecutions.Value,
                Utils.ExtensionVersion);
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
                    $"Worker ${this.settings.WorkerId} lost a lease '{lease.PartitionId}' but didn't own the queue.",
                    Utils.ExtensionVersion);
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
            int defaultPartitionCount)
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
                GetTaskHubInfo(taskHub, defaultPartitionCount));

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
            Guid traceActivityId = StartNewLogicalTraceScope();

            await this.EnsureTaskHubAsync();

            Stopwatch receiveTimeoutStopwatch = Stopwatch.StartNew();
            PendingMessageBatch nextBatch;
            while (true)
            {
                // Every dequeue operation has a common trace ID so that batches of dequeued messages can be correlated together.
                // If messages are dequeued and processed in the same loop iteration, then they'll have the same trace activity ID.
                // If messages are dequeued and saved for later, then the trace activity IDs will be different. In either case,
                // both the dequeue traces and the processing traces will share the same "related" trace activity ID.
                traceActivityId = StartNewLogicalTraceScope();

                var messages = new ConcurrentBag<MessageData>();

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

                            await batch.ParallelForEachAsync(async delegate (CloudQueueMessage queueMessage)
                            {
                                MessageData messageData = await this.messageManager.DeserializeQueueMessageAsync(
                                    queueMessage,
                                    controlQueue.Name);

                                TraceMessageReceived(messageData);
                                messages.Add(messageData);
                            });
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

            OrchestrationInstance instance = nextBatch.Messages[0].TaskMessage.OrchestrationInstance;

            var session = new OrchestrationSession(
                this.storageAccountName,
                this.settings.TaskHubName,
                instance,
                nextBatch.Messages,
                this.FetchMessagesForExtendedSession,
                this.settings.ExtendedSessionIdleTimeout,
                traceActivityId);

            if (!this.activeOrchestrationSessions.TryAdd(instance.InstanceId, session))
            {
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"Session {instance.InstanceId} is being processed twice! Are multiple dispatcher threads running?",
                    Utils.ExtensionVersion);
            }

            session.StartNewLogicalTraceScope();
            foreach (MessageData message in nextBatch.Messages)
            {
                session.TraceProcessingMessage(message, isExtendedSession: false);
            }

            OrchestrationRuntimeState runtimeState = await this.GetOrchestrationRuntimeStateAsync(
                instance.InstanceId,
                instance.ExecutionId,
                cancellationToken);

            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = instance.InstanceId,
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = session.CurrentMessageBatch.Min(msg => msg.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime),
                NewMessages = session.CurrentMessageBatch.Select(m => m.TaskMessage).ToList(),
            };

            if (this.settings.ExtendedSessionsEnabled)
            {
                orchestrationWorkItem.Session = session;
            }

            if (!this.IsExecutableInstance(runtimeState, orchestrationWorkItem.NewMessages, out string warningMessage))
            {
                var eventListBuilder = new StringBuilder(orchestrationWorkItem.NewMessages.Count * 40);
                foreach (TaskMessage msg in orchestrationWorkItem.NewMessages)
                {
                    eventListBuilder.Append(msg.Event.EventType.ToString()).Append(',');
                }

                AnalyticsEventSource.Log.DiscardingWorkItem(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    instance.InstanceId,
                    instance.ExecutionId,
                    orchestrationWorkItem.NewMessages.Count,
                    runtimeState.Events.Count,
                    eventListBuilder.ToString(0, eventListBuilder.Length - 1) /* remove trailing comma */,
                    warningMessage,
                    Utils.ExtensionVersion);

                // The instance has already completed. Delete this message batch.
                CloudQueue controlQueue = await this.GetControlQueueAsync(instance.InstanceId);
                await this.DeleteMessageBatchAsync(session, controlQueue);
                await this.ReleaseTaskOrchestrationWorkItemAsync(orchestrationWorkItem);
                return null;
            }

            return orchestrationWorkItem;
        }

        static Guid StartNewLogicalTraceScope()
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            Guid traceActivityId = Guid.NewGuid();
            AnalyticsEventSource.SetLogicalTraceActivityId(traceActivityId);
            return traceActivityId;
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
                                existingMessage.DequeueCount,
                                Utils.ExtensionVersion);
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
                    string instanceId = nextBatch.OrchestrationInstanceId;

                    // Check to see if the session is waiting for more messages. If it is, signal them.
                    if (this.activeOrchestrationSessions.TryGetValue(instanceId, out OrchestrationSession existingSession))
                    {
                        existingSession.Notify();
                    }
                    else
                    {
                        this.pendingOrchestrationMessageBatches.Remove(node);
                        this.stats.PendingOrchestratorMessages.Increment(-nextBatch.Messages.Count);
                        return nextBatch;
                    }

                    node = node.Next;
                }

                return null;
            }
        }

        void TraceMessageReceived(MessageData data)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            TaskMessage taskMessage = data.TaskMessage;
            CloudQueueMessage queueMessage = data.OriginalQueueMessage;

            AnalyticsEventSource.Log.ReceivedMessage(
                data.ActivityId,
                this.storageAccountName,
                this.settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.Id,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertionTime.Value).TotalMilliseconds),
                queueMessage.DequeueCount,
                queueMessage.NextVisibleTime.GetValueOrDefault().DateTime.ToString("o"),
                data.TotalMessageSizeBytes,
                data.QueueName /* PartitionId */,
                Utils.ExtensionVersion);
        }

        bool IsExecutableInstance(OrchestrationRuntimeState runtimeState, IList<TaskMessage> newMessages, out string message)
        {
            if (runtimeState.ExecutionStartedEvent == null && !newMessages.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                message = runtimeState.Events.Count == 0 ? "No such instance" : "Instance is corrupted";
                return false;
            }

            if (runtimeState.ExecutionStartedEvent != null &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                message = $"Instance is {runtimeState.OrchestrationStatus}";
                return false;
            }

            message = null;
            return true;
        }

        List<MessageData> FetchMessagesForExtendedSession(OrchestrationInstance instance)
        {
            lock (this.pendingOrchestrationMessageBatches)
            {
                // TODO: Optimize this to be something faster than O(N).
                LinkedListNode<PendingMessageBatch> node = this.pendingOrchestrationMessageBatches.First;
                while (node != null)
                {
                    PendingMessageBatch nextBatch = node.Value;
                    if (nextBatch.OrchestrationInstanceId == instance.InstanceId &&
                        (nextBatch.OrchestrationExecutionId == instance.ExecutionId || nextBatch.OrchestrationExecutionId == null))
                    {
                        this.pendingOrchestrationMessageBatches.Remove(node);
                        this.stats.PendingOrchestratorMessages.Increment(-nextBatch.Messages.Count);
                        return nextBatch.Messages;
                    }

                    node = node.Next;
                }

                return null;
            }
        }

        async Task<OrchestrationRuntimeState> GetOrchestrationRuntimeStateAsync(
            string instanceId,
            string expectedExecutionId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return new OrchestrationRuntimeState(await this.trackingStore.GetHistoryEventsAsync(instanceId, expectedExecutionId, cancellationToken));
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
            OrchestrationSession session;
            if (!this.activeOrchestrationSessions.TryGetValue(workItem.InstanceId, out session))
            {
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(CompleteTaskOrchestrationWorkItemAsync)}: Session for instance {workItem.InstanceId} was not found!",
                    Utils.ExtensionVersion);
                return;
            }

            session.StartNewLogicalTraceScope();
            OrchestrationRuntimeState runtimeState = workItem.OrchestrationRuntimeState;

            string instanceId = workItem.InstanceId;
            string executionId = runtimeState.OrchestrationInstance.ExecutionId;

            try
            {
                await this.trackingStore.UpdateStateAsync(runtimeState, instanceId, executionId);
            }
            catch (Exception e)
            {
                // Precondition failure is expected to be handled internally and logged as a warning.
                if ((e as StorageException)?.RequestInformation?.HttpStatusCode != (int)HttpStatusCode.PreconditionFailed)
                {
                    // TODO: https://github.com/Azure/azure-functions-durable-extension/issues/332
                    //       It's possible that history updates may have been partially committed at this point.
                    //       If so, what are the implications of this as far as DurableTask.Core are concerned?
                    AnalyticsEventSource.Log.TrackingStoreUpdateFailure(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        instanceId,
                        executionId,
                        e.ToString(),
                        Utils.ExtensionVersion);
                }

                throw;
            }

            bool addedControlMessages = false;
            bool addedWorkItemMessages = false;

            CloudQueue currentControlQueue = await this.GetControlQueueAsync(instanceId);
            int totalMessageCount = 0;

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueTasks = new List<Task>(runtimeState.NewEvents.Count);
            if (orchestratorMessages?.Count > 0)
            {
                totalMessageCount += orchestratorMessages.Count;
                addedControlMessages = true;

                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    string targetInstanceId = taskMessage.OrchestrationInstance.InstanceId;
                    CloudQueue targetControlQueue = await this.GetControlQueueAsync(targetInstanceId);

                    enqueueTasks.Add(this.EnqueueMessageAsync(session, targetControlQueue, taskMessage, null, this.settings.ControlQueueRequestOptions));
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

                    enqueueTasks.Add(this.EnqueueMessageAsync(session, currentControlQueue, taskMessage, initialVisibilityDelay, this.settings.ControlQueueRequestOptions));
                }
            }

            if (outboundMessages?.Count > 0)
            {
                totalMessageCount += outboundMessages.Count;
                addedWorkItemMessages = true;
                foreach (TaskMessage taskMessage in outboundMessages)
                {
                    enqueueTasks.Add(this.EnqueueMessageAsync(session, this.workItemQueue, taskMessage, null, this.settings.WorkItemQueueRequestOptions));
                }
            }

            if (continuedAsNewMessage != null)
            {
                totalMessageCount++;
                addedControlMessages = true;

                enqueueTasks.Add(this.EnqueueMessageAsync(session, currentControlQueue, continuedAsNewMessage, null, this.settings.ControlQueueRequestOptions));
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

            await this.DeleteMessageBatchAsync(session, currentControlQueue);
        }

        async Task EnqueueMessageAsync(OrchestrationSession session, CloudQueue queue, TaskMessage taskMessage, TimeSpan? initialVisibilityDelay, QueueRequestOptions queueRequestOptions)
        {
            CloudQueueMessage message = await CreateOutboundQueueMessageAsync(
                this.messageManager,
                session.Instance,
                this.storageAccountName,
                this.settings.TaskHubName,
                queue.Name,
                taskMessage);

            try
            {
                await queue.AddMessageAsync(
                    message,
                    null /* timeToLive */,
                    initialVisibilityDelay,
                    queueRequestOptions,
                    session.StorageOperationContext);
            }
            catch (Exception e)
            {
                AnalyticsEventSource.Log.MessageFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    session.Instance.InstanceId,
                    session.Instance.ExecutionId,
                    queue.Name,
                    taskMessage.Event.EventType.ToString(),
                    e.ToString(),
                    Utils.ExtensionVersion);
                throw;
            }
        }

        Task<CloudQueueMessage> CreateOutboundQueueMessageAsync(
            OrchestrationInstance sourceInstance,
            string queueName,
            TaskMessage taskMessage)
        {
            return CreateOutboundQueueMessageAsync(
                this.messageManager,
                sourceInstance,
                this.storageAccountName,
                this.settings.TaskHubName,
                queueName,
                taskMessage);
        }

        static async Task<CloudQueueMessage> CreateOutboundQueueMessageAsync(
            MessageManager messageManager,
            OrchestrationInstance sourceInstance,
            string storageAccountName,
            string taskHub,
            string queueName,
            TaskMessage taskMessage)
        {
            // We transfer to a new trace activity ID every time a new outbound queue message is created.
            Guid outboundTraceActivityId = Guid.NewGuid();

            var data = new MessageData(taskMessage, outboundTraceActivityId, queueName);
            string rawContent = await messageManager.SerializeMessageDataAsync(data);

            AnalyticsEventSource.Log.SendingMessage(
                outboundTraceActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                sourceInstance.InstanceId,
                sourceInstance.ExecutionId,
                Encoding.Unicode.GetByteCount(rawContent),
                data.QueueName /* PartitionId */,
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                Utils.ExtensionVersion);

            return new CloudQueueMessage(rawContent);
        }

        async Task DeleteMessageBatchAsync(OrchestrationSession session, CloudQueue controlQueue)
        {
            Task[] deletes = new Task[session.CurrentMessageBatch.Count];
            for (int i = 0; i < session.CurrentMessageBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = session.CurrentMessageBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = session.CurrentMessageBatch[i].TaskMessage;
                AnalyticsEventSource.Log.DeletingMessage(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    session.Instance.InstanceId,
                    session.Instance.ExecutionId,
                    controlQueue.Name,
                    Utils.ExtensionVersion);
                Task deletetask = controlQueue.DeleteMessageAsync(
                    queueMessage,
                    this.settings.ControlQueueRequestOptions,
                    session.StorageOperationContext);

                // Handle the case where this message was already deleted.
                deletes[i] = this.HandleNotFoundException(
                    deletetask,
                    queueMessage.Id,
                    session.Instance,
                    controlQueue,
                    $"Caller: {nameof(DeleteMessageBatchAsync)}");
            }

            try
            {
                await Task.WhenAll(deletes);
            }
            finally
            {
                this.stats.StorageRequests.Increment(session.CurrentMessageBatch.Count);
            }
        }

        // REVIEW: There doesn't seem to be any code which calls this method.
        //         https://github.com/Azure/durabletask/issues/112
        /// <inheritdoc />
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationSession session;
            if (!this.activeOrchestrationSessions.TryGetValue(workItem.InstanceId, out session))
            {
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(RenewTaskOrchestrationWorkItemLockAsync)}: Session for instance {workItem.InstanceId} was not found!",
                    Utils.ExtensionVersion);
                return;
            }

            session.StartNewLogicalTraceScope();
            string instanceId = workItem.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            try
            {
                await Task.WhenAll(session.CurrentMessageBatch.Select(e =>
                {
                    Task updateTask = controlQueue.UpdateMessageAsync(
                        e.OriginalQueueMessage,
                        this.settings.ControlQueueVisibilityTimeout,
                        MessageUpdateFields.Visibility,
                        this.settings.ControlQueueRequestOptions,
                        session.StorageOperationContext);

                    return this.HandleNotFoundException(
                        updateTask,
                        e.OriginalQueueMessage.Id,
                        session.Instance,
                        controlQueue,
                        $"Caller: {nameof(RenewTaskOrchestrationWorkItemLockAsync)}");
                }));

                workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.ControlQueueVisibilityTimeout);
                this.stats.MessagesUpdated.Increment(session.CurrentMessageBatch.Count);
            }
            finally
            {
                this.stats.StorageRequests.Increment(session.CurrentMessageBatch.Count);
            }
        }

        /// <inheritdoc />
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationSession session;
            if (!this.activeOrchestrationSessions.TryGetValue(workItem.InstanceId, out session))
            {
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(AbandonTaskOrchestrationWorkItemAsync)}: Session for instance {workItem.InstanceId} was not found!",
                    Utils.ExtensionVersion);
                return;
            }

            session.StartNewLogicalTraceScope();
            string instanceId = workItem.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            Task[] updates = new Task[session.CurrentMessageBatch.Count];

            // We "abandon" the message by settings its visibility timeout to zero.
            // This allows it to be reprocessed on this node or another node.
            for (int i = 0; i < session.CurrentMessageBatch.Count; i++)
            {
                CloudQueueMessage queueMessage = session.CurrentMessageBatch[i].OriginalQueueMessage;
                TaskMessage taskMessage = session.CurrentMessageBatch[i].TaskMessage;
                OrchestrationInstance instance = taskMessage.OrchestrationInstance;

                AnalyticsEventSource.Log.AbandoningMessage(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    taskMessage.Event.EventType.ToString(),
                    queueMessage.Id,
                    instance.InstanceId,
                    instance.ExecutionId,
                    controlQueue.Name,
                    Utils.ExtensionVersion);

                Task abandonTask = controlQueue.UpdateMessageAsync(
                    queueMessage,
                    TimeSpan.Zero,
                    MessageUpdateFields.Visibility,
                    this.settings.ControlQueueRequestOptions,
                    session.StorageOperationContext);

                // Message may have been processed and deleted already.
                updates[i] = HandleNotFoundException(
                    abandonTask,
                    queueMessage.Id,
                    instance,
                    controlQueue,
                    $"Caller: {nameof(AbandonTaskOrchestrationWorkItemAsync)}");
            }

            try
            {
                await Task.WhenAll(updates);
            }
            finally
            {
                this.stats.StorageRequests.Increment(session.CurrentMessageBatch.Count);
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchestration work item is abandoned.
        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // Release is local/in-memory only because instances are affinitized to queues and this
            // node already holds the lease for the target control queue.
            if (!this.activeOrchestrationSessions.TryRemove(workItem.InstanceId, out OrchestrationSession activeSession))
            {
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(ReleaseTaskOrchestrationWorkItemAsync)}: Session for instance {workItem.InstanceId} was not found!",
                    Utils.ExtensionVersion);
            }

            return Utils.CompletedTask;
        }
        #endregion

        #region Task Activity Methods
        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsureTaskHubAsync();

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

            Guid traceActivityId = Guid.NewGuid();

            MessageData data = await this.messageManager.DeserializeQueueMessageAsync(
                queueMessage,
                this.workItemQueue.Name);

            var session = new ActivitySession(this.storageAccountName, this.settings.TaskHubName, data, traceActivityId);
            session.StartNewLogicalTraceScope();
            TraceMessageReceived(session.MessageData);
            session.TraceProcessingMessage(data, isExtendedSession: false);

            if (!this.activeActivitySessions.TryAdd(queueMessage.Id, session))
            {
                // This means we're already processing this message. This is never expected since the message
                // should be kept invisible via background calls to RenewTaskActivityWorkItemLockAsync.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    $"Work item queue message with ID = {queueMessage.Id} is being processed multiple times concurrently.",
                    Utils.ExtensionVersion);
                return null;
            }

            this.stats.ActiveActivityExecutions.Increment();

            return new TaskActivityWorkItem
            {
                Id = queueMessage.Id,
                TaskMessage = session.MessageData.TaskMessage,
                LockedUntilUtc = queueMessage.NextVisibleTime.Value.UtcDateTime,
            };
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseTaskMessage)
        {
            ActivitySession session;
            if (!this.activeActivitySessions.TryGetValue(workItem.Id, out session))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    $"Could not find context for work item with ID = {workItem.Id}.",
                    Utils.ExtensionVersion);
                return;
            }

            session.StartNewLogicalTraceScope();
            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
            CloudQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // First, send a response message back. If this fails, we'll try again later since we haven't deleted the
            // work item message yet (that happens next).
            try
            {
                await controlQueue.AddMessageAsync(
                    await this.CreateOutboundQueueMessageAsync(session.Instance, controlQueue.Name, responseTaskMessage),
                    null /* timeToLive */,
                    null /* initialVisibilityDelay */,
                    this.settings.WorkItemQueueRequestOptions,
                    session.StorageOperationContext);
                this.stats.MessagesSent.Increment();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            // Signal the control queue listener thread to poll immediately 
            // to avoid unnecessary delay between sending and receiving.
            this.controlQueueBackoff.Reset();

            string messageId = session.MessageData.OriginalQueueMessage.Id;
            OrchestrationInstance instance = session.Instance;

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            AnalyticsEventSource.Log.DeletingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instanceId,
                instance.ExecutionId,
                this.workItemQueue.Name /* PartitionId */,
                Utils.ExtensionVersion);

            Task deleteTask = this.workItemQueue.DeleteMessageAsync(
                session.MessageData.OriginalQueueMessage,
                this.settings.WorkItemQueueRequestOptions,
                session.StorageOperationContext);

            try
            {
                // Handle the case where the message was already deleted
                await this.HandleNotFoundException(
                    deleteTask,
                    messageId,
                    instance,
                    this.workItemQueue,
                    $"Caller: {nameof(CompleteTaskActivityWorkItemAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (this.activeActivitySessions.TryRemove(workItem.Id, out _))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }

        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            ActivitySession session;
            if (!this.activeActivitySessions.TryGetValue(workItem.Id, out session))
            {
                // The context does not exist - possibly because it was already removed.
                // Expire the work item to prevent subsequent renewal attempts.
                return ExpireWorkItem(workItem);
            }

            session.StartNewLogicalTraceScope();
            string messageId = session.MessageData.OriginalQueueMessage.Id;
            OrchestrationInstance instance = session.Instance;

            AnalyticsEventSource.Log.RenewingMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                instance.InstanceId,
                instance.ExecutionId,
                this.workItemQueue.Name,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                (int)this.settings.WorkItemQueueVisibilityTimeout.TotalSeconds,
                Utils.ExtensionVersion);

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            Task renewTask = this.workItemQueue.UpdateMessageAsync(
                session.MessageData.OriginalQueueMessage,
                this.settings.WorkItemQueueVisibilityTimeout,
                MessageUpdateFields.Visibility,
                this.settings.WorkItemQueueRequestOptions,
                session.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(
                    renewTask,
                    messageId,
                    instance,
                    this.workItemQueue,
                    $"Caller: {nameof(RenewTaskActivityWorkItemLockAsync)}");
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
            ActivitySession session;
            if (!this.activeActivitySessions.TryGetValue(workItem.Id, out session))
            {
                // The context does not exist - possibly because it was already removed.
                AnalyticsEventSource.Log.AssertFailure(
                    this.storageAccountName,
                    this.settings.TaskHubName, 
                    $"Could not find context for work item with ID = {workItem.Id}.",
                    Utils.ExtensionVersion);
                return;
            }

            session.StartNewLogicalTraceScope();
            string messageId = session.MessageData.OriginalQueueMessage.Id;
            OrchestrationInstance instance = workItem.TaskMessage.OrchestrationInstance;

            AnalyticsEventSource.Log.AbandoningMessage(
                this.storageAccountName,
                this.settings.TaskHubName,
                workItem.TaskMessage.Event.EventType.ToString(),
                messageId,
                instance.InstanceId,
                instance.ExecutionId,
                this.workItemQueue.Name,
                Utils.ExtensionVersion);

            // We "abandon" the message by settings its visibility timeout to zero.
            Task abandonTask = this.workItemQueue.UpdateMessageAsync(
                session.MessageData.OriginalQueueMessage,
                TimeSpan.Zero,
                MessageUpdateFields.Visibility,
                this.settings.WorkItemQueueRequestOptions,
                session.StorageOperationContext);

            try
            {
                await this.HandleNotFoundException(
                    abandonTask,
                    messageId,
                    instance,
                    this.workItemQueue,
                    $"Caller: {nameof(AbandonTaskActivityWorkItemAsync)}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            if (this.activeActivitySessions.TryRemove(workItem.Id, out _))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }

        Task HandleNotFoundException(Task storagetask, string messageId, OrchestrationInstance instance, CloudQueue queue, string details)
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
                        instance.InstanceId,
                        instance.ExecutionId,
                        queue.Name,
                        details,
                        Utils.ExtensionVersion);
                }
                else if (t.Exception?.InnerException != null)
                {
                    AnalyticsEventSource.Log.MessageFailure(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        messageId,
                        instance.InstanceId,
                        instance.ExecutionId,
                        queue.Name,
                        t.Exception.InnerException.ToString(),
                        Utils.ExtensionVersion);

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
            await this.EnsureTaskHubAsync();

            CloudQueue controlQueue = await this.GetControlQueueAsync(message.OrchestrationInstance.InstanceId);

            await this.SendTaskOrchestrationMessageInternalAsync(EmptySourceInstance, controlQueue, message);

            ExecutionStartedEvent executionStartedEvent = message.Event as ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                return;
            }

            await this.trackingStore.SetNewExecutionAsync(executionStartedEvent);
        }

        async Task SendTaskOrchestrationMessageInternalAsync(
            OrchestrationInstance sourceInstance,
            CloudQueue controlQueue,
            TaskMessage message)
        {
            await controlQueue.AddMessageAsync(
                await CreateOutboundQueueMessageAsync(
                    this.messageManager,
                    sourceInstance,
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
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(instanceId, allExecutions);
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
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(instanceId, executionId);
        }

        /// <summary>
        /// Get states of the all orchestration instances
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(cancellationToken);
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
                executionId);
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
                OrchestrationState state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                if (state == null || 
                    state.OrchestrationStatus == OrchestrationStatus.Running ||
                    state.OrchestrationStatus == OrchestrationStatus.Pending ||
                    state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
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
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// Also purges the blob storage. Currently only supported if a custom Instance store is provided.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            return this.trackingStore.PurgeHistoryAsync(thresholdDateTimeUtc, timeRangeFilterType);
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
    }
}
