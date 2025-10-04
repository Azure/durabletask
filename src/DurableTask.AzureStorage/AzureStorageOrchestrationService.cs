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
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Query;
    using Newtonsoft.Json;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Azure Storage as the durable store.
    /// </summary>
    public sealed class AzureStorageOrchestrationService :
        IOrchestrationService,
        IOrchestrationServiceClient,
        IDisposable,
        IOrchestrationServiceQueryClient,
        IOrchestrationServicePurgeClient,
        IEntityOrchestrationService
    {
        static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        static readonly OrchestrationInstance EmptySourceInstance = new OrchestrationInstance
        {
            InstanceId = string.Empty,
            ExecutionId = string.Empty
        };

        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly ConcurrentDictionary<string, ControlQueue> allControlQueues;
        readonly WorkItemQueue workItemQueue;
        readonly ConcurrentDictionary<string, ActivitySession> activeActivitySessions;
        readonly MessageManager messageManager;

        readonly ITrackingStore trackingStore;

        readonly ResettableLazy<Task> taskHubCreator;
        readonly BlobPartitionLeaseManager leaseManager;
        readonly AppLeaseManager appLeaseManager;
        readonly OrchestrationSessionManager orchestrationSessionManager;
        readonly IPartitionManager partitionManager;
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

        /// <inheritdoc/>
        public override string ToString()
        {
            string blobAccountName = this.azureStorageClient.BlobAccountName;
            string queueAccountName = this.azureStorageClient.QueueAccountName;
            string tableAccountName = this.azureStorageClient.TableAccountName;

            return blobAccountName == queueAccountName && blobAccountName == tableAccountName
                ? $"AzureStorageOrchestrationService on {blobAccountName}"
                : $"AzureStorageOrchestrationService on {blobAccountName} for blobs, {queueAccountName} for queues, and {tableAccountName} for tables";
        }

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

            this.azureStorageClient = new AzureStorageClient(settings);
            this.stats = this.azureStorageClient.Stats;

            string compressedMessageBlobContainerName = $"{settings.TaskHubName.ToLowerInvariant()}-largemessages";
            this.messageManager = new MessageManager(this.settings, this.azureStorageClient, compressedMessageBlobContainerName);

            this.allControlQueues = new ConcurrentDictionary<string, ControlQueue>();
            for (int index = 0; index < this.settings.PartitionCount; index++)
            {
                var controlQueueName = GetControlQueueName(this.settings.TaskHubName, index);
                ControlQueue controlQueue = new ControlQueue(this.azureStorageClient, controlQueueName, this.messageManager);
                this.allControlQueues.TryAdd(controlQueue.Name, controlQueue);
            }

            var workItemQueueName = GetWorkItemQueueName(this.settings.TaskHubName);
            this.workItemQueue = new WorkItemQueue(this.azureStorageClient, workItemQueueName, this.messageManager);

            if (customInstanceStore == null)
            {
                this.trackingStore = new AzureTableTrackingStore(this.azureStorageClient, this.messageManager);
            }
            else
            {
                this.trackingStore = new InstanceStoreBackedTrackingStore(customInstanceStore);
            }

            this.activeActivitySessions = new ConcurrentDictionary<string, ActivitySession>(StringComparer.OrdinalIgnoreCase);

            this.hubCreationLock = new object();
            this.taskHubCreator = new ResettableLazy<Task>(
                this.GetTaskHubCreatorTask,
                LazyThreadSafetyMode.ExecutionAndPublication);

            this.leaseManager = GetBlobLeaseManager(
                this.azureStorageClient,
                "default");

            this.orchestrationSessionManager = new OrchestrationSessionManager(
                this.azureStorageClient.QueueAccountName,
                this.settings,
                this.stats,
                this.trackingStore);

            if (this.settings.UseTablePartitionManagement && this.settings.UseLegacyPartitionManagement)
            {
                throw new ArgumentException("Cannot use both TablePartitionManagement and LegacyPartitionManagement. For improved reliability, consider using the TablePartitionManager.");  
            }
            else if (this.settings.UseTablePartitionManagement)
            {
                this.partitionManager = new TablePartitionManager(
                    this,
                    this.azureStorageClient);
            }
            else if (this.settings.UseLegacyPartitionManagement)
            {
                this.partitionManager = new LegacyPartitionManager(
                    this,
                    this.azureStorageClient);
            }
            else
            {
                this.partitionManager = new SafePartitionManager(
                    this,
                    this.azureStorageClient,
                    this.orchestrationSessionManager);
            }

            this.appLeaseManager = new AppLeaseManager(
                this.azureStorageClient,
                this.partitionManager,
                this.settings.TaskHubName.ToLowerInvariant() + "-applease",
                this.settings.TaskHubName.ToLowerInvariant() + "-appleaseinfo",
                this.settings.AppLeaseOptions);
        }

        internal string WorkerId => this.settings.WorkerId;

        internal IEnumerable<ControlQueue> AllControlQueues => this.allControlQueues.Values;

        internal IEnumerable<ControlQueue> OwnedControlQueues => this.orchestrationSessionManager.Queues;

        internal WorkItemQueue WorkItemQueue => this.workItemQueue;

        internal ITrackingStore TrackingStore => this.trackingStore;

        internal static string GetControlQueueName(string taskHub, int partitionIndex)
        {
            return GetQueueName(taskHub, $"control-{partitionIndex:00}");
        }

        internal static string GetWorkItemQueueName(string taskHub)
        {
            return GetQueueName(taskHub, "workitems");
        }

        internal static string GetQueueName(string taskHub, string suffix)
        {
            if (string.IsNullOrEmpty(taskHub))
            {
                throw new ArgumentNullException(nameof(taskHub));
            }

            string queueName = $"{taskHub.ToLowerInvariant()}-{suffix}";

            return queueName;
        }

        internal static BlobPartitionLeaseManager GetBlobLeaseManager(
            AzureStorageClient azureStorageClient,
            string leaseType)
        {
            return new BlobPartitionLeaseManager(
                azureStorageClient,
                leaseContainerName: azureStorageClient.Settings.TaskHubName.ToLowerInvariant() + "-leases",
                leaseType: leaseType);
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

            if (string.IsNullOrEmpty(settings.TaskHubName))
            {
                throw new ArgumentNullException(nameof(settings), $"A {nameof(settings.TaskHubName)} value must be configured in the settings.");
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

        /// <summary>
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew
        {
            get { return this.settings.EventBehaviourForContinueAsNew; }
        }

        // We always leave the dispatcher counts at one unless we can find a customer workload that requires more.
        /// <inheritdoc />
        public int TaskActivityDispatcherCount { get; } = 1;

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount { get; } = 1;

        #region IEntityOrchestrationService

        EntityBackendProperties IEntityOrchestrationService.EntityBackendProperties
           => new EntityBackendProperties()
           {
               EntityMessageReorderWindow = TimeSpan.FromMinutes(this.settings.EntityMessageReorderWindowInMinutes),
               MaxEntityOperationBatchSize = this.settings.MaxEntityOperationBatchSize,
               MaxConcurrentTaskEntityWorkItems = this.settings.MaxConcurrentTaskEntityWorkItems,
               SupportsImplicitEntityDeletion = false, // not supported by this backend
               MaximumSignalDelayTime = TimeSpan.FromDays(6),
               UseSeparateQueueForEntityWorkItems = this.settings.UseSeparateQueueForEntityWorkItems,
           };

        EntityBackendQueries IEntityOrchestrationService.EntityBackendQueries
           => new EntityTrackingStoreQueries(
                this.messageManager,
                this.trackingStore,
                this.EnsureTaskHubAsync,
                ((IEntityOrchestrationService)this).EntityBackendProperties,
                this.SendTaskOrchestrationMessageAsync);

        Task<TaskOrchestrationWorkItem> IEntityOrchestrationService.LockNextOrchestrationWorkItemAsync(
          TimeSpan receiveTimeout,
          CancellationToken cancellationToken)
        {
            if (!this.settings.UseSeparateQueueForEntityWorkItems)
            {
                throw new InvalidOperationException("Internal configuration is inconsistent. Backend is using single queue for orchestration/entity dispatch, but frontend is pulling from individual queues.");
            }
            return this.LockNextTaskOrchestrationWorkItemAsync(false, cancellationToken);
        }

        Task<TaskOrchestrationWorkItem> IEntityOrchestrationService.LockNextEntityWorkItemAsync(
           TimeSpan receiveTimeout,
           CancellationToken cancellationToken)
        {
            if (!this.settings.UseSeparateQueueForEntityWorkItems)
            {
                throw new InvalidOperationException("Internal configuration is inconsistent. Backend is using single queue for orchestration/entity dispatch, but frontend is pulling from individual queues.");
            }
            return this.LockNextTaskOrchestrationWorkItemAsync(entitiesOnly: true, cancellationToken);
        }

        #endregion

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
                this.settings.Logger.GeneralError(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"Failed to create the task hub: {e}");

                // Don't want to cache the failed task
                this.taskHubCreator.Reset();
                throw;
            }
        }

        // Internal logic used by the lazy taskHubCreator
        async Task GetTaskHubCreatorTask()
        {
            TaskHubInfo hubInfo = GetTaskHubInfo(this.settings.TaskHubName, this.settings.PartitionCount);
            await this.appLeaseManager.CreateContainerIfNotExistsAsync();

            await this.partitionManager.CreateLeaseStore();

            var tasks = new List<Task>();

            tasks.Add(this.trackingStore.CreateAsync());

            tasks.Add(this.workItemQueue.CreateIfNotExistsAsync());

            foreach (ControlQueue controlQueue in this.allControlQueues.Values)
            {
                tasks.Add(controlQueue.CreateIfNotExistsAsync());
                tasks.Add(this.partitionManager.CreateLease(controlQueue.Name));
            }

            await Task.WhenAll(tasks.ToArray());
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
                if (this.allControlQueues.TryGetValue(partitionId, out ControlQueue controlQueue))
                {
                    tasks.Add(controlQueue.DeleteIfExistsAsync());
                }
            }

            tasks.Add(this.workItemQueue.DeleteIfExistsAsync());

            if (deleteInstanceStore)
            {
                tasks.Add(DeleteTrackingStore());
            }

            tasks.Add(this.partitionManager.DeleteLeases());

            tasks.Add(this.appLeaseManager.DeleteContainerAsync());

            tasks.Add(this.messageManager.DeleteContainerAsync());

            await Task.WhenAll(tasks.ToArray());
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
            await this.CreateIfNotExistsAsync();
            await this.trackingStore.StartAsync();

            // Disable nagling to improve storage access latency:
            // https://blogs.msdn.microsoft.com/windowsazurestorage/2010/06/25/nagles-algorithm-is-not-friendly-towards-small-requests/
            // Ad-hoc testing has shown very nice improvements (20%-50% drop in queue message age for simple scenarios).
            ServicePointManager.FindServicePoint(this.workItemQueue.Uri).UseNagleAlgorithm = false;

            this.shutdownSource?.Dispose();
            this.shutdownSource = new CancellationTokenSource();
            this.statsLoop = Task.Run(() => this.ReportStatsLoop(this.shutdownSource.Token));

            await this.appLeaseManager.StartAsync();

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
            await this.appLeaseManager.StopAsync();
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
                    this.settings.Logger.GeneralError(
                        this.azureStorageClient.QueueAccountName,
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
            this.orchestrationSessionManager.GetStats(
                out int pendingOrchestratorInstances,
                out int pendingOrchestrationMessages,
                out int activeOrchestrationSessions);

            this.settings.Logger.OrchestrationServiceStats(
                this.azureStorageClient.QueueAccountName,
                this.settings.TaskHubName,
                storageRequests,
                messagesSent,
                messagesRead,
                messagesUpdated,
                tableEntitiesWritten,
                tableEntitiesRead,
                pendingOrchestratorInstances,
                pendingOrchestrationMessages,
                activeOrchestrationSessions,
                this.stats.ActiveActivityExecutions.Value);
        }

        internal async Task OnIntentLeaseAquiredAsync(BlobPartitionLease lease)
        {
            var controlQueue = new ControlQueue(this.azureStorageClient, lease.PartitionId, this.messageManager);
            await controlQueue.CreateIfNotExistsAsync();
            this.orchestrationSessionManager.ResumeListeningIfOwnQueue(lease.PartitionId, controlQueue, this.shutdownSource.Token);
        }

        internal Task OnIntentLeaseReleasedAsync(BlobPartitionLease lease, CloseReason reason)
        {
            // Mark the queue as released so it will stop grabbing new messages.
            this.orchestrationSessionManager.ReleaseQueue(lease.PartitionId, reason, "Intent LeaseCollectionBalancer");
            return Utils.CompletedTask;
        }

        internal async Task OnOwnershipLeaseAquiredAsync(BlobPartitionLease lease)
        {
            var controlQueue = new ControlQueue(this.azureStorageClient, lease.PartitionId, this.messageManager);
            await controlQueue.CreateIfNotExistsAsync();
            this.orchestrationSessionManager.AddQueue(lease.PartitionId, controlQueue, this.shutdownSource.Token);

            this.allControlQueues[lease.PartitionId] = controlQueue;
        }

        internal void DropLostControlQueue(TablePartitionLease partition)
        {
            // If lease is lost but we're still dequeuing messages, remove the queue
            if (this.allControlQueues.TryGetValue(partition.RowKey, out ControlQueue controlQueue) &&
                this.OwnedControlQueues.Contains(controlQueue) &&
                partition.CurrentOwner != this.settings.WorkerId)
            {
                this.orchestrationSessionManager.RemoveQueue(partition.RowKey, CloseReason.LeaseLost, nameof(DropLostControlQueue));
            }
        }

        internal Task OnOwnershipLeaseReleasedAsync(BlobPartitionLease lease, CloseReason reason)
        {
            this.orchestrationSessionManager.RemoveQueue(lease.PartitionId, reason, "Ownership LeaseCollectionBalancer");
            return Utils.CompletedTask;
        }

        internal async Task OnTableLeaseAcquiredAsync(TablePartitionLease lease)
        {
            // Check if the worker is already listening to the control queue.
            if (this.allControlQueues.TryGetValue(lease.RowKey, out ControlQueue queue) &&
                this.OwnedControlQueues.Contains(queue))
            {
                return;
            }

            // If the worker hasn't listened to the queue yet, then add it.
            var controlQueue = new ControlQueue(this.azureStorageClient, lease.RowKey, this.messageManager);
            await controlQueue.CreateIfNotExistsAsync();
            this.orchestrationSessionManager.AddQueue(lease.RowKey, controlQueue, this.shutdownSource.Token);

            this.allControlQueues[lease.RowKey] = controlQueue;
        }

        internal async Task DrainTablePartitionAsync(TablePartitionLease lease, CloseReason reason)
        {
            using var cts = new CancellationTokenSource(delay: TimeSpan.FromSeconds(60));
            await this.orchestrationSessionManager.DrainAsync(lease.RowKey, reason, cts.Token, nameof(DrainTablePartitionAsync));
        }

        // Used for testing
        internal IAsyncEnumerable<BlobPartitionLease> ListBlobLeasesAsync(CancellationToken cancellationToken = default)
        {
            return this.partitionManager.GetOwnershipBlobLeasesAsync(cancellationToken);
        }

        // Used for table partition manager testing
        internal IAsyncEnumerable<TablePartitionLease> ListTableLeasesAsync(CancellationToken cancellationToken = default)
        {
            return ((TablePartitionManager)this.partitionManager).GetTableLeasesAsync();
        }

        // Used for table partition manager testing.
        internal void SimulateUnhealthyWorker(CancellationToken testToken)
        {
            ((TablePartitionManager)this.partitionManager).SimulateUnhealthyWorker(testToken);
        }

        // Used for table partition manager testing
        internal void KillPartitionManagerLoop()
        {
            ((TablePartitionManager)this.partitionManager).KillLoop();
        }

        internal static async Task<Queue[]> GetControlQueuesAsync(
            AzureStorageClient azureStorageClient,
            int defaultPartitionCount)
        {
            if (azureStorageClient == null)
            {
                throw new ArgumentNullException(nameof(azureStorageClient));
            }

            string taskHub = azureStorageClient.Settings.TaskHubName;

            // Need to check for leases in Azure Table Storage. Scale Controller calls into this method.
            int partitionCount;
            Table partitionTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.PartitionTableName);
            
            // Check if table partition manager is used. If so, get partition count from table.
            // Else, get the partition count from the blobs.
            if (await partitionTable.ExistsAsync())
            {
                TableQueryResponse<TablePartitionLease> result = partitionTable.ExecuteQueryAsync<TablePartitionLease>();
                partitionCount = await result.CountAsync();
            }
            else
            {
                BlobPartitionLeaseManager inactiveLeaseManager = GetBlobLeaseManager(azureStorageClient, "inactive");

                TaskHubInfo hubInfo = await inactiveLeaseManager.GetOrCreateTaskHubInfoAsync(
                    GetTaskHubInfo(taskHub, defaultPartitionCount),
                    checkIfStale: false);
                partitionCount = hubInfo.PartitionCount;
            };

            var controlQueues = new Queue[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                controlQueues[i] = azureStorageClient.GetQueueReference(GetControlQueueName(taskHub, i));
            }

            return controlQueues;
        }

        internal static Queue GetWorkItemQueue(AzureStorageClient azureStorageClient)
        {
            string queueName = GetWorkItemQueueName(azureStorageClient.Settings.TaskHubName);
            return azureStorageClient.GetQueueReference(queueName);
        }

        static TaskHubInfo GetTaskHubInfo(string taskHub, int partitionCount)
        {
            return new TaskHubInfo(taskHub, DateTime.UtcNow, partitionCount);
        }

        #endregion

        #region Orchestration Work Item Methods
        /// <inheritdoc />
        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            if (this.settings.UseSeparateQueueForEntityWorkItems)
            {
                throw new InvalidOperationException("Internal configuration is inconsistent. Backend is using separate queues for orchestration/entity dispatch, but frontend is pulling from single queue.");
            }
            return LockNextTaskOrchestrationWorkItemAsync(entitiesOnly: false, cancellationToken);
        }

        async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(bool entitiesOnly, CancellationToken cancellationToken)
        {
            Guid traceActivityId = StartNewLogicalTraceScope(useExisting: true);

            await this.EnsureTaskHubAsync();

            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.shutdownSource.Token))
            {
                OrchestrationSession session = null;
                TaskOrchestrationWorkItem orchestrationWorkItem = null;

                try
                {
                    // This call will block until the next session is ready
                    session = await this.orchestrationSessionManager.GetNextSessionAsync(entitiesOnly, linkedCts.Token);
                    if (session == null)
                    {
                        return null;
                    }

                    // Make sure we still own the partition. If not, abandon the session.
                    if (session.ControlQueue.IsReleased)
                    {
                        await this.AbandonAndReleaseSessionAsync(session);
                        return null;
                    }

                    session.StartNewLogicalTraceScope();

                    List<MessageData> outOfOrderMessages = null;

                    foreach (MessageData message in session.CurrentMessageBatch)
                    {
                        if (session.IsOutOfOrderMessage(message))
                        {
                            if (outOfOrderMessages == null)
                            {
                                outOfOrderMessages = new List<MessageData>();
                            }

                            // This can happen if a lease change occurs and a new node receives a message for an 
                            // orchestration that has not yet checkpointed its history. We abandon such messages
                            // so that they can be reprocessed after the history checkpoint has completed.
                            this.settings.Logger.ReceivedOutOfOrderMessage(
                                this.azureStorageClient.QueueAccountName,
                                this.settings.TaskHubName,
                                session.Instance.InstanceId,
                                session.Instance.ExecutionId,
                                session.ControlQueue.Name,
                                message.TaskMessage.Event.EventType.ToString(),
                                Utils.GetTaskEventId(message.TaskMessage.Event),
                                message.OriginalQueueMessage.MessageId,
                                message.Episode.GetValueOrDefault(-1),
                                session.LastCheckpointTime);
                            outOfOrderMessages.Add(message);
                        }
                        else
                        {
                            session.TraceProcessingMessage(
                                message,
                                isExtendedSession: false,
                                partitionId: session.ControlQueue.Name);
                        }
                    }

                    if (outOfOrderMessages?.Count > 0)
                    {
                        // This will also remove the messages from the current batch.
                        await this.AbandonMessagesAsync(session, outOfOrderMessages);
                    }

                    if (session.CurrentMessageBatch.Count == 0)
                    {
                        // All messages were removed. Release the work item.
                        await this.AbandonAndReleaseSessionAsync(session);
                        return null;
                    }

                    // Create or restore Correlation TraceContext

                    TraceContextBase currentRequestTraceContext = null;
                    CorrelationTraceClient.Propagate(
                        () =>
                        {
                            var isReplaying = session.RuntimeState.ExecutionStartedEvent?.IsPlayed ?? false;
                            TraceContextBase parentTraceContext = GetParentTraceContext(session);
                            currentRequestTraceContext = GetRequestTraceContext(isReplaying, parentTraceContext);
                        });

                    orchestrationWorkItem = new TaskOrchestrationWorkItem
                    {
                        InstanceId = session.Instance.InstanceId,
                        LockedUntilUtc = session.CurrentMessageBatch.Min(msg => msg.OriginalQueueMessage.NextVisibleOn.Value.UtcDateTime),
                        NewMessages = session.CurrentMessageBatch.Select(m => m.TaskMessage).ToList(),
                        OrchestrationRuntimeState = session.RuntimeState,
                        Session = this.settings.ExtendedSessionsEnabled ? session : null,
                        TraceContext = currentRequestTraceContext,
                    };

                    if (!this.IsExecutableInstance(session.RuntimeState, orchestrationWorkItem.NewMessages, settings.AllowReplayingTerminalInstances, out string warningMessage))
                    {
                        // If all messages belong to the same execution ID, then all of them need to be discarded.
                        // However, it's also possible to have messages for *any* execution ID batched together with messages
                        // to a *specific* (non-executable) execution ID. Those messages should *not* be discarded since
                        // they might be consumable by another orchestration with the same instance id but different execution ID.
                        var messagesToDiscard = new List<MessageData>();
                        var messagesToAbandon = new List<MessageData>();
                        foreach (MessageData msg in session.CurrentMessageBatch)
                        {
                            if (msg.TaskMessage.OrchestrationInstance.ExecutionId == session.Instance.ExecutionId)
                            {
                                messagesToDiscard.Add(msg);
                            }
                            else
                            {
                                messagesToAbandon.Add(msg);
                            }
                        }

                        // If no messages have a matching execution ID, then delete all of them. This means all the
                        // messages are external (external events, termination, etc.) and were sent to an instance that
                        // doesn't exist or is no longer in a running state.
                        if (messagesToDiscard.Count == 0)
                        {
                            messagesToDiscard.AddRange(messagesToAbandon);
                            messagesToAbandon.Clear();
                        }

                        // Add all abandoned messages to the deferred list. These messages will not be deleted right now.
                        // If they can be matched with another orchestration, then great. Otherwise they will be deleted
                        // the next time they are picked up.
                        messagesToAbandon.ForEach(session.DeferMessage);

                        var eventListBuilder = new StringBuilder(orchestrationWorkItem.NewMessages.Count * 40);
                        foreach (MessageData msg in messagesToDiscard)
                        {
                            eventListBuilder.Append(msg.TaskMessage.Event.EventType.ToString()).Append(',');
                        }

                        this.settings.Logger.DiscardingWorkItem(
                            this.azureStorageClient.QueueAccountName,
                            this.settings.TaskHubName,
                            session.Instance.InstanceId,
                            session.Instance.ExecutionId,
                            orchestrationWorkItem.NewMessages.Count,
                            session.RuntimeState.Events.Count,
                            eventListBuilder.ToString(0, eventListBuilder.Length - 1) /* remove trailing comma */,
                            warningMessage);

                        // The instance has already completed or never existed. Delete this message batch.
                        await this.DeleteMessageBatchAsync(session, messagesToDiscard);
                        await this.ReleaseTaskOrchestrationWorkItemAsync(orchestrationWorkItem);
                        return null;
                    }

                    return orchestrationWorkItem;
                }
                catch (OperationCanceledException)
                {
                    if (session != null)
                    {
                        // host is shutting down - release any queued messages
                        await this.AbandonAndReleaseSessionAsync(session);
                    }

                    return null;
                }
                catch (Exception e)
                {
                    this.settings.Logger.OrchestrationProcessingFailure(
                        this.azureStorageClient.QueueAccountName,
                        this.settings.TaskHubName,
                        session?.Instance.InstanceId ?? string.Empty,
                        session?.Instance.ExecutionId ?? string.Empty,
                        e.ToString());

                    if (orchestrationWorkItem != null)
                    {
                        // The work-item needs to be released so that it can be retried later.
                        await this.ReleaseTaskOrchestrationWorkItemAsync(orchestrationWorkItem);
                    }

                    throw;
                }
            }
        }

        TraceContextBase GetParentTraceContext(OrchestrationSession session)
        {
            var messages = session.CurrentMessageBatch;
            TraceContextBase parentTraceContext = null;
            bool foundEventRaised = false;
            foreach(var message in messages)
            {
                if (message.SerializableTraceContext != null)
                {
                    var traceContext = TraceContextBase.Restore(message.SerializableTraceContext);
                    switch(message.TaskMessage.Event)
                    {
                        // Dependency Execution finished.
                        case TaskCompletedEvent tc:
                        case TaskFailedEvent tf:
                        case SubOrchestrationInstanceCompletedEvent sc:
                        case SubOrchestrationInstanceFailedEvent sf:
                            if (traceContext.OrchestrationTraceContexts.Count != 0)
                            {
                                var orchestrationDependencyTraceContext = traceContext.OrchestrationTraceContexts.Pop();
                                CorrelationTraceClient.TrackDepencencyTelemetry(orchestrationDependencyTraceContext);
                            }

                            parentTraceContext = traceContext;
                            break;
                        // Retry and Timer that includes Dependency Telemetry needs to remove
                        case TimerFiredEvent tf:
                            if (traceContext.OrchestrationTraceContexts.Count != 0)
                                traceContext.OrchestrationTraceContexts.Pop();

                            parentTraceContext = traceContext;
                            break;
                        default:
                            // When internal error happens, multiple message could come, however, it should not be prioritized.
                            if (parentTraceContext == null || 
                                parentTraceContext.OrchestrationTraceContexts.Count < traceContext.OrchestrationTraceContexts.Count)
                            {
                                parentTraceContext = traceContext;
                            }

                            break;
                    }                   
                } else
                {

                    // In this case, we set the parentTraceContext later in this method
                    if (message.TaskMessage.Event is EventRaisedEvent)
                    {
                        foundEventRaised = true;
                    }
                }            
            }

            // When EventRaisedEvent is present, it will not, out of the box, share the same operation
            // identifiers as the rest of the trace events. Thus, we need to explicitely group it with the
            // rest of events by using the context string of the ExecutionStartedEvent.
            if (parentTraceContext is null && foundEventRaised)
            {
                // Restore the parent trace context from the correlation state of the execution start event
                string traceContextString = session.RuntimeState.ExecutionStartedEvent?.Correlation;
                parentTraceContext = TraceContextBase.Restore(traceContextString);
            }
            return parentTraceContext ?? TraceContextFactory.Empty;
        }

        static bool IsActivityOrOrchestrationFailedOrCompleted(IList<MessageData> messages)
        {
            foreach(var message in messages)
            {
                if (message.TaskMessage.Event is DurableTask.Core.History.SubOrchestrationInstanceCompletedEvent ||
                    message.TaskMessage.Event is DurableTask.Core.History.SubOrchestrationInstanceFailedEvent ||
                    message.TaskMessage.Event is DurableTask.Core.History.TaskCompletedEvent ||
                    message.TaskMessage.Event is DurableTask.Core.History.TaskFailedEvent  ||
                    message.TaskMessage.Event is DurableTask.Core.History.TimerFiredEvent)
                {
                    return true;
                }
            }

            return false;
        }

        static TraceContextBase GetRequestTraceContext(bool isReplaying, TraceContextBase parentTraceContext)
        {
            TraceContextBase currentRequestTraceContext = TraceContextFactory.Empty;

            if (!isReplaying)
            {
                var name = $"{TraceConstants.Orchestrator}";
                currentRequestTraceContext = TraceContextFactory.Create(name);
                currentRequestTraceContext.SetParentAndStart(parentTraceContext);
                currentRequestTraceContext.TelemetryType = TelemetryType.Request;
                currentRequestTraceContext.OrchestrationTraceContexts.Push(currentRequestTraceContext);
            }
            else
            {
                // TODO Chris said that there is not case in this root. Double check or write test to prove it.
                bool noCorrelation = parentTraceContext.OrchestrationTraceContexts.Count == 0;
                if (noCorrelation)
                {
                    // Terminate, external events, etc. are examples of messages that not contain any trace context. 
                    // In those cases, we just return an empty trace context and continue on. 
                    return TraceContextFactory.Empty;
                }

                currentRequestTraceContext = parentTraceContext.GetCurrentOrchestrationRequestTraceContext();
                currentRequestTraceContext.OrchestrationTraceContexts = parentTraceContext.OrchestrationTraceContexts.Clone();
                currentRequestTraceContext.IsReplay = true;
                return currentRequestTraceContext;
            }

            return currentRequestTraceContext;
        }

        internal static Guid StartNewLogicalTraceScope(bool useExisting)
        {
            // Starting in DurableTask.Core v2.4.0, a new trace activity will already be
            // started and we don't need to start one ourselves.
            // TODO: When distributed correlation is merged, use that instead.
            Guid traceActivityId;
            if (useExisting && EventSource.CurrentThreadActivityId != Guid.Empty)
            {
                traceActivityId = EventSource.CurrentThreadActivityId;
            }
            else
            {
                // No ambient trace activity ID was found or doesn't apply - create a new one
                traceActivityId = Guid.NewGuid();
            }

            AnalyticsEventSource.SetLogicalTraceActivityId(traceActivityId);
            return traceActivityId;
        }

        internal static void TraceMessageReceived(AzureStorageOrchestrationServiceSettings settings, MessageData data, string storageAccountName)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            TaskMessage taskMessage = data.TaskMessage;
            QueueMessage queueMessage = data.OriginalQueueMessage;

            settings.Logger.ReceivedMessage(
                data.ActivityId,
                storageAccountName,
                settings.TaskHubName,
                taskMessage.Event.EventType.ToString(),
                Utils.GetTaskEventId(taskMessage.Event),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.MessageId,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertedOn.Value).TotalMilliseconds),
                queueMessage.DequeueCount,
                queueMessage.NextVisibleOn.GetValueOrDefault().DateTime.ToString("o"),
                data.TotalMessageSizeBytes,
                data.QueueName /* PartitionId */,
                data.SequenceNumber,
                queueMessage.PopReceipt,
                data.Episode.GetValueOrDefault(-1));
        }

        bool IsExecutableInstance(OrchestrationRuntimeState runtimeState, IList<TaskMessage> newMessages, bool allowReplayingTerminalInstances, out string message)
        {
            if (runtimeState.ExecutionStartedEvent == null && !newMessages.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                var instanceId = newMessages[0].OrchestrationInstance.InstanceId;

                if (DurableTask.Core.Common.Entities.AutoStart(instanceId, newMessages))
                {
                    message = null;
                    return true;
                }
                else
                {
                    // A non-zero event count usually happens when an instance's history is overwritten by a
                    // new instance or by a ContinueAsNew. When history is overwritten by new instances, we
                    // overwrite the old history with new history (with a new execution ID), but this is done
                    // gradually as we build up the new history over time. If we haven't yet overwritten *all*
                    // the old history and we receive a message from the old instance (this happens frequently
                    // with canceled durable timer messages) we'll end up loading just the history that hasn't
                    // been fully overwritten. We know it's invalid because it's missing the ExecutionStartedEvent.
                    message = runtimeState.Events.Count == 0 ? "No such instance" : "Invalid history (may have been overwritten by a newer instance)";
                    return false;
                }
            }

            if (runtimeState.ExecutionStartedEvent != null &&
                !allowReplayingTerminalInstances &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Pending &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Suspended)
            {
                message = $"Instance is {runtimeState.OrchestrationStatus}";
                return false;
            }

            message = null;
            return true;
        }

        async Task AbandonAndReleaseSessionAsync(OrchestrationSession session)
        {
            try
            {
                await this.AbandonSessionAsync(session);
            }
            finally
            {
                await this.ReleaseSessionAsync(session.Instance.InstanceId);
            }
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
            // for backwards compatibility, we transform timer timestamps to UTC prior to persisting in Azure Storage.
            // see: https://github.com/Azure/durabletask/pull/1138
            foreach (var orchestratorMessage in orchestratorMessages)
            {
                Utils.ConvertDateTimeInHistoryEventsToUTC(orchestratorMessage.Event);
            }
            foreach (var timerMessage in timerMessages)
            {
                Utils.ConvertDateTimeInHistoryEventsToUTC(timerMessage.Event);
            }   

            OrchestrationSession session;
            if (!this.orchestrationSessionManager.TryGetExistingSession(workItem.InstanceId, out session))
            {
                this.settings.Logger.AssertFailure(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(CompleteTaskOrchestrationWorkItemAsync)}: Session for instance {workItem.InstanceId} was not found!");
                return;
            }

            session.StartNewLogicalTraceScope();
            OrchestrationRuntimeState runtimeState = newOrchestrationRuntimeState ?? workItem.OrchestrationRuntimeState;

            // Only commit side-effects if the orchestration runtime state is valid (i.e. not corrupted)
            if (!runtimeState.IsValid)
            {
                this.settings.Logger.GeneralWarning(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(CompleteTaskOrchestrationWorkItemAsync)}: Discarding execution results because the orchestration state is invalid.",
                    instanceId: workItem.InstanceId);
                await this.DeleteMessageBatchAsync(session, session.CurrentMessageBatch);
                return;
            }

            string instanceId = workItem.InstanceId;
            string executionId = runtimeState.OrchestrationInstance?.ExecutionId;
            if (executionId == null)
            {
                this.settings.Logger.GeneralWarning(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(CompleteTaskOrchestrationWorkItemAsync)}: Could not find execution id.",
                    instanceId: instanceId);
            }

            // Correlation
            CorrelationTraceClient.Propagate(() =>
                {
                    // In case of Extended Session, Emit the Dependency Telemetry. 
                    if (workItem.IsExtendedSession)
                    {
                        this.TrackExtendedSessionDependencyTelemetry(session);
                    }
                });

            TraceContextBase currentTraceContextBaseOnComplete = null;
            CorrelationTraceClient.Propagate(() =>
                currentTraceContextBaseOnComplete = CreateOrRestoreRequestTraceContextWithDependencyTrackingSettings(
                    workItem.TraceContext,
                    orchestrationState,
                    DependencyTelemetryStarted(
                        outboundMessages,
                        orchestratorMessages,
                        timerMessages,
                        continuedAsNewMessage,
                        orchestrationState)));

            // First, add new messages into the queue. If a failure happens after this, duplicate messages will
            // be written after the retry, but the results of those messages are expected to be de-dup'd later.
            // This provider needs to ensure that response messages are not processed until the history a few
            // lines down has been successfully committed.

            await this.CommitOutboundQueueMessages(
                session,
                outboundMessages,
                orchestratorMessages,
                timerMessages,
                continuedAsNewMessage);

            // correlation
            CorrelationTraceClient.Propagate(() =>
                this.TrackOrchestrationRequestTelemetry(
                    currentTraceContextBaseOnComplete,
                    orchestrationState,
                    $"{TraceConstants.Orchestrator} {Utils.GetTargetClassName(session.RuntimeState.ExecutionStartedEvent?.Name)}"));

            // Next, commit the orchestration history updates. This is the actual "checkpoint". Failures after this
            // will result in a duplicate replay of the orchestration with no side-effects.
            try
            {
                session.ETag = await this.trackingStore.UpdateStateAsync(runtimeState, workItem.OrchestrationRuntimeState, instanceId, executionId, session.ETag, session.TrackingStoreContext);
                // update the runtime state and execution id stored in the session
                session.UpdateRuntimeState(runtimeState);

                // if we deferred some messages, and the execution id of this instance has changed, redeliver them
                if (session.DeferredMessages.Count > 0
                    && executionId != workItem.OrchestrationRuntimeState.OrchestrationInstance?.ExecutionId)
                {
                    var messages = session.DeferredMessages.ToList();
                    session.DeferredMessages.Clear();
                    this.orchestrationSessionManager.AddMessageToPendingOrchestration(session.ControlQueue, messages, session.TraceActivityId, CancellationToken.None);
                }
            }
            // Handle the case where the 'ETag' has changed, which implies another worker has taken over this work item while
            // we were trying to process it. We detect this in 2 cases:
            // Common case: the resulting code is 'PreconditionFailed', which means our ETag no longer matches the one stored.
            // Edge case: the resulting code is 'Conflict'. This can occur if this was the first orchestration work item.
            // The 'Conflict' represents that we attempted to insert a new orchestration history when one already exists.
            catch (DurableTaskStorageException dtse) when (dtse.HttpStatusCode == (int)HttpStatusCode.Conflict || dtse.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                // Precondition failure is expected to be handled internally and logged as a warning.
                // The orchestration dispatcher will handle this exception by abandoning the work item
                throw new SessionAbortedException("Aborting execution due to conflicting completion of the work item by another worker.", dtse);
            }
            catch (Exception e)
            {
                // TODO: https://github.com/Azure/azure-functions-durable-extension/issues/332
                //       It's possible that history updates may have been partially committed at this point.
                //       If so, what are the implications of this as far as DurableTask.Core are concerned?
                this.settings.Logger.OrchestrationProcessingFailure(
                    this.azureStorageClient.TableAccountName,
                    this.settings.TaskHubName,
                    instanceId,
                    executionId,
                    e.ToString());

                throw;
            }

            // Finally, delete the messages which triggered this orchestration execution. This is the final commit.
            await this.DeleteMessageBatchAsync(session, session.CurrentMessageBatch);
        }

        static bool DependencyTelemetryStarted(
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            return 
                (outboundMessages.Count != 0 || orchestratorMessages.Count != 0 || timerMessages.Count != 0) &&
                (orchestrationState.OrchestrationStatus != OrchestrationStatus.Completed) &&
                (orchestrationState.OrchestrationStatus != OrchestrationStatus.Failed);
        }

        void TrackExtendedSessionDependencyTelemetry(OrchestrationSession session)
        {
            List<MessageData> messages = session.CurrentMessageBatch;
            foreach (MessageData message in messages)
            {
                if (message.SerializableTraceContext != null)
                {
                    var traceContext = TraceContextBase.Restore(message.SerializableTraceContext);
                    switch (message.TaskMessage.Event)
                    {
                        // Dependency Execution finished.
                        case TaskCompletedEvent tc:
                        case TaskFailedEvent tf:
                        case SubOrchestrationInstanceCompletedEvent sc:
                        case SubOrchestrationInstanceFailedEvent sf:
                            if (traceContext.OrchestrationTraceContexts.Count != 0)
                            {
                                TraceContextBase orchestrationDependencyTraceContext = traceContext.OrchestrationTraceContexts.Pop();
                                CorrelationTraceClient.TrackDepencencyTelemetry(orchestrationDependencyTraceContext);
                            }

                            break;
                        default:
                            // When internal error happens, multiple message could come, however, it should not be prioritized.
                            break;
                    }
                }
            }
        }

        static TraceContextBase CreateOrRestoreRequestTraceContextWithDependencyTrackingSettings(
            TraceContextBase traceContext,
            OrchestrationState orchestrationState,
            bool dependencyTelemetryStarted)
        {
            TraceContextBase currentTraceContextBaseOnComplete = null;
            
            if (dependencyTelemetryStarted)
            {
                // DependencyTelemetry will be included on an outbound queue
                // See TaskHubQueue
                CorrelationTraceContext.GenerateDependencyTracking = true;
                CorrelationTraceContext.Current = traceContext;
            }
            else
            {
                switch(orchestrationState.OrchestrationStatus)
                {
                    case OrchestrationStatus.Completed:
                    case OrchestrationStatus.Failed:
                        // Completion of the orchestration.
                        TraceContextBase parentTraceContext = traceContext;
                        if (parentTraceContext.OrchestrationTraceContexts.Count >= 1)
                        {
                            currentTraceContextBaseOnComplete = parentTraceContext.OrchestrationTraceContexts.Pop();
                            CorrelationTraceContext.Current = parentTraceContext;
                        }
                        else
                        {
                            currentTraceContextBaseOnComplete = TraceContextFactory.Empty;
                        }

                        break;
                    default:
                        currentTraceContextBaseOnComplete = TraceContextFactory.Empty;
                        break;
                }
            }

            return currentTraceContextBaseOnComplete;
        }

        void TrackOrchestrationRequestTelemetry(
            TraceContextBase traceContext,
            OrchestrationState orchestrationState,
            string operationName)
        {
            switch (orchestrationState.OrchestrationStatus)
            {
                case OrchestrationStatus.Completed:
                case OrchestrationStatus.Failed:
                    if (traceContext != null)
                    {
                        traceContext.OperationName = operationName;
                        CorrelationTraceClient.TrackRequestTelemetry(traceContext);
                    }

                    break;
                default:
                    break;
            }
        }

        async Task CommitOutboundQueueMessages(
            OrchestrationSession session,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage)
        {
            int messageCount =
                (outboundMessages?.Count ?? 0) +
                (orchestratorMessages?.Count ?? 0) +
                (timerMessages?.Count ?? 0) +
                (continuedAsNewMessage != null ? 1 : 0);

            // Second persistence step is to commit outgoing messages to their respective queues. If there is
            // any failures here, then the messages may get written again later.
            var enqueueOperations = new List<TaskHubQueueMessage>(messageCount);
            if (orchestratorMessages?.Count > 0)
            {
                foreach (TaskMessage taskMessage in orchestratorMessages)
                {
                    string targetInstanceId = taskMessage.OrchestrationInstance.InstanceId;
                    ControlQueue targetControlQueue = await this.GetControlQueueAsync(targetInstanceId);

                    enqueueOperations.Add(new TaskHubQueueMessage(targetControlQueue, taskMessage));
                }
            }

            if (timerMessages?.Count > 0)
            {
                foreach (TaskMessage taskMessage in timerMessages)
                {
                    enqueueOperations.Add(new TaskHubQueueMessage(session.ControlQueue, taskMessage));
                }
            }

            if (continuedAsNewMessage != null)
            {
                enqueueOperations.Add(new TaskHubQueueMessage(session.ControlQueue, continuedAsNewMessage));
            }

            if (outboundMessages?.Count > 0)
            {
                foreach (TaskMessage taskMessage in outboundMessages)
                {
                    enqueueOperations.Add(new TaskHubQueueMessage(this.workItemQueue, taskMessage));
                }
            }

            await enqueueOperations.ParallelForEachAsync(
                this.settings.MaxStorageOperationConcurrency,
                op => op.Queue.AddMessageAsync(op.Message, session));
        }

        async Task DeleteMessageBatchAsync(OrchestrationSession session, IList<MessageData> messagesToDelete)
        {
            await messagesToDelete.ParallelForEachAsync(
                this.settings.MaxStorageOperationConcurrency,
                message => session.ControlQueue.DeleteMessageAsync(message, session));
        }

        // REVIEW: There doesn't seem to be any code which calls this method.
        //         https://github.com/Azure/durabletask/issues/112
        /// <inheritdoc />
        public async Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationSession session;
            if (!this.orchestrationSessionManager.TryGetExistingSession(workItem.InstanceId, out session))
            {
                this.settings.Logger.AssertFailure(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(RenewTaskOrchestrationWorkItemLockAsync)}: Session for instance {workItem.InstanceId} was not found!");
                return;
            }

            session.StartNewLogicalTraceScope();
            string instanceId = workItem.InstanceId;
            ControlQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            await session.CurrentMessageBatch.ParallelForEachAsync(
                this.settings.MaxStorageOperationConcurrency,
                message => controlQueue.RenewMessageAsync(message, session));

            workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.ControlQueueVisibilityTimeout);
        }

        /// <inheritdoc />
        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            OrchestrationSession session;
            if (!this.orchestrationSessionManager.TryGetExistingSession(workItem.InstanceId, out session))
            {
                this.settings.Logger.AssertFailure(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"{nameof(AbandonTaskOrchestrationWorkItemAsync)}: Session for instance {workItem.InstanceId} was not found!");
                return Utils.CompletedTask;
            }

            return this.AbandonSessionAsync(session);
        }

        Task AbandonSessionAsync(OrchestrationSession session)
        {
            session.StartNewLogicalTraceScope();
            return this.AbandonMessagesAsync(session, session.CurrentMessageBatch.ToList());
        }

        async Task AbandonMessagesAsync(OrchestrationSession session, IList<MessageData> messages)
        {
            await messages.ParallelForEachAsync(
                this.settings.MaxStorageOperationConcurrency,
                message => session.ControlQueue.AbandonMessageAsync(message, session));

            // Remove the messages from the current batch. The remaining messages
            // may still be able to be processed
            foreach (MessageData message in messages)
            {
                session.CurrentMessageBatch.Remove(message);
            }
        }

        // Called after an orchestration completes an execution episode and after all messages have been enqueued.
        // Also called after an orchestration work item is abandoned.
        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return this.ReleaseSessionAsync(workItem.InstanceId);
        }

        async Task ReleaseSessionAsync(string instanceId)
        {
            if (this.orchestrationSessionManager.TryReleaseSession(
                instanceId,
                this.shutdownSource.Token,
                out OrchestrationSession session))
            {
                // Some messages may need to be discarded
                await session.DiscardedMessages.ParallelForEachAsync(
                    this.settings.MaxStorageOperationConcurrency,
                    message => session.ControlQueue.DeleteMessageAsync(message, session));
            }
        }
        #endregion

        #region Task Activity Methods
        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            await this.EnsureTaskHubAsync();

            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.shutdownSource.Token))
            {
                MessageData message = await this.workItemQueue.GetMessageAsync(linkedCts.Token);

                if (message == null)
                {
                    // shutting down
                    return null;
                }


                Guid traceActivityId = Guid.NewGuid();
                var session = new ActivitySession(this.settings, this.azureStorageClient.QueueAccountName, message, traceActivityId);
                session.StartNewLogicalTraceScope();

                // correlation 
                TraceContextBase requestTraceContext = null;
                CorrelationTraceClient.Propagate(
                    () =>
                    {
                        string name = $"{TraceConstants.Activity} {Utils.GetTargetClassName(((TaskScheduledEvent)session.MessageData.TaskMessage.Event)?.Name)}";
                        requestTraceContext = TraceContextFactory.Create(name);

                        TraceContextBase parentTraceContextBase = TraceContextBase.Restore(session.MessageData.SerializableTraceContext);
                        requestTraceContext.SetParentAndStart(parentTraceContextBase);
                    });

                TraceMessageReceived(this.settings, session.MessageData, this.azureStorageClient.QueueAccountName);
                session.TraceProcessingMessage(message, isExtendedSession: false, this.workItemQueue.Name);

                if (!this.activeActivitySessions.TryAdd(message.Id, session))
                {
                    // This means we're already processing this message. This is never expected since the message
                    // should be kept invisible via background calls to RenewTaskActivityWorkItemLockAsync.
                    this.settings.Logger.AssertFailure(
                        this.azureStorageClient.QueueAccountName,
                        this.settings.TaskHubName,
                        $"Work item queue message with ID = {message.Id} is being processed multiple times concurrently.");
                    return null;
                }

                this.stats.ActiveActivityExecutions.Increment();

                return new TaskActivityWorkItem
                {
                    Id = message.Id,
                    TaskMessage = session.MessageData.TaskMessage,
                    LockedUntilUtc = message.OriginalQueueMessage.NextVisibleOn.Value.UtcDateTime,

                    TraceContextBase = requestTraceContext
                };
            }
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseTaskMessage)
        {
            ActivitySession session;
            if (!this.activeActivitySessions.TryGetValue(workItem.Id, out session))
            {
                // The context does not exist - possibly because it was already removed.
                this.settings.Logger.AssertFailure(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            session.StartNewLogicalTraceScope();

            // Correlation 
            CorrelationTraceClient.Propagate(() => CorrelationTraceContext.Current = workItem.TraceContextBase);

            string instanceId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
            ControlQueue controlQueue = await this.GetControlQueueAsync(instanceId);

            // First, send a response message back. If this fails, we'll try again later since we haven't deleted the
            // work item message yet (that happens next).
            await controlQueue.AddMessageAsync(responseTaskMessage, session);

            // RequestTelemetryTracking
            CorrelationTraceClient.Propagate(
                () =>
                {
                    CorrelationTraceClient.TrackRequestTelemetry(workItem.TraceContextBase);
                });

            // Next, delete the work item queue message. This must come after enqueuing the response message.
            await this.workItemQueue.DeleteMessageAsync(session.MessageData, session);

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

            // Reset the visibility of the message to ensure it doesn't get picked up by anyone else.
            await this.workItemQueue.RenewMessageAsync(session.MessageData, session);

            workItem.LockedUntilUtc = DateTime.UtcNow.Add(this.settings.WorkItemQueueVisibilityTimeout);
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
                this.settings.Logger.AssertFailure(
                    this.azureStorageClient.QueueAccountName,
                    this.settings.TaskHubName,
                    $"Could not find context for work item with ID = {workItem.Id}.");
                return;
            }

            session.StartNewLogicalTraceScope();

            await this.workItemQueue.AbandonMessageAsync(session.MessageData, session);

            if (this.activeActivitySessions.TryRemove(workItem.Id, out _))
            {
                this.stats.ActiveActivityExecutions.Decrement();
            }
        }
        #endregion

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            if (this.settings.MaxCheckpointBatchSize <= 0)
            {
                return false;
            }

            // We will process at most N events at a time. Any remaining events will be
            // scheduled in a subsequent episode. This is useful to ensure we don't exceed the
            // orchestrator queue timeout while trying to checkpoint massive numbers of actions.
            // It also reduces the amount of re-work that needs to be done if there is a failure
            // in the middle of a checkpoint. Note that having a number too small could
            // drastically increase the end-to-end processing time of an orchestration.
            return runtimeState.NewEvents.Count > this.settings.MaxCheckpointBatchSize;
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
            return this.CreateTaskOrchestrationAsync(creationMessage, null);
        }

        /// <summary>
        /// Creates a new orchestration
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <param name="dedupeStatuses">States of previous orchestration executions to be considered while de-duping new orchestrations on the client</param>
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            ExecutionStartedEvent executionStartedEvent = creationMessage.Event as ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                throw new ArgumentException($"Only {nameof(EventType.ExecutionStarted)} messages are supported.", nameof(creationMessage));
            }

            Utils.ConvertDateTimeInHistoryEventsToUTC(creationMessage.Event);

            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsureTaskHubAsync();

            InstanceStatus existingInstance = await this.trackingStore.FetchInstanceStatusAsync(
                creationMessage.OrchestrationInstance.InstanceId);

            if (existingInstance?.State != null && dedupeStatuses != null && dedupeStatuses.Contains(existingInstance.State.OrchestrationStatus))
            {
                // An instance in this state already exists.
                if (this.settings.ThrowExceptionOnInvalidDedupeStatus)
                {
                    throw new OrchestrationAlreadyExistsException($"An Orchestration instance with the status {existingInstance.State.OrchestrationStatus} already exists.");
                }
                
                return;
            }

            if (executionStartedEvent.Generation == null)
            {
                if (existingInstance != null)
                {
                    executionStartedEvent.Generation = existingInstance.State.Generation + 1;
                }
                else
                {
                    executionStartedEvent.Generation = 0;
                }
            }

            ControlQueue controlQueue = await this.GetControlQueueAsync(creationMessage.OrchestrationInstance.InstanceId);
            MessageData startMessage = await this.SendTaskOrchestrationMessageInternalAsync(
                EmptySourceInstance,
                controlQueue,
                creationMessage);

            string inputPayloadOverride = null;
            if (startMessage.CompressedBlobName != null)
            {
                // The input of the orchestration is changed to be a URL to a compressed blob, which
                // is the input queue message. When fetching the orchestration instance status, that
                // blob will be downloaded, decompressed, and the ExecutionStartedEvent.Input value
                // will be returned as the input value.
                inputPayloadOverride = this.messageManager.GetBlobUrl(startMessage.CompressedBlobName);
            }

            await this.trackingStore.SetNewExecutionAsync(
                executionStartedEvent,
                existingInstance?.ETag,
                inputPayloadOverride);
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
            ControlQueue controlQueue = await this.GetControlQueueAsync(message.OrchestrationInstance.InstanceId);
            await this.SendTaskOrchestrationMessageInternalAsync(EmptySourceInstance, controlQueue, message);
        }

        Task<MessageData> SendTaskOrchestrationMessageInternalAsync(
            OrchestrationInstance sourceInstance,
            ControlQueue controlQueue,
            TaskMessage message)
        {
            return controlQueue.AddMessageAsync(message, sourceInstance);
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
            return new OrchestrationState[]
            {
                await this.trackingStore.GetStateAsync(instanceId, allExecutions, fetchInput: true).FirstOrDefaultAsync(),
            };
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
            return await this.trackingStore.GetStateAsync(instanceId, executionId, fetchInput: true);
        }

        /// <summary>
        /// Get the most current execution (generation) of the specified instance.
        /// This method is not part of the IOrchestrationServiceClient interface. 
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="allExecutions">This parameter is not used.</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        /// <returns>List of <see cref="OrchestrationState"/> objects that represent the list of orchestrations.</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions, bool fetchInput = true)
        {
            // Client operations will auto-create the task hub if it doesn't already exist.
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(instanceId, allExecutions, fetchInput).ToListAsync();
        }

        /// <summary>
        /// Gets the state of all orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(cancellationToken).ToListAsync();
        }

        /// <summary>
        /// Gets the state of all orchestration instances that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Fetch status grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Fetch status less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default(CancellationToken))
        {
            await this.EnsureTaskHubAsync();
            return await this.trackingStore.GetStateAsync(createdTimeFrom, createdTimeTo, runtimeStatus, cancellationToken).ToListAsync();
        }

        /// <summary>
        /// Gets the state of all orchestration instances that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Fetch status grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Fetch status less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <param name="top">Top is number of records per one request.</param>
        /// <param name="continuationToken">ContinuationToken of the pager.</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public async Task<DurableStatusQueryResult> GetOrchestrationStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            await this.EnsureTaskHubAsync();
            Page<OrchestrationState> page = await this.trackingStore
                .GetStateAsync(createdTimeFrom, createdTimeTo, runtimeStatus, cancellationToken)
                .AsPages(continuationToken, top)
                .FirstOrDefaultAsync();

            return page != null
                ? new DurableStatusQueryResult { ContinuationToken = page.ContinuationToken, OrchestrationState = page.Values }
                : new DurableStatusQueryResult { OrchestrationState = Array.Empty<OrchestrationState>() };
        }

        /// <summary>
        /// Gets the state of all orchestration instances that match the specified parameters.
        /// </summary>
        /// <param name="condition">Query condition. <see cref="OrchestrationInstanceStatusQueryCondition"/></param>
        /// <param name="top">Top is number of records per one request.</param>
        /// <param name="continuationToken">ContinuationToken of the pager.</param>
        /// <param name="cancellationToken">Cancellation Token</param>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public async Task<DurableStatusQueryResult> GetOrchestrationStateAsync(OrchestrationInstanceStatusQueryCondition condition, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            await this.EnsureTaskHubAsync();
            Page<OrchestrationState> page = await this.trackingStore
                .GetStateAsync(condition, cancellationToken)
                .AsPages(continuationToken, top)
                .FirstOrDefaultAsync();

            return page != null
                ? new DurableStatusQueryResult { ContinuationToken = page.ContinuationToken, OrchestrationState = page.Values }
                : new DurableStatusQueryResult { OrchestrationState = Array.Empty<OrchestrationState>() };
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
        /// Rewinds an orchestration then revives it from rewound state with a generic event message.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration to rewind.</param>
        /// <param name="reason">The reason for rewinding.</param>
        public async Task RewindTaskOrchestrationAsync(string instanceId, string reason)
        {
            List<string> queueIds = await this.trackingStore.RewindHistoryAsync(instanceId).ToListAsync();

            foreach (string id in queueIds)
            {
                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = id
                };

                var startedEvent = new GenericEvent(-1, reason);
                var taskMessage = new TaskMessage
                {
                    OrchestrationInstance = orchestrationInstance,
                    Event = startedEvent
                };

                await SendTaskOrchestrationMessageAsync(taskMessage);
            }
        }

        /// <summary>
        ///  Forces this app to take the AppLease if this app doesn't already have it. To use this, must be using the AppLease feature by setting UseAppLease to true in host.json.
        /// </summary>
        /// <returns>A task that completes when the steal app message is written to storage and the LeaseManagerStarter Task has been restarted.</returns>
        public Task ForceChangeAppLeaseAsync()
        {
            return this.appLeaseManager.ForceChangeAppLeaseAsync();
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <returns>String with formatted JSON array representing the execution history.</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            OrchestrationHistory history = await this.trackingStore.GetHistoryEventsAsync(
                instanceId,
                executionId,
                CancellationToken.None);
            return Utils.SerializeToJson(history.Events);
        }

        /// <summary>
        /// Purge history for an orchestration with a specified instance id.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        public Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId)
        {
            return this.trackingStore.PurgeInstanceHistoryAsync(instanceId);
        }

        /// <summary>
        /// Purge history for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges history grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges history less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        public Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            return this.trackingStore.PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus);
        }

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(string instanceId)
        {
            PurgeHistoryResult storagePurgeHistoryResult = await this.PurgeInstanceHistoryAsync(instanceId);
            return storagePurgeHistoryResult.ToCorePurgeHistoryResult();
        }

        /// <inheritdoc />
        async Task<PurgeResult> IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter)
        {
            PurgeHistoryResult storagePurgeHistoryResult = await this.PurgeInstanceHistoryAsync(
                purgeInstanceFilter.CreatedTimeFrom,
                purgeInstanceFilter.CreatedTimeTo,
                purgeInstanceFilter.RuntimeStatus);
            return storagePurgeHistoryResult.ToCorePurgeHistoryResult();
        }
#nullable enable
        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">The orchestration instance to wait for.</param>
        /// <param name="executionId">The execution ID (generation) of the specified instance.</param>
        /// <param name="timeout">Max timeout to wait. Only positive <see cref="TimeSpan"/> values, <see cref="TimeSpan.Zero"/>, or <see cref="Timeout.InfiniteTimeSpan"/> are allowed.</param>
        /// <param name="cancellationToken">Task cancellation token.</param>
        public async Task<OrchestrationState?> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            bool isInfiniteTimeSpan = timeout == Timeout.InfiniteTimeSpan;
            if (timeout < TimeSpan.Zero && !isInfiniteTimeSpan)
            {
                throw new ArgumentException($"The parameter {nameof(timeout)} cannot be negative." +
                    $" The value for {nameof(timeout)} was '{timeout}'." +
                    $" Please provide either a positive timeout value or Timeout.InfiniteTimeSpan.");
            }

            TimeSpan statusPollingInterval = TimeSpan.FromSeconds(2);
            while (!cancellationToken.IsCancellationRequested)
            {
                OrchestrationState? state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                
                if (state != null &&
                    state.OrchestrationStatus != OrchestrationStatus.Running &&
                    state.OrchestrationStatus != OrchestrationStatus.Suspended &&
                    state.OrchestrationStatus != OrchestrationStatus.Pending &&
                    state.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    if (this.settings.FetchLargeMessageDataEnabled)
                    {
                        state.Input = await this.messageManager.FetchLargeMessageIfNecessary(state.Input);
                        state.Output = await this.messageManager.FetchLargeMessageIfNecessary(state.Output);
                    }
                    return state;
                }
                
                timeout -= statusPollingInterval;
                
                // For a user-provided timeout of `TimeSpan.Zero`,
                // we want to check the status of the orchestration once and then return.
                // Therefore, we check the timeout condition after the status check.
                if (!isInfiniteTimeSpan && (timeout <= TimeSpan.Zero))
                {
                    break;
                }
                await Task.Delay(statusPollingInterval, cancellationToken);
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

        /// <summary>
        /// Download content that was too large to be stored in table storage,
        /// such as the input or output status fields, and for which the blob URI was stored instead.
        /// </summary>
        /// <param name="blobUri">The URI of the blob.</param>
        public Task<string> DownloadBlobAsync(string blobUri)
        {
            return this.messageManager.DownloadAndDecompressAsBytesAsync(new Uri(blobUri));
        }

        #endregion

        // TODO: Change this to a sticky assignment so that partition count changes can
        //       be supported: https://github.com/Azure/azure-functions-durable-extension/issues/1
        async Task<ControlQueue?> GetControlQueueAsync(string instanceId)
        {
            uint partitionIndex = Fnv1aHashHelper.ComputeHash(instanceId) % (uint)this.settings.PartitionCount;
            string queueName = GetControlQueueName(this.settings.TaskHubName, (int)partitionIndex);

            ControlQueue cachedQueue;
            if (!this.allControlQueues.TryGetValue(queueName, out cachedQueue))
            {
                // Lock ensures all callers asking for the same partition get the same queue reference back.
                lock (this.allControlQueues)
                {
                    if (!this.allControlQueues.TryGetValue(queueName, out cachedQueue))
                    {
                        cachedQueue = new ControlQueue(this.azureStorageClient, queueName, this.messageManager);
                        this.allControlQueues.TryAdd(queueName, cachedQueue);
                    }
                }

                // Important to ensure the queue exists, whether the current thread initialized it or not.
                // A slightly better design would be to use a semaphore to block non-initializing threads.
                await cachedQueue.CreateIfNotExistsAsync();
            }

            System.Diagnostics.Debug.Assert(cachedQueue != null);
            return cachedQueue;
        }

        /// <summary>
        /// Disposes of the current object.
        /// </summary>
        public void Dispose()
        {
            this.orchestrationSessionManager.Dispose();
        }

        /// <summary>
        /// Gets the status of all orchestration instances with paging that match the specified conditions.
        /// </summary>
        public async Task<OrchestrationQueryResult> GetOrchestrationWithQueryAsync(OrchestrationQuery query, CancellationToken cancellationToken)
        {
            OrchestrationInstanceStatusQueryCondition convertedCondition = ToAzureStorageCondition(query);
            DurableStatusQueryResult statusContext = await this.GetOrchestrationStateAsync(convertedCondition, query.PageSize, query.ContinuationToken, cancellationToken);
            return ConvertFrom(statusContext);
        }

        private static OrchestrationInstanceStatusQueryCondition ToAzureStorageCondition(OrchestrationQuery condition)
        {
            return new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = condition.RuntimeStatus,
                CreatedTimeFrom = condition.CreatedTimeFrom ?? default(DateTime),
                CreatedTimeTo = condition.CreatedTimeTo ?? default(DateTime),
                TaskHubNames = condition.TaskHubNames,
                InstanceIdPrefix = condition.InstanceIdPrefix,
                FetchInput = condition.FetchInputsAndOutputs,
                ExcludeEntities = condition.ExcludeEntities,
            };
        }

        private static OrchestrationQueryResult ConvertFrom(DurableStatusQueryResult statusContext)
        {
            var results = new List<OrchestrationState>();
            foreach (var state in statusContext.OrchestrationState)
            {
                results.Add(state);
            }

            return new OrchestrationQueryResult(results, statusContext.ContinuationToken);
        }

        class PendingMessageBatch
        {
            public string? OrchestrationInstanceId { get; set; }
            public string? OrchestrationExecutionId { get; set; }

            public List<MessageData> Messages { get; set; } = new List<MessageData>();

            public OrchestrationRuntimeState? Orchestrationstate { get; set; }
        }

        class ResettableLazy<T>
        {
            readonly Func<T> valueFactory;
            readonly LazyThreadSafetyMode threadSafetyMode;

            Lazy<T> lazy;

            // Supress warning because it's incorrect: the lazy variable is initialized in the constructor, in the `Reset()` method
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
            public ResettableLazy(Func<T> valueFactory, LazyThreadSafetyMode mode)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
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

        struct TaskHubQueueMessage
        {
            public TaskHubQueueMessage(TaskHubQueue queue, TaskMessage message)
            {
                this.Queue = queue;
                this.Message = message;
            }

            public TaskHubQueue Queue { get; }

            public TaskMessage Message { get; }
        }
    }
}
#nullable disable
