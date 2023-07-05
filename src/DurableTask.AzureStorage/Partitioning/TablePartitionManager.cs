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

#nullable enable
namespace DurableTask.AzureStorage.Partitioning
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;

    /// <summary>
    /// Partition Manager V3 based on Azure Storage V2.  
    /// </summary>
    sealed class TablePartitionManager : IPartitionManager, IDisposable
    {
        // Used for logging purposes only, indicating that a particular parameter (usually a partition ID) doesn't apply.
        const string NotApplicable = "";
        Task partitionManagerTask;

        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource partitionManagerCancellationSource;
        readonly string storageAccountName;
        readonly TableClient partitionTable;
        readonly TableLeaseManager tableLeaseManager;
        readonly LeaseCollectionBalancerOptions options;

        /// <summary>
        /// Constructor to initiate new instances of TablePartitionManager.
        /// </summary>
        /// <param name="azureStorageClient">Client for the storage account.</param>
        /// <param name="service">The service responsible for initiating or terminating the partition manager.</param>
        public TablePartitionManager(
            AzureStorageOrchestrationService service,
            AzureStorageClient azureStorageClient)
        {
            this.azureStorageClient = azureStorageClient;
            this.service = service;
            this.settings = this.azureStorageClient.Settings;
            string connectionString = this.settings.StorageConnectionString ?? this.settings.StorageAccountDetails.ConnectionString;
            this.storageAccountName = this.azureStorageClient.TableAccountName;
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("A connection string is required to use the table partition manager. Managed identity is not yet supported when using the table partition manager. Please provide a connection string to your storage account in your app settings.");
            }
            this.options = new LeaseCollectionBalancerOptions
            {
                AcquireInterval = this.settings.LeaseAcquireInterval,
                RenewInterval = this.settings.LeaseRenewInterval,
                LeaseInterval = this.settings.LeaseInterval,
                ShouldStealLeases = true
            };
            this.partitionManagerCancellationSource = new CancellationTokenSource();
            this.partitionTable = new TableClient(connectionString, this.settings.PartitionTableName);
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings, this.storageAccountName, this.options);
            this.partitionManagerTask = Task.CompletedTask;
        }


        /// <summary>
        /// Starts the partition management loop for the current worker.
        /// </summary>
        Task IPartitionManager.StartAsync()
        {
            this.partitionManagerTask = Task.Run(() => this.PartitionManagerLoop(this.partitionManagerCancellationSource.Token));
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: NotApplicable,
                details: $"Started the background partition manager loop to acquire and balance partitions.");
            return Task.CompletedTask;
        }


        /// <summary>
        /// This loop is the main loop for worker to manage the partition table. 
        /// Worker will manage the table regularly with default interval.
        /// If the worker is waiting for any other worker's partitions or is going to release any owned partitions, the wait time will be 1 second for timely update.
        /// If worker failed to update the table or any other exceptions occurred, the worker will re-try immediately.
        /// If the failure operations occurred too many times, the wait time will be back to default value to avoid excessive loggings.
        /// </summary>
        async Task PartitionManagerLoop(CancellationToken cancellationToken)
        {
            int maxFailureCount = 10;
            int failureCount = 0;
            bool isSuccessfullyShutsDown = false;
            
            while (!isSuccessfullyShutsDown || !cancellationToken.IsCancellationRequested)
            {
                TimeSpan timeToSleep = this.options.AcquireInterval;
                
                try
                {
                    ReadTableReponse response = await this.tableLeaseManager.ReadAndWriteTable(cancellationToken);
                    
                    isSuccessfullyShutsDown = response.IsReleasedAllLease;                    
                    if (isSuccessfullyShutsDown)
                    {
                        this.settings.Logger.PartitionManagerInfo(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            this.settings.WorkerId,
                            partitionId: NotApplicable,
                            "Successfully released all ownership leases for shutdown.");
                        break;
                    }
                    
                    if (response.IsDrainingPartition || response.WaitForPartition)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                    
                    failureCount = 0;
                }
                // Exception Status 412 represents an out of date ETag
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    failureCount++;
                }
                // Eat any other exceptions.
                catch (Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                       this.storageAccountName,
                       this.settings.TaskHubName,
                       this.settings.WorkerId,
                       partitionId: NotApplicable,
                       details: $"Unexpected error occurred while trying to manage table partition leases: {exception}"); 
                    
                    failureCount++;
                }

                // If table update failed, we re-read the table immediately to obtain the latest ETag.
                // In the case of too many successive failures, we the wait before retrying to prevent excessive logs.
                if (failureCount > 0 && failureCount < maxFailureCount)
                {
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                
                if (cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(timeToSleep);
                }
                else
                {
                    try
                    {
                        // Introduce a token to enable immediate shutdown initiation for the worker, even if it'sat rest when receiving the shutdown request.
                        await Task.Delay(timeToSleep, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        this.settings.Logger.PartitionManagerInfo(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            this.settings.WorkerId,
                            partitionId: NotApplicable,
                            details: $"Requested to cancel partition manager table manage loop. Initiate shutdown process.");
                    }
                }
            }

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: NotApplicable,
                "Stopped background table partition manager loop.");
        }

        /// <summary>
        /// Stop the partition manager. It first stops the partition manager loop to stop involving in balance in case it steals others' partitions in shutdown process.
        /// And then start the Task ShutDown() to release all ownership leases. 
        /// Worker will retry updating the table if the update was failed or any other exceptions occurred.
        /// If the failure operations happened too many times, the loop waiting time will be extended to avoid excessive loggings.
        /// </summary>
        async Task IPartitionManager.StopAsync()
        {
            this.partitionManagerCancellationSource.Cancel();
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: NotApplicable,
                "Started draining the in-memory messages of all owned control queues for shutdown.");

            await this.partitionManagerTask;
        }

        async Task IPartitionManager.CreateLeaseStore()
        {
            await this.partitionTable.CreateIfNotExistsAsync();
        }

        async Task IPartitionManager.CreateLease(string leaseName)
        {
            try
            {
                var lease = new TableLease()
                {
                    // Partition key is an empty string because table storage entities require
                    // specifying a `RowKey` and `PartitionKey`, but `PartitionKey` isn't used on our end.
                    PartitionKey = "",
                    RowKey = leaseName
                };
                await this.partitionTable.AddEntityAsync(lease);

                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    leaseName,
                    "Successfully added the partition to the partition table.");
            }
            catch (RequestFailedException e) when (e.Status == 409 /* The specified entity already exists. */)
            {
                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    leaseName,
                    "This partition already exists in the partition table.");
            }
        }
        
        Task<IEnumerable<BlobLease>> IPartitionManager.GetOwnershipBlobLeases()
        {
            throw new NotImplementedException("This method is not implemented in the TablePartitionManager");
        }

        /// <summary>
        /// Used for internal testing.
        /// </summary>
        internal IEnumerable<TableLease> GetTableLeases()
        {
            return this.partitionTable.Query<TableLease>();
        }

        Task IPartitionManager.DeleteLeases()
        {
            return this.partitionTable.DeleteAsync();
        }

        sealed class TableLeaseManager
        {
            readonly string workerName;
            readonly AzureStorageOrchestrationService service;
            readonly AzureStorageOrchestrationServiceSettings settings;
            readonly TableClient partitionTable;
            readonly string storageAccountName;
            readonly Dictionary<string, Task> backgroundDrainTasks;
            readonly LeaseCollectionBalancerOptions options;

            public TableLeaseManager(
                TableClient table,
                AzureStorageOrchestrationService service,
                AzureStorageOrchestrationServiceSettings settings,
                string storageAccountName,
                LeaseCollectionBalancerOptions options)
            {
                this.partitionTable = table;
                this.service = service;
                this.settings = settings;
                this.storageAccountName = storageAccountName;
                this.workerName = this.settings.WorkerId;
                this.backgroundDrainTasks = new Dictionary<string, Task>();
                this.options = options;
            }

            /// <summary>
            /// Reads the partition table to determine the tasks the worker should do. Used by the PartitionManagerLoop.
            /// During the iteration, the worker will first claim any available partitions with method `TryCalimLease`. 
            /// Subsequently, if the partition is owned by this worker, it will proceed with method `CheckOwnershipLease`. 
            /// However, if the partition is owned by other workers, it will utilize method `CheckOtherWorkerLease`.
            /// </summary>
            /// <returns> The <see cref="ReadTableReponse"/> incidates whether the worker is waiting to claim a stolen lease from other workers or working on releasing any ownership leases.</returns>
            /// <exception cref="RequestFailedException">will be thrown if failed to update the partition table. Partition Manager loop will catch it and re-read the table to get the latest information.</exception>
            public async Task<ReadTableReponse> ReadAndWriteTable(CancellationToken cancellationToken)
            {
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = this.partitionTable.Query<TableLease>();
                var partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int ownershipLeaseCount = 0;

                // If shutdown is requested, stop claiming and balancing partitions. Instead, drain and release all ownership partitions.
                if (cancellationToken.IsCancellationRequested)
                {
                    await this.TryDrainAndReleaseAllPartitions(partitions, response, ownershipLeaseCount);
                    return response;
                }

                foreach (TableLease partition in partitions)
                {
                    // In certain unhealthy situations, a worker may lose the lease without realizing it and continue listening
                    // for queue messages. We check for that case here and stop dequeuing messages if we discover that
                    // another worker currently owns the lease.
                    this.service.DropLostControlQueue(partition);

                    bool claimedLease = false;
                    bool stoleLease = false;
                    bool renewedLease = false;
                    bool drainedLease = false;
                    bool releasedLease = false;
                    ETag etag = partition.ETag;

                    claimedLease = TryClaimLease(partition);
                    string previousOwner = partition.CurrentOwner!;
                    CheckOtherWorkerLease(partition, partitionDistribution, response);
                    await CheckOwnershipLease(partition, response);

                    // Update the table if the lease is claimed, stolen, renewed, drained or released.
                    if (claimedLease || stoleLease || renewedLease || drainedLease || releasedLease)
                    {
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);

                            if (claimedLease)
                            {
                                await this.service.OnTableLeaseAcquiredAsync(partition);
                            }

                            this.LogHelper(partition, claimedLease, stoleLease, renewedLease, drainedLease, releasedLease, previousOwner);
                        }
                        // Exception for failed table update due to ETag expired. 
                        // Re-read the table immediately to get a new ETag.
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Failed to update table entry due to an Etag mismatch. Failed ETag value: '{etag}'.");
                            throw;
                        }
                        // Eat any exceptions
                        catch (Exception exception)
                        {
                            this.settings.Logger.PartitionManagerError(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Unexpected error occurred when updating the table: {exception.Message}");
                        }
                    }

                    // Check if a lease is available for the current worker.
                    bool TryClaimLease(TableLease partition)
                    {
                        //Check if the partition is empty, expired or stolen by the current worker and claim it.
                        bool isEmptyLease = (partition.CurrentOwner == null && partition.NextOwner == null);
                        bool isExpired = (DateTime.UtcNow >= partition.ExpiresAt);
                        bool isStolenByMe = (partition.CurrentOwner == null && partition.NextOwner == this.workerName);

                        bool isLeaseAvailable = isEmptyLease || isExpired || isStolenByMe;
                        if (isLeaseAvailable)
                        {
                            this.ClaimLease(partition);
                        }
                        return isLeaseAvailable;
                    }

                    // Check ownership lease. 
                    // If the lease is not stolen by others, renew it.
                    // If the lease is stolen by others, check if starts drainning or if finishes drainning.
                    async Task CheckOwnershipLease(TableLease partition, ReadTableReponse response)
                    {
                        if (partition.CurrentOwner != this.workerName)
                        {
                            return;
                        }

                        // Lease is not stolen by others.
                        if (partition.NextOwner == null)
                        {
                            ownershipLeaseCount++;
                        }
                        else // Lease is stolen by others.
                        {
                            response.IsDrainingPartition = true;
                            // In case operations on dictionary and isDraining are not sync, always check first if we have any drain tasks in the dictionary.
                            bool isDrainTaskExists = this.backgroundDrainTasks.TryGetValue(partition.RowKey!, out Task? task);

                            if (isDrainTaskExists)
                            {
                                // Check if drain has been finished.
                                // If finished, release the lease.
                                if (task.IsCompleted == true)
                                {
                                    this.settings.Logger.PartitionManagerInfo(
                                        this.storageAccountName,
                                        this.settings.TaskHubName,
                                        this.settings.WorkerId,
                                        partition.RowKey,
                                        $"Successfully drained partition. Lease will be released.");

                                    this.backgroundDrainTasks.Remove(partition.RowKey!);
                                    this.ReleaseLease(partition);
                                    releasedLease = true;
                                    await task;// catch exception for drain tasks
                                }
                            }
                            else // If lease hasn't start draining, then drain it.
                            {
                                this.DrainLease(partition, CloseReason.LeaseLost);
                                drainedLease = true;
                            }
                        }
                        // As long as the lease is not released, keep renewing the lease in case lease expired since it's still draining. 
                        if (partition.CurrentOwner != null)
                        {
                            this.RenewLease(partition);
                            renewedLease = true;
                        }
                    }

                    //If the lease is other worker's lease. Store it to the dictionary for future balance.
                    //If the other worker is shutting down, steal the lease.
                    void CheckOtherWorkerLease(TableLease partition, Dictionary<string, List<TableLease>> partitionDistribution, ReadTableReponse response)
                    {
                        bool isOtherWorkerCurrentLease = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == false);
                        bool isOtherWorkerStolenLease = (partition.CurrentOwner != this.workerName && partition.NextOwner != null);
                        bool isOwnerShuttingDown = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == true);

                        string owner;
                        //If the lease is other worker's current lease, add partition to the dictionary with CurrentOwner as key.
                        if (isOtherWorkerCurrentLease)
                        {
                            owner = partition.CurrentOwner!;
                            this.AddToDictionary(partition, partitionDistribution, owner);
                        }

                        // If other workers' lease is stolen, suppose lease tranfer could finish successfully, and add partition to the dictionary with NextOwner as key. 
                        if (isOtherWorkerStolenLease)
                        {
                            owner = partition.NextOwner!;
                            //If the lease was stolen by _this_ worker, we increase its currently owned lease count.
                            if (owner == this.workerName)
                            {
                                ownershipLeaseCount++;
                                response.WaitForPartition = true;
                            }
                            //If the lease is stolen by other workers, add it to the partitionDistribution dictionary with NextOwner as key.
                            else
                            {
                                this.AddToDictionary(partition, partitionDistribution, owner);
                            }
                        }

                        //If the lease belongs to a worker that is shutting down, and it's not stole by others, then steal it.
                        if (isOwnerShuttingDown)
                        {
                            previousOwner = partition.CurrentOwner!;
                            this.StealLease(partition);
                            stoleLease = true;
                            response.WaitForPartition = true;
                        }
                    }


                }
                await this.BalanceLeases(partitionDistribution, partitions, ownershipLeaseCount, response);
                
                return response;
            }

            // This method performs a worker-level partition rebalancing process.
            // The idea is to calculate the expected number of leases per worker in a fully balanced scenario
            //  (a.k.a their partition quota) and for each worker to steal partitions from others
            // until they have met their quota. 
            // A few remarks:
            // (1) The quota of partitions per worker is the number of partitions divided by the number of workers. If these two number can not be evenly divided, then the difference between quota of partitions per workers should not exceed one.
            // (2) Workers only steal from workers that have exceeded their quota
            // Exception will be thrown if the update operation fails due to outdated etagso that worker could re-read the table again to get the latest information.
            // Any other exceptions will be caught to logs. 
            async Task BalanceLeases(Dictionary<string, List<TableLease>> partitionDistribution, Pageable<TableLease> partitions, int ownershipLeaseCount, ReadTableReponse response)
            {
                // Check if there is any other active worker, if not, skip the balance process.
                if (partitionDistribution.Count == 0)
                {
                    return;
                }

                int averageLeasesCount = (partitions.Count()) / (partitionDistribution.Count + 1);
                if (averageLeasesCount < ownershipLeaseCount)
                {
                    // Already have enough leases, and thus no need to steal, just stop the balancing.
                    return;
                }

                //Check all the other active workers when my ownership lease number is not larger than the average number.
                foreach (KeyValuePair<string, List<TableLease>> pair in partitionDistribution)
                {
                    int numLeasesToSteal = averageLeasesCount - ownershipLeaseCount;

                    //Already have enough partitions, exit the loop.
                    if (numLeasesToSteal < 0)
                    {
                        break;
                    }

                    // Only steal leases from takshub workers who have more leases than average.
                    // If this task hub worker's lease num is smaller or equal to the average number, just skip this worker.
                    int numExcessiveLease = pair.Value.Count() - averageLeasesCount;
                    if (numExcessiveLease <= 0)
                    {
                        continue;
                    }

                    // The balancing condition requires that the differences in the number of leases assigned to each worker should not exceed 1, if the total number of partitions is not evenly divisible by the number of active workers.
                    // Thus, the maximum number of leases a worker can own is the average number of leases per worker plus one in this case.
                    // If a worker has more than one lease difference than average and _this_worker has not reached the maximum, it should steal an additional lease.
                    if (numLeasesToSteal == 0 && numExcessiveLease > 1)
                    {
                        numLeasesToSteal = 1;
                    }

                    numLeasesToSteal = Math.Min(numLeasesToSteal, numExcessiveLease);
                    for (int i = 0; i < numLeasesToSteal; i++)
                    {
                        ownershipLeaseCount++;
                        TableLease partition = pair.Value[i];
                        ETag etag = partition.ETag;
                        string previousOwner = partition.CurrentOwner!;
                        this.StealLease(partition);

                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            
                            this.settings.Logger.LeaseStealingSucceeded(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                previousOwner,
                                leaseType: NotApplicable,
                                partitionId: partition.RowKey);
                            
                            response.WaitForPartition = true;
                        }
                        // Exception for failed table update due to ETag expired. 
                        // Re-read the table immediately to get the latest one.
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Failed to update table entry due to an Etag mismatch. Failed ETag value: '{etag}'");
                            throw;
                        }
                        // Eat any exceptions during lease stealing.
                        catch (Exception exception)
                        {
                            this.settings.Logger.PartitionManagerWarning(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Unexpected error occurred in stealing partition lease: {exception}");
                        }
                    }
                }
            }

            void ClaimLease(TableLease lease)
            {
                lease.CurrentOwner = this.workerName;
                lease.NextOwner = null;
                lease.OwnedSince = DateTime.UtcNow;
                lease.LastRenewal = DateTime.UtcNow;
                lease.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
                lease.IsDraining = false;
            }

            void DrainLease(TableLease lease, CloseReason reason)
            {
                Task task = this.service.DrainTableLeaseAsync(lease, reason);
                string partitionId = lease.RowKey!;
                this.backgroundDrainTasks.Add(partitionId, task);
                lease.IsDraining = true;
            }

            void ReleaseLease(TableLease lease)
            {
                lease.IsDraining = false;
                lease.CurrentOwner = null;
            }

            void RenewLease(TableLease lease)
            {
                lease.LastRenewal = DateTime.UtcNow;
                lease.ExpiresAt = DateTime.UtcNow.Add(this.options.LeaseInterval);
            }

            void StealLease(TableLease lease)
            {
                lease.NextOwner = this.workerName;
            }
           
            // Log operations on partition table.
            void LogHelper(
                TableLease partition,
                bool claimedLease,
                bool stoleLease,
                bool renewedLease,
                bool drainedLease,
                bool releasedLease,
                string previousOwner)
            {
                if (claimedLease)
                {
                    this.settings.Logger.LeaseAcquisitionSucceeded(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partitionId: partition.RowKey,
                        leaseType: NotApplicable);
                }
                if (stoleLease)
                {
                    this.settings.Logger.LeaseStealingSucceeded(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        previousOwner,
                        leaseType: NotApplicable,
                        partitionId: partition.RowKey);
                }
                if (releasedLease)
                {
                    this.settings.Logger.LeaseRemoved(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partition.RowKey,
                        token: NotApplicable,
                        leaseType: NotApplicable);
                }
                if (drainedLease)
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partitionId: partition.RowKey,
                        details: "Draining partition");
                }
                if (renewedLease)
                {
                    this.settings.Logger.LeaseRenewalResult(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partitionId: partition.RowKey,
                        success: true,
                        token: NotApplicable,
                        leaseType: NotApplicable,
                        details: "Successfully renewed partition lease");
                }
            }

            // Add any other active worker's lease to dictionary partitionDistribution for further balance process.
            void AddToDictionary(TableLease partition, Dictionary<string, List<TableLease>> partitionDistribution, string owner)
            {
                if (partitionDistribution.ContainsKey(owner))
                {
                    partitionDistribution[owner].Add(partition);
                }
                else
                {
                    partitionDistribution.Add(owner, new List<TableLease> { partition });
                }
            }

            /// <summary>
            /// Stops the partition manager, releasing this worker's ownership leases.
            /// </summary>
            /// <returns>
            /// Returns <c>null</c> if the worker successfully shuts down.
            /// </returns>
            /// <exception cref="RequestFailedException">Failed to update the partition table due to an out of date ETag. The method StopAsync is expected to catch it and re-read the partition table immediately. </exception>
            async Task TryDrainAndReleaseAllPartitions(Pageable<TableLease> partitions, ReadTableReponse response, int ownershipLeaseCount)
            {
                response.IsDrainingPartition = true;
                
                foreach (TableLease partition in partitions.Where(p => p.CurrentOwner == this.workerName))
                {
                    bool drainedLease = false;
                    bool releasedLease = false;
                    bool renewedLease = false;

                    ownershipLeaseCount++;
                    ETag etag = partition.ETag;

                    if (this.backgroundDrainTasks.TryGetValue(partition.RowKey!, out Task? drainTask))
                    {
                        // Check if draining process has finished. If so, release the lease.
                        if (drainTask.IsCompleted)
                        {
                            this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                details: "Successfully drained partition. Lease will be released.");

                            this.backgroundDrainTasks.Remove(partition.RowKey!);
                            this.ReleaseLease(partition);
                            releasedLease = true;
                            ownershipLeaseCount--;
                            await drainTask; // Surface any exceptions from the drain process.
                        }
                        else // If draining process is ongoing, we keep renewing the lease to prevent it from expiring
                        {
                            this.RenewLease(partition);
                            renewedLease = true;
                        }
                    }
                    else 
                    {
                        this.DrainLease(partition, CloseReason.Shutdown);
                        this.RenewLease(partition);
                        renewedLease = true;
                        drainedLease = true;
                    }
                        
                    try
                    {
                        await this.partitionTable.UpdateEntityAsync(partition, etag, TableUpdateMode.Replace);
                        this.LogHelper(
                            partition,
                            claimedLease: false,
                            stoleLease: false,
                            renewedLease,
                            drainedLease,
                            releasedLease,
                            previousOwner: NotApplicable);
                    }
                    // Exception Status 412 represents an out of date ETag.
                    catch (RequestFailedException ex) when (ex.Status == 412)
                    {
                        this.settings.Logger.PartitionManagerWarning(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            this.settings.WorkerId,
                            partition.RowKey,
                            $"Failed to update table entry due to an Etag mismatch. Failed ETag value: '{etag}'");
                        throw;
                    }
                    // Eat any other exceptions.
                    catch (Exception exception)
                    {
                        this.settings.Logger.PartitionManagerError(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            this.settings.WorkerId,
                            partition.RowKey,
                            $"Unexpected error occurred in shutdown when updating lease: {exception}");
                    }
                }

                response.IsReleasedAllLease = (ownershipLeaseCount == 0);
            }
        }

        /// <summary>
        ///The Response class describes the behavior of the ReadandWrite method in the PartitionManager worker class. 
        ///If the worker is about to drain and release a lease, the method sets the WorkonDrain flag to true. 
        ///If the worker is going to acquire another lease from other worker, it sets the waitforPartition flag to true. 
        ///When either of these flags is true, the sleep time of the worker changes to 1 second.
        /// </summary>
        class ReadTableReponse
        {
            //If set to true, it indicates that the worker is working on release lease. 
            public bool IsDrainingPartition { get; set; } = false;
            
            //If set to true, it indicates that the worker is waiting for a lease to be released.
            public bool WaitForPartition { get; set; } = false;

            //If set to true, it indicates that the worker successfully released all ownership leases for shutdown.
            public bool IsReleasedAllLease { get; set; } = false;
        }

        //internal used for testing
        internal void SimulateUnhealthyWorker(CancellationToken testToken)
        {
            _ = Task.Run(() => this.PartitionManagerLoop(testToken));
        }

        //internal used for testing
        internal void KillLoop()
        {
            this.partitionManagerCancellationSource.Cancel();
        }

        public void Dispose()
        {
            this.partitionManagerCancellationSource.Dispose();
        }
    }
}
