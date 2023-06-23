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
    /// Partition ManagerV3 based on the Azure Storage V2.  
    /// </summary>
    sealed class TablePartitionManager : IPartitionManager, IDisposable
    {
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource partitionManagerCancellationSource;
        readonly string connectionString;
        readonly string storageAccountName;
        readonly TableClient partitionTable;
        readonly TableLeaseManager tableLeaseManager;
        readonly LeaseCollectionBalancerOptions options;

        /// <summary>
        /// constructor to initiate new instances of TablePartitionManager
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
            this.connectionString = this.settings.StorageConnectionString ?? this.settings.StorageAccountDetails.ConnectionString;
            this.storageAccountName = this.azureStorageClient.TableAccountName;
            if (string.IsNullOrEmpty(this.connectionString))
            {
                throw new InvalidOperationException("A connection string is required to use the table partition manager. Managed identity is not yet supported when using the table partition manager. Please provide a connection string to your storage account in your app.");
            }
            this.options = new LeaseCollectionBalancerOptions
            {
                AcquireInterval = this.settings.LeaseAcquireInterval,
                RenewInterval = this.settings.LeaseRenewInterval,
                LeaseInterval = this.settings.LeaseInterval,
                ShouldStealLeases = true
            };
            this.partitionManagerCancellationSource = new CancellationTokenSource();
            this.partitionTable = new TableClient(this.connectionString, this.settings.PartitionTableName);
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings, this.storageAccountName, this.options);
        }


        /// <summary>
        /// Starts the partition management loop for the current worker.
        /// </summary>
        Task IPartitionManager.StartAsync()
        {
            _ = Task.Run(() => this.PartitionManagerLoop(this.partitionManagerCancellationSource.Token));
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: "", //Empty string as it does not target any particular partition, but rather only initiates the partition manager.
                details: $"Starts the background partition manager loop to acquire and balance partitions.");
            return Task.CompletedTask;
        }


        /// <summary>
        /// This loop is the main loop for worker to manage the partition table. 
        /// Worker will manage the table regularly with default interval.
        /// If the worker is waiting for any other worker's partitions or is going to release any owned partitions, the wait time will be 1 second for timely update.
        /// If worker failed to update the table or any other exceptions occurred, the worker will re-try immediately.
        /// If the failure operations occurred too many times, the wait time will be back to default value to avoid excessive loggings.
        /// </summary>
        async Task PartitionManagerLoop(CancellationToken token)
        {
            int maxFailureCount = 10;
            int failureCount = 0;
            
            while (!token.IsCancellationRequested)
            {
                TimeSpan timeToSleep = this.options.AcquireInterval;
                try
                {
                    ReadTableReponse response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.WorkOnDrain|| response.WaitForPartition)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                    failureCount = 0;
                }
                // Exception of failed table update due to ETag expired. 
                // Re-read the table immediately to get a new ETag.
                // Extend the wait time when operations failed multiple times to avoid excessive loggings.
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    failureCount++;
                }
                // Eat any other exceptions.
                catch(Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                       this.storageAccountName,
                       this.settings.TaskHubName,
                       this.settings.WorkerId,
                       partitionId: "",
                       details: $"Unexpected error occurred while trying to manage table partition leases: {exception}"); 
                    
                    failureCount++;
                }

                try
                {
                    // If failed to update the table, re-read the table immediately to get the latest ETag.
                    // If failed too many times, extend the wait time to reduce excessive logs.
                    if (failureCount != 0 && failureCount < maxFailureCount)
                    {
                        timeToSleep = TimeSpan.FromSeconds(0);
                    }
                    await Task.Delay(timeToSleep, token);
                }
                catch (OperationCanceledException)
                {
                    this.settings.Logger.PartitionManagerInfo(
                       this.storageAccountName,
                       this.settings.TaskHubName,
                       this.settings.WorkerId,
                       partitionId: "",
                       $"Background partition manager table manage loop is canceled.");
                }
            }

            this.settings.Logger.PartitionManagerInfo(
                       this.storageAccountName,
                       this.settings.TaskHubName,
                       this.settings.WorkerId,
                       partitionId: "",
                       $"Background partition manager table manage loop stops.");
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
                "",
                $"Started draining the in-memory messages of all owned control queues for shutdown.");

            int maxFailureCount = 10;
            int failureCount = 0;
            bool hasCompleted = false;

            // Shutting down is to drain all the ownership leases and then release them.
            // The worker checks the partition table every 1 second to see if the draining finishes to ensure timely updates.
            var timeToSleep = TimeSpan.FromSeconds(1);

            //Waiting for all the ownership leases draining process to be finished. 
            while (!hasCompleted)
            {
                try
                {
                    // Returns true if the shutdown completed successfully or false if shutdown hasn't finished.
                    hasCompleted = await this.tableLeaseManager.ShutDown();
                    failureCount = 0;
                }
                // Exception for failed table update due to ETag expired. 
                // Re-read the table immediately to get a new ETag.
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    failureCount++;
                }
                // Eat any other exceptions.
                catch(Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        "",
                        $"Unexpected error occurred in stopping partition manager. Will continue retrying. Error details: {exception}");

                    failureCount++;
                }
                
                if(failureCount != 0 && failureCount < maxFailureCount)
                {
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                //If failed to update the table too many times, extend the wait time to reeduce excessive loggings.
                else if(failureCount > maxFailureCount)
                {
                    timeToSleep = this.options.AcquireInterval;
                }
                await Task.Delay(timeToSleep);
            };

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Successfully released all ownership leases for shutdown.");
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
                    PartitionKey = "",
                    RowKey = leaseName
                };
                await this.partitionTable.AddEntityAsync(lease);

                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    leaseName,
                    $"Successfully created the partition {leaseName} in the partition table.");
            }
            catch (RequestFailedException e) when (e.Status == 409 /* The specified entity already exists. */)
            {
                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    leaseName,
                    $"The partition {leaseName} already exists in the table.");
            }
        }
        
        Task<IEnumerable<BlobLease>> IPartitionManager.GetOwnershipBlobLeases()
        {
            throw new NotImplementedException("This method is not implemented in the TablePartitionManager");
        }

        /// <summary>
        /// internal use for testing . 
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
            readonly Dictionary<string, Task> drainTask;
            readonly LeaseCollectionBalancerOptions options;

            public TableLeaseManager(TableClient table, AzureStorageOrchestrationService service, AzureStorageOrchestrationServiceSettings settings, string storageAccountName, LeaseCollectionBalancerOptions options)
            {
                this.partitionTable = table;
                this.service = service;
                this.settings = settings;
                this.storageAccountName = storageAccountName;
                this.workerName = this.settings.WorkerId;
                this.drainTask = new Dictionary<string, Task>();
                this.options = options;
            }

            /// <summary>
            /// Reads the partition table to determine the tasks the worker should do. Used by the PartitionManagerLoop.
            /// </summary>
            /// <returns> The <see cref="ReadTableReponse"/> incidates whether the worker is waiting to claim a stolen lease from other workers or working on releasing any ownership leases.</returns>
            /// <exception cref="RequestFailedException">will be thrown if failed to update the partition table. Partition Manager loop will catch it and re-read the table to get the latest information.</exception>
            public async Task<ReadTableReponse> ReadAndWriteTable()
            {
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = partitionTable.Query<TableLease>();
                var partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int ownershipLeaseCount = 0;

                foreach (TableLease partition in partitions)
                {
                    // In certain unhealthy situations, a worker may lose the lease without realizing it and continue listening
                    // for queue messages. We check for that case here and stop dequeuing messages if we discover that
                    // another worker currently owns the lease.
                    this.service.DropLostControlQueues(partition);

                    bool ClaimedLease = false;
                    bool StoleLease = false;
                    bool RenewedLease = false;
                    bool DrainedLease = false;
                    bool ReleasedLease = false;
                    ETag etag = partition.ETag;

                    ClaimedLease = TryClaimLease(partition);
                    string previousOwner = partition.CurrentOwner!;
                    CheckOtherWorkerLease(partition, partitionDistribution, response);
                    CheckOwnershipLease(partition, response);

                    // Update the table if the lease is claimed, stolen, renewed, drained or released.
                    if (ClaimedLease || StoleLease || RenewedLease || DrainedLease || ReleasedLease)
                    {
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            
                            if(ClaimedLease)
                            {
                                await this.service.OnTableLeaseAcquiredAsync(partition);
                            }
                            
                            this.LogHelper(partition, ClaimedLease, StoleLease, RenewedLease, DrainedLease, ReleasedLease, previousOwner);
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
                                    $"Failed to update { partition.RowKey} due to concurrency. Re-read the table to get the latest Etag.");
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
                                $"Unexpected error occurred when updating the table: {exception.Message}"
                                );   
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
                    async void CheckOwnershipLease(TableLease partition, ReadTableReponse response)
                    {
                        if (partition.CurrentOwner == this.workerName)
                        {
                            // Lease is not stolen by others.
                            if (partition.NextOwner == null)
                            {
                                ownershipLeaseCount++;
                            }
                            else// Lease is stolen by others.
                            {
                                response.WorkOnDrain = true;
                                // In case operations on dictionary and isDraining are not sync, always check first if we have any drain tasks in the dictionary.
                                bool isDrainTaskExists = this.drainTask.TryGetValue(partition.RowKey!, out Task? task);

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
                                            $"Successfully drained the in-memory messages of lease {partition.RowKey}. Lease will be released.");
                                       
                                        this.drainTask.Remove(partition.RowKey!);
                                        this.ReleaseLease(partition);
                                        ReleasedLease = true;
                                        await task;// catch exception for drain tasks
                                    }
                                }
                                else // If lease hasn't start draining, then drain it.
                                {
                                    this.DrainLease(partition, CloseReason.LeaseLost);
                                    DrainedLease = true;
                                }
                            }
                            // As long as the lease is not released, keep renewing the lease in case lease expired since it's still draining. 
                            if (partition.CurrentOwner != null)
                            {
                                this.RenewLease(partition);
                                RenewedLease = true;
                            }
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
                            StoleLease = true;
                            response.WaitForPartition = true;
                        }
                    }

                }

                // Balance partitions.
                try
                {
                    await this.BalanceLeases(partitionDistribution, partitions, ownershipLeaseCount, response);
                }
                // Exception for failed table update due to ETag expired. 
                // Re-read the table immediately to get a new ETag.
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    throw;
                }
                

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
            public async Task BalanceLeases(Dictionary<string, List<TableLease>> partitionDistribution, Pageable<TableLease> partitions, int ownershipLeaseCount, ReadTableReponse response)
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
                                leaseType: "", //leaseType empty. Because there is no leaseType in the table partition manager.
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
                            $"Failed to update {partition.RowKey} due to concurrency. Re-read the table to get the latest Etag.");
                            throw;
                        }
                        // Eat any exceptions during lease stealing.
                        catch (Exception exception)
                        {
                            this.settings.Logger.PartitionManagerError(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Unexpected error occurred in stealing {partition.RowKey} : {exception}"
                                );
                        }
                    }
                }
            }

            public void ClaimLease(TableLease lease)
            {
                lease.CurrentOwner = this.workerName;
                lease.NextOwner = null;
                lease.OwnedSince = DateTime.UtcNow;
                lease.LastRenewal = DateTime.UtcNow;
                lease.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
                lease.IsDraining = false;
            }

            public void DrainLease(TableLease lease, CloseReason reason)
            {
                Task task = this.service.DrainTableLeaseAsync(lease, reason);
                string partitionId = lease.RowKey!;
                this.drainTask.Add(partitionId, task);
                lease.IsDraining = true;
            }

            public void ReleaseLease(TableLease lease)
            {
                lease.IsDraining = false;
                lease.CurrentOwner = null;
            }

            public void RenewLease(TableLease lease)
            {
                lease.LastRenewal = DateTime.UtcNow;
                lease.ExpiresAt = DateTime.UtcNow.Add(this.options.LeaseInterval);
            }

            public void StealLease(TableLease lease)
            {
                lease.NextOwner = this.workerName;
            }
           
            // Log operations on partition table.
            void LogHelper(TableLease partition, bool ClaimedLease, bool StoleLease,bool RenewedLease, bool DrainedLease, bool ReleasedLease, string previousOwner )
            {
                if (ClaimedLease)
                {
                    this.settings.Logger.LeaseAcquisitionSucceeded(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partition.RowKey,
                        "");//leaseType empty cause there is no leaseType in table partition manager.
                }
                if (StoleLease)
                {
                    this.settings.Logger.LeaseStealingSucceeded(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        previousOwner,
                        "", //leaseType empty. Because there is no leaseType in the table partition manager.
                        partition.RowKey);
                }
                if (ReleasedLease)
                {
                    this.settings.Logger.LeaseRemoved(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partition.RowKey,
                        "",//Token empty cause there is no token in table partition manager.
                        "");//Leasetype empty cause there is no leaseType in table partition manager.
                }
                if (DrainedLease)
                {
                    this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partition.RowKey,
                    $"Starts draining the in-memory message of {partition.RowKey}.");
                }
                if (RenewedLease)
                {
                    this.settings.Logger.LeaseRenewalResult(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partition.RowKey,
                        true,
                        "",
                        "",
                        $"Successfully renewed the lease of " + partition.RowKey);
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
            /// Stops the partition manager, release all ownership leases.
            /// </summary>
            /// <returns>
            /// Returns <c>null</c> if the worker successfully shuts down.
            /// </returns>
            /// <exception cref="RequestFailedException">Failed to update the partition table due to concurrency. Throw this exception so the StopAync() can cathch it to re-read the table immediately.</exception>
            public async Task<bool> ShutDown()
            {
                Pageable<TableLease> partitions = this.partitionTable.Query<TableLease>();
                int ownershipLeaseCount = 0;
                foreach (TableLease partition in partitions)
                {
                    bool DrainedLease = false;
                    bool ReleasedLease = false;
                    bool RenewedLease = false;

                    if (partition.CurrentOwner == this.workerName)
                    {
                        ownershipLeaseCount++;
                        ETag etag = partition.ETag;

                        bool isDrainTaskExists = this.drainTask.TryGetValue(partition.RowKey!, out Task? task);
                        if (isDrainTaskExists)
                        {
                            // Check if draining process has been finished.If so, release the lease.
                            if (task.IsCompleted == true)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     this.storageAccountName,
                                     this.settings.TaskHubName,
                                     this.settings.WorkerId,
                                     partition.RowKey,
                                     $"Successfully drained the in-memory messages of lease {partition.RowKey} for shutdown. Lease will be released.");

                                this.drainTask.Remove(partition.RowKey!);
                                this.ReleaseLease(partition);
                                ReleasedLease = true;
                                ownershipLeaseCount--;
                                await task; // Catch any exceptions happened in the drain process.
                            }
                            else// If draining process is not finished, keep renewing the lease in case it's expired during the draining.
                            {
                                this.RenewLease(partition);
                                RenewedLease = true;
                            }
                        }
                        else 
                        {
                            this.DrainLease(partition, CloseReason.Shutdown);
                            this.RenewLease(partition);
                            RenewedLease = true;
                            DrainedLease = true;
                        }
                        
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            this.LogHelper(partition, false, false, RenewedLease, DrainedLease, ReleasedLease, "");
                            
                        }
                        // Catch exception of failed table update due to ETag expired. 
                        // Re-read the table immediately to get the latest ETag.
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Failed to update {partition.RowKey} due to concurrency. Re-read the table to get the latest Etag.");
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
                                $"Unexpected error occurred in shutdown when updating lease {partition.RowKey} : {exception}"
                                );
                        }
                    }

                }
                bool releasedAllLeases = (ownershipLeaseCount == 0);
                return releasedAllLeases;
            }

           
        }

        /// <summary>
        ///The Response class describes the behavior of the ReadandWrite method in the PartitionManager worker class. 
        ///If the virtual machine is about to drain and release a lease, the method sets the WorkonDrain flag to true. 
        ///If the VM is going to acquire another lease from other VM, it sets the waitforPartition flag to true. 
        ///When either of these flags is true, the sleep time of the VM changes to 1 second.
        /// </summary>
        class ReadTableReponse
        {
            //If set to true, it indicates that the VM is working on release lease. 
            public bool WorkOnDrain { get; set; } = false;
            
            //If set to true, it indicates that the VM is waiting for a lease to be released.
            public bool WaitForPartition { get; set; } = false;
        }

        //only used for testing.
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
            partitionManagerCancellationSource.Dispose();
        }
    }
}
