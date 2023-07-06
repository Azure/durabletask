﻿//  ----------------------------------------------------------------------------------
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
    /// Partition Manager V3 based on Azure Storage Tables.
    /// </summary>
    /// <remarks>
    /// Previous partition managers were based on Azure Storage blobs, which are more complex to manage
    /// and have more expensive per-transaction costs, particularly in Azure Storage V2 accounts. This
    /// table storage-based partition manager aims to be both simpler, cheaper, easier to debug, and 
    /// faster when it comes to rebalancing.
    /// </remarks>
    sealed class TablePartitionManager : IPartitionManager, IDisposable
    {
        // Used for logging purposes only, indicating that a particular parameter (usually a partition ID) doesn't apply.
        const string NotApplicable = "";

        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource gracefulShutdownTokenSource;
        readonly CancellationTokenSource forcefulShutdownTokenSource;
        readonly string storageAccountName;
        readonly TableClient partitionTable;
        readonly TableLeaseManager tableLeaseManager;
        readonly LeaseCollectionBalancerOptions options;

        Task partitionManagerTask;

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
            this.storageAccountName = this.azureStorageClient.TableAccountName;

            string connectionString = this.settings.StorageConnectionString ?? this.settings.StorageAccountDetails.ConnectionString;
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("A connection string is required to use the table partition manager. Managed identity is not yet supported when using the table partition manager. Please provide a connection string to your storage account in your app settings.");
            }

            this.options = new LeaseCollectionBalancerOptions
            {
                AcquireInterval = this.settings.LeaseAcquireInterval,
                LeaseInterval = this.settings.LeaseInterval,
                ShouldStealLeases = true
            };
            this.gracefulShutdownTokenSource = new CancellationTokenSource();
            this.forcefulShutdownTokenSource = new CancellationTokenSource();
            this.partitionTable = new TableClient(connectionString, this.settings.PartitionTableName);
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings, this.storageAccountName, this.options);
            this.partitionManagerTask = Task.CompletedTask;
        }


        /// <summary>
        /// Starts the partition management loop for the current worker.
        /// </summary>
        Task IPartitionManager.StartAsync()
        {
            // Run the partition manager loop in the background
            this.partitionManagerTask = this.PartitionManagerLoop(
                this.gracefulShutdownTokenSource.Token,
                this.forcefulShutdownTokenSource.Token);
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
        /// Loop will end after shutdown is requested and the worker successfully released all ownership leases.
        /// </summary>
        /// <param name="gracefulShutdownToken">Cancellation of this token initiates the graceful shutdown process of the partition manager.</param>
        /// <param name="forcefulShutdownToken">Cancellation of this token forcefully aborts the partition manager loop.</param>
        async Task PartitionManagerLoop(CancellationToken gracefulShutdownToken, CancellationToken forcefulShutdownToken)
        {
            const int MaxFailureCount = 10;

            int consecutiveFailureCount = 0;
            bool isShuttingDown = gracefulShutdownToken.IsCancellationRequested;
            
            while (true)
            {
                TimeSpan timeToSleep = this.options.AcquireInterval;
                
                try
                {
                    ReadTableReponse response = await this.tableLeaseManager.ReadAndWriteTableAsync(isShuttingDown, forcefulShutdownToken);

                    // If shutdown is requested and already released all ownership leases, then break the loop. 
                    if (isShuttingDown && response.ReleasedAllLeases)
                    {
                        this.settings.Logger.PartitionManagerInfo(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            this.settings.WorkerId,
                            partitionId: NotApplicable,
                            "Successfully released all ownership leases for shutdown.");
                        break;
                    }
                    
                    // Poll more frequently if we are draining a partition or waiting for a partition to be released
                    // by another worker. This is a temporary state and we want to try and be as responsive to updates
                    // as possible to minimize the time spent in this state, which is effectively downtime for orchestrations.
                    if (response.IsDrainingPartition || response.IsWaitingForPartitionRelease)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                    
                    consecutiveFailureCount = 0;
                }
                // Exception Status 412 represents an out of date ETag. We already logged this.
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    consecutiveFailureCount++;
                }
                // Eat any unexpected exceptions.
                catch (Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        partitionId: NotApplicable,
                        details: $"Unexpected error occurred while trying to manage table partition leases: {exception}");
                    
                    consecutiveFailureCount++;
                }

                // If table update failed, we re-read the table immediately to obtain the latest ETag.
                // In the case of too many successive failures, we the wait before retrying to prevent excessive logs.
                if (consecutiveFailureCount > 0 && consecutiveFailureCount < MaxFailureCount)
                {
                    timeToSleep = TimeSpan.FromSeconds(0);
                }

                try
                {
                    if (isShuttingDown || forcefulShutdownToken.IsCancellationRequested)
                    {
                        // If shutdown is required, we sleep for a short period to ensure a relatively fast shutdown process
                        await Task.Delay(TimeSpan.FromSeconds(1), forcefulShutdownToken);
                    }
                    else
                    {
                        // Normal case: the amount of time we sleep varies depending on the situation.
                        await Task.Delay(timeToSleep, gracefulShutdownToken);
                    }
                }
                catch (OperationCanceledException) when (gracefulShutdownToken.IsCancellationRequested)
                {
                    // Shutdown requested, but we still need to release all leases
                    if (!isShuttingDown)
                    {
                        isShuttingDown = true;
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
            this.gracefulShutdownTokenSource.Cancel();
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: NotApplicable,
                "Started draining the in-memory messages of all owned control queues for shutdown.");

            // Wait 10 minutes for the partition manager to shutdown gracefully. Otherwise force a shutdown.
            var timeout = TimeSpan.FromMinutes(10);
            var timeoutTask = Task.Delay(Timeout.Infinite, this.forcefulShutdownTokenSource.Token);
            this.forcefulShutdownTokenSource.CancelAfter(timeout);
            await Task.WhenAny(this.partitionManagerTask, timeoutTask);

            if (timeoutTask.IsCompleted)
            {
                throw new TimeoutException(
                    $"Timed-out waiting for the partition manager to shut down. Timeout duration: {timeout}",
                    timeoutTask.Exception?.InnerException);
            }

            // Surface any unhandled exceptions
            await this.partitionManagerTask;

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId: NotApplicable,
                "Table partition manager stopped successfully.");
        }

        async Task IPartitionManager.CreateLeaseStore()
        {
            await this.partitionTable.CreateIfNotExistsAsync();
        }

        async Task IPartitionManager.CreateLease(string partitionId)
        {
            try
            {
                var newPartitionEntry = new TableLease { RowKey = partitionId };
                await this.partitionTable.AddEntityAsync(newPartitionEntry);

                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partitionId,
                    "Successfully added the partition to the partition table.");
            }
            catch (RequestFailedException e) when (e.Status == 409 /* The specified entity already exists. */)
            {
                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partitionId,
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
            /// If the shutdown is requested, then stop regular claim and balance process, and call method `TryDrainAndReleaseAllPartitions` to release all ownership leases.
            /// </summary>
            /// <param name="isShuttingDown">Indicates that the partition manager is in the process of shutting down.</param>
            /// <param name="forcefulShutdownToken">Cancellation of this token forcefully aborts the partition manager loop.</param>
            /// <returns> The <see cref="ReadTableReponse"/> incidates whether the worker is waiting to claim a stolen lease from other workers or working on releasing any ownership leases.</returns>
            /// <exception cref="RequestFailedException">will be thrown if failed to update the partition table. Partition Manager loop will catch it and re-read the table to get the latest information.</exception>
            public async Task<ReadTableReponse> ReadAndWriteTableAsync(bool isShuttingDown, CancellationToken forcefulShutdownToken)
            {
                var response = new ReadTableReponse();

                IReadOnlyList<TableLease> partitions = this.partitionTable.Query<TableLease>(cancellationToken: forcefulShutdownToken).ToList();
                var partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int ownershipLeaseCount = 0;

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

                    // String previousOwner is for the steal process logs. Only used for stealing leases of any worker which is in shutdown process in this loop.
                    string previousOwner = partition.CurrentOwner ?? this.workerName;

                    if (!isShuttingDown)
                    {
                        claimedLease = this.TryClaimLease(partition);

                        this.CheckOtherWorkersLeases(
                            partition,
                            partitionDistribution,
                            response,
                            ref ownershipLeaseCount,
                            ref previousOwner,
                            ref stoleLease);

                        this.RenewOrReleaseMyLease(
                            partition,
                            response,
                            ref ownershipLeaseCount,
                            ref releasedLease,
                            ref drainedLease,
                            ref renewedLease);
                    }
                    else
                    {
                        // If shutdown is requested, we drain and release all ownership partitions.
                        this.TryDrainAndReleaseAllPartitions(
                            partition,
                            response,
                            ref ownershipLeaseCount,
                            ref releasedLease,
                            ref drainedLease,
                            ref renewedLease);
                    }

                    // Save updates to the partition entity if the lease is claimed, stolen, renewed, drained or released.
                    if (claimedLease || stoleLease || renewedLease || drainedLease || releasedLease)
                    {
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, TableUpdateMode.Replace, forcefulShutdownToken);
                        }
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

                        if (claimedLease)
                        {
                            // Notify the orchestration session manager that we acquired a lease for one of the partitions.
                            // This will cause it to start reading control queue messages for that partition.
                            await this.service.OnTableLeaseAcquiredAsync(partition);
                        }

                        this.LogHelper(partition, claimedLease, stoleLease, renewedLease, drainedLease, releasedLease, previousOwner);
                    }
                }

                // Separately from lease acquisition/renewal, make sure the partitions are evenly balanced across workers.
                await this.BalanceLeasesAsync(partitionDistribution, partitions, ownershipLeaseCount, response, forcefulShutdownToken);

                // If shutdown is requested and the worker releases all ownership leases, then set the response.IsReleasesAllLease to true to notify the partitionManagerLoop to stop.
                if (isShuttingDown)
                {
                    response.ReleasedAllLeases = ownershipLeaseCount == 0;
                }

                return response;
            }


            /// <summary>
            /// Checks to see if a lease is available to be claimed by the current worker.
            /// </summary>
            /// <param name="partition">The partition to check.</param>
            bool TryClaimLease(TableLease partition)
            {
                //Check if the partition is empty, expired or stolen by the current worker and claim it.
                bool isEmptyLease = partition.CurrentOwner == null && partition.NextOwner == null;
                bool isExpired = DateTime.UtcNow >= partition.ExpiresAt;
                bool isStolenByMe = partition.CurrentOwner == null && partition.NextOwner == this.workerName;

                bool isLeaseAvailable = isEmptyLease || isExpired || isStolenByMe;
                if (isLeaseAvailable)
                {
                    this.ClaimLease(partition);
                    return true;
                }

                return false;
            }

            // Check ownership lease. 
            // If the lease is not stolen by others, renew it.
            // If the lease is stolen by others, check if starts drainning or if finishes drainning.
            void RenewOrReleaseMyLease(
                TableLease partition,
                ReadTableReponse response,
                ref int ownershipLeaseCount,
                ref bool releasedLease,
                ref bool drainedLease,
                ref bool renewedLease)
            {
                if (partition.CurrentOwner != this.workerName)
                {
                    // We don't own this lease, so nothing for us to do here.
                    return;
                }

                if (partition.NextOwner == null)
                {
                    // We still own the lease and nobody is trying to steal it.
                    ownershipLeaseCount++;
                }
                else
                {
                    // Somebody is trying to steal the lease. Start draining so that we can release it.
                    response.IsDrainingPartition = true;

                    // In case operations on dictionary and isDraining are out of sync, always check first if we have any drain tasks in the dictionary.
                    // NOTE: This logic is largely redundant with logic in TryDrainAndReleaseAllPartitions. Changes to one
                    //       method may need to be copied to the other method.
                    if (this.backgroundDrainTasks.TryGetValue(partition.RowKey!, out Task? drainTask))
                    {
                        // If the drain task has finished, we can safely release the lease.
                        if (drainTask.IsCompleted)
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
                            drainTask.GetAwaiter().GetResult(); // surface any exceptions that may have occurred during the drain
                            return;
                        }
                    }
                    else
                    {
                        // Start the draining process. This will add a background task to the backgroundDrainTasks dictionary.
                        this.DrainPartition(partition, CloseReason.LeaseLost);
                        drainedLease = true;
                    }
                }

                // Keep renewing the lease until the drain task completes.
                this.RenewLease(partition);
                renewedLease = true;
            }

            //If the lease is other worker's lease. Store it to the dictionary for future balance.
            //If the other worker is shutting down, steal the lease.
            void CheckOtherWorkersLeases(
                TableLease partition,
                Dictionary<string, List<TableLease>> partitionDistribution,
                ReadTableReponse response,
                ref int ownershipLeaseCount,
                ref string previousOwner,
                ref bool stoleLease)
            {
                bool isOtherWorkersLease = partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == false;
                bool isOtherWorkerStealingLease = partition.CurrentOwner != this.workerName && partition.NextOwner != null;
                bool isOwnerShuttingDown = partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == true;

                string owner;

                //If the lease is other worker's current lease, add partition to the dictionary with CurrentOwner as key.
                if (isOtherWorkersLease)
                {
                    owner = partition.CurrentOwner!;
                    AddToDictionary(partition, partitionDistribution, owner);
                }

                // If other worker's lease is stolen, assume the lease tranfer will finish successfully and add partition to the dictionary with NextOwner as key. 
                if (isOtherWorkerStealingLease)
                {
                    owner = partition.NextOwner!;

                    // If the lease was stolen by _this_ worker, we increase its currently owned lease count.
                    if (owner == this.workerName)
                    {
                        ownershipLeaseCount++;
                        response.IsWaitingForPartitionRelease = true;
                    }
                    // If the lease is stolen by other workers, add it to the partitionDistribution dictionary with NextOwner as key.
                    else
                    {
                        AddToDictionary(partition, partitionDistribution, owner);
                    }
                }

                // If the lease belongs to a worker that is shutting down, and it's not being stolen by another worker, steal it.
                if (isOwnerShuttingDown)
                {
                    previousOwner = partition.CurrentOwner!;
                    this.StealLease(partition);
                    stoleLease = true;
                    response.IsWaitingForPartitionRelease = true;
                }
            }

            // Method for draining and releasing all ownership partitions.
            // This method will only be called when shutdown is requested.
            void TryDrainAndReleaseAllPartitions(
                TableLease partition,
                ReadTableReponse response,
                ref int ownershipLeaseCount,
                ref bool releasedLease,
                ref bool drainedLease,
                ref bool renewedLease)
            {
                response.IsDrainingPartition = true;

                if (partition.CurrentOwner != this.workerName)
                {
                    // If the lease is not owned by this worker, we don't need to drain it.
                    return;
                }

                ownershipLeaseCount++;

                // NOTE: This logic is largely redundant with logic in RenewOrReleaseMyLease. Changes to one
                //       method may need to be copied to the other method.
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

                        drainTask.GetAwaiter().GetResult(); // Surface any exceptions from the drain process.
                    }
                    else // If draining process is ongoing, we keep renewing the lease to prevent it from expiring
                    {
                        this.RenewLease(partition);
                        renewedLease = true;
                    }
                }
                else
                {
                    this.DrainPartition(partition, CloseReason.Shutdown);
                    this.RenewLease(partition);
                    renewedLease = true;
                    drainedLease = true;
                }
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
            async Task BalanceLeasesAsync(
                Dictionary<string, List<TableLease>> partitionDistribution,
                IReadOnlyList<TableLease> partitions,
                int ownershipLeaseCount,
                ReadTableReponse response,
                CancellationToken forceShutdownToken)
            {
                if (partitionDistribution.Count == 0)
                {
                    // No partitions to be balanced.
                    return;
                }

                int averageLeasesCount = partitions.Count / (partitionDistribution.Count + 1);
                if (averageLeasesCount < ownershipLeaseCount)
                {
                    // Partitions are already balanced across all workers.
                    return;
                }

                // Check all the other active workers when my ownership lease number is not larger than the average number.
                foreach (IReadOnlyList<TableLease> ownedPartitions in partitionDistribution.Values)
                {
                    int numLeasesToSteal = averageLeasesCount - ownershipLeaseCount;
                    if (numLeasesToSteal < 0)
                    {
                        // The current worker already has enough partitions.
                        break;
                    }

                    // Only steal leases from takshub workers who have more leases than average.
                    // If this task hub worker's lease num is smaller or equal to the average number, just skip this worker.
                    int numExcessiveLease = ownedPartitions.Count - averageLeasesCount;
                    if (numExcessiveLease <= 0)
                    {
                        continue;
                    }

                    // The balancing condition requires that the differences in the number of leases assigned to each worker should not exceed 1, if the total number of partitions is not evenly divisible by the number of active workers.
                    // Thus, the maximum number of leases a worker can own is the average number of leases per worker plus one in this case.
                    // If a worker has more than one lease difference than average and _this_ worker has not reached the maximum, it should steal an additional lease.
                    if (numLeasesToSteal == 0 && numExcessiveLease > 1)
                    {
                        numLeasesToSteal = 1;
                    }

                    numLeasesToSteal = Math.Min(numLeasesToSteal, numExcessiveLease);
                    for (int i = 0; i < numLeasesToSteal; i++)
                    {
                        ownershipLeaseCount++;
                        TableLease partition = ownedPartitions[i];
                        ETag etag = partition.ETag;
                        string previousOwner = partition.CurrentOwner!;
                        this.StealLease(partition);

                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(
                                partition,
                                etag,
                                TableUpdateMode.Replace,
                                forceShutdownToken);
                            
                            this.settings.Logger.LeaseStealingSucceeded(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                previousOwner,
                                leaseType: NotApplicable,
                                partitionId: partition.RowKey);
                            
                            response.IsWaitingForPartitionRelease = true;
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412 /* ETag conflict */)
                        {
                            this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Failed to update table entry due to an Etag mismatch. Failed ETag value: '{etag}'");

                            // Another worker already modified this partition entry. Let the exception bubble up to the main
                            // loop, which will re-read the table immediately to get the latest updates.
                            throw;
                        }
                        catch (Exception exception)
                        {
                            // Eat any exceptions during lease stealing because we want to keep iterating through the partition list.
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

            void DrainPartition(TableLease lease, CloseReason reason)
            {
                Task task = this.service.DrainTablePartitionAsync(lease, reason);
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
            static void AddToDictionary(TableLease partition, Dictionary<string, List<TableLease>> partitionDistribution, string owner)
            {
                if (partitionDistribution.TryGetValue(owner, out List<TableLease> ownedPartitions))
                {
                    ownedPartitions.Add(partition);
                }
                else
                {
                    partitionDistribution.Add(owner, new List<TableLease> { partition });
                }
            }
        }

        /// <summary>
        /// The Response class describes the behavior of the ReadandWrite method in the PartitionManager worker class. 
        /// If the worker is about to drain and release a lease, the method sets the WorkonDrain flag to true. 
        /// If the worker is going to acquire another lease from other worker, it sets the waitforPartition flag to true. 
        /// When either of these flags is true, the sleep time of the worker changes to 1 second.
        /// </summary>
        class ReadTableReponse
        {
            /// <summary>
            /// True if the worker is working on release lease. 
            /// </summary>
            public bool IsDrainingPartition { get; set; } = false;

            /// <summary>
            /// True if the worker is waiting for a lease to be released.
            /// </summary>
            public bool IsWaitingForPartitionRelease { get; set; } = false;

            /// <summary>
            /// True if the worker successfully released all ownership leases for shutdown.
            /// </summary>
            public bool ReleasedAllLeases { get; set; } = false;
        }

        //internal used for testing
        internal void SimulateUnhealthyWorker(CancellationToken testToken)
        {
            _ = this.PartitionManagerLoop(
                gracefulShutdownToken: testToken,
                forcefulShutdownToken: CancellationToken.None);
        }

        //internal used for testing
        internal void KillLoop()
        {
            this.gracefulShutdownTokenSource.Cancel();
        }

        public void Dispose()
        {
            this.gracefulShutdownTokenSource.Dispose();
            this.forcefulShutdownTokenSource.Dispose();
        }
    }
}
