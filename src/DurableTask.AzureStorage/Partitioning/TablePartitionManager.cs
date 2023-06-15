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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;


    /// <summary>
    /// Partition ManagerV3 based on the Azure Storage V3.  
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
            if(this.connectionString == null)
            {
                throw new Exception("Connection string is null. Managed identity is not supported in the table partition manager yet.");
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
        /// This method create a new instance of the class TableLeaseManager that represents the worker. 
        /// And then start the loop that the worker keeps operating on the table. 
        /// </summary>
        async Task IPartitionManager.StartAsync()
        {
            await Task.Factory.StartNew(() => this.PartitionManagerLoop(this.partitionManagerCancellationSource.Token));
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "", //Empty string as it does not target any particular partition, but rather only initiates the partition manager.
                $"Startes acquiring and balancing leases.");
        }

        async Task PartitionManagerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                TimeSpan timeToSleep = this.options.AcquireInterval;
                try
                {
                    ReadTableReponse response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.WorkOnRelease || response.WaitForPartition)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                }
                catch(RequestFailedException ex) when (ex.Status == 412)
                {
                    // if the worker failed to update the table, re-read the table immediately without waiting.
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                catch(Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                       this.storageAccountName,
                       this.settings.TaskHubName,
                       this.settings.WorkerId,
                       "",
                       $"Errors in partition manager reading the table : {exception}");
                }
                await Task.Delay(timeToSleep, token);
            }
        }

        /// <summary>
        /// This method will stop the partition manager. It first stops the task ReadAndWriteTable().
        /// And then start the Task ShutDown() until all the leases in the worker is drained. 
        /// </summary>
        async Task IPartitionManager.StopAsync()
        {
            this.partitionManagerCancellationSource.Cancel();
            
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Starts draining the in-memory messages of all ownership leases for shutdown.");

            bool isFinish = false;
            //Shutting down is to drain all the current leases and then release them.
            //Tthe worker checks table every 1 second to see if the realease of all ownership lease finishes to ensure timely updates.
            TimeSpan timeToSleep = TimeSpan.FromSeconds(1);

            //Waiting for finish draining all the ownership leases and release them. 
            while (!isFinish)
            {
                try
                {
                    isFinish = await this.tableLeaseManager.ShutDown();
                }
                //If the worker failed to update the table due to concurrency, re-read the table immediately without wait.
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                //Eat any other exceptions.
                catch(Exception exception)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.settings.WorkerId,
                        "",
                        $"Error in stopping partition manager : {exception}");
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
            readonly Dictionary<string, Task> tasks;
            readonly LeaseCollectionBalancerOptions options;

            public TableLeaseManager(TableClient table, AzureStorageOrchestrationService service, AzureStorageOrchestrationServiceSettings settings, string storageAccountName, LeaseCollectionBalancerOptions options)
            {
                this.partitionTable = table;
                this.service = service;
                this.settings = settings;
                this.storageAccountName = storageAccountName;
                this.workerName = this.settings.WorkerId;
                this.tasks = new Dictionary<string, Task>();
                this.options = options;
            }

            /// <summary>
            /// This method called in the PartitionManagerLoop. It reads the partition table and then determines the tasks the worker should do.
            /// </summary>
            /// <returns>
            /// Returns <c>null</c> if the worker successfully updates the table.
            /// </returns>
            /// <exception cref="RequestFailedException">Thrown if failed to update the table.
            /// </exception>
            public async Task<ReadTableReponse> ReadAndWriteTable()
            {
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = partitionTable.Query<TableLease>();
                var partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int ownershipLeasesNum = 0;

                foreach (TableLease partition in partitions)
                {
                    // Check to see if we're listening to any queues that we shouldn't be.
                    // If so, remove them from the OwnedControlQueues to stop listening to it.
                    this.service.DropLostControlQueues(partition);

                    bool isClaimedLease = false;
                    bool isStealedLease = false;
                    bool isRenewdLease = false;
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;
                    ETag etag = partition.ETag;

                    IsLeaseAvailableToClaim(partition);
                    string oldOwner = partition.CurrentOwner!;
                    CheckOtherWorkerLease(partition, partitionDistribution, response);
                    CheckOwnershipLease(partition, response);

                    // Update the table if the lease is claimed, stolen, renewed, drained or released.
                    if (isClaimedLease || isStealedLease || isRenewdLease || isDrainedLease || isReleasedLease)
                    {
                        if (isStealedLease)
                        {
                            this.settings.Logger.AttemptingToStealLease(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    oldOwner,
                                    "",//leaseType empty cause there is no leaseType in table partition manager.
                                    partition.RowKey);
                        }
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            
                            if(isClaimedLease)
                            {
                                await this.service.TableLeaseAcquiredAsync(partition);
                                this.settings.Logger.LeaseAcquisitionSucceeded(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    "" );//leaseType empty cause there is no leaseType in table partition manager.
                            }
                            if(isStealedLease)
                            {
                                this.settings.Logger.LeaseStealingSucceeded(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    oldOwner,
                                    "", //leaseType empty. Because there is no leaseType in the table partition manager.
                                    partition.RowKey);
                            }
                            if(isReleasedLease)
                            {
                                this.settings.Logger.LeaseRemoved(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    "",//Token empty cause there is no token in table partition manager.
                                    "");//Leasetype empty cause there is no leaseType in table partition manager.
                            }
                            if(isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} starts draining {partition.RowKey}.");
                            }
                            if(isRenewdLease)
                            {
                                this.settings.Logger.LeaseRenewalResult(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    true,
                                    "",
                                    "",
                                    $"Successfully renewed the lease of "+ partition.RowKey);
                            }
                        }
                        //Exception will be thrown due to concurrency.
                        //Dequeue loop will catch it and re-read the table immediately to get the latest ETag.
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
                                $"{exception.Message}"
                                );   
                        }
                    }

                    //Check if a lease is available for the current worker.
                    void IsLeaseAvailableToClaim(TableLease partition)
                    {
                        //Check if the partition is empty, expired or stolen by the current worker and claim it.
                        bool isEmptyLease = (partition.CurrentOwner == null && partition.NextOwner == null);
                        // test agagain with no nextowner 
                        bool isExpired = (DateTime.UtcNow >= partition.ExpiresAt);
                        bool isStolenByMe = (partition.CurrentOwner == null && partition.NextOwner == this.workerName);

                        if (isEmptyLease || isExpired || isStolenByMe)
                        {
                            this.ClaimLease(partition);
                            isClaimedLease = true;
                        }
                    }
                    
                    //Check ownership lease. 
                    // If the lease is not stolen by others, renew it.
                    // If the lease is stolen by others, check if starts drainning or if finishes drainning.
                    void CheckOwnershipLease(TableLease partition, ReadTableReponse response)
                    {
                        if (partition.CurrentOwner == this.workerName)
                        {
                            if (partition.NextOwner == null)
                            {
                                ownershipLeasesNum++;
                            }
                            else
                            {
                                response.WorkOnRelease = true;

                                if (partition.IsDraining)
                                {
                                    if (this.tasks.TryGetValue(partition.RowKey!, out Task? task) && task.IsCompleted == true)
                                    {
                                        this.ReleaseLease(partition);
                                        isReleasedLease = true;
                                    }
                                }
                                else
                                {
                                    this.DrainLease(partition, CloseReason.LeaseLost);
                                    isDrainedLease = true;
                                }
                            }
                            if (partition.CurrentOwner != null)
                            {
                                this.RenewLease(partition);
                                isRenewdLease = true;
                            }
                        }
                    }

                    //If the lease is other worker's lease. Store it to the dictionary for future balance.
                    //If the other worker is shutting down, steal the lease.
                    void CheckOtherWorkerLease(TableLease partition, Dictionary<string, List<TableLease>> partitionDistribution, ReadTableReponse response)
                    {
                        bool isOtherWorkerCurrentLease = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == false);
                        bool isAnyWorkerFutureLease = (partition.CurrentOwner != this.workerName && partition.NextOwner != null);
                        bool isOtherWorkerShutDownLease = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == true);

                        //If the lease is other worker's current lease, add partition to the dictionary with CurrentOwner as key.
                        if (isOtherWorkerCurrentLease)
                        {
                            string currentOwner = partition.CurrentOwner!;
                            if (partitionDistribution.ContainsKey(currentOwner))
                            {
                                partitionDistribution[currentOwner].Add(partition);
                            }
                            else
                            {
                                partitionDistribution.Add(currentOwner, new List<TableLease> { partition });
                            }
                        }

                        // If other workers' lease is stolen, suppose lease tranfer could finish successfully, and add partition to the dictionary with NextOwner as key. 
                        if (isAnyWorkerFutureLease)
                        {
                            string nextOwner = partition.NextOwner!;
                            //If the NextOwner of the lease is the current worker, just plus 1 to the ownershipLeasesNum.
                            if (nextOwner == this.workerName)
                            {
                                ownershipLeasesNum++;
                                response.WaitForPartition = true;
                            }
                            //If the lease is stolen by other workers, add it to the partitionDistribution dictionary with NextOwner as key.
                            else
                            {
                                if (partitionDistribution.ContainsKey(nextOwner))
                                {
                                    partitionDistribution[nextOwner].Add(partition);
                                }
                                else
                                {
                                    partitionDistribution.Add(nextOwner, new List<TableLease> { partition });
                                }
                            }
                        }

                        //If the lease belongs to a worker that is shutting down, steal it.
                        if (isOtherWorkerShutDownLease)
                        {
                            oldOwner = partition.CurrentOwner!;
                            this.StealLease(partition);
                            isStealedLease = true;
                            response.WaitForPartition = true;
                        }
                    }

                }

                // Balancing leases.
                try
                {
                    await this.LeaseBalancer(partitionDistribution, partitions, ownershipLeasesNum, response);
                }
                catch (RequestFailedException ex) when (ex.Status == 412)
                {
                    throw;
                }
                

                return response;

            }

            //This method is for the balance process.
            //First check if there is any other active worker, if not, skip the balance process.
            //If is, then calculate the average number of leases per worker for balance.
            //If ownership lease are less than the average number, then steal leases from other workers whose leases are more than average number.
            //Exception will be thrown if the update operation fails to get the latest information.
            //Any other exceptions will be caught to log. 
            public async Task LeaseBalancer(Dictionary<string, List<TableLease>> partitionDistribution, Pageable<TableLease> partitions, int ownershipLeasesNum, ReadTableReponse response)
            {
                if (partitionDistribution.Count != 0)
                {
                    int averageLeases = (partitions.Count()) / (partitionDistribution.Count + 1);
                    
                    //If the number of leases per worker is 0, then set it to 1.
                    if (averageLeases == 0)
                    {
                        averageLeases = 1;
                    }
                    
                    int numToSteal = averageLeases - ownershipLeasesNum;
                    if (numToSteal <= 0)
                    {
                        // No need to steal, just stop the balancing
                        return;
                    }

                    foreach (KeyValuePair<string, List<TableLease>> pair in partitionDistribution)
                    {
                        // Already stole enough partitions, end the loop and exit the method.
                        if (numToSteal == 0)
                        {
                            break;
                        }

                        // Only steal leases from workers who have more leases than average.
                        int currentWorkerOwnershipLeases = pair.Value.Count;
                        if (currentWorkerOwnershipLeases <= averageLeases)
                        {
                            continue;
                        }
                        
                        foreach (TableLease partition in pair.Value)
                        {
                            if(numToSteal > 0 && currentWorkerOwnershipLeases > averageLeases)
                            {
                                numToSteal--;
                                currentWorkerOwnershipLeases--;
                                ETag etag = partition.ETag;
                                string oldOwner = partition.CurrentOwner!;
                                this.StealLease(partition);
                                try
                                {
                                    await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                                    this.settings.Logger.LeaseStealingSucceeded(
                                        this.storageAccountName,
                                        this.settings.TaskHubName,
                                        this.settings.WorkerId,
                                        oldOwner,
                                        "", //leaseType empty. Because there is no leaseType in the table partition manager.
                                        partition.RowKey);
                                    response.WaitForPartition = true;
                                }
                                //Exception will be thrown due to concurrency.
                                //Dequeue loop will catch it and re-read the table immediately to get the latest ETag.
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
                                        $"Error in stealing {partition.RowKey} : {exception}"
                                        );
                                }
                            }
                            else
                            {
                                break;
                            }
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
                lease.IsDraining = true;
                var task = Task.Run(() => this.service.TableLeaseDrainAsync(lease, reason));
                string partitionId = lease.RowKey!;
                if (this.tasks.ContainsKey(partitionId))
                {
                    this.tasks[partitionId] = task;
                }
                else
                {
                    this.tasks.Add(partitionId, task);
                }
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

            /// <summary>
            /// Used to stop the partition manager. It first completes all ownership leases and then stops the partition manager.
            /// </summary>
            /// <returns>
            /// Returns <c>null</c> if the worker successfully updates the table.
            /// </returns>
            /// <exception cref="RequestFailedException">Thrown if failed to update the table.
            /// </exception>
            public async Task<bool> ShutDown()
            {
                Pageable<TableLease> partitions = this.partitionTable.Query<TableLease>();
                int ownershipLeasesNum = 0;
                foreach (TableLease partition in partitions)
                {
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;
                    bool isRenewedLease = false;

                    if (partition.CurrentOwner == this.workerName)
                    {
                        ownershipLeasesNum++;
                        ETag etag = partition.ETag;
                        
                        if (partition.IsDraining)
                        {
                            if (this.tasks.TryGetValue(partition.RowKey!, out Task? task) && task.IsCompleted == true)
                            {
                                this.ReleaseLease(partition);
                                isReleasedLease = true;
                                ownershipLeasesNum--;
                            }
                            else
                            {
                                this.RenewLease(partition);
                                isRenewedLease = true;
                            }
                        }
                        else
                        {
                            this.DrainLease(partition, CloseReason.Shutdown);
                            this.RenewLease(partition);
                            isRenewedLease = true;
                            isDrainedLease = true;
                        }
                        
                        try
                        {
                            await this.partitionTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if (isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     this.storageAccountName,
                                     this.settings.TaskHubName,
                                     this.settings.WorkerId,
                                     partition.RowKey,
                                     $"Starts draining the in-memory message of {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.LeaseRemoved(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    "",//Token empty cause there is no token in table partition manager.
                                    "");//Leasetype empty cause there is no leaseType in table partition manager.
                            }
                            if (isRenewedLease)
                            {
                                this.settings.Logger.LeaseRenewalResult(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    true,
                                    "",
                                    "",
                                    $"Succeessfully renewed the lease of {partition.RowKey} during shutdown.");
                            }
                            
                        }
                        //Exception will be thrown due to concurrency.
                        //Dequeue loop will catch it and re-read the table immediately to get the latest ETag
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
                                $"Error in shutdown lease {partition.RowKey} : {exception}"
                                );
                        }
                    }

                }
                var isReleasedAllLease = (ownershipLeasesNum == 0);
                return isReleasedAllLease;
            }
        }

        /// <summary>
        ///The Response class describes the behavior of the ReadandWrite method in the PartitionManager worker class. 
        ///If the virtual machine is about to be drained, the method sets the WorkonRelease flag to true. 
        ///If the VM is going to acquire another lease, it sets the waitforPartition flag to true. 
        ///When either of these flags is true, the sleep time of the VM changes from 15 seconds to 1 second.
        /// </summary>
        class ReadTableReponse
        {
            //If set to true, it indicates that the VM is working on release lease. 
            public bool WorkOnRelease { get; set; } = false;
            
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
