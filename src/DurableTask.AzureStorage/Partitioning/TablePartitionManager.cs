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
    /// Partition ManagerV3 is based on the Table storage.  
    /// </summary>
    sealed class TablePartitionManager : IPartitionManager, IDisposable
    {
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource partitionManagerCancellationSource;
        readonly string partitionTableName;
        readonly string storageAccountName;
        TableServiceClient tableServiceClient;
        TableClient partitionTable;
        TableLeaseManager tableLeaseManager;

        /// <summary>
        /// constructor to initiate new instances of TablePartitionManager
        /// </summary>
        /// <param name="azureStorageClient"></param>
        /// <param name="service"></param>
        /// <param name="settings"></param>
        public TablePartitionManager(
            AzureStorageClient azureStorageClient,
            AzureStorageOrchestrationService service,
            AzureStorageOrchestrationServiceSettings settings)
        {
            this.azureStorageClient = azureStorageClient;
            this.service = service;
            this.settings = settings;
            this.partitionTableName = this.settings.PartitionTableName;
            this.storageAccountName = this.azureStorageClient.TableAccountName;
            this.partitionManagerCancellationSource = new CancellationTokenSource();
            this.tableServiceClient = new TableServiceClient(this.settings.StorageConnectionString);
            this.partitionTable = tableServiceClient.GetTableClient(this.partitionTableName);
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings, this.storageAccountName);
        }

        
        /// <summary>
        /// This method create a new instance of the class TableLeaseManager that represents the worker. 
        /// And then start the loop that the worker keeps operating on the table. 
        /// </summary>
        /// <returns></returns>
        async Task IPartitionManager.StartAsync()
        {
            await Task.Factory.StartNew(() => this.PartitionManagerLoop(this.partitionManagerCancellationSource.Token));
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",//empty partition id
                $"Worker {this.settings.WorkerId} starts working on acquiring and balancing leases.");
        }

        async Task PartitionManagerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                TimeSpan timeToSleep = this.settings.LeaseRenewInterval;
                try
                {
                    ReadTableReponse response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.WorkOnRelease || response.WaitForPartition)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                }
                catch
                {
                    // if the worker failed to update the table, re-read the table immediately without waiting.
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                Task.Delay(timeToSleep, token).Wait();
            }
        }

        /// <summary>
        /// This method will stop the partition manager. It first stops the task ReadAndWriteTable().
        /// And then start the Task ShutDown() until all the leases in the worker is drained. 
        /// </summary>
        /// <returns></returns>
        async Task IPartitionManager.StopAsync()
        {
            this.partitionManagerCancellationSource.Cancel();
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} strats draining all ownership leases.");

            bool shouldRetry = false;
            //Shutting down is to drian all the current leases and then release them.
            //Thus the worker checks table every one sec to ensure timely updates.
            TimeSpan timeToSleep = TimeSpan.FromSeconds(1);

            while (!shouldRetry)
            {
                try
                {
                    shouldRetry = await this.tableLeaseManager.ShutDown();
                }
                catch
                {
                    //if the worker fails to update the table, re-read the table immediately withou wait.
                    timeToSleep = TimeSpan.FromSeconds(0);
                }
                await Task.Delay(timeToSleep);
            };

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} releases all ownership leases.");
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
        /// <returns></returns>
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
            readonly TableClient myTable;
            readonly string storageAccountName;
            Dictionary<string, Task> tasks;

            public TableLeaseManager(TableClient table, AzureStorageOrchestrationService service, AzureStorageOrchestrationServiceSettings settings, string storageAccountName)
            {
                this.myTable = table;
                this.service = service;
                this.settings = settings;
                this.storageAccountName = storageAccountName;
                this.workerName = this.settings.WorkerId;
                this.tasks = new Dictionary<string, Task>();
            }

            public async Task<ReadTableReponse> ReadAndWriteTable()
            {
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                Dictionary<string, List<TableLease>> partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int leaseNum = 0;

                foreach (TableLease partition in partitions)
                {
                    // Check to see if we're listening to any queues that we shouldn't be
                    this.service.DropLostControlQueues(partition);

                    bool isClaimedLease = false;
                    bool isStealedLease = false;
                    bool isRenewdLease = false;
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    //Check if the partition is empty, expired or stolen by me and claim it.
                    bool isEmptyLease = (partition.CurrentOwner == null && partition.NextOwner == null);
                    bool isExpiresAt = (DateTime.UtcNow >= partition.ExpiresAt && partition.NextOwner == null);
                    bool isStolenByMe = (partition.CurrentOwner == null && partition.NextOwner == this.workerName);

                    ETag etag = partition.ETag;

                    if (isEmptyLease || isExpiresAt || isStolenByMe)
                    {
                        ClaimLease(partition);
                        isClaimedLease = true;
                    }

                    bool isOtherWorkerCurrentLease = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == false);
                    bool isAnyWorkerFutureLease = (partition.CurrentOwner != this.workerName && partition.NextOwner != null);
                    bool isOtherWorkerShutDownLease = (partition.CurrentOwner != this.workerName && partition.NextOwner == null && partition.IsDraining == true);

                    //If the lease is other worker's current lease, add it to the partitionDistribution dictionary for further balance.
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

                    
                    if (isAnyWorkerFutureLease)
                    {
                        string nextOwner = partition.NextOwner!;
                        //If the next owner is me, consider it as me for now and add to the leaseNum for further balance.
                        if (nextOwner == this.workerName)
                        {
                            leaseNum++;
                            response.WaitForPartition = true;
                        }
                        //If the lease is other worker's future lease, add it to the partitionDistribution dictionary for further balance.
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

                    //If the lease belongs to a worker that shuts down, steal it.
                    if (isOtherWorkerShutDownLease)
                    {
                        this.StealLease(partition);
                        isStealedLease = true;
                        response.WaitForPartition = true;
                    }

                    //If the lease is mine, check if it's stolen.
                    //If so drain and release it. If not, renew it.
                    if (partition.CurrentOwner == this.workerName)
                    {
                        if (partition.NextOwner == null)
                        {
                            leaseNum++;
                        }
                        else
                        {
                            response.WorkOnRelease = true;

                            if (partition.IsDraining == false)
                            {
                                this.DrainLease(partition, CloseReason.LeaseLost);
                                isDrainedLease = true;
                            }
                            else
                            {
                                if (this.tasks[partition.RowKey!].IsCompleted == true)
                                {
                                    this.ReleaseLease(partition);
                                    isReleasedLease = true;
                                }
                            }
                        }
                        if (partition.CurrentOwner != null)
                        {
                            this.RenewLease(partition);
                            isRenewdLease = true;
                        }
                    }

                    if (isClaimedLease || isStealedLease || isRenewdLease || isDrainedLease || isReleasedLease)
                    {
                        try
                        {
                            await this.myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if(isClaimedLease)
                            {
                                await this.service.TableLeaseAcquiredAsync(partition);
                                this.settings.Logger.PartitionManagerInfo(
                                    this.storageAccountName,
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    $"Worker {this.settings.WorkerId} acquires the lease of {partition.RowKey}.");
                            }
                            if (isStealedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} steals the lease of {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} releases the lease of {partition.RowKey}.");
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
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            throw ex;
                        }
                    }
                }

                //This if condition is for the balance process.
                //First check if there is any other worker, if not, skip the balance process.
                //If so, then calculate the number of leases per worker for balance.
                //If the number of leases per worker is 0, then set it to 1.
                //If owned lease are less than the balancing number, then steal lease from other workers whose lease is more than balancing number.
                if (partitionDistribution.Count != 0)
                {
                    int numLeasePerWorkerForBalance = (partitions.Count()) / (partitionDistribution.Count + 1);
                    if (numLeasePerWorkerForBalance == 0)
                    {
                        numLeasePerWorkerForBalance = 1;
                    }
                    int numOfLeaseToSteal = numLeasePerWorkerForBalance - leaseNum;

                    while (numOfLeaseToSteal > 0)
                    {
                        int n = 0;
                        foreach (KeyValuePair<string, List<TableLease>> pair in partitionDistribution)
                        {
                            n++;
                            int currentWorkerNumofLeases = pair.Value.Count;
                            if (currentWorkerNumofLeases > numLeasePerWorkerForBalance)
                            {
                                foreach (TableLease partition in pair.Value)
                                {
                                    {
                                        numOfLeaseToSteal--;
                                        currentWorkerNumofLeases--;
                                        ETag etag = partition.ETag;
                                        StealLease(partition);
                                        try
                                        {
                                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                                            this.settings.Logger.PartitionManagerInfo(
                                                this.storageAccountName,
                                                this.settings.TaskHubName,
                                                this.settings.WorkerId,
                                                partition.RowKey,
                                                $"Worker {this.settings.WorkerId} steals the lease of {partition.RowKey}.");
                                            response.WaitForPartition = true;
                                        }
                                        catch (RequestFailedException ex) when (ex.Status == 412)
                                        {
                                            throw ex;
                                        }
                                    }

                                    if (currentWorkerNumofLeases == numLeasePerWorkerForBalance || numOfLeaseToSteal == 0) { break; }
                                }
                            }
                            if (numOfLeaseToSteal == 0) { break; }
                        }
                        if (n == partitionDistribution.Count) { break; }
                    }
                }
                return response;
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
                lease.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
            }

            public void StealLease(TableLease lease)
            {
                lease.NextOwner = this.workerName;
            }

            public async Task<bool> ShutDown()
            {
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                int leaseNum = 0;
                foreach (TableLease partition in partitions)
                {
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    if (partition.CurrentOwner == workerName)
                    {
                        leaseNum++;
                        ETag etag = partition.ETag;
                        
                        if (partition.IsDraining == false)
                        {
                            DrainLease(partition, CloseReason.Shutdown);
                            RenewLease(partition);
                            isDrainedLease = true;
                        }
                        else
                        {
                            if (tasks[partition.RowKey!].IsCompleted == true)
                            {
                                ReleaseLease(partition);
                                isReleasedLease = true;
                                leaseNum--;
                            }
                            else
                            {
                                RenewLease(partition);
                            }
                        }
                        
                        try
                        {
                            await this.myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if (isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     this.storageAccountName,
                                     this.settings.TaskHubName,
                                     this.settings.WorkerId,
                                     partition.RowKey,
                                     $"Worker {this.settings.WorkerId} starts draining the lease of{partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     this.storageAccountName,
                                     this.settings.TaskHubName,
                                     this.settings.WorkerId,
                                     partition.RowKey,
                                     $"Worker {this.settings.WorkerId} releases the lease of {partition.RowKey}.");
                            }
                            
                        }
                        catch (RequestFailedException ex) when (ex.Status == 412)
                        {
                            throw ex;
                        }
                    }

                }
                if (leaseNum == 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }

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
            public bool WorkOnRelease { get; set; } = false;
            public bool WaitForPartition { get; set; } = false;
        }

        //only used for testing
        //Used to simulate worker becomes unhealthy and restarts again after 1 minute
        internal async void SimulateUnhealthyWorker(CancellationToken testToken)
        {
            await Task.Factory.StartNew(() => this.PartitionManagerLoop(testToken));
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
