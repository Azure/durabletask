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

    /// <summary>
    /// Partition ManagerV3 is based on the Table storage.  
    /// </summary>
    class TablePartitionManager : IPartitionManager
    {
        //readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource partitionManagerCancellationSource;
        readonly string tableName;
        //readonly string storageAccountName;
        TableServiceClient tableServiceClient;
        TableClient partitionTable;
        TableLeaseManager tableLeaseManager;

        /// <summary>
        /// constructor to initiate new instances of TablePartitionManager
        /// </summary>
        /// <param name="service"></param>
        /// <param name="settings"></param>
        public TablePartitionManager(
            AzureStorageOrchestrationService service,
            AzureStorageOrchestrationServiceSettings settings)
        {
            this.service = service;
            this.settings = settings;
            this.tableName = this.CreateTableName();
            this.partitionManagerCancellationSource = new CancellationTokenSource();
            this.tableServiceClient = new TableServiceClient(this.settings.StorageConnectionString);
            this.partitionTable = tableServiceClient.GetTableClient(this.tableName);
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings);
        }
        
        string CreateTableName()
        {
            string name = this.settings.AppName + "Partitions";
            //check if tableName has illegal symbol
            if (name.Contains("-"))
            {
                name.Replace("-", string.Empty);
            }
            return name;
        }
        /// <summary>
        /// This method create a new instance of the class TableLeaseManager that represents the worker. 
        /// And then start the loop that the worker keeps operating on the table. 
        /// </summary>
        /// <returns></returns>
        async Task IPartitionManager.StartAsync()
        {
            //this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.service, this.settings);

            await Task.Factory.StartNew(() => this.PartitionManagerLoop(this.partitionManagerCancellationSource.Token));
            this.settings.Logger.PartitionManagerInfo(
                "this.storageAccountName",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",//empty partition id
                $"Worker {this.settings.WorkerId} starts working.");
        }

        async Task PartitionManagerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                TimeSpan timeToSleep = this.settings.LeaseRenewInterval;
                try
                {
                    var response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.WorkOnRelease || response.WaitForPartition)
                    {
                        timeToSleep = TimeSpan.FromSeconds(1);
                    }
                }
                catch
                {
                    // if the worker failed to update the table, re-read the table immediately without wait.
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
            partitionManagerCancellationSource.Dispose();
            this.settings.Logger.PartitionManagerInfo(
                "this.storageAccountName",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} starts shutting down.");

            bool shouldRetry = false;
            //Shutting down is about draining all the lease and thus the worker checks table every one sec to ensure timely updates.
            TimeSpan timeToSleep = TimeSpan.FromSeconds(1);

            var task = Task.Run(async() =>
            {
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
                }
            });
            await task;

            this.settings.Logger.PartitionManagerInfo(
                "this.storageAccountName",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} finishes shutting down.");
        }

        async Task IPartitionManager.CreateLeaseStore()
        {
            try
            {
                await this.partitionTable.CreateIfNotExistsAsync();
            }
            catch (RequestFailedException ex)
            {
                this.settings.Logger.PartitionManagerError(
                    "this.storageAccountName",
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    "",
                    $"Failed to create table Partitions. {ex.Message}");
            }
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
                this.settings.Logger.PartitionManagerError(
                    "this.storageAccountName",
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
            var partitions = this.partitionTable.Query<TableLease>();
            return partitions;
        }

        //internal used for testing
        internal void KillLoop()
        {
            this.partitionManagerCancellationSource.Cancel();
            partitionManagerCancellationSource.Dispose();
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
            Dictionary<string, Task> tasks;

            public TableLeaseManager(TableClient table, AzureStorageOrchestrationService service, AzureStorageOrchestrationServiceSettings settings)
            {
                this.myTable = table;
                this.service = service;
                this.settings = settings;
                this.workerName = this.settings.WorkerId;
                this.tasks = new Dictionary<string, Task>();
            }

            public async Task<ReadTableReponse> ReadAndWriteTable()
            {
                
                var response = new ReadTableReponse();
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                Dictionary<string, List<TableLease>> partitionDistribution = new Dictionary<string, List<TableLease>>(); 
                int leaseNum = 0;

                foreach (var partition in partitions)
                {
                    bool isClaimedLease = false;
                    bool isStealedLease = false;
                    bool isRenewdLease = false;
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    bool isEmptyLease = (partition.CurrentOwner == null && partition.NextOwner == null);
                    bool isExpiresAt = (DateTime.UtcNow >= partition.ExpiresAt && partition.NextOwner == null);
                    bool isStolen = (partition.CurrentOwner == null && partition.NextOwner == this.workerName);

                    var etag = partition.ETag;

                    if (isEmptyLease || isExpiresAt || isStolen)
                    {
                        ClaimLease(partition);
                        isClaimedLease = true;

                    }

                    bool isOtherWorkerCurrentLease = (partition.CurrentOwner != workerName && partition.NextOwner == null && partition.IsDraining == false);
                    bool isOtherWorkerFutureLease = (partition.CurrentOwner != workerName && partition.NextOwner != null);
                    bool isOtherWorkerShutDownLease = (partition.CurrentOwner != workerName && partition.NextOwner == null && partition.IsDraining == true);

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

                    if (isOtherWorkerFutureLease)
                    {
                        if (partitionDistribution.ContainsKey(partition.NextOwner!))
                        {
                            partitionDistribution[partition.NextOwner!].Add(partition);
                        }
                        else
                        {
                            partitionDistribution.Add(partition.NextOwner!, new List<TableLease> { partition });
                        }
                        if (partition.NextOwner == workerName)
                        {
                            leaseNum++;
                            response.WaitForPartition = true;
                        }
                    }

                    if (isOtherWorkerShutDownLease)
                    {
                        StealLease(partition);
                        isStealedLease = true;
                        response.WaitForPartition = true;
                    }

                    if (partition.CurrentOwner == workerName)
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
                                DrainLease(partition, CloseReason.LeaseLost);
                                isDrainedLease = true;
                            }
                            else
                            {
                                if (tasks[partition.RowKey!].IsCompleted == true)
                                {
                                    ReleaseLease(partition);
                                    isReleasedLease = true;
                                }
                            }
                        }
                        if (partition.CurrentOwner != null)
                        {
                            RenewLease(partition);
                            isRenewdLease = true;
                        }
                    }

                    if (isClaimedLease || isStealedLease || isRenewdLease || isDrainedLease || isReleasedLease)
                    {
                        try
                        {
                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if(isClaimedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                    // this attribute will be added and used after we implement the class AzureStorageClient later
                                    "this.storageAccountName", 
                                    this.settings.TaskHubName,
                                    this.settings.WorkerId,
                                    partition.RowKey,
                                    $"Worker {this.settings.WorkerId} acquires the lease of {partition.RowKey}.");
                            }
                            if (isStealedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "this.storageAccountName",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} steals the lease of {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "this.storageAccountName",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} releases the lease of {partition.RowKey}.");
                            }
                            if(isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "this.storageAccountName",
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

                if (partitionDistribution.Count != 0)
                {
                    int numLeasePerWorkerForBalance = (partitions.Count()) / (partitionDistribution.Count + 1);
                    if (numLeasePerWorkerForBalance == 0) numLeasePerWorkerForBalance = 1;
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
                                        var etag = partition.ETag;
                                        StealLease(partition);
                                        try
                                        {
                                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                                            this.settings.Logger.PartitionManagerInfo(
                                                "this.storageAccountName",
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
                lease.CurrentOwner = workerName;
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
                if (tasks.ContainsKey(lease.RowKey!))
                {
                    tasks[lease.RowKey!] = task;
                }
                else
                {
                    tasks.Add(lease.RowKey!, task);
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
                lease.NextOwner = workerName;
            }

            public async Task<bool> ShutDown()
            {
                Pageable<TableLease> partitions = myTable.Query<TableLease>();
                int leaseNum = 0;
                foreach (var partition in partitions)
                {
                    bool isDrainedLease = false;
                    bool isReleasedLease = false;

                    if (partition.CurrentOwner == workerName)
                    {
                        leaseNum++;
                        var etag = partition.ETag;
                        
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
                            await myTable.UpdateEntityAsync(partition, etag, (TableUpdateMode)1);
                            if (isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     "this.storageAccountName",
                                     this.settings.TaskHubName,
                                     this.settings.WorkerId,
                                     partition.RowKey,
                                     $"Worker {this.settings.WorkerId} stats draining {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                     "this.storageAccountName",
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
    }
}
