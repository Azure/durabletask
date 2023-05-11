namespace DurableTask.AzureStorage.Partitioning
{
    // app lease manager change paritionmanager interface pass it  azurestorageorchstrationservice 
    using Azure.Data.Tables;
    using Azure;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text.Json;
    using System.Collections.Concurrent;
    using DurableTask.AzureStorage.Storage;

    /// <summary>
    /// Partition ManagerV3 is based on the Table storage.  
    /// </summary>
    class TablePartitionManager : IPartitionManager
    {
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly CancellationTokenSource source;
        readonly CancellationToken token;
        readonly string workerName;

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
            this.workerName = this.settings.WorkerId;
            this.source = new CancellationTokenSource();
            this.token = this.source.Token;
        }

        /// <summary>
        /// This represents the Table which the manager operates on. 
        /// </summary>
        TableClient partitionTable;
        
        /// <summary>
        /// The worker which will read and write on the table.
        /// </summary>
        TableLeaseManager tableLeaseManager;
        
        /// <summary>
        /// This method create a new instance of the class TableLeaseManager that represents the worker. 
        /// And then start the loop that the worker keeps operating on the table. 
        /// </summary>
        /// <returns></returns>
        async Task IPartitionManager.StartAsync()
        {
            this.tableLeaseManager = new TableLeaseManager(this.partitionTable, this.settings);

            await Task.Factory.StartNew(() => this.PartitionManagerLoop(token));
            this.settings.Logger.PartitionManagerInfo(
                "",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} starts working.");
        }

        async Task PartitionManagerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                int timeToSleep = 15000;
                try
                {
                    var response = await this.tableLeaseManager.ReadAndWriteTable();
                    if (response.workOnRelease || response.waitForPartition)
                    {
                        timeToSleep = 1000;
                    }
                }
                catch
                {
                    timeToSleep = 0;
                }

                await Task.Delay(timeToSleep, token);
            }
        }

        /// <summary>
        /// This method will stop the worker. It first stops the task ReadAndWriteTable().
        /// And then start the Task ShutDown() until all the leases in the worker is drained. 
        /// </summary>
        /// <returns></returns>
        async Task IPartitionManager.StopAsync()
        {
            this.source.Cancel();
            this.settings.Logger.PartitionManagerInfo(
                "",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} starts shutting down.");
            
            await this.PartitionManagerStopLoop();

            this.settings.Logger.PartitionManagerInfo(
                "",
                this.settings.TaskHubName,
                this.settings.WorkerId,
                "",
                $"Worker {this.settings.WorkerId} finishes shutting down.");
        }

        async Task PartitionManagerStopLoop()
        {
            bool shouldRetry = false;
            int timeToSleep = 1000;
            
            while (!shouldRetry)
            {
                try
                {
                    shouldRetry = await this.tableLeaseManager.ShutDown();
                }
                catch
                {
                    timeToSleep = 0;
                }
                await Task.Delay(timeToSleep);
            }
        }

        async Task IPartitionManager.CreateLeaseStore()
        {
            string tableName = this.settings.AppName + "PartitionTable";
            // will change later since managed identity can be different
            var tableServiceClient = new TableServiceClient(this.settings.StorageConnectionString);
            await tableServiceClient.CreateTableIfNotExistsAsync(tableName);
            this.partitionTable = tableServiceClient.GetTableClient(tableName);
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
            catch(RequestFailedException)
            {
                //log info about that entity has already exists
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
        internal Pageable<TableLease> GetTableLeases()
        {
            var partitions = this.partitionTable.Query<TableLease>();
            return partitions;
        }

        internal void KillLoop()
        {
            this.source.Cancel();
        }

        async Task IPartitionManager.DeleteLeases()
        {
            await this.partitionTable.DeleteAsync();
        }

        sealed class TableLeaseManager
        {
            string workerName;
            AzureStorageOrchestrationServiceSettings settings;
            TableClient myTable;
            Dictionary<string, Task> tasks;

            public TableLeaseManager(TableClient table, AzureStorageOrchestrationServiceSettings settings)
            {
                this.myTable = table;
                this.settings = settings;
                this.workerName = this.settings.WorkerId;
                tasks = new Dictionary<string, Task>();
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
                        if (partitionDistribution.ContainsKey(partition.CurrentOwner))
                        {
                            partitionDistribution[partition.CurrentOwner].Add(partition);
                        }
                        else
                        {
                            partitionDistribution.Add(partition.CurrentOwner, new List<TableLease> { partition });
                        }
                    }

                    if (isOtherWorkerFutureLease)
                    {
                        if (partitionDistribution.ContainsKey(partition.NextOwner))
                        {
                            partitionDistribution[partition.NextOwner].Add(partition);
                        }
                        else
                        {
                            partitionDistribution.Add(partition.NextOwner, new List<TableLease> { partition });
                        }
                        if (partition.NextOwner == workerName)
                        {
                            leaseNum++;
                            response.waitForPartition = true;
                        }
                    }

                    if (isOtherWorkerShutDownLease)
                    {
                        StealLease(partition);
                        isStealedLease = true;
                        response.waitForPartition = true;
                    }

                    if (partition.CurrentOwner == workerName)
                    {
                        if (partition.NextOwner == null)
                        {
                            leaseNum++;
                        }
                        else
                        {
                            response.workOnRelease = true;

                            if (partition.IsDraining == false)
                            {
                                DrainLease(partition);
                                isDrainedLease = true;
                            }
                            else
                            {
                                if (tasks[partition.RowKey].IsCompleted == true)
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
                                "",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} acquires the lease of {partition.RowKey}.");
                            }
                            if (isStealedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} steals the lease of {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId} releases the lease of {partition.RowKey}.");
                            }
                            if(isDrainedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                "",
                                this.settings.TaskHubName,
                                this.settings.WorkerId,
                                partition.RowKey,
                                $"Worker {this.settings.WorkerId}  starts draining {partition.RowKey}.");
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
                                                "",
                                                this.settings.TaskHubName,
                                                this.settings.WorkerId,
                                                partition.RowKey,
                                                $"Worker {this.settings.WorkerId} steals the lease of {partition.RowKey}.");
                                            response.waitForPartition = true;
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


            public void ClaimLease(TableLease partition)
            {
                partition.CurrentOwner = workerName;
                partition.NextOwner = null;
                partition.OwnedSince = DateTime.UtcNow;
                partition.LastRenewal = DateTime.UtcNow;
                partition.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
                partition.IsDraining = false;
            }
            public void DrainLease(TableLease partition)
            {
                partition.IsDraining = true;

                var task = Task.Run(() =>
                {
                    Thread.CurrentThread.Name = partition.RowKey;
                    Thread.Sleep(10000);
                }
                );
                if (tasks.ContainsKey(partition.RowKey))
                {
                    tasks[partition.RowKey] = task;
                }
                else
                {
                    tasks.Add(partition.RowKey, task);
                }
            }

            public void ReleaseLease(TableLease partition)
            {
                partition.IsDraining = false;
                partition.CurrentOwner = null;
            }
            public void RenewLease(TableLease partition) 
            {
                partition.ExpiresAt = DateTime.UtcNow.AddMinutes(1);
            }


            public void StealLease(TableLease partition)
            {
                partition.NextOwner = workerName;
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
                            DrainLease(partition);
                            RenewLease(partition);
                            isDrainedLease = true;
                        }
                        else
                        {
                            if (tasks[partition.RowKey].IsCompleted == true)
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
                                                "",
                                                this.settings.TaskHubName,
                                                this.settings.WorkerId,
                                                partition.RowKey,
                                                $"Worker {this.settings.WorkerId} stats draining {partition.RowKey}.");
                            }
                            if (isReleasedLease)
                            {
                                this.settings.Logger.PartitionManagerInfo(
                                                "",
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
            public bool workOnRelease { get; set; } = false;
            public bool waitForPartition { get; set; } = false;
        }
    }
}
