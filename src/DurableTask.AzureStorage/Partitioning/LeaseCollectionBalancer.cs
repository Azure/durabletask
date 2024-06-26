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

namespace DurableTask.AzureStorage.Partitioning
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class LeaseCollectionBalancer<T> where T : Lease
    {
        readonly string leaseType;
        readonly string accountName;
        readonly string taskHub;
        readonly string workerName;
        readonly ILeaseManager<T> leaseManager;
        readonly LeaseCollectionBalancerOptions options;
        readonly AzureStorageOrchestrationServiceSettings settings;

        readonly ConcurrentDictionary<string, T> currentlyOwnedShards;
        readonly ConcurrentDictionary<string, T> keepRenewingDuringClose;
        readonly LeaseObserverManager leaseObserverManager;
        readonly Func<string, bool> shouldAquireLeaseDelegate;
        readonly Func<string, bool> shouldRenewLeaseDelegate;

        int isStarted;
        bool shutdownComplete;
        Task renewTask;
        Task takerTask;
        CancellationTokenSource leaseTakerCancellationTokenSource;
        CancellationTokenSource leaseRenewerCancellationTokenSource;

        public LeaseCollectionBalancer(
            string leaseType,
            AzureStorageOrchestrationServiceSettings settings,
            string blobAccountName,
            ILeaseManager<T> leaseManager, 
            LeaseCollectionBalancerOptions options,
            Func<string, bool> shouldAquireLeaseDelegate = null,
            Func<string, bool> shouldRenewLeaseDelegate = null)
        {
            this.leaseType = leaseType;
            this.accountName = blobAccountName;
            this.taskHub = settings.TaskHubName;
            this.workerName = settings.WorkerId;
            this.leaseManager = leaseManager;
            this.options = options;
            this.settings = settings;

            this.shouldAquireLeaseDelegate = shouldAquireLeaseDelegate ?? DefaultLeaseDecisionDelegate;
            this.shouldRenewLeaseDelegate = shouldRenewLeaseDelegate ?? DefaultLeaseDecisionDelegate;

            this.currentlyOwnedShards = new ConcurrentDictionary<string, T>();
            this.keepRenewingDuringClose = new ConcurrentDictionary<string, T>();
            this.leaseObserverManager = new LeaseObserverManager(this);
        }

        private static bool DefaultLeaseDecisionDelegate(string leaseId)
        {
            return true;
        }

        public ConcurrentDictionary<string, T> GetCurrentlyOwnedLeases()
        {
            return this.currentlyOwnedShards;
        }

        public async Task InitializeAsync()
        {
            var leases = new List<T>();
            await foreach (T lease in this.leaseManager.ListLeasesAsync())
            {
                if (string.Compare(lease.Owner, this.workerName, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    bool renewed = await this.RenewLeaseAsync(lease);
                    if (renewed)
                    {
                        leases.Add(lease);
                    }
                }
            }

            var addLeaseTasks = new List<Task>();
            foreach (T lease in leases)
            {
                this.settings.Logger.PartitionManagerInfo(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    $"Acquired {this.leaseType} lease for PartitionId '{lease.PartitionId}' on startup.");

                addLeaseTasks.Add(this.AddLeaseAsync(lease));
            }

            await Task.WhenAll(addLeaseTasks.ToArray());
        }

        public async Task StartAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 1, 0) != 0)
            {
                throw new InvalidOperationException($"{nameof(LeaseCollectionBalancer<T>)} has already started");
            }

            this.shutdownComplete = false;
            this.leaseTakerCancellationTokenSource = new CancellationTokenSource();
            this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

            this.renewTask = await Task.Factory.StartNew(() => this.LeaseRenewer());
            this.takerTask = await Task.Factory.StartNew(() => this.LeaseTakerAsync());
        }

        public async Task StopAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 0, 1) != 1)
            {
                //idempotent
                return;
            }

            if (this.takerTask != null)
            {
                this.leaseTakerCancellationTokenSource.Cancel();
                await this.takerTask;
            }

            await this.ShutdownAsync();
            this.shutdownComplete = true;

            if (this.renewTask != null)
            {
                this.leaseRenewerCancellationTokenSource.Cancel();
                await this.renewTask;
            }

            this.leaseTakerCancellationTokenSource = null;
            this.leaseRenewerCancellationTokenSource = null;
        }

        public Task<IDisposable> SubscribeAsync(
            Func<T, Task> leaseAquiredDelegate,
            Func<T, CloseReason, Task> leaseReleasedDelegate)
        {
            var leaseObserver = new LeaseObserver<T>(leaseAquiredDelegate, leaseReleasedDelegate);
            return this.leaseObserverManager.SubscribeAsync(leaseObserver);
        }

        public async Task TryReleasePartitionAsync(string partitionId, string leaseToken)
        {
            T lease;
            if (this.currentlyOwnedShards.TryGetValue(partitionId, out lease) &&
                lease.Token.Equals(leaseToken, StringComparison.OrdinalIgnoreCase))
            {
                await this.RemoveLeaseAsync(lease, true);
            }
        }

        async Task LeaseRenewer()
        {
            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                string.Empty /* partitionId */,
                $"Starting background renewal of {this.leaseType} leases with interval: {this.options.RenewInterval}.");

            while (this.isStarted == 1 || !this.shutdownComplete)
            {
                try
                {
                    var nonRenewedLeases = new ConcurrentBag<T>();
                    var renewTasks = new List<Task>();

                    // Renew leases for all currently owned partitions in parallel
                    foreach (T lease in this.currentlyOwnedShards.Values)
                    {
                        if (this.shouldRenewLeaseDelegate(lease.PartitionId))
                        {
                            renewTasks.Add(this.RenewLeaseAsync(lease).ContinueWith(renewResult =>
                            {
                                if (!renewResult.Result)
                                {
                                    // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
                                    nonRenewedLeases.Add(lease);
                                }
                            }));
                        } 
                        else 
                        {
                            nonRenewedLeases.Add(lease);
                        }
                    }

                    // Renew leases for all partitions currently in shutdown 
                    var failedToRenewShutdownLeases = new List<T>();
                    foreach (T shutdownLease in this.keepRenewingDuringClose.Values)
                    {
                        if (this.shouldRenewLeaseDelegate(shutdownLease.PartitionId))
                        {
                            renewTasks.Add(this.RenewLeaseAsync(shutdownLease).ContinueWith(renewResult =>
                            {
                                if (!renewResult.Result)
                                {
                                    // Keep track of all failed attempts to renew shutdown leases so we can remove them from further renew attempts
                                    failedToRenewShutdownLeases.Add(shutdownLease);
                                }
                            }));
                        }
                    }

                    // Wait for all renews to complete
                    await Task.WhenAll(renewTasks.ToArray());

                    // Trigger shutdown of all partitions we did not successfully renew leases for
                    await nonRenewedLeases.ParallelForEachAsync(lease => this.RemoveLeaseAsync(lease, false));

                    // Now remove all failed renewals of shutdown leases from further renewals
                    foreach (T failedToRenewShutdownLease in failedToRenewShutdownLeases)
                    {
                        T removedLease = null;
                        this.keepRenewingDuringClose.TryRemove(failedToRenewShutdownLease.PartitionId, out removedLease);
                    }

                    await Task.Delay(this.options.RenewInterval, this.leaseRenewerCancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        string.Empty /* partitionId */,
                        $"Background renewal task for {this.leaseType} leases was canceled.");
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        string.Empty,
                        $"Failed during {this.leaseType} lease renewal: {ex}");
                }
            }

            this.currentlyOwnedShards.Clear();
            this.keepRenewingDuringClose.Clear();
            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                string.Empty /* partitionId */,
                $"Background renewer task for {this.leaseType} leases completed.");
        }

        async Task LeaseTakerAsync()
        {
            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                string.Empty /* partitionId */,
                $"Starting to check for available {this.leaseType} leases with interval: {this.options.AcquireInterval}.");

            while (this.isStarted == 1)
            {
                try
                {
                    var availableLeases = await this.TakeLeasesAsync();

                    var addLeaseTasks = new List<Task>();
                    foreach (var kvp in availableLeases)
                    {
                        addLeaseTasks.Add(this.AddLeaseAsync(kvp.Value));
                    }

                    await Task.WhenAll(addLeaseTasks.ToArray());
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.accountName, 
                        this.taskHub,
                        this.workerName,
                        string.Empty,
                        $"Failed during {this.leaseType} acquisition: {ex}");
                }

                try
                {
                    await Task.Delay(this.options.AcquireInterval, this.leaseTakerCancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        string.Empty /* partitionId */,
                        $"Background AcquireLease task for {this.leaseType} leases was canceled.");
                }
            }

            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                string.Empty /* partitionId */,
                $"Background AcquireLease task for {this.leaseType} leases completed.");
        }

        async Task<IDictionary<string, T>> TakeLeasesAsync()
        {
            var allShards = new Dictionary<string, T>();
            var takenLeases = new Dictionary<string, T>();
            var workerToShardCount = new Dictionary<string, int>();
            var expiredLeases = new List<T>();

            await foreach (T lease in this.leaseManager.ListLeasesAsync())
            {
                if (!this.shouldAquireLeaseDelegate(lease.PartitionId))
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        $"Skipping {this.leaseType} lease aquiring for {lease.PartitionId}");
                    continue;
                }

                allShards.Add(lease.PartitionId, lease);
                if (await lease.IsExpiredAsync() || string.IsNullOrWhiteSpace(lease.Owner))
                {
                    expiredLeases.Add(lease);
                }
                else
                {
                    int count;
                    string assignedTo = lease.Owner;
                    if (workerToShardCount.TryGetValue(assignedTo, out count))
                    {
                        workerToShardCount[assignedTo] = count + 1;
                    }
                    else
                    {
                        workerToShardCount.Add(assignedTo, 1);
                    }
                }
            }

            if (!workerToShardCount.ContainsKey(this.workerName))
            {
                workerToShardCount.Add(this.workerName, 0);
            }

            int shardCount = allShards.Count;
            int workerCount = workerToShardCount.Count;

            if (shardCount > 0)
            {
                int target = 1;
                if (shardCount > workerCount)
                {
                    target = (int)Math.Ceiling((double)shardCount / (double)workerCount);
                }

                int myCount = workerToShardCount[this.workerName];

                // if we are stealing intent leases, we are trying to take as many as required to reach balance
                // if we are aquiring owner leases, we want to acquire all of the expired leases
                int moreShardsNeeded = this.options.ShouldStealLeases ? target - myCount : expiredLeases.Count;

                if (moreShardsNeeded > 0)
                {
                    if (expiredLeases.Count > 0)
                    {
                        foreach (T leaseToTake in expiredLeases)
                        {
                            if (moreShardsNeeded == 0) break;

                            this.settings.Logger.LeaseAcquisitionStarted(
                                this.accountName,
                                this.taskHub,
                                this.workerName,
                                leaseToTake.PartitionId,
                                this.leaseType);

                            bool leaseAcquired = await this.AcquireLeaseAsync(leaseToTake);
                            if (leaseAcquired)
                            {
                                this.settings.Logger.LeaseAcquisitionSucceeded(
                                    this.accountName,
                                    this.taskHub,
                                    this.workerName,
                                    leaseToTake.PartitionId,
                                    this.leaseType);

                                takenLeases.Add(leaseToTake.PartitionId, leaseToTake);

                                moreShardsNeeded--;
                            }
                        }
                    }
                    else if (this.options.ShouldStealLeases)
                    {
                        KeyValuePair<string, int> workerToStealFrom = default(KeyValuePair<string, int>);
                        foreach (var kvp in workerToShardCount)
                        {
                            if (kvp.Equals(default(KeyValuePair<string, int>)) || workerToStealFrom.Value < kvp.Value)
                            {
                                workerToStealFrom = kvp;
                            }
                        }

                        if (workerToStealFrom.Value > target - (moreShardsNeeded > 1 ? 1 : 0))
                        {
                            foreach (var kvp in allShards)
                            {
                                if (string.Equals(kvp.Value.Owner, workerToStealFrom.Key, StringComparison.OrdinalIgnoreCase))
                                {
                                    T leaseToTake = kvp.Value;
                                    this.settings.Logger.AttemptingToStealLease(
                                        this.accountName,
                                        this.taskHub,
                                        this.workerName,
                                        workerToStealFrom.Key,
                                        this.leaseType,
                                        leaseToTake.PartitionId);

                                    bool leaseStolen = await this.StealLeaseAsync(leaseToTake);
                                    if (leaseStolen)
                                    {
                                        this.settings.Logger.LeaseStealingSucceeded(
                                            this.accountName,
                                            this.taskHub,
                                            this.workerName,
                                            workerToStealFrom.Key,
                                            this.leaseType,
                                            leaseToTake.PartitionId);

                                        takenLeases.Add(leaseToTake.PartitionId, leaseToTake);

                                        moreShardsNeeded--;

                                        // Only steal one lease at a time
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return takenLeases;
        }

        async Task ShutdownAsync()
        {
            var shutdownTasks = this.currentlyOwnedShards.Values.Select<T, Task>((lease) =>
            {
                return RemoveLeaseAsync(lease, true);
            });

            await Task.WhenAll(shutdownTasks);
        }

        async Task<bool> RenewLeaseAsync(T lease)
        {
            bool renewed;
            string errorMessage = string.Empty;

            try
            {
                this.settings.Logger.StartingLeaseRenewal(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    lease.Token,
                    this.leaseType);
                renewed = await this.leaseManager.RenewAsync(lease);
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;

                if (ex is LeaseLostException ||
                    ex is ArgumentException)
                {
                    renewed = false;
                }
                else
                {
                    // Eat any exceptions during renew and keep going.
                    // Consider the lease as renewed.  Maybe lease store outage is causing the lease to not get renewed.
                    renewed = true;
                }
            }

            this.settings.Logger.LeaseRenewalResult(
                this.accountName,
                this.taskHub,
                this.workerName,
                lease.PartitionId,
                renewed,
                lease.Token,
                this.leaseType,
                errorMessage);

            if (!renewed)
            {
                this.settings.Logger.LeaseRenewalFailed(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    lease.Token,
                    this.leaseType,
                    errorMessage);
            }

            return renewed;
        }

        async Task<bool> AcquireLeaseAsync(T lease)
        {
            bool acquired = false;
            try
            {
                acquired = await this.leaseManager.AcquireAsync(lease, this.workerName);
            }
            catch (LeaseLostException)
            {
                this.settings.Logger.LeaseAcquisitionFailed(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    this.leaseType);
            }
            catch (Exception ex)
            {
                // Eat any exceptions during acquiring lease.
                this.settings.Logger.PartitionManagerError(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    $"Error in {this.leaseType} lease acquisition: {ex}");
            }

            return acquired;
        }

        async Task<bool> StealLeaseAsync(T lease)
        {
            bool stolen = false;
            try
            {
                stolen = await this.leaseManager.AcquireAsync(lease, this.workerName);
            }
            catch (LeaseLostException)
            {
                // Concurrency issue in stealing the lease, someone else got it before us
                this.settings.Logger.LeaseStealingFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId, this.leaseType);
            }
            catch (ArgumentException)
            {
                // Concurrency issue in stealing the lease, someone else got it before us
                this.settings.Logger.LeaseStealingFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId, this.leaseType);
            }
            catch (Exception ex)
            {
                // Eat any exceptions during stealing
                this.settings.Logger.PartitionManagerError(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    $"Failure in {this.leaseType} lease stealing: {ex}");
            }

            return stolen;
        }

        async Task AddLeaseAsync(T lease)
        {
            if (this.currentlyOwnedShards.TryAdd(lease.PartitionId, lease))
            {
                bool failedToInitialize = false;
                try
                {
                    await this.leaseObserverManager.NotifyShardAcquiredAsync(lease);
                }
                catch (Exception ex)
                {
                    failedToInitialize = true;

                    // Eat any exceptions during notification of observers
                    this.settings.Logger.PartitionManagerError(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        $"Failed to notify observers of {this.leaseType} lease acquisition: {ex}");
                }

                // We need to release the lease if we fail to initialize the processor, so some other node can pick up the parition
                if (failedToInitialize)
                {
                    await this.RemoveLeaseAsync(lease, true);
                }
            }
            else
            {
                // We already acquired lease for this partition but it looks like we previously owned this partition 
                // and haven't completed the shutdown process for it yet.  Release lease for possible others hosts to 
                // pick it up.
                try
                {
                    this.settings.Logger.PartitionManagerWarning(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        $"Unable to add {this.leaseType} lease with PartitionId '{lease.PartitionId}' with lease token '{lease.Token}' to currently owned leases.");

                    await this.RemoveLeaseAsync(lease, true);
                    this.settings.Logger.LeaseRemoved(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        lease.Token,
                        this.leaseType);
                }
                catch (LeaseLostException)
                {
                    this.settings.Logger.LeaseRemovalFailed(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        lease.Token,
                        this.leaseType);
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        $"Encountered a failure when removing a {this.leaseType} lease we previuosly owned: {ex}");
                }
            }
        }

        async Task RemoveLeaseAsync(T lease, bool hasOwnership)
        {
            CloseReason reason = hasOwnership ? CloseReason.Shutdown : CloseReason.LeaseLost;

            if (lease != null && this.currentlyOwnedShards != null && this.currentlyOwnedShards.TryRemove(lease.PartitionId, out lease))
            {
                this.settings.Logger.PartitionRemoved(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    lease.PartitionId,
                    lease.Token);

                try
                {
                    if (hasOwnership)
                    {
                        this.keepRenewingDuringClose.TryAdd(lease.PartitionId, lease);
                    }

                    // Notify the host that we lost shard so shutdown can be triggered on the host
                    await this.leaseObserverManager.NotifyShardReleasedAsync(lease, reason);
                }
                catch (Exception ex)
                {
                    // Eat any exceptions during notification of observers
                    this.settings.Logger.PartitionManagerError(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        lease.PartitionId,
                        $"Encountered exception while notifying observers of {this.leaseType} lease release: {ex}");
                }
                finally
                {
                    if (hasOwnership)
                    {
                        this.keepRenewingDuringClose.TryRemove(lease.PartitionId, out lease);
                    }
                }

                if (hasOwnership)
                {
                    try
                    {
                        await this.leaseManager.ReleaseAsync(lease);
                        this.settings.Logger.LeaseRemoved(
                            this.accountName,
                            this.taskHub,
                            this.workerName,
                            lease.PartitionId,
                            lease.Token,
                            this.leaseType);
                    }
                    catch (LeaseLostException)
                    {
                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
                    }
                    catch (Exception ex)
                    {
                        this.settings.Logger.PartitionManagerError(
                            this.accountName,
                            this.taskHub,
                            this.workerName,
                            lease.PartitionId,
                            $"Encountered failure while releasing owned {this.leaseType}: {ex}");
                    }
                }
            }
        }


        sealed class LeaseObserverManager
        {
            readonly LeaseCollectionBalancer<T> partitionManager;
            readonly List<LeaseObserver<T>> observers;

            public LeaseObserverManager(LeaseCollectionBalancer<T> partitionManager)
            {
                this.partitionManager = partitionManager;
                this.observers = new List<LeaseObserver<T>>();
            }

            public async Task<IDisposable> SubscribeAsync(LeaseObserver<T> observer)
            {
                if (!this.observers.Contains(observer))
                {
                    this.observers.Add(observer);

                    foreach (var lease in this.partitionManager.currentlyOwnedShards.Values)
                    {
                        try
                        {
                            await observer.OnLeaseAquiredAsync(lease);
                        }
                        catch (Exception ex)
                        {
                            // Eat any exceptions during notification of observers
                            this.partitionManager.settings.Logger.PartitionManagerError(
                                this.partitionManager.accountName,
                                this.partitionManager.taskHub,
                                this.partitionManager.workerName,
                                lease.PartitionId,
                                $"Failed during notification of observers of {this.partitionManager.leaseType} lease: {ex}");
                        }
                    }
                }

                return new Unsubscriber(this.observers, observer);
            }

            public async Task NotifyShardAcquiredAsync(T lease)
            {
                foreach (var observer in this.observers)
                {
                    await observer.OnLeaseAquiredAsync(lease);
                }
            }

            public async Task NotifyShardReleasedAsync(T lease, CloseReason reason)
            {
                foreach (var observer in this.observers)
                {
                    await observer.OnLeaseReleasedAsync(lease, reason);
                }
            }
        }

        sealed class Unsubscriber : IDisposable
        {
            readonly List<LeaseObserver<T>> _observers;
            readonly LeaseObserver<T> _observer;

            internal Unsubscriber(List<LeaseObserver<T>> observers, LeaseObserver<T> observer)
            {
                this._observers = observers;
                this._observer = observer;
            }

            public void Dispose()
            {
                if (_observers.Contains(_observer))
                    _observers.Remove(_observer);
            }
        }
    }
}
