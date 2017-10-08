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

    sealed class PartitionManager<T> where T : Lease
    {
        readonly string accountName;
        readonly string taskHub;
        readonly string workerName;
        readonly ILeaseManager<T> leaseManager;
        readonly PartitionManagerOptions options;
        readonly ConcurrentDictionary<string, T> currentlyOwnedShards;
        readonly ConcurrentDictionary<string, T> keepRenewingDuringClose;
        readonly PartitionObserverManager partitionObserverManager;

        int isStarted;
        bool shutdownComplete;
        Task renewTask;
        Task takerTask;
        CancellationTokenSource leaseTakerCancellationTokenSource;
        CancellationTokenSource leaseRenewerCancellationTokenSource;

        public PartitionManager(string accountName, string taskHub, string workerName, ILeaseManager<T> leaseManager, PartitionManagerOptions options)
        {
            this.accountName = accountName;
            this.taskHub = taskHub;
            this.workerName = workerName;
            this.leaseManager = leaseManager;
            this.options = options;

            this.currentlyOwnedShards = new ConcurrentDictionary<string, T>();
            this.keepRenewingDuringClose = new ConcurrentDictionary<string, T>();
            this.partitionObserverManager = new PartitionObserverManager(this);
        }

        public async Task InitializeAsync()
        {
            var leases = new List<T>();
            foreach (T lease in await this.leaseManager.ListLeasesAsync())
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
                AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, $"Acquired lease for PartitionId '{lease.PartitionId}' on startup.");
                addLeaseTasks.Add(this.AddLeaseAsync(lease));
            }

            await Task.WhenAll(addLeaseTasks.ToArray());
        }

        public async Task StartAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 1, 0) != 0)
            {
                throw new InvalidOperationException("PartitionManager has already started");
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

        public Task<IDisposable> SubscribeAsync(IPartitionObserver<T> observer)
        {
            return this.partitionObserverManager.SubscribeAsync(observer);
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
            AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, $"Starting background renewal of leases with interval: {this.options.RenewInterval}.");

            while (this.isStarted == 1 || !shutdownComplete)
            {
                try
                {
                    var failedToRenewLeases = new ConcurrentBag<T>();
                    var renewTasks = new List<Task>();

                    // Renew leases for all currently owned partitions in parallel
                    foreach (T lease in this.currentlyOwnedShards.Values)
                    {
                        renewTasks.Add(this.RenewLeaseAsync(lease).ContinueWith(renewResult =>
                        {
                            if (!renewResult.Result)
                            {
                                // Keep track of all failed attempts to renew so we can trigger shutdown for these partitions
                                failedToRenewLeases.Add(lease);
                            }
                        }));
                    }

                    // Renew leases for all partitions currently in shutdown 
                    var failedToRenewShutdownLeases = new List<T>();
                    foreach (T shutdownLeases in this.keepRenewingDuringClose.Values)
                    {
                        renewTasks.Add(this.RenewLeaseAsync(shutdownLeases).ContinueWith(renewResult =>
                        {
                            if (!renewResult.Result)
                            {
                                // Keep track of all failed attempts to renew shutdown leases so we can remove them from further renew attempts
                                failedToRenewShutdownLeases.Add(shutdownLeases);
                            }
                        }));
                    }

                    // Wait for all renews to complete
                    await Task.WhenAll(renewTasks.ToArray());

                    // Trigger shutdown of all partitions we failed to renew leases
                    await failedToRenewLeases.ParallelForEachAsync(lease => this.RemoveLeaseAsync(lease, false));

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
                    AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, $"Background renewal task was canceled.");
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
                }
            }

            this.currentlyOwnedShards.Clear();
            this.keepRenewingDuringClose.Clear();
            AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, "Background renewer task completed.");
        }

        async Task LeaseTakerAsync()
        {
            AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, $"Starting to check for available leases with interval: {this.options.AcquireInterval}.");

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
                    AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
                }

                try
                {
                    await Task.Delay(this.options.AcquireInterval, this.leaseTakerCancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, $"Background AcquireLease task was canceled.");
                }
            }

            AnalyticsEventSource.Log.PartitionManagerInfo(this.accountName, this.taskHub, this.workerName, "Background AcquireLease task completed.");
        }

        async Task<IDictionary<string, T>> TakeLeasesAsync()
        {
            var allShards = new Dictionary<string, T>();
            var takenLeases = new Dictionary<string, T>();
            var workerToShardCount = new Dictionary<string, int>();
            var expiredLeases = new List<T>();

            foreach (T lease in await this.leaseManager.ListLeasesAsync())
            {
                allShards.Add(lease.PartitionId, lease);
                if (lease.IsExpired() || string.IsNullOrWhiteSpace(lease.Owner))
                {
                    expiredLeases.Add(lease);
                }
                else
                {
                    int count = 0;
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
                int moreShardsNeeded = target - myCount;

                if (moreShardsNeeded > 0)
                {
                    HashSet<T> shardsToAcquire = new HashSet<T>();
                    if (expiredLeases.Count > 0)
                    {
                        foreach (T leaseToTake in expiredLeases)
                        {
                            if (moreShardsNeeded == 0) break;

                            AnalyticsEventSource.Log.LeaseAcquisitionStarted(this.accountName, this.taskHub, this.workerName, leaseToTake.PartitionId);
                            bool leaseAcquired = await this.AcquireLeaseAsync(leaseToTake);
                            if (leaseAcquired)
                            {
                                AnalyticsEventSource.Log.LeaseAcquisitionSucceeded(this.accountName, this.taskHub, this.workerName, leaseToTake.PartitionId);
                                takenLeases.Add(leaseToTake.PartitionId, leaseToTake);

                                moreShardsNeeded--;
                            }
                        }
                    }
                    else
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
                                    AnalyticsEventSource.Log.AttemptingToStealLease(this.accountName, this.taskHub, this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId);
                                    bool leaseStolen = await this.StealLeaseAsync(leaseToTake);
                                    if (leaseStolen)
                                    {
                                        AnalyticsEventSource.Log.LeaseStealingSucceeded(this.accountName, this.taskHub, this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId);
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
            bool renewed = false;
            string errorMessage = string.Empty;

            try
            {
                AnalyticsEventSource.Log.StartingLeaseRenewal(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);
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

            AnalyticsEventSource.Log.LeaseRenewalResult(this.accountName, this.taskHub, this.workerName, lease.PartitionId, renewed, lease.Token, errorMessage);
            if (!renewed)
            {
                AnalyticsEventSource.Log.LeaseRenewalFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token, errorMessage);
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
                AnalyticsEventSource.Log.LeaseAcquisitionFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId);
            }
            catch (Exception ex)
            {
                // Eat any exceptions during acquiring lease.
                AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
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
                AnalyticsEventSource.Log.LeaseStealingFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId);
            }
            catch (Exception ex)
            {
                // Eat any exceptions during stealing
                AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
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
                    await this.partitionObserverManager.NotifyShardAcquiredAsync(lease);
                }
                catch (Exception ex)
                {
                    failedToInitialize = true;

                    // Eat any exceptions during notification of observers
                    AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
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
                    AnalyticsEventSource.Log.PartitionManagerWarning(this.accountName, this.taskHub, this.workerName, $"Unable to add PartitionId '{lease.PartitionId}' with lease token '{lease.Token}' to currently owned partitions.");

                    await this.leaseManager.ReleaseAsync(lease);
                    AnalyticsEventSource.Log.LeaseRemoved(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);
                }
                catch (LeaseLostException)
                {
                    // We have already shutdown the processor so we can ignore any LeaseLost at this point
                    AnalyticsEventSource.Log.LeaseRemovalFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
                }
            }
        }

        async Task RemoveLeaseAsync(T lease, bool hasOwnership)
        {
            CloseReason reason = hasOwnership ? CloseReason.Shutdown : CloseReason.LeaseLost;

            if (lease != null && this.currentlyOwnedShards != null && this.currentlyOwnedShards.TryRemove(lease.PartitionId, out lease))
            {
                AnalyticsEventSource.Log.PartitionRemoved(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);

                try
                {
                    if (hasOwnership)
                    {
                        this.keepRenewingDuringClose.TryAdd(lease.PartitionId, lease);
                    }

                    // Notify the host that we lost shard so shutdown can be triggered on the host
                    await this.partitionObserverManager.NotifyShardReleasedAsync(lease, reason);
                }
                catch (Exception ex)
                {
                    // Eat any exceptions during notification of observers
                    AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
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
                        AnalyticsEventSource.Log.LeaseRemoved(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);
                    }
                    catch (LeaseLostException)
                    {
                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
                        AnalyticsEventSource.Log.LeaseRemovalFailed(this.accountName, this.taskHub, this.workerName, lease.PartitionId, lease.Token);
                    }
                    catch (Exception ex)
                    {
                        AnalyticsEventSource.Log.PartitionManagerError(this.accountName, this.taskHub, this.workerName, ex);
                    }
                }
            }
        }

        sealed class PartitionObserverManager
        {
            readonly PartitionManager<T> partitionManager;
            readonly List<IPartitionObserver<T>> observers;

            public PartitionObserverManager(PartitionManager<T> partitionManager)
            {
                this.partitionManager = partitionManager;
                this.observers = new List<IPartitionObserver<T>>();
            }

            public async Task<IDisposable> SubscribeAsync(IPartitionObserver<T> observer)
            {
                if (!this.observers.Contains(observer))
                {
                    this.observers.Add(observer);

                    foreach (var lease in this.partitionManager.currentlyOwnedShards.Values)
                    {
                        try
                        {
                            await observer.OnPartitionAcquiredAsync(lease);
                        }
                        catch (Exception ex)
                        {
                            // Eat any exceptions during notification of observers
                            AnalyticsEventSource.Log.PartitionManagerError(partitionManager.accountName, partitionManager.taskHub, partitionManager.workerName, ex);
                        }
                    }
                }

                return new Unsubscriber(this.observers, observer);
            }

            public async Task NotifyShardAcquiredAsync(T lease)
            {
                foreach (var observer in this.observers)
                {
                    await observer.OnPartitionAcquiredAsync(lease);
                }
            }

            public async Task NotifyShardReleasedAsync(T lease, CloseReason reason)
            {
                foreach (var observer in this.observers)
                {
                    await observer.OnPartitionReleasedAsync(lease, reason);
                }
            }
        }

        sealed class Unsubscriber : IDisposable
        {
            readonly List<IPartitionObserver<T>> _observers;
            readonly IPartitionObserver<T> _observer;

            internal Unsubscriber(List<IPartitionObserver<T>> observers, IPartitionObserver<T> observer)
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
