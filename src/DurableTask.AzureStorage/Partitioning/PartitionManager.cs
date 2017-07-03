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
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class PartitionManager<T> where T : Lease
    {
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

        public PartitionManager(string workerName, ILeaseManager<T> leaseManager, PartitionManagerOptions options)
        {
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
            foreach (Task<T> leaseTask in this.leaseManager.ListLeases())
            {
                T lease = await leaseTask;
                if (string.Compare(lease.Owner, this.workerName, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    bool renewed = await this.RenewLeaseAsync(lease);
                    if (renewed)
                    {
                        leases.Add(lease);
                    }
                    else
                    {
                        AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' unable to renew lease '{1}' on startup.",
                            this.workerName, lease.PartitionId));
                    }
                }
            }

            var addLeaseTasks = new List<Task>();
            foreach (T lease in leases)
            {
                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' acquired lease for PartitionId '{1}' on startup.",
                    this.workerName, lease.PartitionId));
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
            while (this.isStarted == 1 || !shutdownComplete)
            {
                try
                {
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' starting renewal of Leases.", this.workerName));

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
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' Renewer task canceled.", this.workerName));
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.LogPartitionException(ex);
                }
            }

            this.currentlyOwnedShards.Clear();
            this.keepRenewingDuringClose.Clear();
            AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' Renewer task completed.", this.workerName));
        }

        async Task LeaseTakerAsync()
        {
            while (this.isStarted == 1)
            {
                try
                {
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' starting to check for available leases.", this.workerName));
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
                    AnalyticsEventSource.Log.LogPartitionException(ex);
                }

                try
                {
                    await Task.Delay(this.options.AcquireInterval, this.leaseTakerCancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' AcquireLease task canceled.", this.workerName));
                }
            }

            AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' AcquireLease task completed.", this.workerName));
        }

        async Task<IDictionary<string, T>> TakeLeasesAsync()
        {
            var allShards = new Dictionary<string, T>();
            var takenLeases = new Dictionary<string, T>();
            var workerToShardCount = new Dictionary<string, int>();
            var expiredLeases = new List<T>();

            foreach (var leaseTask in this.leaseManager.ListLeases())
            {
                T lease = await leaseTask;
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

                            AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' attempting to take lease for PartitionId '{1}'.",
                                this.workerName, leaseToTake.PartitionId));
                            bool leaseAcquired = await this.AcquireLeaseAsync(leaseToTake);
                            if (leaseAcquired)
                            {
                                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' successfully acquired lease for PartitionId '{1}'.",
                                    this.workerName, leaseToTake.PartitionId));
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
                                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' attempting to steal lease from '{1}' for PartitionId '{2}'.",
                                        this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId));
                                    bool leaseStolen = await this.StealLeaseAsync(leaseToTake);
                                    if (leaseStolen)
                                    {
                                        AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' stole lease from '{1}' for PartitionId '{2}'.",
                                            this.workerName, workerToStealFrom.Key, leaseToTake.PartitionId));
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
            string errorMessage = null;

            try
            {
                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' renewing lease for PartitionId '{1}' with lease token '{2}'",
                        this.workerName, lease.PartitionId, lease.Token));

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

            AnalyticsEventSource.Log.LogPartitionInfo(
                string.Format(CultureInfo.InvariantCulture, "Host '{0}' attempted to renew lease for PartitionId '{1}' and lease token '{2}' with result: '{3}'. error = {4}",
                this.workerName, lease.PartitionId, lease.Token ?? string.Empty, renewed, errorMessage ?? string.Empty));

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
                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' failed to acquire lease for PartitionId '{1}' due to conflict.",
                    this.workerName, lease.PartitionId));
            }
            catch (Exception ex)
            {
                // Eat any exceptions during acquiring lease.
                AnalyticsEventSource.Log.LogPartitionException(ex);
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
                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' failed to steal lease for PartitionId '{1}' due to conflict.",
                    this.workerName, lease.PartitionId));
            }
            catch (Exception ex)
            {
                // Eat any exceptions during stealing
                AnalyticsEventSource.Log.LogPartitionException(ex);
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
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' opening event processor for PartitionId '{1}' and lease token '{2}'",
                        this.workerName, lease.PartitionId, lease.Token));

                    await this.partitionObserverManager.NotifyShardAcquiredAsync(lease);

                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' opened event processor for PartitionId '{1}' and lease token '{2}'",
                       this.workerName, lease.PartitionId, lease.Token));
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' failed to initialize processor for PartitionId '{1}' and lease token '{2}'",
                       this.workerName, lease.PartitionId, lease.Token));

                    failedToInitialize = true;
                    // Eat any exceptions during notification of observers
                    AnalyticsEventSource.Log.LogPartitionException(ex);
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
                    AnalyticsEventSource.Log.LogPartitionWarning(string.Format(CultureInfo.InvariantCulture, "Host '{0}' unable to add PartitionId '{1}' with lease token '{2}' to currently owned partitions.",
                        this.workerName, lease.PartitionId, lease.Token));

                    await this.leaseManager.ReleaseAsync(lease);

                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' successfully released lease on PartitionId '{1}' with lease token '{2}'",
                        this.workerName, lease.PartitionId, lease.Token));
                }
                catch (LeaseLostException)
                {
                    // We have already shutdown the processor so we can ignore any LeaseLost at this point
                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' failed to release lease for PartitionId '{1}' with lease token '{2}' due to conflict.",
                        this.workerName, lease.PartitionId, lease.Token));
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.LogPartitionException(ex);
                }
            }
        }

        async Task RemoveLeaseAsync(T lease, bool hasOwnership)
        {
            CloseReason reason = hasOwnership ? CloseReason.Shutdown : CloseReason.LeaseLost;

            if (lease != null && this.currentlyOwnedShards != null && this.currentlyOwnedShards.TryRemove(lease.PartitionId, out lease))
            {
                AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' successfully removed PartitionId '{1}' with lease token '{2}' from currently owned partitions.",
                    this.workerName, lease.PartitionId, lease.Token));

                try
                {
                    if (hasOwnership)
                    {
                        this.keepRenewingDuringClose.TryAdd(lease.PartitionId, lease);
                    }

                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' closing event processor for PartitionId '{1}' and lease token '{2}' with reason '{3}'",
                        this.workerName, lease.PartitionId, lease.Token, reason));

                    // Notify the host that we lost shard so shutdown can be triggered on the host
                    await this.partitionObserverManager.NotifyShardReleasedAsync(lease, reason);

                    AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' closed event processor for PartitionId '{1}' and lease token '{2}' with reason '{3}'",
                        this.workerName, lease.PartitionId, lease.Token, reason));
                }
                catch (Exception ex)
                {
                    // Eat any exceptions during notification of observers
                    AnalyticsEventSource.Log.LogPartitionException(ex);
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
                        AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' successfully released lease on PartitionId '{1}' with lease token '{2}'",
                            this.workerName, lease.PartitionId, lease.Token));
                    }
                    catch (LeaseLostException)
                    {
                        // We have already shutdown the processor so we can ignore any LeaseLost at this point
                        AnalyticsEventSource.Log.LogPartitionInfo(string.Format(CultureInfo.InvariantCulture, "Host '{0}' failed to release lease for PartitionId '{1}' with lease token '{2}' due to conflict.",
                            this.workerName, lease.PartitionId, lease.Token));
                    }
                    catch (Exception ex)
                    {
                        AnalyticsEventSource.Log.LogPartitionException(ex);
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
                            AnalyticsEventSource.Log.LogPartitionException(ex);
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
