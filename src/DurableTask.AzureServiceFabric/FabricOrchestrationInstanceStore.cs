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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Core.Tracking;
    using DurableTask.AzureServiceFabric.TaskHelpers;
    using DurableTask.AzureServiceFabric.Tracing;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    // Potential todo:
    //   - Support for querying state across executions (makes sense only after ContinuedAsNew is supported)
    //   - Support writing multiple state events for a given orchestration instance/execution (?)
    //   - Support writing/querying/purging history events
    class FabricOrchestrationInstanceStore : IFabricOrchestrationServiceInstanceStore
    {
        const string InstanceStoreCollectionNamePrefix = Constants.CollectionNameUniquenessPrefix + "InstSt_";
        const string TimeFormatString =  "yyyy-MM-dd-HH";
        const string TimeFormatStringPrefix = "yyyy-MM-dd-";
        readonly IReliableStateManager stateManager;
        readonly CancellationToken cancellationToken;
        readonly ConcurrentDictionary<OrchestrationInstance, AsyncManualResetEvent> orchestrationWaiters = new ConcurrentDictionary<OrchestrationInstance, AsyncManualResetEvent>(OrchestrationInstanceComparer.Default);

        IReliableDictionary<string, OrchestrationState> instanceStore;
        IReliableDictionary<string, List<string>> executionIdStore;

        public FabricOrchestrationInstanceStore(IReliableStateManager stateManager, CancellationToken token)
        {
            this.stateManager = stateManager;
            this.cancellationToken = token;
        }

        public int MaxExecutionIdsLength { get; set; } = 100;

        public Task InitializeStoreAsync(bool recreate)
        {
            if (recreate)
            {
                return DeleteStoreAsync();
            }

            return Task.CompletedTask;
        }

        public async Task StartAsync()
        {
            await EnsureStoreInitializedAsync();
            var nowait = CleanupOldDictionariesAsync();
        }

        public async Task DeleteStoreAsync()
        {
            await this.stateManager.RemoveAsync(Constants.InstanceStoreDictionaryName);
            await this.stateManager.RemoveAsync(Constants.ExecutionStoreDictionaryName);
        }

        public async Task WriteEntitiesAsync(ITransaction transaction, IEnumerable<InstanceEntityBase> entities)
        {
            await EnsureStoreInitializedAsync();
            foreach (var entity in entities)
            {
                var state = entity as OrchestrationStateInstanceEntity;
                if (state != null && state.State != null)
                {
                    var instance = state.State.OrchestrationInstance;
                    string key = GetKey(instance.InstanceId, instance.ExecutionId);

                    if (state.State.OrchestrationStatus.IsRunningOrPending())
                    {
                        if (state.State.OrchestrationStatus == OrchestrationStatus.Pending)
                        {
                            await this.executionIdStore.AddOrUpdateAsync(transaction, instance.InstanceId, new List<string> { instance.ExecutionId },
                                (k, old) =>
                                {
                                    old.Add(instance.ExecutionId);
                                    if (old.Count > this.MaxExecutionIdsLength)
                                    {
                                        // Remove first 10% items.
                                        int skipItemsLength = (int)(this.MaxExecutionIdsLength * 0.1);
                                        old = old.Skip(skipItemsLength).ToList();
                                    }

                                    return old;
                                });
                        }

                        await this.instanceStore.AddOrUpdateAsync(transaction, key, state.State, (k, oldValue) => state.State);
                    }
                    else
                    {
                        var backupDictionaryName = GetDictionaryKeyFromTimeFormat(DateTime.UtcNow);
                        // It's intentional to not pass 'transaction' parameter to this call, this API doesn't seem to follow
                        // see-what-you-commit-within-the-transaction rules. If we add it within the transaction and immediately
                        // try to AddOrUpdateAsync an entry in dictionary that doesn't work.
                        var backupDictionary = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, OrchestrationState>>(backupDictionaryName);
                        await backupDictionary.AddOrUpdateAsync(transaction, key, state.State, (k, oldValue) => state.State);
                        await this.instanceStore.TryRemoveAsync(transaction, key);
                    }
                }
                else
                {
                    throw new NotSupportedException();
                }
            }
        }

        public async Task<IList<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            if (allInstances)
            {
                throw new NotImplementedException("Querying for state across all executions for an orchestration is not supported, only the latest execution can be queried");
            }

            await EnsureStoreInitializedAsync();

            string latestExecutionId = (await GetExecutionIds(instanceId))?.Last();

            if (latestExecutionId != null)
            {
                var state = await GetOrchestrationStateAsync(instanceId, latestExecutionId);
                if (state != null)
                {
                    return new List<OrchestrationStateInstanceEntity>() { state };
                }
            }

            return ImmutableList<OrchestrationStateInstanceEntity>.Empty;
        }

        public async Task<List<string>> GetExecutionIds(string instanceId)
        {
            List<string> executionIds = null;

            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var tx = this.stateManager.CreateTransaction())
                {
                    var executionIdsValue = await this.executionIdStore.TryGetValueAsync(tx, instanceId);
                    if (executionIdsValue.HasValue)
                    {
                        executionIds = executionIdsValue.Value;
                    }
                }
            }, uniqueActionIdentifier: $"Orchestration Instance Id = {instanceId}, Action = {nameof(FabricOrchestrationInstanceStore)}.{nameof(GetExecutionIds)}");

            return executionIds;
        }

        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            await EnsureStoreInitializedAsync();
            var queryKey = this.GetKey(instanceId, executionId);
            var result = await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var state = await this.instanceStore.TryGetValueAsync(txn, queryKey);
                    if (state.HasValue)
                    {
                        return new OrchestrationStateInstanceEntity()
                        {
                            State = state.Value,
                        };
                    }
                }
                return null;
            }, uniqueActionIdentifier: $"Orchestration Instance Id = {instanceId}, ExecutionId = {executionId}, Action = {nameof(FabricOrchestrationInstanceStore)}.{nameof(GetOrchestrationStateAsync)}:QueryInstanceStore");

            if (result != null)
            {
                return result;
            }

            // If querying for orchestration which completed an hour ago, we won't return the results.
            var now = DateTime.UtcNow;
            for (int i = 0; i < 2; i++)
            {
                var backupDictionaryName = GetDictionaryKeyFromTimeFormat(now - TimeSpan.FromHours(i));
                var backupDictionary = await this.stateManager.TryGetAsync<IReliableDictionary<string, OrchestrationState>>(backupDictionaryName);

                if (backupDictionary.HasValue)
                {
                    result = await RetryHelper.ExecuteWithRetryOnTransient(async () =>
                    {
                        using (var txn = this.stateManager.CreateTransaction())
                        {
                            var state = await backupDictionary.Value.TryGetValueAsync(txn, queryKey);
                            if (state.HasValue)
                            {
                                return new OrchestrationStateInstanceEntity()
                                {
                                    State = state.Value,
                                };
                            }
                        }

                        return null;
                    }, uniqueActionIdentifier: $"Orchestration Instance Id = {instanceId}, ExecutionId = {executionId}, Action = {nameof(FabricOrchestrationInstanceStore)}.{nameof(GetOrchestrationStateAsync)}:QueryBackupInstanceStore {backupDictionaryName}");

                    if (result != null)
                    {
                        return result;
                    }
                }
            }

            return null;
        }

        public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        // Todo: This is incomplete and inaccurate implementation done for testing purposes.
        // The method will cleanup state for every orchestration happening in the hour time window of given time,
        // for example, if the given time is 9.35, it will delete state for all orchestrations that
        // are completed between 9.00 to 9.59!!!
        public async Task PurgeOrchestrationHistoryEventsAsync(DateTime threshholdHourlyDateTimeUtc)
        {
            try
            {
                await this.stateManager.RemoveAsync(GetDictionaryKeyFromTimeFormat(threshholdHourlyDateTimeUtc));
            }
            catch (Exception e)
            {
                ServiceFabricProviderEventSource.Tracing.LogProxyServiceError($"PurgeOrchestrationHistoryEventsAsync failed with exception {e.ToString()}");
            }
        }

        string GetKey(string instanceId, string executionId)
        {
            return string.Concat(instanceId, "_", executionId);
        }

        string GetDictionaryKeyFromTimeFormat(DateTime time)
        {
            return GetInstanceStoreBackupDictionaryKey(time, TimeFormatString);
        }

        string GetDictionaryKeyFromTimePrefixFormat(DateTime time)
        {
            return GetInstanceStoreBackupDictionaryKey(time, TimeFormatStringPrefix);
        }

        string GetInstanceStoreBackupDictionaryKey(DateTime time, string formatString)
        {
            return InstanceStoreCollectionNamePrefix + time.ToString(formatString);
        }

        Task CleanupDayOldDictionariesAsync()
        {
            return Utils.RunBackgroundJob(async () =>
            {
                var purgeTime = GetDictionaryKeyFromTimePrefixFormat(DateTime.UtcNow - TimeSpan.FromDays(1));

                for (int i = 0; i < 24; i++)
                {
                    await this.stateManager.RemoveAsync($"{purgeTime}{i:D2}");
                }
            }, initialDelay: TimeSpan.FromMinutes(5), delayOnSuccess: TimeSpan.FromHours(12), delayOnException: TimeSpan.FromHours(1), actionName: $"{nameof(CleanupDayOldDictionariesAsync)}", token: this.cancellationToken);
        }

        Task CleanupOldDictionariesAsync()
        {
            return Utils.RunBackgroundJob(async () =>
            {
                List<string> toDelete = new List<string>();
                List<string> toKeep = new List<string>();
                var currentTime = DateTime.UtcNow;
                var ttl = TimeSpan.FromDays(1);

                var enumerationTime = await Utils.MeasureAsync(async () =>
                {
                    var enumerator = this.stateManager.GetAsyncEnumerator();
                    while (await enumerator.MoveNextAsync(this.cancellationToken))
                    {
                        var storeName = enumerator.Current.Name.AbsolutePath.Trim('/');
                        if (storeName.StartsWith(InstanceStoreCollectionNamePrefix))
                        {
                            DateTime storeTime;
                            var stringDate = storeName.Substring(InstanceStoreCollectionNamePrefix.Length);
                            if (DateTime.TryParseExact(stringDate, TimeFormatString, CultureInfo.InvariantCulture, DateTimeStyles.None, out storeTime)
                                && (currentTime - storeTime > ttl))
                            {
                                toDelete.Add(storeName);
                                continue;
                            }
                        }

                        toKeep.Add(storeName);
                    }
                });
                ServiceFabricProviderEventSource.Tracing.LogTimeTaken($"Enumerating all reliable states (count: {toDelete.Count + toKeep.Count})", enumerationTime.TotalMilliseconds);

                ServiceFabricProviderEventSource.Tracing.ReliableStateManagement($"Deleting {toDelete.Count} stores", String.Join(",", toDelete));
                ServiceFabricProviderEventSource.Tracing.ReliableStateManagement($"All remaining {toKeep.Count} stores", String.Join(",", toKeep));

                foreach (var storeName in toDelete)
                {
                    var deleteTime = await Utils.MeasureAsync(async () =>
                    {
                        await this.stateManager.RemoveAsync(storeName);
                    });
                    ServiceFabricProviderEventSource.Tracing.LogTimeTaken($"Deleting reliable state {storeName}", deleteTime.TotalMilliseconds);
                }
            }, initialDelay: TimeSpan.FromMinutes(5), delayOnSuccess: TimeSpan.FromHours(1), delayOnException: TimeSpan.FromMinutes(10), actionName: $"{nameof(CleanupOldDictionariesAsync)}", token: this.cancellationToken);
        }

        async Task EnsureStoreInitializedAsync()
        {
            if (this.instanceStore == null)
            {
                this.instanceStore = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, OrchestrationState>>(Constants.InstanceStoreDictionaryName);
            }
            if (this.executionIdStore == null)
            {
                this.executionIdStore = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, List<string>>>(Constants.ExecutionStoreDictionaryName);
            }
        }

        public async Task<OrchestrationStateInstanceEntity> WaitForOrchestrationAsync(OrchestrationInstance instance, TimeSpan timeout)
        {
            var currentState = await this.GetOrchestrationStateAsync(instance.InstanceId, instance.ExecutionId);

            // If querying state for an orchestration that's not started or completed and state cleaned up, we will immediately return null.
            if (currentState?.State == null)
            {
                return null;
            }

            if (currentState.State.OrchestrationStatus.IsTerminalState())
            {
                return currentState;
            }

            var waiter = this.orchestrationWaiters.GetOrAdd(instance, new AsyncManualResetEvent());
            bool completed = await waiter.WaitAsync(timeout, this.cancellationToken);

            if (!completed)
            {
                this.orchestrationWaiters.TryRemove(instance, out waiter);
            }

            currentState = await this.GetOrchestrationStateAsync(instance.InstanceId, instance.ExecutionId);
            if (currentState?.State != null && currentState.State.OrchestrationStatus.IsTerminalState())
            {
                return currentState;
            }

            return null;
        }

        public void OnOrchestrationCompleted(OrchestrationInstance instance)
        {
            if (this.orchestrationWaiters.TryRemove(instance, out AsyncManualResetEvent resetEvent))
            {
                resetEvent.Set();
            }
        }
    }
}
