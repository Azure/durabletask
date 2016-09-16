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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Tracking;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    // Potential todo:
    //   - Support for querying state across executions (makes sense only after ContinuedAsNew is supported)
    //   - Support writing multiple state events for a given orchestration instance/execution (?)
    //   - Support writing/querying/purging history events
    public class FabricOrchestrationInstanceStore : IFabricOrchestrationServiceInstanceStore
    {
        const string TimeFormatString = "yyyy-MM-dd-HH";
        const string TimeFormatStringPrefix = "yyyy-MM-dd-";
        readonly IReliableStateManager stateManager;
        readonly CancellationTokenSource cancellationTokenSource;
        IReliableDictionary<string, OrchestrationState> instanceStore;

        public FabricOrchestrationInstanceStore(IReliableStateManager stateManager)
        {
            this.stateManager = stateManager;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task InitializeStoreAsync(bool recreate)
        {
            if (recreate)
            {
                await DeleteStoreAsync();
            }
        }

        public async Task StartAsync()
        {
            this.instanceStore = await this.GetOrAddInstanceStoreDictionary();
            var nowait = CleanupDayOldDictionaries();
        }

        public Task StopAsync(bool isForced)
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        public async Task DeleteStoreAsync()
        {
            await this.stateManager.RemoveAsync(Constants.InstanceStoreDictionaryName);
        }

        public async Task WriteEntitesAsync(ITransaction transaction, IEnumerable<InstanceEntityBase> entities)
        {
            var backupDictionaryName = GetDictionaryKeyFromTime(DateTime.UtcNow);
            foreach (var entity in entities)
            {
                var state = entity as OrchestrationStateInstanceEntity;
                if (state != null && state.State != null)
                {
                    string key = GetKey(state.State.OrchestrationInstance.InstanceId, state.State.OrchestrationInstance.ExecutionId);

                    //Todo : Use the utility function after merging from the other branch
                    var status = state.State.OrchestrationStatus;
                    if (status == OrchestrationStatus.Running || status == OrchestrationStatus.Pending)
                    {
                        await this.instanceStore.AddOrUpdateAsync(transaction, key, state.State,
                            (k, oldValue) => state.State);
                    }
                    else
                    {
                        // It's intentional to not pass 'transaction' parameter to this call, this API doesn't seem to follow
                        // see-what-you-commit-within-the-transaction rules. If we add it within the transaction and immediately
                        // try to AddOrUpdateAsync an entry in dictionary that doesn't work.
                        var backupDictionary = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, OrchestrationState>>(backupDictionaryName);
                        await backupDictionary.AddOrUpdateAsync(transaction, key, state.State,
                            (k, oldValue) => state.State);
                        await this.instanceStore.TryRemoveAsync(transaction, key);
                    }
                }
                else
                {
                    throw new NotSupportedException();
                }
            }
        }

        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            throw new NotImplementedException();
        }

        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            var queryKey = this.GetKey(instanceId, executionId);
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

                // If querying for orchestration which completed an hour ago, we won't return the results.
                var now = DateTime.UtcNow;
                for (int i = 0; i < 2; i++)
                {
                    var backupDictionaryName = GetDictionaryKeyFromTime(now - TimeSpan.FromHours(i));
                    var backupDictionary = await this.stateManager.TryGetAsync<IReliableDictionary<string, OrchestrationState>>(backupDictionaryName);

                    if (backupDictionary.HasValue)
                    {
                        state = await backupDictionary.Value.TryGetValueAsync(txn, queryKey);
                        if (state.HasValue)
                        {
                            return new OrchestrationStateInstanceEntity()
                            {
                                State = state.Value,
                            };
                        }
                    }
                }
            }

            return null;
        }

        public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        // Todo: This is incomplete and inaccurate implemenation done for testing purposes.
        // The method will cleanup state for every orchestration happening in the hour time window of given time,
        // for example, if the given time is 9.35, it will delete state for all orchestrations that
        // are completed between 9.00 to 9.59!!!
        public Task PurgeOrchestrationHistoryEventsAsync(DateTime threshholdHourlyDateTimeUtc)
        {
            return this.stateManager.RemoveAsync(GetDictionaryKeyFromTime(threshholdHourlyDateTimeUtc));
        }

        Task<IReliableDictionary<string, OrchestrationState>> GetOrAddInstanceStoreDictionary()
        {
            return this.stateManager.GetOrAddAsync<IReliableDictionary<string, OrchestrationState>>(Constants.InstanceStoreDictionaryName);
        }

        string GetKey(string instanceId, string executionId)
        {
            return string.Concat(instanceId, "_", executionId);
        }

        string GetDictionaryKeyFromTime(DateTime time)
        {
            return time.ToString(TimeFormatString);
        }

        async Task CleanupDayOldDictionaries()
        {
            while (!this.cancellationTokenSource.IsCancellationRequested)
            {
                var purgeTime = (DateTime.UtcNow - TimeSpan.FromDays(1)).ToString(TimeFormatStringPrefix);

                for (int i = 0; i < 24; i++)
                {
                    await this.stateManager.RemoveAsync($"{purgeTime}{i:D2}");
                }

                await Task.Delay(TimeSpan.FromHours(12), this.cancellationTokenSource.Token).ConfigureAwait(false);
            }
        }
    }
}
