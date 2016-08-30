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
    using System.Threading.Tasks;
    using DurableTask.Tracking;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    // For now a simple instance store with no history tracking and only latest state is persisted
    public class FabricOrchestrationInstanceStore : IOrchestrationServiceInstanceStore
    {
        IReliableStateManager stateManager;

        public FabricOrchestrationInstanceStore(IReliableStateManager stateManager)
        {
            this.stateManager = stateManager;
        }

        public int MaxHistoryEntryLength { get; }

        public async Task InitializeStoreAsync(bool recreate)
        {
            if (recreate)
            {
                await DeleteStoreAsync();
            }
        }

        public async Task DeleteStoreAsync()
        {
            await this.stateManager.RemoveAsync(Constants.InstanceStoreDictionaryName);
        }

        public async Task<object> WriteEntitesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            var instaceStore = await this.GetOrAddInstanceStoreDictionary();
            using (var txn = this.stateManager.CreateTransaction())
            {
                foreach (var entity in entities)
                {
                    var state = entity as OrchestrationStateInstanceEntity;
                    if (state != null)
                    {
                        string key = GetKey(state.State.OrchestrationInstance.InstanceId, state.State.OrchestrationInstance.ExecutionId);
                        await instaceStore.AddOrUpdateAsync(txn, key, state.State,
                            (k, oldValue) => state.State); //Todo: Need to check for sequence number?
                    }
                }

                await txn.CommitAsync();
            }
            return null;
        }

        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitesAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<object> DeleteEntitesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            throw new NotImplementedException();
        }

        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            var instaceStore = await this.GetOrAddInstanceStoreDictionary();
            using (var txn = this.stateManager.CreateTransaction())
            {
                var state = await instaceStore.TryGetValueAsync(txn, this.GetKey(instanceId, executionId));
                if (state.HasValue)
                {
                    return new OrchestrationStateInstanceEntity()
                    {
                        State = state.Value,
                    };
                }
            }

            return null;
        }

        public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public Task<object> WriteJumpStartEntitesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            throw new NotImplementedException();
        }

        public Task<object> DeleteJumpStartEntitesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitesAsync(int top)
        {
            throw new NotImplementedException();
        }

        Task<IReliableDictionary<string, OrchestrationState>> GetOrAddInstanceStoreDictionary()
        {
            return this.stateManager.GetOrAddAsync<IReliableDictionary<string, OrchestrationState>>(Constants.InstanceStoreDictionaryName);
        }

        string GetKey(string instanceId, string executionId)
        {
            return String.Concat(instanceId, "_", executionId);
        }
    }
}
