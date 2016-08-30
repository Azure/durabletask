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

using System.Collections.Immutable;
using System.Diagnostics;
using DurableTask.History;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public class FabricOrchestrationServiceClient : IOrchestrationServiceClient
    {
        IReliableStateManager stateManager;
        IOrchestrationServiceInstanceStore instanceStore;

        public FabricOrchestrationServiceClient(IReliableStateManager stateManager, IOrchestrationServiceInstanceStore instanceStore)
        {
            this.stateManager = stateManager;
            this.instanceStore = instanceStore;
        }

        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            if (!(creationMessage.Event is ExecutionStartedEvent))
            {
                throw new Exception("Invalid creation message");
            }

            await this.SendTaskOrchestrationMessageAsync(creationMessage);
        }

        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            var orchestrations = await this.GetOrAddOrchestrationsAsync();

            //Todo: Need to understand how dedup should be done here, the whole execution id is a little unclear
            using (var txn = this.stateManager.CreateTransaction())
            {
                var sessionId = message.OrchestrationInstance.InstanceId;

                //Todo: This is the same code as SessionsProvider.AppendMessages, can perhaps reuse somehow?
                Func<string, PersistentSession> newSessionFactory = (sid) => PersistentSession.CreateWithNewMessage(sid, message);

                await orchestrations.AddOrUpdateAsync(txn, sessionId,
                    addValueFactory: newSessionFactory,
                    updateValueFactory: (ses, oldValue) => oldValue.AppendMessage(message));

                await txn.CommitAsync();
            }
        }

        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            throw new NotImplementedException();
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            ThrowIfInstanceStoreNotConfigured();
            var stateInstance = await this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return stateInstance?.State;
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            ThrowIfInstanceStoreNotConfigured();

            var timeoutSeconds = timeout.TotalSeconds;

            while (timeoutSeconds > 0 && !cancellationToken.IsCancellationRequested)
            {
                var currentState = await this.GetOrchestrationStateAsync(instanceId, executionId);

                if (currentState != null &&
                    currentState.OrchestrationStatus != OrchestrationStatus.Pending &&
                    currentState.OrchestrationStatus != OrchestrationStatus.Running)
                {
                    return currentState;
                }

                await Task.Delay(2000, cancellationToken);
                timeoutSeconds -= 2;
            }

            return null;
        }

        async Task<IReliableDictionary<string, PersistentSession>> GetOrAddOrchestrationsAsync()
        {
            return await this.stateManager.GetOrAddAsync<IReliableDictionary<string, PersistentSession>>(Constants.OrchestrationDictionaryName);
        }

        void ThrowIfInstanceStoreNotConfigured()
        {
            if (this.instanceStore == null)
            {
                throw new InvalidOperationException("Instance store is not configured");
            }
        }
    }
}
