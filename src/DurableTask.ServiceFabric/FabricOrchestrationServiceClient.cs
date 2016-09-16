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
    using DurableTask.History;
    using Microsoft.ServiceFabric.Data;

    public class FabricOrchestrationServiceClient : IOrchestrationServiceClient
    {
        IReliableStateManager stateManager;
        IFabricOrchestrationServiceInstanceStore instanceStore;
        SessionsProvider orchestrationProvider;

        public FabricOrchestrationServiceClient(IReliableStateManager stateManager, SessionsProvider orchestrationProvider, IFabricOrchestrationServiceInstanceStore instanceStore)
        {
            this.stateManager = stateManager;
            this.orchestrationProvider = orchestrationProvider;
            this.instanceStore = instanceStore;
        }

        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            if (!(creationMessage.Event is ExecutionStartedEvent))
            {
                throw new Exception("Invalid creation message");
            }

            //Todo: Need to throw if the same instance and execution id is already running

            await this.SendTaskOrchestrationMessageAsync(creationMessage);
            ProviderEventSource.Instance.LogOrchestrationCreated(creationMessage.OrchestrationInstance.InstanceId);
        }

        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.orchestrationProvider.AppendMessageAsync(txn, message);
                await txn.CommitAsync();
            }
        }

        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            await SendTaskOrchestrationMessageAsync(taskMessage);
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
            ThrowIfInstanceStoreNotConfigured();

            if (timeRangeFilterType != OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter)
            {
                throw new NotSupportedException("Purging is supported only for Orchestration completed time filter.");
            }

            return this.instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            ThrowIfInstanceStoreNotConfigured();

            var timeoutSeconds = timeout.TotalSeconds;

            while (timeoutSeconds > 0 && !cancellationToken.IsCancellationRequested)
            {
                var currentState = await this.GetOrchestrationStateAsync(instanceId, executionId);

                if (currentState != null && currentState.OrchestrationStatus.IsTerminalState())
                {
                    return currentState;
                }

                await Task.Delay(2000, cancellationToken);
                timeoutSeconds -= 2;
            }

            return null;
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
