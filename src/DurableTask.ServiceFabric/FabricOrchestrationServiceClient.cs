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
    using DurableTask.Tracking;
    using Microsoft.ServiceFabric.Data;

    class FabricOrchestrationServiceClient : IOrchestrationServiceClient
    {
        readonly IReliableStateManager stateManager;
        readonly IFabricOrchestrationServiceInstanceStore instanceStore;
        readonly SessionsProvider orchestrationProvider;

        public FabricOrchestrationServiceClient(IReliableStateManager stateManager, SessionsProvider orchestrationProvider, IFabricOrchestrationServiceInstanceStore instanceStore)
        {
            this.stateManager = stateManager;
            this.orchestrationProvider = orchestrationProvider;
            this.instanceStore = instanceStore;
        }

        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            ExecutionStartedEvent startEvent = creationMessage.Event as ExecutionStartedEvent;
            if (startEvent == null)
            {
                throw new Exception("Invalid creation message");
            }

            var instance = creationMessage.OrchestrationInstance;

            //Todo: Does this need to throw only if the orchestration is not running Or an orchestration was ever created before with the same id?
            if (await this.orchestrationProvider.SessionExists(instance.InstanceId))
            {
                throw new InvalidOperationException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' is already running.");
            }

            using (var tx = this.stateManager.CreateTransaction())
            {
                await this.orchestrationProvider.AppendMessageAsync(tx, creationMessage);
                await WriteExecutionStartedEventToInstanceStore(tx, startEvent);
                await tx.CommitAsync();
                this.orchestrationProvider.TryEnqueueSession(creationMessage.OrchestrationInstance.InstanceId);
                ProviderEventSource.Log.OrchestrationCreated(instance.InstanceId, instance.ExecutionId);
            }
        }

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return this.orchestrationProvider.AppendMessageAsync(message);
        }

        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            throw new NotImplementedException();
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

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            ThrowIfInstanceStoreNotConfigured();

            var stateInstances = await this.instanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);

            var result = new List<OrchestrationState>();
            foreach(var stateInstance in stateInstances)
            {
                if (stateInstance != null)
                {
                    result.Add(stateInstance.State);
                }
            }
            return result;
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

        //Todo: Timeout logic seems to be broken, don't know why, need to investigate.
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

        Task WriteExecutionStartedEventToInstanceStore(ITransaction tx, ExecutionStartedEvent startEvent)
        {
            var createdTime = DateTime.UtcNow;
            var initialState = new OrchestrationState()
            {
                Name = startEvent.Name,
                Version = startEvent.Version,
                OrchestrationInstance = startEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = startEvent.Input,
                Tags = startEvent.Tags,
                CreatedTime = createdTime,
                LastUpdatedTime = createdTime
            };

            return this.instanceStore.WriteEntitesAsync(tx, new InstanceEntityBase[]
            {
                new OrchestrationStateInstanceEntity()
                {
                    State = initialState
                }
            });
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
