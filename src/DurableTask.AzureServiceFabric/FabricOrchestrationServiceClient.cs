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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracking;
    using DurableTask.AzureServiceFabric.Stores;
    using DurableTask.AzureServiceFabric.TaskHelpers;
    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.ServiceFabric.Data;

    using Newtonsoft.Json;

    class FabricOrchestrationServiceClient : IOrchestrationServiceClient
    {
        readonly IReliableStateManager stateManager;
        readonly IFabricOrchestrationServiceInstanceStore instanceStore;
        readonly SessionProvider orchestrationProvider;

        public FabricOrchestrationServiceClient(IReliableStateManager stateManager, SessionProvider orchestrationProvider, IFabricOrchestrationServiceInstanceStore instanceStore)
        {
            this.stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
            this.orchestrationProvider = orchestrationProvider ?? throw new ArgumentNullException(nameof(orchestrationProvider));
            this.instanceStore = instanceStore ?? throw new ArgumentNullException(nameof(instanceStore));
        }

        #region IOrchestrationServiceClient
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            creationMessage.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            ExecutionStartedEvent startEvent = creationMessage.Event as ExecutionStartedEvent;
            if (startEvent == null)
            {
                await this.SendTaskOrchestrationMessageAsync(creationMessage);
                return;
            }

            var instance = creationMessage.OrchestrationInstance;

            var added = await RetryHelper.ExecuteWithRetryOnTransient<bool>(async () =>
            {
                using (var tx = this.stateManager.CreateTransaction())
                {
                    if (await this.orchestrationProvider.TryAddSession(tx, new TaskMessageItem(creationMessage)))
                    {
                        await WriteExecutionStartedEventToInstanceStore(tx, startEvent);
                        await tx.CommitAsync();
                        return true;
                    }

                    return false;
                }
            }, uniqueActionIdentifier: $"Orchestration = '{instance}', Action = '{nameof(CreateTaskOrchestrationAsync)}'");

            if (added)
            {
                string message = string.Format("Orchestration with instanceId : '{0}' and executionId : '{1}' is Created.", instance.InstanceId, instance.ExecutionId);
                ServiceFabricProviderEventSource.Tracing.LogOrchestrationInformation(instance.InstanceId, instance.ExecutionId, message);
                this.orchestrationProvider.TryEnqueueSession(creationMessage.OrchestrationInstance);
            }
            else
            {
                throw new OrchestrationAlreadyExistsException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' is already running.");
            }
        }

        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            // Todo: Support for dedupeStatuses?
            if (dedupeStatuses != null)
            {
                throw new NotSupportedException($"DedupeStatuses are not supported yet with service fabric provider");
            }

            creationMessage.OrchestrationInstance.InstanceId.EnsureValidInstanceId();

            return CreateTaskOrchestrationAsync(creationMessage);
        }

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            return this.orchestrationProvider.AppendMessageAsync(new TaskMessageItem(message));
        }

        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            foreach(var message in messages)
            {
                message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
                await this.SendTaskOrchestrationMessageAsync(message);
            }
        }

        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            instanceId.EnsureValidInstanceId();
            var latestExecutionId = (await this.instanceStore.GetExecutionIds(instanceId)).Last();

            if (latestExecutionId == null)
            {
                throw new ArgumentException($"No execution id found for given instanceId {instanceId}, can only terminate the latest execution of a given orchestration");
            }

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId, ExecutionId = latestExecutionId },
                Event = new ExecutionTerminatedEvent(-1, reason)
            };

            await SendTaskOrchestrationMessageAsync(taskMessage);
        }

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            instanceId.EnsureValidInstanceId();
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
            instanceId.EnsureValidInstanceId();
            var stateInstance = await this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return stateInstance?.State;
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            instanceId.EnsureValidInstanceId();

            // Other implementations returns full history for the execution.
            // This implementation returns just the final history, i.e., state.
            var result = JsonConvert.SerializeObject(this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId));
            return Task.FromResult(result);
        }

        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            if (timeRangeFilterType != OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter)
            {
                throw new NotSupportedException("Purging is supported only for Orchestration completed time filter.");
            }

            return this.instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            instanceId.EnsureValidInstanceId();
            var instance = new OrchestrationInstance() { InstanceId = instanceId, ExecutionId = executionId };
            var state = await this.instanceStore.WaitForOrchestrationAsync(instance, timeout);
            return state?.State;
        }
        #endregion

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

            return this.instanceStore.WriteEntitiesAsync(tx, new InstanceEntityBase[]
            {
                new OrchestrationStateInstanceEntity()
                {
                    State = initialState
                }
            });
        }
    }
}
