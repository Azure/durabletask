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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.History;
    using DurableTask.Serializing;
    using DurableTask.Tracking;
    using Microsoft.ServiceFabric.Data;
    using Newtonsoft.Json;

    class FabricOrchestrationServiceClient : IOrchestrationServiceClient, IFabricProviderClient
    {
        readonly IReliableStateManager stateManager;
        readonly IFabricOrchestrationServiceInstanceStore instanceStore;
        readonly SessionsProvider orchestrationProvider;
        readonly JsonDataConverter FormattingConverter = new JsonDataConverter(new JsonSerializerSettings() { Formatting = Formatting.Indented });

        public FabricOrchestrationServiceClient(IReliableStateManager stateManager, SessionsProvider orchestrationProvider, IFabricOrchestrationServiceInstanceStore instanceStore)
        {
            this.stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
            this.orchestrationProvider = orchestrationProvider ?? throw new ArgumentNullException(nameof(orchestrationProvider));
            this.instanceStore = instanceStore ?? throw new ArgumentNullException(nameof(instanceStore));
        }

        #region IOrchestrationServiceClient
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            ExecutionStartedEvent startEvent = creationMessage.Event as ExecutionStartedEvent;
            if (startEvent == null)
            {
                await this.SendTaskOrchestrationMessageAsync(creationMessage);
                return;
            }

            var instance = creationMessage.OrchestrationInstance;

            //Todo: Does this need to throw only if the orchestration is not running Or an orchestration was ever created before with the same id?
            if (await this.orchestrationProvider.SessionExists(instance))
            {
                throw new InvalidOperationException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' is already running.");
            }

            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var tx = this.stateManager.CreateTransaction())
                {
                    await this.orchestrationProvider.AppendMessageAsync(tx, new TaskMessageItem(creationMessage));
                    await WriteExecutionStartedEventToInstanceStore(tx, startEvent);
                    await tx.CommitAsync();
                }
            }, uniqueActionIdentifier: $"Orchestration = '{instance}', Action = '{nameof(CreateTaskOrchestrationAsync)}'");

            ProviderEventSource.Log.OrchestrationCreated(instance.InstanceId, instance.ExecutionId);
            this.orchestrationProvider.TryEnqueueSession(creationMessage.OrchestrationInstance);
        }

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return this.orchestrationProvider.AppendMessageAsync(new TaskMessageItem(message));
        }

        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            throw new NotImplementedException();
        }

        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            var latestExecutionId = await this.instanceStore.GetLatestExecutionId(instanceId);

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
            var stateInstance = await this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId);
            return stateInstance?.State;
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
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
            var instance = new OrchestrationInstance() { InstanceId = instanceId, ExecutionId = executionId };
            var state = await this.instanceStore.WaitForOrchestrationAsync(instance, timeout);
            return state?.State;
        }
        #endregion

        #region IFabricProviderClient
        public async Task<IEnumerable<OrchestrationInstance>> GetRunningOrchestrations()
        {
            var sessions = await this.orchestrationProvider.GetSessions();
            return sessions.Select(s => s.SessionId);
        }

        public async Task<string> GetOrchestrationRuntimeState(string instanceId)
        {
            var session = await this.orchestrationProvider.GetSession(instanceId);
            if (session == null)
            {
                throw new ArgumentException($"There is no running or pending Orchestration with the instanceId {instanceId}");
            }
            return FormattingConverter.Serialize(session.SessionState);
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

            return this.instanceStore.WriteEntitesAsync(tx, new InstanceEntityBase[]
            {
                new OrchestrationStateInstanceEntity()
                {
                    State = initialState
                }
            });
        }
    }
}
