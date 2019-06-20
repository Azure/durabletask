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
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracking;
    using Microsoft.ServiceFabric.Data;

    class FabricOrchestrationServiceClient : IOrchestrationServiceClient
    {
        readonly IReliableStateManager stateManager;
        readonly IFabricOrchestrationServiceInstanceStore instanceStore;
        readonly SessionsProvider orchestrationProvider;

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
                ProviderEventSource.Log.OrchestrationCreated(instance.InstanceId, instance.ExecutionId);
                this.orchestrationProvider.TryEnqueueSession(creationMessage.OrchestrationInstance);
            }
            else
            {
                throw new InvalidOperationException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' is already running.");
            }
        }

        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            // Todo: Support for dedupeStatuses?
            if (dedupeStatuses != null)
            {
                throw new NotSupportedException($"DedupeStatuses are not supported yet with service fabric provider");
            }

            return CreateTaskOrchestrationAsync(creationMessage);
        }

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return this.orchestrationProvider.AppendMessageAsync(new TaskMessageItem(message));
        }

        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            foreach(var message in messages)
            {
                await this.SendTaskOrchestrationMessageAsync(message);
            }
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

        /// <summary>
        /// Wait till the specified time span for the latest orchestration execution to complete.
        /// The service bus client implementation of this API ignores the input executionId parameter and
        /// only returns if the latest execution of a given orchestration instance is completed before
        /// the specified time out. Perhaps there is a good reason - for an orchestration which uses ContinueAsNew,
        /// the orchestration is not really complete though one execution may have been completed (typically with
        /// ContinueAsNew status), the user of this API would most likely want to return from this
        /// method only if the latest execution of such an orchestration is completed instead of a given
        /// iteration. Hence, we match the service fabric provider functionality to be the same.
        /// </summary>
        /// <param name="instanceId">The instanceId of an orchestation to wait to complete.</param>
        /// <param name="executionId">Ignored and instead the latest execution of Orchestration is considered.</param>
        /// <param name="timeout">Timespan of wait. If the orchestration completes before the specified
        /// time span, the API immediately returns with the state of Orchestration. Otherwise, the API
        /// waits till the time span and returns the state of Orchestration at that time.</param>
        /// <param name="cancellationToken">Cancellation token for preempting the waiting before the timeout.</param>
        /// <returns>The state of orchestration with the given instanceId if it completes before the timespace. Otherwise null.</returns>
        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var state = await this.instanceStore.WaitForOrchestrationAsync(instanceId, timeout);
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
