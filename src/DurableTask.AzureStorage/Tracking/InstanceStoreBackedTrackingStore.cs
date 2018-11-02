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

namespace DurableTask.AzureStorage.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracking;

    class InstanceStoreBackedTrackingStore : TrackingStoreBase
    {

        readonly IOrchestrationServiceInstanceStore instanceStore;

        /// <inheritdoc />
        public InstanceStoreBackedTrackingStore(IOrchestrationServiceInstanceStore instanceStore)
        {
            this.instanceStore = instanceStore;
        }

        /// <inheritdoc />
        public override Task CreateAsync()
        {
            return this.instanceStore.InitializeStoreAsync(false);
        }

        /// <inheritdoc />
        public override Task DeleteAsync()
        {
            return this.instanceStore.DeleteStoreAsync();
        }

        /// <summary>
        /// Instance Store Does not Support this currently
        /// </summary>
        /// <returns></returns>
        public override Task<bool> ExistsAsync()
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override async Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            //If no execution Id is provided get the latest executionId by getting the latest state
            if (expectedExecutionId == null)
            {
                expectedExecutionId = (await instanceStore.GetOrchestrationStateAsync(instanceId, false)).FirstOrDefault()?.State.OrchestrationInstance.ExecutionId;
            }

            var events = await instanceStore.GetOrchestrationHistoryEventsAsync(instanceId, expectedExecutionId);

            if (events == null || !events.Any())
            {
                return new OrchestrationHistory(EmptyHistoryEventList);
            }
            else
            {
                return new OrchestrationHistory(events.Select(x => x.HistoryEvent).ToList());
            }
        }

        public override Task<IList<string>> RewindHistoryAsync(string instanceId, IList<string> failedLeaves, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput = true)
        {
            IEnumerable<OrchestrationStateInstanceEntity> states = await instanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            return states?.Select(s => s.State).ToList() ?? new List<OrchestrationState>();
        }

        /// <inheritdoc />
        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput = true)
        {
            if (executionId == null)
            {
                return (await GetStateAsync(instanceId, false)).FirstOrDefault();
            }
            else
            {
                return (await instanceStore.GetOrchestrationStateAsync(instanceId, executionId))?.State;
            }
        }

        /// <inheritdoc />
        public override Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override Task<IList<OrchestrationState>> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default(CancellationToken))
       {
            throw new NotImplementedException();
        }
    
        /// <inheritdoc />
        public override Task<DurableStatusQueryResult> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            return instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);
        }

        /// <inheritdoc />
        public override async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = executionStartedEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = executionStartedEvent.Timestamp,
                LastUpdatedTime = DateTime.UtcNow,
                CompletedTime = Core.Common.DateTimeUtils.MinDateTime
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity()
            {
                State = orchestrationState,
                SequenceNumber = 0
            };

            await this.instanceStore.WriteEntitiesAsync(new[] { orchestrationStateEntity });
        }

        public override Task UpdateStatusForRewindAsync(string instanceId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override Task StartAsync()
        {
            //NOP
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public override async Task<string> UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId, string eTag)
        {
            int oldEventsCount = (runtimeState.Events.Count - runtimeState.NewEvents.Count);
            await instanceStore.WriteEntitiesAsync(runtimeState.NewEvents.Select((x, i) =>
            {
                return new OrchestrationWorkItemInstanceEntity()
                {
                    HistoryEvent = x,
                    ExecutionId = executionId,
                    InstanceId = instanceId,
                    SequenceNumber = i + oldEventsCount,
                    EventTimestamp = x.Timestamp
                };

            }));

            await instanceStore.WriteEntitiesAsync(new InstanceEntityBase[]
            {
                    new OrchestrationStateInstanceEntity()
                    {
                        State = Core.Common.Utils.BuildOrchestrationState(runtimeState),
                        SequenceNumber = runtimeState.Events.Count
                    }
            });

            return null;
        }
    }
}
