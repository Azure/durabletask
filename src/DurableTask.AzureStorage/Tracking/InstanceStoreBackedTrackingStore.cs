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
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
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
        public override Task CreateAsync(CancellationToken cancellationToken = default)
        {
            return this.instanceStore.InitializeStoreAsync(false);
        }

        /// <inheritdoc />
        public override Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            return this.instanceStore.DeleteStoreAsync();
        }

        /// <inheritdoc />
        public override async Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            //If no execution Id is provided get the latest executionId by getting the latest state
            if (expectedExecutionId == null)
            {
                expectedExecutionId = (await this.instanceStore.GetOrchestrationStateAsync(instanceId, false)).FirstOrDefault()?.State.OrchestrationInstance.ExecutionId;
            }

            var events = await this.instanceStore.GetOrchestrationHistoryEventsAsync(instanceId, expectedExecutionId);
            IList<HistoryEvent> history = events?.Select(x => x.HistoryEvent).ToList();
            return new OrchestrationHistory(history ?? Array.Empty<HistoryEvent>());
        }

        /// <inheritdoc />
        public override async Task<InstanceStatus> FetchInstanceStatusAsync(string instanceId, CancellationToken cancellationToken = default)
        {
            OrchestrationState state = await this.GetStateAsync(instanceId, executionId: null, cancellationToken: cancellationToken);
            return state != null ? new InstanceStatus(state) : null;
        }

        /// <inheritdoc />
        public override async IAsyncEnumerable<OrchestrationState> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput = true, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            IEnumerable<OrchestrationStateInstanceEntity> states = await instanceStore.GetOrchestrationStateAsync(instanceId, allExecutions);
            foreach (var s in states ?? Array.Empty<OrchestrationStateInstanceEntity>())
            {
                yield return s.State;
            }
        }

        /// <inheritdoc />
        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput = true, CancellationToken cancellationToken = default)
        {
            if (executionId == null)
            {
                return await this.GetStateAsync(instanceId, false, cancellationToken: cancellationToken).FirstOrDefaultAsync(cancellationToken);
            }
            else
            {
                return (await this.instanceStore.GetOrchestrationStateAsync(instanceId, executionId))?.State;
            }
        }

        /// <inheritdoc />
        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken = default)
        {
            return this.instanceStore.PurgeOrchestrationHistoryEventsAsync(thresholdDateTimeUtc, timeRangeFilterType);
        }

        /// <inheritdoc />
        public override async Task<bool> SetNewExecutionAsync(
            ExecutionStartedEvent executionStartedEvent,
            ETag? eTag /* not used */,
            string inputStatusOverride,
            CancellationToken cancellationToken = default)
        {
            var orchestrationState = new OrchestrationState()
            {
                Name = executionStartedEvent.Name,
                Version = executionStartedEvent.Version,
                OrchestrationInstance = executionStartedEvent.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = inputStatusOverride ?? executionStartedEvent.Input,
                Tags = executionStartedEvent.Tags,
                CreatedTime = executionStartedEvent.Timestamp,
                LastUpdatedTime = DateTime.UtcNow,
                CompletedTime = Core.Common.DateTimeUtils.MinDateTime,
                ScheduledStartTime = executionStartedEvent.ScheduledStartTime,
                Generation = executionStartedEvent.Generation
            };

            var orchestrationStateEntity = new OrchestrationStateInstanceEntity()
            {
                State = orchestrationState,
                SequenceNumber = 0
            };

            await this.instanceStore.WriteEntitiesAsync(new[] { orchestrationStateEntity });
            return true;
        }

        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken = default)
        {
            //NOP
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public override async Task<ETag?> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, ETag? eTag, object executionData, CancellationToken cancellationToken = default)
        {
            //In case there is a runtime state for an older execution/iteration as well that needs to be committed, commit it.
            //This may be the case if a ContinueAsNew was executed on the orchestration
            if (newRuntimeState != oldRuntimeState)
            {
                eTag = await UpdateStateAsync(oldRuntimeState, instanceId, oldRuntimeState.OrchestrationInstance.ExecutionId, eTag, cancellationToken);
            }

            return await UpdateStateAsync(newRuntimeState, instanceId, executionId, eTag, cancellationToken);
        }

        /// <inheritdoc />
        private async Task<ETag?> UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId, ETag? eTag, CancellationToken cancellationToken = default)
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
