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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using DurableTask.Core;
    using DurableTask.Core.History;

    abstract class TrackingStoreBase : ITrackingStore
    {
        /// <inheritdoc />
        public abstract Task CreateAsync(CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public abstract Task DeleteAsync(CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public virtual Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public virtual IAsyncEnumerable<string> RewindHistoryAsync(string instanceId, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<InstanceStatus> FetchInstanceStatusAsync(string instanceId, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public abstract IAsyncEnumerable<OrchestrationState> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public abstract Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public virtual IAsyncEnumerable<OrchestrationState> GetStateAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual IAsyncEnumerable<OrchestrationState> GetStateAsync(IEnumerable<string> instanceIds, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual AsyncPageable<OrchestrationState> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public virtual AsyncPageable<OrchestrationState> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public virtual Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<bool> SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent, ETag? eTag, string inputStatusOverride, CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public virtual Task UpdateStatusForRewindAsync(string instanceId, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task StartAsync(CancellationToken cancellationToken = default);

        /// <inheritdoc />
        public abstract Task<ETag?> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, ETag? eTag, object executionData, CancellationToken cancellationToken = default);
    }
}
