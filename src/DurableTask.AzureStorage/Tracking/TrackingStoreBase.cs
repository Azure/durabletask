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
    using DurableTask.Core;
    using DurableTask.Core.History;

    abstract class TrackingStoreBase : ITrackingStore
    {
        protected static readonly HistoryEvent[] EmptyHistoryEventList = new HistoryEvent[0];

        /// <inheritdoc />
        public abstract Task CreateAsync();

        /// <inheritdoc />
        public abstract Task DeleteAsync();

        /// <inheritdoc />
        public virtual Task<bool> ExistsAsync()
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken));

        /// <inheritdoc />
        public virtual Task<IList<string>> RewindHistoryAsync(string instanceId, IList<string> failedLeaves, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput);
        
        /// <inheritdoc />
        public abstract Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput);

        /// <inheritdoc />
        public virtual Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual Task<IList<OrchestrationState>> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual Task<DurableStatusQueryResult> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotSupportedException();
        }

        public virtual Task<DurableStatusQueryResult> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);

        /// <inheritdoc />
        public virtual Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public virtual Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task<bool> SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent, bool ignoreExistingInstances, string inputStatusOverride);

        /// <inheritdoc />
        public virtual Task UpdateStatusForRewindAsync(string instanceId)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public abstract Task StartAsync();

        /// <inheritdoc />
        public abstract Task<string> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, string eTag);
    }
}
