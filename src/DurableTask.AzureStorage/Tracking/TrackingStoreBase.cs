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
        public abstract Task<bool> ExistsAsync();

        /// <inheritdoc />
        public abstract Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken));

        /// <inheritdoc />
        public abstract Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions);
        
        /// <inheritdoc />
        public abstract Task<OrchestrationState> GetStateAsync(string instanceId, string executionId);

        /// <inheritdoc />
        public abstract Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <inheritdoc />
        public abstract Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);

        /// <inheritdoc />
        public abstract Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent);

        /// <inheritdoc />
        public abstract Task StartAsync();

        /// <inheritdoc />
        public abstract Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId);
    }
}
