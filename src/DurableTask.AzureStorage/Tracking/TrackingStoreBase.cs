using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.AzureStorage.Tracking
{
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
        public abstract Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);

        /// <inheritdoc />
        public abstract Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent);

        /// <inheritdoc />
        public abstract Task StartAsync();

        /// <inheritdoc />
        public abstract Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId);
    }
}
