﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using DurableTask.Core.Tracking;

    /// <summary>
    /// Instance Store provider interface to allow storage and lookup for orchestration state and event history
    /// </summary>
    public interface IOrchestrationServiceInstanceStore
    {
        /// <summary>
        /// Gets the maximum length a history entry can be so it can be truncated if necessary
        /// </summary>
        /// <returns>The maximum length</returns>
        int MaxHistoryEntryLength { get; }

        /// <summary>
        /// Runs initialization to prepare the instance store for use
        /// </summary>
        /// <param name="recreate">Flag to indicate whether the store should be recreated.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task InitializeStoreAsync(bool recreate, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes instances instance store
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task DeleteStoreAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes a list of history events to instance store
        /// </summary>
        /// <param name="entities">List of history events to write</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get a list of state events from instance store
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The matching orchestration state or null if not found</returns>
        IAsyncEnumerable<OrchestrationStateInstanceEntity> GetEntitiesAsync(string instanceId, string executionId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes a list of history events from instance store
        /// </summary>
        /// <param name="entities">List of history events to delete</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a list of orchestration states for a given instance
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="allInstances">Flag indication whether to get all history execution ids or just the most recent</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>List of matching orchestration states</returns>
        IAsyncEnumerable<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, bool allInstances, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the orchestration state for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The matching orchestration state or null if not found</returns>
        Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the list of history events for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return history for</param>
        /// <param name="executionId">The execution id to return history for</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>List of history events</returns>
        IAsyncEnumerable<OrchestrationWorkItemInstanceEntity> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Purges history from storage for given time range
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The datetime in UTC to use as the threshold for purging history</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>The number of history events purged.</returns>
        Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes a list of jump start events to instance store
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <param name="entities">List of jump start events to write</param>
        Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes a list of jump start events from instance store
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <param name="entities">List of jump start events to delete</param>
        Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get a list of jump start events from instance store
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>A pageable collection of jump start events</returns>
        AsyncPageable<OrchestrationJumpStartInstanceEntity> GetJumpStartEntitiesAsync(CancellationToken cancellationToken = default);
    }
}
