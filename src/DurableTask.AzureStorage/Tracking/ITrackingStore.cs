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

    /// <summary>
    /// Defines a store which maintains the runtime state for the AzureStorageOrchestrationService
    /// </summary>
    interface ITrackingStore
    {
        /// <summary>
        /// Create Tracking Store Resources if they don't already exist
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task CreateAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Delete Tracking Store Resources if they already exist
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task DeleteAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Do the Resources for the tracking store already exist
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<bool> ExistsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Start up the Tracking Store before use
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task StartAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Get History Events from the Store
        /// </summary>
        /// <param name="instanceId">InstanceId for</param>
        /// <param name="expectedExecutionId">ExcutionId for the execution that we want this retrieve for. If null the latest execution will be retrieved</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Queries by InstanceId and locates failure - then calls function to wipe ExecutionIds
        /// </summary>
        /// <param name="instanceId">InstanceId for orchestration</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        IAsyncEnumerable<string> RewindHistoryAsync(string instanceId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Update State in the Tracking store for a particular orchestration instance and execution base on the new runtime state
        /// </summary>
        /// <param name="newRuntimeState">The New RuntimeState</param>
        /// <param name="oldRuntimeState">The RuntimeState for an olderExecution</param>
        /// <param name="instanceId">InstanceId for the Orchestration Update</param>
        /// <param name="executionId">ExecutionId for the Orchestration Update</param>
        /// <param name="eTag">The ETag value to use for safe updates</param>
        /// <param name="trackingStoreContext">Additional context for the execution that is maintained by the tracking store.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<ETag?> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, ETag? eTag, object trackingStoreContext, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get The Orchestration State for the Latest or All Executions
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="allExecutions">True if states for all executions are to be fetched otherwise only the state for the latest execution of the instance is fetched</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        IAsyncEnumerable<OrchestrationState> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get The Orchestration State for a particular orchestration instance execution
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="executionId">Execution Id</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput, CancellationToken cancellationToken = default);

        /// <summary>
        /// Fetches the latest instance status of the specified orchestration instance.
        /// </summary>
        /// <param name="instanceId">The ID of the orchestration.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>Returns the instance status or <c>null</c> if none was found.</returns>
        Task<InstanceStatus> FetchInstanceStatusAsync(string instanceId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get The Orchestration State for querying all orchestration instances
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns></returns>
        IAsyncEnumerable<OrchestrationState> GetStateAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Fetches instances status for multiple orchestration instances.
        /// </summary>
        /// <param name="instanceIds">The list of instances to query for.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        IAsyncEnumerable<OrchestrationState> GetStateAsync(IEnumerable<string> instanceIds, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get The Orchestration State for querying orchestration instances by the condition
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTimeFrom</param>
        /// <param name="createdTimeTo">CreatedTimeTo</param>
        /// <param name="runtimeStatus">RuntimeStatus</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns></returns>
        AsyncPageable<OrchestrationState> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get The Orchestration State for querying orchestration instances by the condition
        /// </summary>
        /// <param name="condition">Condition</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns></returns>
        AsyncPageable<OrchestrationState> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, CancellationToken cancellationToken = default);

        /// <summary>
        /// Used to set a state in the tracking store whenever a new execution is initiated from the client
        /// </summary>
        /// <param name="executionStartedEvent">The Execution Started Event being queued</param>
        /// <param name="eTag">The eTag value to use for optimistic concurrency or <c>null</c> to overwrite any existing execution status.</param>
        /// <param name="inputPayloadOverride">An override value to use for the Input column. If not specified, uses <see cref="ExecutionStartedEvent.Input"/>.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>Returns <c>true</c> if the record was created successfully; <c>false</c> otherwise.</returns>
        Task<bool> SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent, ETag? eTag, string inputPayloadOverride, CancellationToken cancellationToken = default);

        /// <summary>
        /// Used to update a state in the tracking store to pending whenever a rewind is initiated from the client
        /// </summary>
        /// <param name="instanceId">The instance being rewound</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task UpdateStatusForRewindAsync(string instanceId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Purge The History and state  which is older than thresholdDateTimeUtc based on the timestamp type specified by timeRangeFilterType
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Timestamp threshold, data older than this will be removed</param>
        /// <param name="timeRangeFilterType">timeRangeFilterType governs the type of time stamp that will be used for decision making</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Purge the history for a concrete instance 
        /// </summary>
        /// <param name="instanceId">Instance ID</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Purge the orchestration history for instances that match the conditions
        /// </summary>
        /// <param name="createdTimeFrom">Start creation time for querying instances for purging</param>
        /// <param name="createdTimeTo">End creation time for querying instances for purging</param>
        /// <param name="runtimeStatus">List of runtime status for querying instances for purging. Only Completed, Terminated, or Failed will be processed</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default);
    }
}
