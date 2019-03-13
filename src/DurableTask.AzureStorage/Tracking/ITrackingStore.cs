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

    /// <summary>
    /// Defines a store which maintains the runtime state for the AzureStorageOrchestrationService
    /// </summary>
    interface ITrackingStore
    {
        /// <summary>
        /// Create Tracking Store Resources if they don't already exist
        /// </summary>
        Task CreateAsync();

        /// <summary>
        /// Delete Tracking Store Resources if they already exist
        /// </summary>
        Task DeleteAsync();

        /// <summary>
        /// Do the Resources for the tracking store already exist
        /// </summary>
        Task<bool> ExistsAsync();

        /// <summary>
        /// Start up the Tracking Store before use
        /// </summary>
        Task StartAsync();

        /// <summary>
        /// Get History Events from the Store
        /// </summary>
        /// <param name="instanceId">InstanceId for</param>
        /// <param name="expectedExecutionId">ExcutionId for the execution that we want this retrieve for. If null the latest execution will be retrieved</param>
        /// <param name="cancellationToken">CancellationToken if abortion is needed</param>
        Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Queries by InstanceId and locates failure - then calls function to wipe ExecutionIds
        /// </summary>
        /// <param name="instanceId">InstanceId for orchestration</param>
        /// <param name="failedLeaves">List of failed orchestrators to send to message queue - no failed sub-orchestrators</param>
        /// <param name="cancellationToken">CancellationToken if abortion is needed</param>
        Task<IList<string>> RewindHistoryAsync(string instanceId, IList<string> failedLeaves, CancellationToken cancellationToken);

        /// <summary>
        /// Update State in the Tracking store for a particular orchestration instance and execution base on the new runtime state
        /// </summary>
        /// <param name="newRuntimeState">The New RuntimeState</param>
        /// <param name="oldRuntimeState">The RuntimeState for an olderExecution</param>
        /// <param name="instanceId">InstanceId for the Orchestration Update</param>
        /// <param name="executionId">ExecutionId for the Orchestration Update</param>
        /// <param name="eTag">The ETag value to use for safe updates</param>
        Task<string> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, string eTag);

        /// <summary>
        /// Get The Orchestration State for the Latest or All Executions
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="allExecutions">True if states for all executions are to be fetched otherwise only the state for the latest execution of the instance is fetched</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput);

        /// <summary>
        /// Get The Orchestration State for a particular orchestration instance execution
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="executionId">Execution Id</param>
        /// <param name="fetchInput">If set, fetch and return the input for the orchestration instance.</param>
        Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput);

        /// <summary>
        /// Get The Orchestration State for querying all orchestration instances
        /// </summary>
        /// <returns></returns>
        Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Get The Orchestration State for querying orchestration instances by the condition
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTimeFrom</param>
        /// <param name="createdTimeTo">CreatedTimeTo</param>
        /// <param name="runtimeStatus">RuntimeStatus</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <returns></returns>
        Task<IList<OrchestrationState>> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Get The Orchestration State for querying orchestration instances by the condition
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTimeFrom</param>
        /// <param name="createdTimeTo">CreatedTimeTo</param>
        /// <param name="runtimeStatus">RuntimeStatus</param>
        /// <param name="top">Top</param>
        /// <param name="continuationToken">Continuation token</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <returns></returns>
        Task<DurableStatusQueryResult> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Get The Orchestration State for querying orchestration instances by the condition
        /// </summary>
        /// <param name="condition">Condition</param>
        /// <param name="top">Top</param>
        /// <param name="continuationToken">ContinuationToken</param>
        /// <param name="cancellationToken">CancellationToken</param>
        /// <returns></returns>
        Task<DurableStatusQueryResult> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Used to set a state in the tracking store whenever a new execution is initiated from the client
        /// </summary>
        /// <param name="executionStartedEvent">The Execution Started Event being queued</param>
        /// <param name="ignoreExistingInstances">When <c>true</c>, this operation is a no-op if an execution with this ID already exists.</param>
        /// <param name="inputStatusOverride">An override value to use for the Input column. If not specified, uses <see cref="ExecutionStartedEvent.Input"/>.</param>
        /// <returns>Returns <c>true</c> if the record was created successfully; <c>false</c> otherwise.</returns>
        Task<bool> SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent, bool ignoreExistingInstances, string inputStatusOverride);

        /// <summary>
        /// Used to update a state in the tracking store to pending whenever a rewind is initiated from the client
        /// </summary>
        /// <param name="instanceId">The instance being rewound</param>
        Task UpdateStatusForRewindAsync(string instanceId);

        /// <summary>
        /// Purge The History and state  which is older than thresholdDateTimeUtc based on the timestamp type specified by timeRangeFilterType
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Timestamp threshold, data older than this will be removed</param>
        /// <param name="timeRangeFilterType">timeRangeFilterType governs the type of time stamp that will be used for decision making</param>
        Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);

        /// <summary>
        /// Purge the history for a concrete instance 
        /// </summary>
        /// <param name="instanceId">Instance ID</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId);

        /// <summary>
        /// Purge the orchestration history for instances that match the conditions
        /// </summary>
        /// <param name="createdTimeFrom">Start creation time for querying instances for purging</param>
        /// <param name="createdTimeTo">End creation time for querying instances for purging</param>
        /// <param name="runtimeStatus">List of runtime status for querying instances for purging. Only Completed, Terminated, or Failed will be processed</param>
        /// <returns>Class containing number of storage requests sent, along with instances and rows deleted/purged</returns>
        Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus);
    }
}
