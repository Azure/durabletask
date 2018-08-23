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
        Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Update State in the Tracking store for a particular orchestration instance and execution base on the new runtime state
        /// </summary>
        /// <param name="runtimeState">The New RuntimeState</param>
        /// <param name="instanceId">InstanceId for the Orchestration Update</param>
        /// <param name="executionId">ExecutionId for the Orchestration Update</param>
        Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId);

        /// <summary>
        /// Get The Orchestration State for the Latest or All Executions
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="allExecutions">True if states for all executions are to be fetched otherwise only the state for the latest execution of the instance is fetched</param>
        Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions);

        /// <summary>
        /// Get The Orchestration State for a particular orchestration instance execution
        /// </summary>
        /// <param name="instanceId">Instance Id</param>
        /// <param name="executionId">Execution Id</param>
        Task<OrchestrationState> GetStateAsync(string instanceId, string executionId);

        /// <summary>
        /// Get The Orchestration State for querying all orchestration instances
        /// </summary>
        /// <returns></returns>
        Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Used to set a state in the tracking store whenever a new execution is initiated from the client
        /// </summary>
        /// <param name="executionStartedEvent">The Execution Started Event being queued</param>
        Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent);

        /// <summary>
        /// Purge The History and state  which is older than thresholdDateTimeUtc based on the timestamp type specified by timeRangeFilterType
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Timestamp threshold, data older than this will be removed</param>
        /// <param name="timeRangeFilterType">timeRangeFilterType governs the type of time stamp that will be used for decision making</param>
        Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }
}
