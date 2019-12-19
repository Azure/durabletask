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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Core.Tracking;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// A store for supporting orchestration service in service fabric world.
    /// </summary>
    internal interface IFabricOrchestrationServiceInstanceStore
    {
        /// <summary>
        /// Runs initialization to prepare the instance store for use
        /// </summary>
        /// <param name="recreate">Flag to indicate whether the store should be recreated.</param>
        Task InitializeStoreAsync(bool recreate);

        /// <summary>
        /// Starts the instance store object.
        /// </summary>
        /// <returns></returns>
        Task StartAsync();

        /// <summary>
        /// Deletes instances instance store
        /// </summary>
        Task DeleteStoreAsync();

        /// <summary>
        /// Writes a list of history events to instance store
        /// </summary>
        /// <param name="transaction">Service fabric transaction in which this operation is executed.</param>
        /// <param name="entities">List of history events to write</param>
        Task WriteEntitiesAsync(ITransaction transaction, IEnumerable<InstanceEntityBase> entities);

        /// <summary>
        /// Gets a list of orchestration states for a given instance
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="allInstances">Flag indication whether to get all history execution ids or just the most recent</param>
        /// <returns>List of matching orchestration states</returns>
        Task<IList<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances);

        /// <summary>
        /// Gets the orchestration state for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <returns>The matching orchestation state or null if not found</returns>
        Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId);

        /// <summary>
        /// Gets the list of history events for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return history for</param>
        /// <param name="executionId">The execution id to return history for</param>
        /// <returns>List of history events</returns>
        Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId);

        /// <summary>
        /// Purges history from storage for given time range
        /// </summary>
        /// <param name="thresholdHourlyDateTimeUtc">The datetime in UTC to use as the threshold for purging history</param>
        Task PurgeOrchestrationHistoryEventsAsync(DateTime thresholdHourlyDateTimeUtc);

        void OnOrchestrationCompleted(OrchestrationInstance instance);

        Task<OrchestrationStateInstanceEntity> WaitForOrchestrationAsync(OrchestrationInstance instance, TimeSpan timeout);

        Task<List<string>> GetExecutionIds(string instanceId);
    }
}
