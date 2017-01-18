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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DurableTask.Tracking
{

    /// <summary>
    /// Defines interface for state provider service.
    /// StateProvider handles typically two kinds of records: History Events and Orchetration States.
    /// HistoryEvent is defined by class <see cref="OrchestrationHistoryEvent"/>. For more information about 
    /// history event types please see <see cref="HistoryEvent"/>.
    /// Second type of record, which is persisted by state provider service is defined by class <see cref="OrchestrationState"/>.
    /// </summary>
    public interface IStateProvider
    {
        /// <summary>
        /// Queries for orchestration states according to specified query.
        /// </summary>
        /// <param name="stateQuery">The query used for search.</param>
        /// <param name="continuationToken">By using of this token, provider can implement paging.
        /// Default value is null, which means first page.</param>
        /// <param name="count">Number of rows to be retrieved. -1 means default number of records will be retrieved.
        /// Default number might be different by providers. For example default (-1) could mean return all records.</param>
        /// <returns>The list of orchestrstion states, which matches specified query.</returns>
        Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery,
            object continuationToken = null, int count = -1);


        /// <summary>
        /// Reads all history events related to orchestration isntance.
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="executionId"></param>
        /// <returns></returns>
        /// <remarks>Not that paging is currentlly not supported.</remarks>
        Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(
            string instanceId, 
            string executionId);

  
        /// <summary>
        /// Writes history events in the store.
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        Task<object> WriteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities);


        /// <summary>
        /// Write orchestration states to the store.
        /// </summary>
        /// <param name="states"></param>
        /// <returns></returns>
        Task<object> WriteStateAsync(IEnumerable<OrchestrationState> states);
        

        /// <summary>
        /// Deletes the Orchestration instance history from the store.
        /// </summary>
        /// <param name="thresholdDateTimeUtc"></param>
        /// <param name="timeRangeFilterType"></param>
        /// <returns></returns>
        Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc,
         OrchestrationStateTimeRangeFilterType timeRangeFilterType);


        /// <summary>
        /// Deletes the store if already exists.
        /// </summary>
        /// <returns>Task</returns>
        Task DeleteStoreIfExistsAsync();


        /// <summary>
        /// Creates the store if it is not already created.
        /// </summary>
        /// <returns>Task</returns>
        Task CreateStoreIfNotExistsAsync();

    }
}
