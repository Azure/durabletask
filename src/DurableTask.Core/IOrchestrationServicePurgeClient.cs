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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Orchestration Service interface for purging instance history
    /// </summary>
    public interface IOrchestrationServicePurgeClient
    {
        // Purge instance history operations

        /// <summary>
        /// Purge history for an orchestration with a specified instance id.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration.</param>
        /// <returns><see cref="PurgeHistoryResult"/> object containing number of storage requests sent, along with instances and rows deleted/purged.</returns>
        Task<PurgeHistoryResult> PurgeInstanceStateAsync(string instanceId);

        /// <summary>
        /// Purge history for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime of orchestrations. Purges history grater than this value.</param>
        /// <param name="createdTimeTo">CreatedTime of orchestrations. Purges history less than this value.</param>
        /// <param name="runtimeStatus">RuntimeStatus of orchestrations. You can specify several status.</param>
        /// <returns><see cref="PurgeHistoryResult"/> object containing number of storage requests sent, along with instances and rows deleted/purged.</returns>
        Task<PurgeHistoryResult> PurgeInstanceStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus);
    }
}