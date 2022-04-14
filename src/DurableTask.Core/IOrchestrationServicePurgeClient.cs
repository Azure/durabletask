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
        /// <returns><see cref="PurgeResult"/> object containing more information about the purged instance.</returns>
        Task<PurgeResult> PurgeInstanceStateAsync(string instanceId);

        /// <summary>
        /// Purge history for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="purgeInstanceFilter"> A <see cref="PurgeInstanceFilter"/>object that determines which orchestration instance to purge.</param>
        /// <returns><see cref="PurgeResult"/> object containing more information about the purged instance.</returns>
        Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter);
    }
}