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
    /// Orchestration Service client interface for purging orchestration instance state.
    /// </summary>
    public interface IOrchestrationServicePurgeClient
    {
        /// <summary>
        /// Purge state for an orchestration with a specified instance ID.
        /// </summary>
        /// <param name="instanceId">Instance ID of the orchestration to purge.</param>
        /// <returns>A <see cref="PurgeResult"/> object containing more information about the purged instance.</returns>
        Task<PurgeResult> PurgeInstanceStateAsync(string instanceId);

        /// <summary>
        /// Purge state for orchestrations that match the specified parameters.
        /// </summary>
        /// <param name="purgeInstanceFilter">A <see cref="PurgeInstanceFilter"/> object that determines which orchestration instances to purge.</param>
        /// <returns>A <see cref="PurgeResult"/> object containing more information about the purged instances.</returns>
        Task<PurgeResult> PurgeInstanceStateAsync(PurgeInstanceFilter purgeInstanceFilter);
    }
}