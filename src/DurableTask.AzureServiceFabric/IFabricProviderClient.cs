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

    /// <summary>
    /// Provides additional useful functionality which is not available through <see cref="TaskHubClient"/>.
    /// </summary>
    internal interface IFabricProviderClient
    {
        /// <summary>
        /// Gets all the orchestration instances which are currently running or pending.
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<OrchestrationInstance>> GetRunningOrchestrationsAsync();

        /// <summary>
        /// Gets runtime state of a running or pending orchestration which includes the history events.
        /// </summary>
        /// <param name="instanceId">The <see cref="OrchestrationInstance.InstanceId"/>
        /// of the orchestration.</param>
        /// <returns>Returns serialized runtime state which includes all the history events if the
        /// orchestration is running or pending.</returns>
        /// <remarks>The API is intended for diagnostics purpose, so current implementation returns
        /// a formatted json serialized string which can be very large.</remarks>
        /// <exception cref="ArgumentException">When the orchestration already completed or was never started.</exception>
        Task<string> GetOrchestrationRuntimeStateAsync(string instanceId);
    }
}
