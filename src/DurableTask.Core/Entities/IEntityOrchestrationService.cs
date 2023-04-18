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
namespace DurableTask.Core.Entities
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extends <see cref="IOrchestrationService"/> with methods that support processing of entities. 
    /// </summary>
    public interface IEntityOrchestrationService : IOrchestrationService
    {
        /// <summary>
        /// The entity orchestration service.
        /// </summary>
        /// <returns>An object containing properties of the entity backend.</returns>
        EntityBackendProperties GetEntityBackendProperties();

        /// <summary>
        /// Configures the orchestration service backend so entities and orchestrations are kept in two separate queues, and can be fetched separately.
        /// </summary>
        void ProcessEntitiesSeparately();

        /// <summary>
        /// Specialized variant of <see cref="IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(TimeSpan, CancellationToken)"/> that
        /// fetches only work items for true orchestrations, not entities. 
        /// </summary>
        Task<TaskOrchestrationWorkItem> LockNextOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        /// <summary>
        /// Specialized variant of <see cref="IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(TimeSpan, CancellationToken)"/> that
        /// fetches only work items for entities, not plain orchestrations.
        /// </summary>
        Task<TaskOrchestrationWorkItem> LockNextEntityWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken);
    }
}