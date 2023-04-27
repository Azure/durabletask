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
    /// Defines a tracking store which needs to maintain additional information about each execution.
    /// </summary>
    interface ITrackingStoreWithExecutionData : ITrackingStore
    {
        /// <summary>
        /// Update State in the Tracking store for a particular orchestration instance and execution base on the new runtime state
        /// </summary>
        /// <param name="newRuntimeState">The New RuntimeState</param>
        /// <param name="oldRuntimeState">The RuntimeState for an olderExecution</param>
        /// <param name="instanceId">InstanceId for the Orchestration Update</param>
        /// <param name="executionId">ExecutionId for the Orchestration Update</param>
        /// <param name="eTag">The ETag value to use for safe updates</param>
        /// <param name="trackingStoreData">The additional data that is maintained for this execution.</param>
        Task<string> UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, string eTag, object trackingStoreData);
    }
}
