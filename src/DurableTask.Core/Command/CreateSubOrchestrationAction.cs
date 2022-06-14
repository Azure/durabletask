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
#nullable enable
namespace DurableTask.Core.Command
{
    using System.Collections.Generic;

    /// <summary>
    /// Orchestrator action for creating sub-orchestrations.
    /// </summary>
    public class CreateSubOrchestrationAction : OrchestratorAction
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <inheritdoc/>
        public override OrchestratorActionType OrchestratorActionType => OrchestratorActionType.CreateSubOrchestration;

        /// <summary>
        /// The name of the sub-orchestrator to start.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// The version of the sub-orchestrator to start.
        /// </summary>
        public string? Version { get; set; }

        /// <summary>
        /// The instance ID of the created sub-orchestration.
        /// </summary>
        public string? InstanceId { get; set; }

        /// <summary>
        /// The input of the sub-orchestration.
        /// </summary>
        public string? Input { get; set; }

        /// <summary>
        /// Tags to be applied to the sub-orchestration.
        /// </summary>
        public IDictionary<string, string>? Tags { get; set; }
    }
}