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
    /// <summary>
    /// Orchestrator action for sending external events to other orchestrations.
    /// </summary>
    public class SendEventOrchestratorAction : OrchestratorAction
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <inheritdoc/>
        public override OrchestratorActionType OrchestratorActionType => OrchestratorActionType.SendEvent;

        /// <summary>
        /// The orchestration instance to receive the event.
        /// </summary>
        public OrchestrationInstance? Instance { get; set; }

        /// <summary>
        /// The name of the external event.
        /// </summary>
        public string? EventName { get; set; }

        /// <summary>
        /// The payload data of the external event.
        /// </summary>
        public string? EventData { get; set; }
    }
}