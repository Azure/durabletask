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
    using DurableTask.Core.History;

    /// <summary>
    /// Action associated with an orchestrator reaching a terminal state.
    /// </summary>
    public class OrchestrationCompleteOrchestratorAction : OrchestratorAction
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// Gets the completion status of the orchestration.
        /// </summary>
        public OrchestrationStatus OrchestrationStatus { get; set; }

        /// <inheritdoc/>
        public override OrchestratorActionType OrchestratorActionType => OrchestratorActionType.OrchestrationComplete;

        /// <summary>
        /// Gets or sets the result of the orchestration.
        /// </summary>
        public string? Result { get; set; }

        /// <summary>
        /// More details about how an orchestration completed, such as unhandled exception details.
        /// </summary>
        public string? Details { get; set; }

        /// <summary>
        /// Gets or sets error information associated with an orchestration failure, if applicable.
        /// </summary>
        public FailureDetails? FailureDetails { get; set; }

        /// <summary>
        /// For continue-as-new scenarios, gets the new version of the orchestrator to start.
        /// </summary>
        public string? NewVersion { get; set; }

        /// <summary>
        /// Gets a list of events that should be carried over when continuing an orchestration as new.
        /// </summary>
        public IList<HistoryEvent> CarryoverEvents { get; } = new List<HistoryEvent>();
    }
}