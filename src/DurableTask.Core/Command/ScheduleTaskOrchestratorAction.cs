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
using System.Collections.Generic;

namespace DurableTask.Core.Command
{
    /// <summary>
    /// Orchestrator action for scheduling activity tasks.
    /// </summary>
    public class ScheduleTaskOrchestratorAction : OrchestratorAction
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <inheritdoc/>
        public override OrchestratorActionType OrchestratorActionType => OrchestratorActionType.ScheduleOrchestrator;

        /// <summary>
        /// Gets or sets the name of the activity to schedule.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the version of the activity to schedule.
        /// </summary>
        public string? Version { get; set; }

        /// <summary>
        /// Gets or sets the input of the activity to schedule.
        /// </summary>
        public string? Input { get; set; }

        // TODO: This property is not used and should be removed or made obsolete
        internal string? Tasklist { get; set; }

        /// <summary>
        /// Gets or sets a dictionary of tags of string, string
        /// </summary>
        public IDictionary<string, string>? Tags { get; set; }
    }
}