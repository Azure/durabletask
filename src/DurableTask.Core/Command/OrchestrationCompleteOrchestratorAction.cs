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

namespace DurableTask.Core.Command
{
    using System.Collections.Generic;
    using DurableTask.Core.History;

    public class OrchestrationCompleteOrchestratorAction : OrchestratorAction
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationCompleteOrchestratorAction"/> class.
        /// </summary>
        public OrchestrationCompleteOrchestratorAction()
        {
            CarryoverEvents = new List<HistoryEvent>();
        }

        /// <inheritdoc />
        public override OrchestratorActionType OrchestratorActionType => OrchestratorActionType.OrchestrationComplete;

        public OrchestrationStatus OrchestrationStatus { get; set; }

        public string Result { get; set; }

        public string Details { get; set; }

        public string NewVersion { get; set; }

        public IList<HistoryEvent> CarryoverEvents { get; }
    }
}