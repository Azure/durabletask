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
    /// <summary>
    /// Enumeration of orchestrator actions.
    /// </summary>
    public enum OrchestratorActionType
    {
        /// <summary>
        /// A new task was scheduled by the orchestrator.
        /// </summary>
        ScheduleOrchestrator,

        /// <summary>
        /// A sub-orchestration was scheduled by the orchestrator.
        /// </summary>
        CreateSubOrchestration,

        /// <summary>
        /// A timer was scheduled by the orchestrator.
        /// </summary>
        CreateTimer,

        /// <summary>
        /// An outgoing external event was scheduled by the orchestrator.
        /// </summary>
        SendEvent,

        /// <summary>
        /// The orchestrator completed.
        /// </summary>
        OrchestrationComplete,
    }
}