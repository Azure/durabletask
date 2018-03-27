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

namespace DurableTask.Core.History
{
    /// <summary>
    /// Enumeration of event types for orchestration, activity and history events
    /// </summary>
    public enum EventType
    {
        /// <summary>
        /// Orchestration execution has started event
        /// </summary>
        ExecutionStarted,

        /// <summary>
        /// Orchestration execution has completed event
        /// </summary>
        ExecutionCompleted,

        /// <summary>
        /// Orchestration execution has failed event
        /// </summary>
        ExecutionFailed,

        /// <summary>
        /// Orchestration was terminated event
        /// </summary>
        ExecutionTerminated,

        /// <summary>
        /// Task Activity scheduled event 
        /// </summary>
        TaskScheduled,

        /// <summary>
        /// Task Activity completed event
        /// </summary>
        TaskCompleted,

        /// <summary>
        /// Task Activity failed event
        /// </summary>
        TaskFailed,

        /// <summary>
        /// Sub Orchestration instance created event
        /// </summary>
        SubOrchestrationInstanceCreated,

        /// <summary>
        /// Sub Orchestration instance completed event
        /// </summary>
        SubOrchestrationInstanceCompleted,

        /// <summary>
        /// Sub Orchestration instance failed event
        /// </summary>
        SubOrchestrationInstanceFailed,

        /// <summary>
        /// Timer created event
        /// </summary>
        TimerCreated,

        /// <summary>
        /// Timer fired event
        /// </summary>
        TimerFired,

        /// <summary>
        /// Orchestration has started event
        /// </summary>
        OrchestratorStarted,

        /// <summary>
        /// Orchestration has completed event
        /// </summary>
        OrchestratorCompleted,

        /// <summary>
        /// External Event raised to orchestration event
        /// </summary>
        EventRaised,

        /// <summary>
        /// Orchestration Continued as new event
        /// </summary>
        ContinueAsNew,

        /// <summary>
        /// Generic event for tracking event existance
        /// </summary>
        GenericEvent,

        /// <summary>
        /// Orchestration state history event
        /// </summary>
        HistoryState,
    }
}