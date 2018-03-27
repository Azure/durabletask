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

namespace DurableTask.Core
{
    /// <summary>
    /// Enum describing the status of the orchestration
    /// </summary>
    public enum OrchestrationStatus
    {
        /// <summary>
        /// Orchestration state of running
        /// </summary>
        Running,

        /// <summary>
        /// Orchestration state of complete
        /// </summary>
        Completed,

        /// <summary>
        /// Orchestration state of continued as new (this instance complete, continued in a new instance)
        /// </summary>
        ContinuedAsNew,

        /// <summary>
        /// Orchestration state of failed
        /// </summary>
        Failed,

        /// <summary>
        /// Orchestration state of gracefully canceled
        /// </summary>
        Canceled,

        /// <summary>
        /// Orchestration state of abruptly shut down
        /// </summary>
        Terminated, 

        /// <summary>
        /// Orchestration state of pending (not yet running)
        /// </summary>
        Pending
    }
}