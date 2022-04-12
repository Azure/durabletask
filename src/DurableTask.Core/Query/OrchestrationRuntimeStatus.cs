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

namespace DurableTask.Core.Query
{
    // Must be kept consistent with DurableTask.Core.OrchestrationStatus:
    // https://github.com/Azure/durabletask/blob/master/src/DurableTask.Core/OrchestrationStatus.cs

    /// <summary>
    /// Represents the possible runtime execution status values for an orchestration instance.
    /// </summary>
    public enum OrchestrationRuntimeStatus
    {
        /// <summary>
        /// The status of the orchestration could not be determined.
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The orchestration is running (it may be actively running or waiting for input).
        /// </summary>
        Running = 0,

        /// <summary>
        /// The orchestration ran to completion.
        /// </summary>
        Completed = 1,

        /// <summary>
        /// The orchestration completed with ContinueAsNew as is in the process of restarting.
        /// </summary>
        ContinuedAsNew = 2,

        /// <summary>
        /// The orchestration failed with an error.
        /// </summary>
        Failed = 3,

        /// <summary>
        /// The orchestration was canceled.
        /// </summary>
        Canceled = 4,

        /// <summary>
        /// The orchestration was terminated via an API call.
        /// </summary>
        Terminated = 5,

        /// <summary>
        /// The orchestration was scheduled but has not yet started.
        /// </summary>
        Pending = 6,
    }
}
