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

namespace DurableTask.AzureStorage
{
    using System;

    /// <summary>
    /// Settings to control the memory throttle settings.
    /// </summary>
    public class MemoryThrottleSettings
    {
        /// <summary>
        /// Enables OrchestrationHistoryLoadThrottle feature, which attempts to throttle Orchestration history load when non enough memory is available.
        /// </summary>
        public bool UseOrchestrationHistoryLoadThrottle { get; set; } = false;

        /// <summary>
        /// Gets or sets the total process memory in MBytes when using OrchestrationHistoryLoadThrottle.
        /// </summary>
        public int TotalProcessMemoryMBytes { get; set; } = (int)1.5 * 1024;

        /// <summary>
        /// Gets or sets the memory buffer in MBytes when using OrchestrationHistoryLoadThrottle, representing additional memory needed in an application at a given time other than orchestration histories.
        /// </summary>
        public int MemoryBufferMBytes { get; set; } = (int)0.25 * 1024;

        /// <summary>
        /// Creates an instance of <see cref="MemoryThrottleSettings"/> with following default values:
        ///     a) UseOrchestrationHistoryLoadThrottle = false
        ///     b) TotalProcessMemoryMBytes = 1.5 GB
        ///     c) MemoryBufferMBytes = 0.25 GB
        /// </summary>
        public static MemoryThrottleSettings DefaultSettings => new MemoryThrottleSettings();
    }
}
