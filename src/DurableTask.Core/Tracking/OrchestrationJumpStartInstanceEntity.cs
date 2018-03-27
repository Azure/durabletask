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

namespace DurableTask.Core.Tracking
{
    using System;

    /// <summary>
    /// History Entity for Orchestration Jumpstart state
    /// </summary>
    public class OrchestrationJumpStartInstanceEntity : InstanceEntityBase
    {
        /// <summary>
        /// The start time of the jump start event
        /// </summary>
        public DateTime JumpStartTime;

        /// <summary>
        /// Orchestration state of the jump start instance
        /// </summary>
        public OrchestrationState State;
    }
}
