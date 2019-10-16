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

using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Represents options for different states that an existing orchestrator can be in to be able to be overwritten by
    /// an attempt to start a new instance with the same instance Id.
    /// </summary>
    public enum OverridableStates
    {
        /// <summary>
        /// Option to start a new orchestrator instance with an existing instnace Id when the existing
        /// instance is in any state.
        /// </summary>
        AnyState,

        /// <summary>
        /// Option to only start a new orchestrator instance with an existing instance Id when the existing
        /// instance is in a termincated, failed, or completed state.
        /// </summary>
        IfTerminatedFailedOrCompleted
    }
}
