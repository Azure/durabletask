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
using DurableTask.Core.Serializing;

namespace DurableTask.Core.Entities
{    
    /// <summary>
    /// Options that are used for configuring how a TaskEntity executes entity operations. 
    /// </summary>
    public class EntityExecutionOptions
    {
        /// <summary>
        /// The data converter used for converting inputs and outputs for operations.
        /// </summary>
        public DataConverter MessageDataConverter { get; set; } = JsonDataConverter.Default;

        /// <summary>
        /// The data converter used for the entity state.
        /// </summary>
        public DataConverter StateDataConverter { get; set; } = JsonDataConverter.Default;

        /// <summary>
        /// The data converter used for exceptions.
        /// </summary>
        public DataConverter ErrorDataConverter { get; set; } = JsonDataConverter.Default;

        /// <summary>
        /// If true, all effects of an entity operation (all state changes and all actions) are rolled back
        /// if the entity operation completes with an exception.
        /// Implementations may override this setting.
        /// </summary>
        public bool RollbackOnExceptions { get; set; } = true;

        /// <summary>
        /// Information about backend entity support.
        /// </summary>
        internal EntityBackendProperties EntityBackendProperties { get; set; }

        /// <summary>
        /// The mode that is used for propagating errors, as specified in the <see cref="TaskHubWorker"/>.
        /// </summary>
        internal ErrorPropagationMode ErrorPropagationMode { get; set; }
    }
}