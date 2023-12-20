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
namespace DurableTask.Core.Entities.OperationFormat
{
    using System.Collections.Generic;

    /// <summary>
    /// The results of executing a batch of operations on the entity out of process.
    /// </summary>
    public class EntityBatchResult
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The results of executing the operations in the batch. If there were (non-application-level) errors, the length of this list may
        /// be shorter than the number of requests. In that case, <see cref="FailureDetails"/> contains the reason why not all requests 
        /// were processed.
        /// </summary>
        public List<OperationResult>? Results { get; set; }

        /// <summary>
        /// The list of actions (outgoing messages) performed while executing the operations in the batch. Can be empty.
        /// </summary>
        public List<OperationAction>? Actions { get; set; }

        /// <summary>
        /// The state of the entity after executing the batch,
        /// or null if the entity has no state (e.g. if it has been deleted).
        /// </summary>
        public string? EntityState { get; set; }

        /// <summary>
        /// Contains the failure details, if there was a failure to process all requests (fewer results were returned than requests) 
        /// </summary>
        public FailureDetails? FailureDetails { get; set; }
    }
}
