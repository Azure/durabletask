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
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Text;
 
    /// <summary>
    /// The results of executing a batch of operations on the entity out of process.
    /// </summary>
    public class OperationBatchResult
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The results of executing the operations in the batch. The length of this list must match
        /// the size of the batch if all messages were processed; In particular, all execution errors must be reported as a result.
        /// However, this list of results can be shorter than the list of operations if
        /// some suffix of the operation list was skipped, e.g. due to shutdown, send throttling, or timeouts.  
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
    }
}
