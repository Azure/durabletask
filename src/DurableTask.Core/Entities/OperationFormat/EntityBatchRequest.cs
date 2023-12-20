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
    /// A request for execution of a batch of operations on an entity.
    /// </summary>
    public class EntityBatchRequest
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The instance id for this entity.
        /// </summary>
        public string? InstanceId { get; set; }

        /// <summary>
        /// The current state of the entity, or null if the entity does not exist.
        /// </summary>
        public string? EntityState { get; set; }

        /// <summary>
        /// The list of operations to be performed on the entity.
        /// </summary>
        public List<OperationRequest>? Operations { get; set; }
    }
}
