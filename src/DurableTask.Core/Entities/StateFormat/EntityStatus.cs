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
namespace DurableTask.Core.Entities
{
    using System.Runtime.Serialization;

    /// <summary>
    /// Information about the current status of an entity. Excludes potentially large data
    /// (such as the entity state, or the contents of the queue) so it can always be read with low latency.
    /// </summary>
    [DataContract]
    public class EntityStatus
    {
        /// <summary>
        /// Whether this entity exists or not.
        /// </summary>
        [DataMember(Name = "entityExists", EmitDefaultValue = false)]
        public bool EntityExists { get; set; }

        /// <summary>
        /// The size of the queue, i.e. the number of operations that are waiting for the current operation to complete.
        /// </summary>
        [DataMember(Name = "queueSize", EmitDefaultValue = false)]
        public int QueueSize { get; set; }

        /// <summary>
        /// The instance id of the orchestration that currently holds the lock of this entity.
        /// </summary>
        [DataMember(Name = "lockedBy", EmitDefaultValue = false)]
        public string? LockedBy { get; set; }
    }
}
