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
namespace DurableTask.Core.Entities
{
    using System;
    using System.Runtime.Serialization;
    using Newtonsoft.Json;

    /// <summary>
    /// A unique identifier for an entity, consisting of entity name and entity key.
    /// </summary>
    [DataContract]
    public readonly struct EntityId : IEquatable<EntityId>, IComparable
    {
        /// <summary>
        /// Create an entity id for an entity.
        /// </summary>
        /// <param name="entityName">The name of this class of entities.</param>
        /// <param name="entityKey">The entity key.</param>
        public EntityId(string entityName, string entityKey)
        {
            if (string.IsNullOrEmpty(entityName))
            {
                throw new ArgumentNullException(nameof(entityName), "Invalid entity id: entity name must not be a null or empty string.");
            }

            this.EntityName = entityName;
            this.EntityKey = entityKey ?? throw new ArgumentNullException(nameof(entityKey), "Invalid entity id: entity key must not be null.");
        }

        /// <summary>
        /// The name for this class of entities.
        /// </summary>
        [DataMember(Name = "name", IsRequired = true)]
        public readonly string EntityName;

        /// <summary>
        /// The entity key. Uniquely identifies an entity among all entities of the same name.
        /// </summary>
        [DataMember(Name = "key", IsRequired = true)]
        public readonly string EntityKey;

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"@{this.EntityName}@{this.EntityKey}";
        }

        internal static string GetSchedulerIdPrefixFromEntityName(string entityName)
        {
            return $"@{entityName}@";
        }

        /// <summary>
        /// Returns the entity ID for a given instance ID.
        /// </summary>
        /// <param name="instanceId">The instance ID.</param>
        /// <returns>the corresponding entity ID.</returns>
        public static EntityId FromString(string instanceId)
        {
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }
            var pos = instanceId.IndexOf('@', 1);
            if (pos <= 0)
            {
                throw new ArgumentException("instanceId is not a valid entityId", nameof(instanceId));
            }
            var entityName = instanceId.Substring(1, pos - 1);
            var entityKey = instanceId.Substring(pos + 1);
            return new EntityId(entityName, entityKey);
        }

      
        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return (obj is EntityId other) && this.Equals(other);
        }

        /// <inheritdoc/>
        public bool Equals(EntityId other)
        {
            return (this.EntityName,this.EntityKey).Equals((other.EntityName, other.EntityKey));
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (this.EntityName, this.EntityKey).GetHashCode();
        }

        /// <inheritdoc/>
        public int CompareTo(object obj)
        {
            var other = (EntityId)obj;
            return (this.EntityName, this.EntityKey).CompareTo((other.EntityName, other.EntityKey));
        }
    }
}
