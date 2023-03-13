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
    using Newtonsoft.Json;

    /// <summary>
    /// A unique identifier for an entity, consisting of entity name and entity key.
    /// </summary>
    public struct EntityId : IEquatable<EntityId>, IComparable
    {
        [JsonIgnore]
        private string schedulerId;

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

            this.EntityName = entityName.ToLowerInvariant();
            this.EntityKey = entityKey ?? throw new ArgumentNullException(nameof(entityKey), "Invalid entity id: entity key must not be null.");
            this.schedulerId = GetSchedulerId(this.EntityName, this.EntityKey);
        }

        /// <summary>
        /// The name for this class of entities.
        /// </summary>
        [JsonProperty(PropertyName = "name", Required = Required.Always)]
        public string EntityName { get; private set; } // do not remove set, is needed by Json Deserializer

        /// <summary>
        /// The entity key. Uniquely identifies an entity among all entities of the same name.
        /// </summary>
        [JsonProperty(PropertyName = "key", Required = Required.Always)]
        public string EntityKey { get; private set; } // do not remove set, is needed by Json Deserializer

        /// <summary>
        /// Returns the instance ID for a given entity ID.
        /// </summary>
        /// <param name="entityId">The entity ID.</param>
        /// <returns>The corresponding instance ID.</returns>
        public static string GetInstanceIdFromEntityId(EntityId entityId)
        {
            return GetSchedulerId(entityId.EntityName, entityId.EntityKey);
        }

        private static string GetSchedulerId(string entityName, string entityKey)
        {
            return $"@{entityName}@{entityKey}";
        }

        internal static string GetSchedulerIdPrefixFromEntityName(string entityName)
        {
            return $"@{entityName.ToLowerInvariant()}@";
        }

        /// <summary>
        /// Returns the entity ID for a given instance ID.
        /// </summary>
        /// <param name="instanceId">The instance ID.</param>
        /// <returns>the corresponding entity ID.</returns>
        public static EntityId GetEntityIdFromInstanceId(string instanceId)
        {
            var pos = instanceId.IndexOf('@', 1);
            var entityName = instanceId.Substring(1, pos - 1);
            var entityKey = instanceId.Substring(pos + 1);
            return new EntityId(entityName, entityKey);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            // The scheduler id could be null if the object was deserialized.
            if (this.schedulerId == null)
            {
                this.schedulerId = GetInstanceIdFromEntityId(this);
            }

            return this.schedulerId;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return (obj is EntityId other) && this.Equals(other);
        }

        /// <inheritdoc/>
        public bool Equals(EntityId other)
        {
            return this.ToString().Equals(other.ToString());
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }

        /// <inheritdoc/>
        public int CompareTo(object obj)
        {
            var other = (EntityId)obj;
            return this.ToString().CompareTo(other.ToString());
        }
    }
}
