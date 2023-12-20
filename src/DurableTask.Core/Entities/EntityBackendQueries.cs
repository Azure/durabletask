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
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Encapsulates support for entity queries, at the abstraction level of a storage backend.
    /// </summary>
    public abstract class EntityBackendQueries
    {
        /// <summary>
        /// Tries to get the entity with ID of <paramref name="id"/>.
        /// </summary>
        /// <param name="id">The ID of the entity to get.</param>
        /// <param name="includeState"><c>true</c> to include entity state in the response, <c>false</c> to not.</param>
        /// <param name="includeStateless">whether to include metadata for entities without user-defined state.</param>
        /// <param name="cancellation">The cancellation token to cancel the operation.</param>
        /// <returns>a response containing metadata describing the entity.</returns>
        public abstract Task<EntityMetadata?> GetEntityAsync(
            EntityId id, bool includeState = false, bool includeStateless = false, CancellationToken cancellation = default);

        /// <summary>
        /// Queries entity instances based on the conditions specified in <paramref name="query"/>.
        /// </summary>
        /// <param name="query">The query filter.</param>
        /// <param name="cancellation">The cancellation token to cancel the operation.</param>
        /// <returns>One page of query results.</returns>
        public abstract Task<EntityQueryResult> QueryEntitiesAsync(EntityQuery query, CancellationToken cancellation);

        /// <summary>
        /// Cleans entity storage. See <see cref="CleanEntityStorageRequest"/> for the different forms of cleaning available.
        /// </summary>
        /// <param name="request">The request which describes what to clean.</param>
        /// <param name="cancellation">The cancellation token to cancel the operation.</param>
        /// <returns>A task that completes when the operation is finished.</returns>
        public abstract Task<CleanEntityStorageResult> CleanEntityStorageAsync(
            CleanEntityStorageRequest request = default, CancellationToken cancellation = default);

        /// <summary>
        /// Metadata about an entity, as returned by queries.
        /// </summary>
        public struct EntityMetadata
        {
            /// <summary>
            /// Gets or sets the ID for this entity.
            /// </summary>
            public EntityId EntityId { get; set; }

            /// <summary>
            /// Gets or sets the time the entity was last modified.
            /// </summary>
            public DateTime LastModifiedTime { get; set; }

            /// <summary>
            /// Gets the size of the backlog queue, if there is a backlog, and if that metric is supported by the backend.
            /// </summary>
            public int BacklogQueueSize { get; set; }

            /// <summary>
            /// Gets the instance id of the orchestration that has locked this entity, or null if the entity is not locked.
            /// </summary>
            public string? LockedBy { get; set; }

            /// <summary>
            /// Gets or sets the serialized state for this entity. Can be null if the query
            /// specified to not include the state, or to include deleted entities.
            /// </summary>
            public string? SerializedState { get; set; }
        }

        /// <summary>
        /// A description of an entity query.
        /// </summary>
        /// <remarks>
        /// The default query returns all entities (does not specify any filters).
        /// </remarks>
        public struct EntityQuery
        {
            /// <summary>
            /// Gets or sets the optional starts-with expression for the entity instance ID.
            /// </summary>
            public string? InstanceIdStartsWith { get; set; }

            /// <summary>
            /// Gets or sets a value indicating to include only entity instances which were last modified after the provided time.
            /// </summary>
            public DateTime? LastModifiedFrom { get; set; }

            /// <summary>
            /// Gets or sets a value indicating to include only entity instances which were last modified before the provided time.
            /// </summary>
            public DateTime? LastModifiedTo { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether to include state in the query results or not.
            /// </summary>
            public bool IncludeState { get; set; }

            /// <summary>
            /// Gets a value indicating whether to include metadata about transient entities.
            /// </summary>
            /// <remarks> Transient entities are entities that do not have an application-defined state, but for which the storage provider is
            /// tracking metadata for synchronization purposes.
            /// For example, a transient entity may be observed when the entity is in the process of being created or deleted, or
            /// when the entity has been locked by a critical section. By default, transient entities are not included in queries since they are
            /// considered to "not exist" from the perspective of the user application.
            /// </remarks>
            public bool IncludeTransient { get; set; }

            /// <summary>
            /// Gets or sets the desired size of each page to return.
            /// </summary>
            /// <remarks>
            /// If no size is specified, the backend may choose an appropriate page size based on its implementation.
            /// Note that the size of the returned page may be smaller or larger than the requested page size, and cannot
            /// be used to determine whether the end of the query has been reached.
            /// </remarks>
            public int? PageSize { get; set; }

            /// <summary>
            /// Gets or sets the continuation token to resume a previous query.
            /// </summary>
            public string? ContinuationToken { get; set; }
        }

        /// <summary>
        /// A page of results.
        /// </summary>
        public struct EntityQueryResult
        {
            /// <summary>
            /// Gets or sets the query results.
            /// </summary>
            public IEnumerable<EntityMetadata> Results { get; set; }

            /// <summary>
            /// Gets or sets the continuation token to continue this query, if not null.
            /// </summary>
            public string? ContinuationToken { get; set; }
        }

        /// <summary>
        /// Request struct for <see cref="EntityBackendQueries.CleanEntityStorageAsync"/>.
        /// </summary>
        public struct CleanEntityStorageRequest
        {
            /// <summary>
            /// Gets or sets a value indicating whether to remove empty entities.
            /// </summary>
            public bool RemoveEmptyEntities { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether to release orphaned locks or not.
            /// </summary>
            public bool ReleaseOrphanedLocks { get; set; }

            /// <summary>
            /// Gets or sets the continuation token to resume a previous <see cref="CleanEntityStorageRequest"/>.
            /// </summary>
            public string? ContinuationToken { get; set; }
        }

        /// <summary>
        /// Result struct for <see cref="EntityBackendQueries.CleanEntityStorageAsync"/>.
        /// </summary>
        public struct CleanEntityStorageResult
        {
            /// <summary>
            /// Gets or sets the number of empty entities removed.
            /// </summary>
            public int EmptyEntitiesRemoved { get; set; }

            /// <summary>
            /// Gets or sets the number of orphaned locks that were removed.
            /// </summary>
            public int OrphanedLocksReleased { get; set; }

            /// <summary>
            /// Gets or sets the continuation token to continue the <see cref="CleanEntityStorageRequest"/>, if not null.
            /// </summary>
            public string? ContinuationToken { get; set; }
        }
    }
}
