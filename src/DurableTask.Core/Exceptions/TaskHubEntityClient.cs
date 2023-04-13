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

namespace DurableTask.Core
{
    using DurableTask.Core.Logging;
    using DurableTask.Core.Serializing;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Threading;
    using DurableTask.Core.Entities;
    using DurableTask.Core.History;
    using System.Linq;
    using System.Diagnostics;
    using DurableTask.Core.Query;
    using Newtonsoft.Json;

    /// <summary>
    ///     Client used to manage and query entity instances
    /// </summary>
    public class TaskHubEntityClient
    {
        readonly DataConverter messageDataConverter;
        readonly DataConverter stateDataConverter;
        readonly LogHelper logHelper;
        readonly EntityBackendProperties backendProperties;
        readonly IOrchestrationServiceQueryClient queryClient;
        readonly IOrchestrationServicePurgeClient purgeClient;

        /// <summary>
        /// The orchestration service client for this task hub client
        /// </summary>
        public IOrchestrationServiceClient ServiceClient { get; }

        private void CheckEntitySupport(string name)
        {
            if (this.backendProperties == null)
            {
                throw new InvalidOperationException($"{nameof(TaskHubEntityClient)}.{name} is not supported because the chosen backend does not implement {nameof(IEntityOrchestrationService)}.");
            }
        }

        private void CheckQuerySupport(string name)
        {
            if (this.queryClient == null)
            {
                throw new InvalidOperationException($"{nameof(TaskHubEntityClient)}.{name} is not supported because the chosen backend does not implement {nameof(IOrchestrationServiceQueryClient)}.");
            }
        }

        private void CheckPurgeSupport(string name)
        {
            if (this.purgeClient == null)
            {
                throw new InvalidOperationException($"{nameof(TaskHubEntityClient)}.{name} is not supported because the chosen backend does not implement {nameof(IOrchestrationServicePurgeClient)}.");
            }
        }

        /// <summary>
        ///    Create a new TaskHubEntityClient from a given TaskHubClient.
        /// </summary>
        /// <param name="client">The taskhub client.</param>
        /// <param name="stateDataConverter">The <see cref="DataConverter"/> to use for entity state deserialization, or null if the default converter should be used for that purpose.</param>
        public TaskHubEntityClient(TaskHubClient client, DataConverter stateDataConverter = null)
        {
            this.ServiceClient = client.ServiceClient;
            this.messageDataConverter = client.DefaultConverter;
            this.stateDataConverter = stateDataConverter ?? client.DefaultConverter;
            this.logHelper = client.LogHelper;
            this.backendProperties = (client.ServiceClient as IEntityOrchestrationService)?.GetEntityBackendProperties();
            this.queryClient = client.ServiceClient as IOrchestrationServiceQueryClient;
            this.purgeClient = client.ServiceClient as IOrchestrationServicePurgeClient;
        }

        /// <summary>
        /// Signals an entity to perform an operation.
        /// </summary>
        /// <param name="entityId">The target entity.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The input for the operation.</param>
        /// <param name="scheduledTimeUtc">A future time for which to schedule the start of this operation, or null if is should start as soon as possible.</param>
        /// <returns>A task that completes when the message has been reliably enqueued.</returns>
        public async Task SignalEntityAsync(EntityId entityId, string operationName, object operationInput = null, DateTime? scheduledTimeUtc = null)
        {
            this.CheckEntitySupport(nameof(SignalEntityAsync));

            (DateTime original, DateTime capped)? scheduledTime = null;
            if (scheduledTimeUtc.HasValue)
            {
                DateTime original = scheduledTimeUtc.Value.ToUniversalTime();
                DateTime capped = this.backendProperties.GetCappedScheduledTime(DateTime.UtcNow, original);
                scheduledTime = (original, capped);
            }

            var guid = Guid.NewGuid(); // unique id for this request
            var instanceId = entityId.ToString();
            var instance = new OrchestrationInstance() { InstanceId = instanceId };

            string serializedInput = null;
            if (operationInput != null)
            {
                serializedInput = this.messageDataConverter.Serialize(operationInput);
            }

            EventToSend eventToSend = ClientEntityContext.EmitOperationSignal(
                instance,
                guid,
                operationName,
                serializedInput,
                scheduledTime);
 
            string serializedEventContent = this.messageDataConverter.Serialize(eventToSend.EventContent);

            var eventRaisedEvent = new EventRaisedEvent(-1, serializedEventContent)
            {
                Name = eventToSend.EventName
            };

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = instance,
                Event = eventRaisedEvent,
            };

            this.logHelper.RaisingEvent(instance, eventRaisedEvent);

            await this.ServiceClient.SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <summary>
        /// Tries to read the current state of an entity.
        /// </summary>
        /// <typeparam name="T">The JSON-serializable type of the entity.</typeparam>
        /// <param name="entityId">The target entity.</param>
        /// <returns>a response containing the current state of the entity.</returns>
        public async Task<StateResponse<T>> ReadEntityStateAsync<T>(EntityId entityId)
        {
            var instanceId = entityId.ToString();

            this.logHelper.FetchingInstanceState(instanceId);
            IList<OrchestrationState> stateList = await this.ServiceClient.GetOrchestrationStateAsync(instanceId, allExecutions:false);

            OrchestrationState state = stateList?.FirstOrDefault();
            if (state != null
                && state.OrchestrationInstance != null
                && state.Input != null)
            {
                if (ClientEntityContext.TryGetEntityStateFromSerializedSchedulerState(state.Input, out string serializedEntityState))
                {
                    return new StateResponse<T>()
                    {
                        EntityExists = true,
                        EntityState = this.messageDataConverter.Deserialize<T>(serializedEntityState),
                    };
                }
            }

            return new StateResponse<T>()
            {
                EntityExists = false,
                EntityState = default,
            };
        }

        /// <summary>
        /// The response returned by <see cref="ReadEntityStateAsync"/>.
        /// </summary>
        /// <typeparam name="T">The JSON-serializable type of the entity.</typeparam>
        public struct StateResponse<T>
        {
            /// <summary>
            /// Whether this entity has a state or not.
            /// </summary>
            /// <remarks>An entity initially has no state, but a state is created and persisted in storage once operations access it.</remarks>
            public bool EntityExists { get; set; }

            /// <summary>
            /// The current state of the entity, if it exists, or default(<typeparamref name="T"/>) otherwise.
            /// </summary>
            public T EntityState { get; set; }
        }

        /// <summary>
        /// Gets the status of all entity instances that match the specified query conditions.
        /// </summary>
        /// <param name="query">Return entity instances that match the specified query conditions.</param>
        /// <param name="cancellationToken">Cancellation token that can be used to cancel the query operation.</param>
        /// <returns>Returns a page of entity instances and a continuation token for fetching the next page.</returns>
        public async Task<QueryResult> ListEntitiesAsync(Query query, CancellationToken cancellationToken)
        {
            this.CheckEntitySupport(nameof(ListEntitiesAsync));
            this.CheckQuerySupport(nameof(ListEntitiesAsync));

            OrchestrationQuery innerQuery = new OrchestrationQuery()
            {
                FetchInputsAndOutputs = query.FetchState,
                ContinuationToken = query.ContinuationToken,
                CreatedTimeFrom = query.LastOperationFrom,
                CreatedTimeTo = query.LastOperationTo,
                InstanceIdPrefix = "@",
                PageSize = query.PageSize,
                RuntimeStatus = null,
                TaskHubNames = query.TaskHubNames,
            };

            bool unsatisfiable = false;
            
            void ApplyPrefixConjunction(string prefix)
            {
                if (innerQuery.InstanceIdPrefix.Length >= prefix.Length)
                {
                    unsatisfiable = unsatisfiable || !innerQuery.InstanceIdPrefix.StartsWith(prefix);
                }
                else
                {
                    unsatisfiable = unsatisfiable || !prefix.StartsWith(innerQuery.InstanceIdPrefix);
                    innerQuery.InstanceIdPrefix = prefix;
                }
            }

            if (query.InstanceIdPrefix != null)
            {
                ApplyPrefixConjunction(query.InstanceIdPrefix);
            }
            if (query.EntityName != null)
            {
                ApplyPrefixConjunction(EntityId.GetSchedulerIdPrefixFromEntityName(query.EntityName)); 
            }
           
            if (unsatisfiable)
            {
                return new QueryResult()
                {
                    Entities = new List<EntityStatus>(),
                    ContinuationToken = null,
                };
            }

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            QueryResult entityResult = new QueryResult()
            {
                Entities = new List<EntityStatus>(),
                ContinuationToken = innerQuery.ContinuationToken,
            };

            do
            {
                var result = await queryClient.GetOrchestrationWithQueryAsync(innerQuery, cancellationToken).ConfigureAwait(false);
                entityResult.Entities.AddRange(result.OrchestrationState
                    .Select(ConvertStatusResult)
                    .Where(status => status != null));
                entityResult.ContinuationToken = innerQuery.ContinuationToken = result.ContinuationToken;
            }
            while ( // run multiple queries if no records are found, but never in excess of 100ms
                entityResult.ContinuationToken != null
                && !entityResult.Entities.Any()
                && stopwatch.ElapsedMilliseconds <= 100
                && !cancellationToken.IsCancellationRequested);

            return entityResult;

            EntityStatus ConvertStatusResult(OrchestrationState orchestrationState)
            {
                string state = null;
                bool hasState = false;

                if (query.FetchState && orchestrationState.Input != null)
                {
                    hasState = ClientEntityContext.TryGetEntityStateFromSerializedSchedulerState(orchestrationState.Input, out state);
                }
                else if (orchestrationState.Status != null && orchestrationState.Status != "null")
                {
                    var entityStatus = new DurableTask.Core.Entities.StateFormat.EntityStatus();
                    JsonConvert.PopulateObject(orchestrationState.Status, entityStatus, Serializer.InternalSerializerSettings);
                    hasState = entityStatus.EntityExists;
                }

                if (hasState || query.IncludeDeleted)
                {
                    return new EntityStatus()
                    {
                        EntityId = EntityId.FromString(orchestrationState.OrchestrationInstance.InstanceId),
                        LastOperationTime = orchestrationState.CreatedTime,
                        State = state,
                    };
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Query condition for searching the status of entity instances.
        /// </summary>
        public class Query
        {
            /// <summary>
            /// If not null, return only entities whose name matches this name.
            /// </summary>
            public string EntityName { get; set; }

            /// <summary>
            /// If not null, return only entities whose instance id starts with this prefix.
            /// </summary>
            public string InstanceIdPrefix { get; set; }

            /// <summary>
            /// If not null, return only entity instances which had operations after this DateTime.
            /// </summary>
            public DateTime? LastOperationFrom { get; set; }

            /// <summary>
            /// If not null, return only entity instances which had operations before this DateTime.
            /// </summary>
            public DateTime? LastOperationTo { get; set; }

            /// <summary>
            /// If not null, return only entity instances from task hubs whose name is in this list.
            /// </summary>
            public ICollection<string> TaskHubNames { get; set; }

            /// <summary>
            /// Number of records per one request. The default value is 100.
            /// </summary>
            /// <remarks>
            /// Requests may return fewer records than the specified page size, even if there are more records.
            /// Always check the continuation token to determine whether there are more records.
            /// </remarks>
            public int PageSize { get; set; } = 100;

            /// <summary>
            /// ContinuationToken of the pager. 
            /// </summary>
            public string ContinuationToken { get; set; }

            /// <summary>
            /// Determines whether the query results include the state of the entity.
            /// </summary>
            public bool FetchState { get; set; } = false;

            /// <summary>
            /// Determines whether the results may include entities that currently have no state (such as deleted entities). 
            /// </summary>
            /// <remarks>The effects of this vary by backend. Some backends do not retain deleted entities, so this parameter is irrelevant in that situation.</remarks>
            public bool IncludeDeleted { get; set; } = false;
        }

        /// <summary>
        /// A partial result of an entity status query.
        /// </summary>
        public class QueryResult
        {
            /// <summary>
            /// Gets or sets a collection of statuses of entity instances matching the query description.
            /// </summary>
            /// <value>A collection of entity instance status values.</value>
            public List<EntityStatus> Entities { get; set; }

            /// <summary>
            /// Gets or sets a token that can be used to resume the query with data not already returned by this query.
            /// </summary>
            /// <value>A server-generated continuation token or <c>null</c> if there are no further continuations.</value>
            public string ContinuationToken { get; set; }
        }

        /// <summary>
        /// The status of an entity, as returned by entity queries.
        /// </summary>
        public class EntityStatus
        {
            /// <summary>
            /// The EntityId of the queried entity instance.
            /// </summary>
            /// <value>
            /// The unique EntityId of the instance.
            /// </value>
            public EntityId EntityId { get; set; }

            /// <summary>
            /// The time of the last operation processed by the entity instance.
            /// </summary>
            /// <value>
            /// The last operation time in UTC.
            /// </value>
            public DateTime LastOperationTime { get; set; }

            /// <summary>
            /// The current state of the entity instance, or null if states were not fetched or the entity has no state.
            /// </summary>
            public string State { get; set; }
        }

        /// <summary>
        /// Removes empty entities from storage and releases orphaned locks.
        /// </summary>
        /// <remarks>An entity is considered empty, and is removed, if it has no state, is not locked, and has
        /// been idle for more than <see cref="EntityBackendProperties.EntityMessageReorderWindow"/> minutes.
        /// Locks are considered orphaned, and are released, if the orchestration that holds them is not in state <see cref="OrchestrationStatus.Running"/>. This
        /// should not happen under normal circumstances, but can occur if the orchestration instance holding the lock
        /// exhibits replay nondeterminism failures, or if it is explicitly purged.</remarks>
        /// <param name="removeEmptyEntities">Whether to remove empty entities.</param>
        /// <param name="releaseOrphanedLocks">Whether to release orphaned locks.</param>
        /// <param name="cancellationToken">Cancellation token that can be used to cancel the operation.</param>
        /// <returns>A task that completes when the operation is finished.</returns>
        public async Task<CleanEntityStorageResult> CleanEntityStorageAsync(bool removeEmptyEntities, bool releaseOrphanedLocks, CancellationToken cancellationToken)
        {
            this.CheckEntitySupport(nameof(CleanEntityStorageAsync));
            this.CheckQuerySupport(nameof(CleanEntityStorageAsync));

            if (removeEmptyEntities)
            {
                this.CheckPurgeSupport(nameof(CleanEntityStorageAsync));
            }

            DateTime now = DateTime.UtcNow;
            CleanEntityStorageResult finalResult = default;

            var query = new OrchestrationQuery()
            {
                InstanceIdPrefix = "@",
                FetchInputsAndOutputs = false,
            };

            // list all entities (without fetching the input) and for each one that requires action,
            // perform that action. Waits for all actions to finish after each page.
            do
            {
                var page = await this.queryClient.GetOrchestrationWithQueryAsync(query, cancellationToken);

                List<Task> tasks = new List<Task>();
                foreach (var state in page.OrchestrationState)
                {
                    var status = new DurableTask.Core.Entities.StateFormat.EntityStatus();
                    JsonConvert.PopulateObject(state.Status, status, Serializer.InternalSerializerSettings);

                    if (releaseOrphanedLocks && status.LockedBy != null)
                    {
                        tasks.Add(CheckForOrphanedLockAndFixIt(state.OrchestrationInstance.InstanceId, status.LockedBy));
                    }

                    if (removeEmptyEntities)
                    {
                        bool isEmptyEntity = !status.EntityExists && status.LockedBy == null && status.QueueSize == 0;
                        bool safeToRemoveWithoutBreakingMessageSorterLogic = (this.backendProperties.EntityMessageReorderWindow == TimeSpan.Zero) ?
                            true : (now - state.LastUpdatedTime > this.backendProperties.EntityMessageReorderWindow);
                        if (isEmptyEntity && safeToRemoveWithoutBreakingMessageSorterLogic)
                        {
                            tasks.Add(DeleteIdleOrchestrationEntity(state));
                        }
                    }
                }

                async Task DeleteIdleOrchestrationEntity(OrchestrationState state)
                {
                    var purgeResult = await this.purgeClient.PurgeInstanceStateAsync(state.OrchestrationInstance.InstanceId);
                    Interlocked.Add(ref finalResult.NumberOfEmptyEntitiesRemoved, purgeResult.DeletedInstanceCount);
                }

                async Task CheckForOrphanedLockAndFixIt(string instanceId, string lockOwner)
                {
                    bool lockOwnerIsStillRunning = false;

                    IList<OrchestrationState> stateList = await this.ServiceClient.GetOrchestrationStateAsync(lockOwner, allExecutions: false);
                    OrchestrationState state = stateList?.FirstOrDefault();
                    if (state != null)
                    {
                        lockOwnerIsStillRunning = 
                            (state.OrchestrationStatus == OrchestrationStatus.Running 
                          || state.OrchestrationStatus == OrchestrationStatus.Suspended);
                    }

                    if (!lockOwnerIsStillRunning)
                    {
                        // the owner is not a running orchestration. Send a lock release.
                        OrchestrationInstance targetInstance = new OrchestrationInstance()
                        {
                            InstanceId = instanceId,
                        };

                        var eventToSend = ClientEntityContext.EmitUnlockForOrphanedLock(targetInstance, lockOwner);

                        string serializedEventContent = this.messageDataConverter.Serialize(eventToSend.EventContent);

                        var eventRaisedEvent = new EventRaisedEvent(-1, serializedEventContent)
                        {
                            Name = eventToSend.EventName
                        };

                       var taskMessage = new TaskMessage
                        {
                            OrchestrationInstance = targetInstance,
                            Event = eventRaisedEvent,
                        };

                        this.logHelper.RaisingEvent(targetInstance, eventRaisedEvent); 

                        await this.ServiceClient.SendTaskOrchestrationMessageAsync(taskMessage);

                        Interlocked.Increment(ref finalResult.NumberOfOrphanedLocksRemoved);
                    }
                }

                await Task.WhenAll(tasks);
                query.ContinuationToken = page.ContinuationToken;
            }
            while (query.ContinuationToken != null);

            return finalResult;
        }

        /// <summary>
        /// The result of a clean entity storage operation.
        /// </summary>
        public struct CleanEntityStorageResult
        {
            /// <summary>
            /// The number of orphaned locks that were removed.
            /// </summary>
            public int NumberOfOrphanedLocksRemoved;

            /// <summary>
            /// The number of entities whose metadata was removed from storage.
            /// </summary>
            public int NumberOfEmptyEntitiesRemoved;
        }
    }
}
