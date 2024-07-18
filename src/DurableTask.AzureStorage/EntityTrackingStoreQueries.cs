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
//  ----------------------------------------------------------------------------------using System;
#nullable enable
namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;

    class EntityTrackingStoreQueries : EntityBackendQueries
    {
        readonly MessageManager messageManager;
        readonly ITrackingStore trackingStore;
        readonly Func<Task> ensureTaskHub;
        readonly EntityBackendProperties properties;
        readonly Func<TaskMessage, Task> sendEvent;

        static TimeSpan timeLimitForCleanEntityStorageLoop = TimeSpan.FromSeconds(5);

        public EntityTrackingStoreQueries(
            MessageManager messageManager,
            ITrackingStore trackingStore, 
            Func<Task> ensureTaskHub,
            EntityBackendProperties properties,
            Func<TaskMessage, Task> sendEvent)
        {
            this.messageManager = messageManager;
            this.trackingStore = trackingStore;
            this.ensureTaskHub = ensureTaskHub;
            this.properties = properties;
            this.sendEvent = sendEvent;
        }

        public async override Task<EntityMetadata?> GetEntityAsync(
            EntityId id, 
            bool includeState = false, 
            bool includeStateless = false,
            CancellationToken cancellation = default(CancellationToken))
        {
            await this.ensureTaskHub();
            OrchestrationState? state = await this.trackingStore.GetStateAsync(id.ToString(), allExecutions: false, fetchInput: includeState).FirstOrDefaultAsync();
            return await this.GetEntityMetadataAsync(state, includeStateless, includeState);
        }

        public async override Task<EntityQueryResult> QueryEntitiesAsync(EntityQuery filter, CancellationToken cancellation)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition()
            {
                InstanceId = null,
                InstanceIdPrefix = string.IsNullOrEmpty(filter.InstanceIdStartsWith) ? "@" : filter.InstanceIdStartsWith,
                CreatedTimeFrom = filter.LastModifiedFrom ?? default(DateTime),
                CreatedTimeTo = filter.LastModifiedTo ?? default(DateTime),
                FetchInput = filter.IncludeState,
                FetchOutput = false,
                ExcludeEntities = false,
            };

            if (condition.InstanceIdPrefix![0] != '@')
            {
                condition.InstanceIdPrefix = $"@{condition.InstanceIdPrefix}";
            }  

            await this.ensureTaskHub();

            List<EntityMetadata> entityResult;
            string? continuationToken = filter.ContinuationToken;
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            do
            {
                Page<OrchestrationState>? page = await this.trackingStore.GetStateAsync(condition, cancellation).AsPages(continuationToken, filter.PageSize ?? 100).FirstOrDefaultAsync();
                DurableStatusQueryResult result = page != null
                    ? new DurableStatusQueryResult { ContinuationToken = page.ContinuationToken, OrchestrationState = page.Values }
                    : new DurableStatusQueryResult { OrchestrationState = Array.Empty<OrchestrationState>() };
                entityResult = await ConvertResultsAsync(result.OrchestrationState);
                continuationToken = result.ContinuationToken;
            }
            while ( // continue query right away if the page is completely empty, but never in excess of 100ms
                continuationToken != null
                && entityResult.Count == 0
                && stopwatch.ElapsedMilliseconds <= 100);

            return new EntityQueryResult()
            {
                Results = entityResult,
                ContinuationToken = continuationToken,
            };

            async ValueTask<List<EntityMetadata>> ConvertResultsAsync(IEnumerable<OrchestrationState> states)
            {
                entityResult = new List<EntityMetadata>();
                foreach (OrchestrationState entry in states)
                {
                    EntityMetadata? entityMetadata = await this.GetEntityMetadataAsync(entry, filter.IncludeTransient, filter.IncludeState);
                    if (entityMetadata.HasValue)
                    {
                        entityResult.Add(entityMetadata.Value);
                    }
                }
                return entityResult;
            }
        }

        public async override Task<CleanEntityStorageResult> CleanEntityStorageAsync(CleanEntityStorageRequest request = default(CleanEntityStorageRequest), CancellationToken cancellation = default(CancellationToken))
        {
            DateTime now = DateTime.UtcNow;
            string? continuationToken = request.ContinuationToken;
            int emptyEntitiesRemoved = 0;
            int orphanedLocksReleased = 0;
            var stopwatch = Stopwatch.StartNew();

            var condition = new OrchestrationInstanceStatusQueryCondition()
            {
                InstanceIdPrefix = "@",
                FetchInput = false,
                FetchOutput = false,
                ExcludeEntities = false,
            };

            await this.ensureTaskHub();

            // list all entities (without fetching the input) and for each one that requires action,
            // perform that action. Waits for all actions to finish after each page.
            do
            {
                Page<OrchestrationState>? states = await this.trackingStore.GetStateAsync(condition, cancellation).AsPages(continuationToken, 100).FirstOrDefaultAsync();
                DurableStatusQueryResult page = states != null
                    ? new DurableStatusQueryResult { ContinuationToken = states.ContinuationToken, OrchestrationState = states.Values }
                    : new DurableStatusQueryResult { OrchestrationState = Array.Empty<OrchestrationState>() };
                continuationToken = page.ContinuationToken;

                var tasks = new List<Task>();
                foreach (OrchestrationState state in page.OrchestrationState)
                {
                    EntityStatus? status = ClientEntityHelpers.GetEntityStatus(state.Status);
                    if (status != null)
                    {
                        if (request.ReleaseOrphanedLocks && status.LockedBy != null)
                        {
                            tasks.Add(CheckForOrphanedLockAndFixIt(state, status.LockedBy));
                        }

                        if (request.RemoveEmptyEntities)
                        {
                            bool isEmptyEntity = !status.EntityExists && status.LockedBy == null && status.BacklogQueueSize == 0;
                            bool safeToRemoveWithoutBreakingMessageSorterLogic = 
                                (now - state.LastUpdatedTime > this.properties.EntityMessageReorderWindow);
                            if (isEmptyEntity && safeToRemoveWithoutBreakingMessageSorterLogic)
                            {
                                tasks.Add(DeleteIdleOrchestrationEntity(state));
                            }
                        }
                    }
                }

                async Task DeleteIdleOrchestrationEntity(OrchestrationState state)
                {
                    PurgeHistoryResult result = await this.trackingStore.PurgeInstanceHistoryAsync(state.OrchestrationInstance.InstanceId);      
                    Interlocked.Add(ref emptyEntitiesRemoved, result.InstancesDeleted);
                }

                async Task CheckForOrphanedLockAndFixIt(OrchestrationState state, string lockOwner)
                {
                    OrchestrationState? ownerState
                        = await this.trackingStore.GetStateAsync(lockOwner, allExecutions: false, fetchInput: false).FirstOrDefaultAsync();

                    bool OrchestrationIsRunning(OrchestrationStatus? status)
                        => status != null && (status == OrchestrationStatus.Running || status == OrchestrationStatus.Suspended);

                    if (! OrchestrationIsRunning(ownerState?.OrchestrationStatus))
                    {
                        // the owner is not a running orchestration. Send a lock release.
                        EntityMessageEvent eventToSend = ClientEntityHelpers.EmitUnlockForOrphanedLock(state.OrchestrationInstance, lockOwner);
                        await this.sendEvent(eventToSend.AsTaskMessage());
                        Interlocked.Increment(ref orphanedLocksReleased);
                    }         
                }

                await Task.WhenAll(tasks);
            }
            while (continuationToken != null & stopwatch.Elapsed <= timeLimitForCleanEntityStorageLoop);

            return new CleanEntityStorageResult()
            {
                EmptyEntitiesRemoved = emptyEntitiesRemoved,
                OrphanedLocksReleased = orphanedLocksReleased,
                ContinuationToken = continuationToken,
            };
        }

        async ValueTask<EntityMetadata?> GetEntityMetadataAsync(OrchestrationState? state, bool includeTransient, bool includeState)
        {
            if (state == null)
            {
                return null;
            }

            if (!includeState)
            {
                if (!includeTransient)
                {
                    // it is possible that this entity was logically deleted even though its orchestration was not purged yet.
                    // we can check this efficiently (i.e. without deserializing anything) by looking at just the custom status
                    if (!EntityStatus.TestEntityExists(state.Status))
                    {
                        return null;
                    }
                }

                EntityStatus? status = ClientEntityHelpers.GetEntityStatus(state.Status);

                return new EntityMetadata()
                {
                    EntityId = EntityId.FromString(state.OrchestrationInstance.InstanceId),
                    LastModifiedTime = state.CreatedTime,
                    BacklogQueueSize = status?.BacklogQueueSize ?? 0,
                    LockedBy = status?.LockedBy,
                    SerializedState = null, // we were instructed to not include the state
                };
            }
            else
            {
                // first, retrieve the entity scheduler state (= input of the orchestration state), possibly from blob storage.
                string serializedSchedulerState;
                if (MessageManager.TryGetLargeMessageReference(state.Input, out Uri blobUrl))
                {
                    serializedSchedulerState = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobUrl);
                }
                else
                {
                    serializedSchedulerState = state.Input;
                }

                // next, extract the entity state from the scheduler state
                string? serializedEntityState = ClientEntityHelpers.GetEntityState(serializedSchedulerState);

                // return the result to the user
                if (!includeTransient && serializedEntityState == null)
                {
                    return null;
                }
                else
                {
                    EntityStatus? status = ClientEntityHelpers.GetEntityStatus(state.Status);

                    return new EntityMetadata()
                    {
                        EntityId = EntityId.FromString(state.OrchestrationInstance.InstanceId),
                        LastModifiedTime = state.CreatedTime,
                        BacklogQueueSize = status?.BacklogQueueSize ?? 0,
                        LockedBy = status?.LockedBy,
                        SerializedState = serializedEntityState,
                    };
                }
            }
        }
    }
}
