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

namespace DurableTask.ServiceBus.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus.Common;

    /// <summary>
    /// Azure Table Instance store provider to allow storage and lookup for orchestration state event history with query support
    /// </summary>
    public class AzureTableInstanceStore : IOrchestrationServiceInstanceStore
    {
        const int MaxDisplayStringLengthForAzureTableColumn = (1024 * 24) - 20;

        readonly AzureTableClient tableClient;

        /// <summary>
        /// Creates a new AzureTableInstanceStore using the supplied hub name and table connection string
        /// </summary>
        /// <param name="hubName">The hub name for this instance store</param>
        /// <param name="tableConnectionString">Azure table connection string</param>
        public AzureTableInstanceStore(string hubName, string tableConnectionString)
        {
            if (string.IsNullOrWhiteSpace(tableConnectionString))
            {
                throw new ArgumentException("Invalid connection string", nameof(tableConnectionString));
            }

            if (string.IsNullOrWhiteSpace(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            this.tableClient = new AzureTableClient(hubName, tableConnectionString);

            // Workaround an issue with Storage that throws exceptions for any date < 1600 so DateTime.Min cannot be used
            DateTimeUtils.SetMinDateTimeForStorageEmulator();
        }

        /// <summary>
        /// Creates a new AzureTableInstanceStore using the supplied hub name and cloud storage account
        /// </summary>
        /// <param name="hubName">The hub name for this instance store</param>
        /// <param name="client">Client for Azure Table Storage</param>
        public AzureTableInstanceStore(string hubName, TableServiceClient client)
        {
            this.tableClient = new AzureTableClient(hubName, client);

            // Workaround an issue with Storage that throws exceptions for any date < 1600 so DateTime.Min cannot be used
            DateTimeUtils.SetMinDateTimeForStorageEmulator();
        }

        /// <summary>
        /// Runs initialization to prepare the storage for use
        /// </summary>
        /// <param name="recreateStorage">Flag to indicate whether the storage should be recreated.</param>
        public async Task InitializeStoreAsync(bool recreateStorage)
        {
            // Keep calls sequential, running in parallel can be flaky, in particular the storage emulator will fail 50%+ of the time
            if (recreateStorage)
            {
                await DeleteStoreAsync();
            }

            await this.tableClient.CreateTableIfNotExistsAsync();
            await this.tableClient.CreateJumpStartTableIfNotExistsAsync();
        }

        /// <summary>
        /// Deletes instances storage
        /// </summary>
        public async Task DeleteStoreAsync()
        {
            // Keep calls sequential, running in parallel can be flaky, in particular the storage emulator will fail 50%+ of the time
            await this.tableClient.DeleteTableIfExistsAsync();
            await this.tableClient.DeleteJumpStartTableIfExistsAsync();
        }

        /// <summary>
        /// Gets the maximum length a history entry can be so it can be truncated if necessary
        /// </summary>
        /// <returns>The maximum length</returns>
        public int MaxHistoryEntryLength => MaxDisplayStringLengthForAzureTableColumn;

        /// <summary>
        /// Writes a list of history events to storage with retries for transient errors
        /// </summary>
        /// <param name="entities">List of history events to write</param>
        public Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            return this.tableClient.WriteEntitiesAsync(entities.Select(HistoryEventToTableEntity));
        }

        /// <summary>
        /// Get a list of state events from instance store
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>An asynchronous enumerable over the matching states.</returns>
        public IAsyncEnumerable<OrchestrationStateInstanceEntity> GetEntitiesAsync(string instanceId, string executionId, CancellationToken cancellationToken = default)
        {
            return this.tableClient
                .QueryOrchestrationStatesAsync(
                    new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId),
                    cancellationToken)
                .Select(r => TableStateToStateEvent(r.State));
        }

        /// <summary>
        /// Deletes a list of history events from storage with retries for transient errors
        /// </summary>
        /// <param name="entities">List of history events to delete</param>
        public Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            return this.tableClient.DeleteEntitiesAsync(entities.Select(HistoryEventToTableEntity));
        }

        /// <summary>
        /// Gets a list of orchestration states for a given instance
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="allInstances">Flag indicating whether to get all history execution ids or just the most recent</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>An asynchronous enumerable over the matching states.</returns>
        public IAsyncEnumerable<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, bool allInstances, CancellationToken cancellationToken = default)
        {
            OrchestrationStateQuery query = new OrchestrationStateQuery().AddInstanceFilter(instanceId);
            query = allInstances ? query : query.AddStatusFilter(OrchestrationStatus.ContinuedAsNew, FilterComparisonType.NotEquals);

            // Fetch unscheduled orchestrations from JumpStart table
            // We need to get this first to avoid a race condition.
            IAsyncEnumerable<AzureTableOrchestrationStateEntity> jumpStartEntities = this.tableClient.QueryJumpStartOrchestrationsAsync(query, cancellationToken);

            IAsyncEnumerable<AzureTableOrchestrationStateEntity> stateEntities = this.tableClient.QueryOrchestrationStatesAsync(query, cancellationToken);

            IAsyncEnumerable<OrchestrationState> states = stateEntities.Select(stateEntity => stateEntity.State);
            IAsyncEnumerable<OrchestrationState> jumpStartStates = jumpStartEntities
                .Select(j => j.State)
                .WhereAwaitWithCancellation((js, t) => states.AllAsync(s => s.OrchestrationInstance.InstanceId != js.OrchestrationInstance.InstanceId, t));

            IAsyncEnumerable<OrchestrationState> newStates = states.Concat(jumpStartStates);

            return allInstances
                ? newStates.Select(TableStateToStateEvent)
                : GetFirstOrDefault(
                    newStates
                        .OrderByDescending(x => x.LastUpdatedTime)
                        .Select(TableStateToStateEvent),
                    cancellationToken);

            static async IAsyncEnumerable<OrchestrationStateInstanceEntity> GetFirstOrDefault(IAsyncEnumerable<OrchestrationStateInstanceEntity> results, [EnumeratorCancellation] CancellationToken cancellationToken)
            {
                // If there are no elements, return the default value instead
                yield return await results.FirstOrDefaultAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Gets the orchestration state for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <returns>The matching orchestration state or null if not found</returns>
        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            AzureTableOrchestrationStateEntity result = await this.tableClient
                .QueryOrchestrationStatesAsync(new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId))
                .FirstOrDefaultAsync()
                .ConfigureAwait(false);

            // ReSharper disable once ConvertIfStatementToNullCoalescingExpression
            if (result == null)
            {
                // Query from JumpStart table
                result = await this.tableClient
                    .QueryJumpStartOrchestrationsAsync(new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId))
                    .FirstOrDefaultAsync()
                    .ConfigureAwait(false);
            }

            return result != null ? TableStateToStateEvent(result.State) : null;
        }

        /// <summary>
        /// Gets the list of history events for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return history for</param>
        /// <param name="executionId">The execution id to return history for</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>An asynchronous enumerable over the matching history events.</returns>
        public IAsyncEnumerable<OrchestrationWorkItemInstanceEntity> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId, CancellationToken cancellationToken = default)
        {
            return this.tableClient
                .ReadOrchestrationHistoryEventsAsync(instanceId, executionId, cancellationToken)
                .Select(TableHistoryEntityToWorkItemEvent)
                .OrderBy(ee => ee.SequenceNumber);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
        /// <returns>An asynchronous enumerable over the matching state.</returns>
        public IAsyncEnumerable<OrchestrationState> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery,
            CancellationToken cancellationToken = default)
        {
            IAsyncEnumerable<AzureTableOrchestrationStateEntity> result = this.tableClient.QueryOrchestrationStatesAsync(stateQuery, cancellationToken);

            // Query from JumpStart table
            IAsyncEnumerable<AzureTableOrchestrationStateEntity> jumpStartEntities = this.tableClient.QueryJumpStartOrchestrationsAsync(stateQuery, cancellationToken);

            // ReSharper disable PossibleMultipleEnumeration
            IAsyncEnumerable<AzureTableOrchestrationStateEntity> newStates = result
                .Concat(jumpStartEntities
                    .WhereAwaitWithCancellation((js, t) => result.AllAsync(s => s.State.OrchestrationInstance.InstanceId != js.State.OrchestrationInstance.InstanceId, t)));

            return newStates.Select(stateEntity => stateEntity.State);
        }

        /// <summary>
        /// Purges history from storage for given time range
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The datetime in UTC to use as the threshold for purging history</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        /// <returns>The number of history events purged.</returns>
        public async Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            AsyncPageable<AzureTableOrchestrationStateEntity> resultSegment = this.tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddTimeRangeFilter(DateTimeUtils.MinDateTime, thresholdDateTimeUtc, timeRangeFilterType));

            int purgeCount = 0;
            await foreach (Page<AzureTableOrchestrationStateEntity> page in resultSegment.AsPages())
            {
                await PurgeOrchestrationHistorySegmentAsync(page.Values, default).ConfigureAwait(false);
                purgeCount += page.Values.Count;
            }

            return purgeCount;
        }

        async Task PurgeOrchestrationHistorySegmentAsync(
            IEnumerable<AzureTableOrchestrationStateEntity> orchestrationStateEntitySegment, CancellationToken cancellation)
        {
            List<AzureTableOrchestrationHistoryEventEntity>[] historyEntities = await Task.WhenAll(
                orchestrationStateEntitySegment.Select(entity => this.tableClient
                    .ReadOrchestrationHistoryEventsAsync(
                        entity.State.OrchestrationInstance.InstanceId,
                        entity.State.OrchestrationInstance.ExecutionId,
                        cancellation)
                    .ToListAsync(cancellation).AsTask()));

            IEnumerable<Task> historyDeleteTasks = historyEntities.Select(
                historyEventList => this.tableClient.DeleteEntitiesAsync(historyEventList)).Cast<Task>();

            // need to serialize history deletes before the state deletes so we don't leave orphaned history events
            await Task.WhenAll(historyDeleteTasks).ConfigureAwait(false);
            await Task.WhenAll(this.tableClient.DeleteEntitiesAsync(orchestrationStateEntitySegment)).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a list of jump start events to instance store
        /// </summary>
        /// <param name="entities">List of jump start events to write</param>
        public Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            IEnumerable<AzureTableOrchestrationJumpStartEntity> jumpStartEntities = entities.Select(e => new AzureTableOrchestrationJumpStartEntity(e));
            return this.tableClient.WriteJumpStartEntitiesAsync(jumpStartEntities);
        }

        /// <summary>
        /// Deletes a list of jump start events from instance store
        /// </summary>
        /// <param name="entities">List of jump start events to delete</param>
        public Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            IEnumerable<AzureTableOrchestrationJumpStartEntity> jumpStartEntities = entities.Select(e => new AzureTableOrchestrationJumpStartEntity(e));
            return this.tableClient.DeleteJumpStartEntitiesAsync(jumpStartEntities);
        }

        /// <summary>
        /// Get a list of jump start events from instance store
        /// </summary>
        /// <param name = "cancellationToken" > The token to monitor for cancellation requests.The default value is <see cref = "CancellationToken.None" />.</param >
        /// <returns>An asynchronous enumerable over the matching state.</returns>
        public IAsyncEnumerable<OrchestrationJumpStartInstanceEntity> GetJumpStartEntitiesAsync(CancellationToken cancellationToken = default)
        {
            return this.tableClient
                .QueryJumpStartOrchestrationsAsync(
                    DateTime.UtcNow.AddDays(-AzureTableClient.JumpStartTableScanIntervalInDays),
                    DateTime.UtcNow,
                    cancellationToken)
                .Select(x => x.OrchestrationJumpStartInstanceEntity);
        }

        AzureTableCompositeTableEntity HistoryEventToTableEntity(InstanceEntityBase historyEvent)
        {
            OrchestrationWorkItemInstanceEntity workItemEvent;
            OrchestrationStateInstanceEntity historyStateEvent;

            if ((workItemEvent = historyEvent as OrchestrationWorkItemInstanceEntity) != null)
            {
                return new AzureTableOrchestrationHistoryEventEntity(
                    workItemEvent.InstanceId,
                    workItemEvent.ExecutionId,
                    workItemEvent.SequenceNumber,
                    workItemEvent.EventTimestamp,
                    workItemEvent.HistoryEvent);
            }
            else if ((historyStateEvent = historyEvent as OrchestrationStateInstanceEntity) != null)
            {
                return new AzureTableOrchestrationStateEntity(historyStateEvent.State);
            }
            else
            {
                throw new InvalidOperationException($"Invalid history event type: {historyEvent.GetType()}");
            }
        }

        // ReSharper disable once UnusedMember.Local
        InstanceEntityBase TableEntityToHistoryEvent(AzureTableCompositeTableEntity entity)
        {
            AzureTableOrchestrationHistoryEventEntity workItemEntity;
            AzureTableOrchestrationStateEntity historyStateEntity;

            if ((workItemEntity = entity as AzureTableOrchestrationHistoryEventEntity) != null)
            {
                return new OrchestrationWorkItemInstanceEntity
                {
                    InstanceId = workItemEntity.InstanceId,
                    ExecutionId = workItemEntity.ExecutionId,
                    SequenceNumber = workItemEntity.SequenceNumber,
                    EventTimestamp = workItemEntity.TaskTimeStamp,
                    HistoryEvent = workItemEntity.HistoryEvent
                };
            }
            else if ((historyStateEntity = entity as AzureTableOrchestrationStateEntity) != null)
            {
                return new OrchestrationStateInstanceEntity { State = historyStateEntity.State };
            }
            else
            {
                throw new InvalidOperationException($"Invalid entity event type: {entity.GetType()}");
            }
        }

        OrchestrationStateInstanceEntity TableStateToStateEvent(OrchestrationState state)
        {
            return new OrchestrationStateInstanceEntity { State = state };
        }

        OrchestrationWorkItemInstanceEntity TableHistoryEntityToWorkItemEvent(AzureTableOrchestrationHistoryEventEntity entity)
        {
            return new OrchestrationWorkItemInstanceEntity
            {
                InstanceId = entity.InstanceId,
                ExecutionId = entity.ExecutionId,
                SequenceNumber = entity.SequenceNumber,
                EventTimestamp = entity.TaskTimeStamp,
                HistoryEvent = entity.HistoryEvent
            };
        }
    }
}
