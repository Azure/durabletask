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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracking;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Azure Table Instance store provider to allow storage and lookup for orchestration state event history with query support
    /// </summary>
    public class AzureTableInstanceStore : IOrchestrationServiceInstanceStore
    {
        const int MaxDisplayStringLengthForAzureTableColumn = (1024 * 24) - 20;
        const int MaxRetriesTableStore = 5;
        const int IntervalBetweenRetriesSecs = 5;

        static readonly DataConverter DataConverter = new JsonDataConverter();

        readonly AzureTableClient tableClient;

        /// <summary>
        /// Creates a new AzureTableInstanceStore using the supplied hub name and table connection string
        /// </summary>
        /// <param name="hubName">The hubname for this instance store</param>
        /// <param name="tableConnectionString">Azure table connection string</param>
        public AzureTableInstanceStore(string hubName, string tableConnectionString)
        {
            this.tableClient = new AzureTableClient(hubName, tableConnectionString);

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
        /// Gets the maximum length a history entry can be so it can be truncated if neccesary
        /// </summary>
        /// <returns>The maximum length</returns>
        public int MaxHistoryEntryLength => MaxDisplayStringLengthForAzureTableColumn;

        /// <summary>
        /// Writes a list of history events to storage with retries for transient errors
        /// </summary>
        /// <param name="entities">List of history events to write</param>
        public async Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            return await Utils.ExecuteWithRetries(() => this.tableClient.WriteEntitiesAsync(entities.Select(HistoryEventToTableEntity)),
                                string.Empty,
                                "WriteEntitiesAsync",
                                MaxRetriesTableStore,
                                IntervalBetweenRetriesSecs);
        }

        /// <summary>
        /// Get a list of state events from instance store
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <returns>The matching orchestation state or null if not found</returns>
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitiesAsync(string instanceId, string executionId)
        {
            IEnumerable<AzureTableOrchestrationStateEntity> results =
                await this.tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId)).ConfigureAwait(false);

            return results.Select(r => TableStateToStateEvent(r.State));
        }

        /// <summary>
        /// Deletes a list of history events from storage with retries for transient errors
        /// </summary>
        /// <param name="entities">List of history events to delete</param>
        public async Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            return await Utils.ExecuteWithRetries(() => this.tableClient.DeleteEntitiesAsync(entities.Select(HistoryEventToTableEntity)),
                                string.Empty,
                                "DeleteEntitiesAsync",
                                MaxRetriesTableStore,
                                IntervalBetweenRetriesSecs);
        }

        /// <summary>
        /// Gets a list of orchestration states for a given instance
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="allInstances">Flag indiciation whether to get all history execution ids or just the most recent</param>
        /// <returns>List of matching orchestration states</returns>
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            // Fetch unscheduled orchestrations from JumpStart table
            // We need to get this first to avoid a race condition.
            IEnumerable<AzureTableOrchestrationStateEntity> jumpStartEntities = await this.tableClient.QueryJumpStartOrchestrationsAsync(new OrchestrationStateQuery()
                    .AddInstanceFilter(instanceId)).ConfigureAwait(false);

            IEnumerable<AzureTableOrchestrationStateEntity> stateEntities =
                            await tableClient.QueryOrchestrationStatesAsync(new OrchestrationStateQuery()
                                .AddInstanceFilter(instanceId)).ConfigureAwait(false);

            IEnumerable <OrchestrationState> states = stateEntities.Select(stateEntity => stateEntity.State);
            IEnumerable<OrchestrationState> jumpStartStates = jumpStartEntities.Select(j => j.State)
                .Where(js => !states.Any(s => s.OrchestrationInstance.InstanceId == js.OrchestrationInstance.InstanceId));

            var newStates = states.Concat(jumpStartStates);

            if (allInstances)
            {
                return newStates.Select(TableStateToStateEvent); ;
            }

            foreach (OrchestrationState state in newStates)
            {
                // TODO: This will just return the first non-ContinuedAsNew orchestration and not the latest one.
                if (state.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    return new List<OrchestrationStateInstanceEntity>() { TableStateToStateEvent(state) };
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the orchestration state for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return state for</param>
        /// <param name="executionId">The execution id to return state for</param>
        /// <returns>The matching orchestation state or null if not found</returns>
        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            AzureTableOrchestrationStateEntity result =
                (await this.tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId)).ConfigureAwait(false)).FirstOrDefault();


            if (result == null)
            {
                // Query from JumpStart table
                result = (await this.tableClient.QueryJumpStartOrchestrationsAsync(
                       new OrchestrationStateQuery()
                        .AddInstanceFilter(instanceId, executionId))
                        .ConfigureAwait(false))
                    .FirstOrDefault();
            }

            return (result != null) ? TableStateToStateEvent(result.State) : null;
        }

        /// <summary>
        /// Gets the list of history events for a given instance and execution id
        /// </summary>
        /// <param name="instanceId">The instance id to return history for</param>
        /// <param name="executionId">The execution id to return history for</param>
        /// <returns>List of history events</returns>
        public async Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            IEnumerable<AzureTableOrchestrationHistoryEventEntity> entities =
                await this.tableClient.ReadOrchestrationHistoryEventsAsync(instanceId, executionId);
            return entities.Select(TableHistoryEntityToWorkItemEvent).OrderBy(ee => ee.SequenceNumber);
        }

        /// <summary>
        ///     Get a list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <returns></returns>
        public async Task<IEnumerable<OrchestrationState>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            IEnumerable<AzureTableOrchestrationStateEntity> result =
                await tableClient.QueryOrchestrationStatesAsync(stateQuery).ConfigureAwait(false);

            // Query from JumpStart table
            IEnumerable<AzureTableOrchestrationStateEntity> jumpStartEntities = await this.tableClient.QueryJumpStartOrchestrationsAsync(stateQuery).ConfigureAwait(false);

            IEnumerable<AzureTableOrchestrationStateEntity> newStates = result.Concat(jumpStartEntities
                .Where(js => !result.Any(s => s.State.OrchestrationInstance.InstanceId == js.State.OrchestrationInstance.InstanceId)));

            return newStates.Select(stateEntity => stateEntity.State);
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query. Segment size is controlled by the service.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <returns></returns>
        public Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken)
        {
            return QueryOrchestrationStatesSegmentedAsync(stateQuery, continuationToken, -1);
        }

        /// <summary>
        ///     Get a segmented list of orchestration states from the instance storage table which match the specified
        ///     orchestration state query.
        /// </summary>
        /// <param name="stateQuery">Orchestration state query to execute</param>
        /// <param name="continuationToken">The token returned from the last query execution. Can be null for the first time.</param>
        /// <param name="count">Count of elements to return. Service will decide how many to return if set to -1.</param>
        /// <returns></returns>
        public async Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken, int count)
        {
            TableContinuationToken tokenObj = null;

            if (continuationToken != null)
            {
                tokenObj = DeserializeTableContinuationToken(continuationToken);
            }

            TableQuerySegment<AzureTableOrchestrationStateEntity> results =
                await
                    tableClient.QueryOrchestrationStatesSegmentedAsync(stateQuery, tokenObj, count)
                        .ConfigureAwait(false);

            return new OrchestrationStateQuerySegment
            {
                Results = results.Results.Select(s => s.State),
                ContinuationToken = results.ContinuationToken == null
                    ? null
                    : SerializeTableContinuationToken(results.ContinuationToken)
            };
        }

        /// <summary>
        /// Purges history from storage for given time range
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The datetime in UTC to use as the threshold for purging history</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        /// <returns>The number of history events purged.</returns>
        public async Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            TableContinuationToken continuationToken = null;

            int purgeCount = 0;
            do
            {
                TableQuerySegment<AzureTableOrchestrationStateEntity> resultSegment =
                    (await tableClient.QueryOrchestrationStatesSegmentedAsync(
                        new OrchestrationStateQuery()
                            .AddTimeRangeFilter(DateTimeUtils.MinDateTime, thresholdDateTimeUtc, timeRangeFilterType),
                        continuationToken, 100)
                        .ConfigureAwait(false));

                continuationToken = resultSegment.ContinuationToken;

                if (resultSegment.Results != null)
                {
                    await PurgeOrchestrationHistorySegmentAsync(resultSegment).ConfigureAwait(false);
                    purgeCount += resultSegment.Results.Count;
                }
            } while (continuationToken != null);

            return purgeCount;
        }

        async Task PurgeOrchestrationHistorySegmentAsync(
            TableQuerySegment<AzureTableOrchestrationStateEntity> orchestrationStateEntitySegment)
        {
            var stateEntitiesToDelete = new List<AzureTableOrchestrationStateEntity>(orchestrationStateEntitySegment.Results);

            var historyEntitiesToDelete = new ConcurrentBag<IEnumerable<AzureTableOrchestrationHistoryEventEntity>>();
            await Task.WhenAll(orchestrationStateEntitySegment.Results.Select(
                entity => Task.Run(async () =>
                {
                    IEnumerable<AzureTableOrchestrationHistoryEventEntity> historyEntities =
                        await
                            tableClient.ReadOrchestrationHistoryEventsAsync(
                                entity.State.OrchestrationInstance.InstanceId,
                                entity.State.OrchestrationInstance.ExecutionId).ConfigureAwait(false);

                    historyEntitiesToDelete.Add(historyEntities);
                })));

            List<Task> historyDeleteTasks = historyEntitiesToDelete.Select(
                historyEventList => tableClient.DeleteEntitiesAsync(historyEventList)).Cast<Task>().ToList();

            // need to serialize history deletes before the state deletes so we dont leave orphaned history events
            await Task.WhenAll(historyDeleteTasks).ConfigureAwait(false);
            await Task.WhenAll(tableClient.DeleteEntitiesAsync(stateEntitiesToDelete)).ConfigureAwait(false);
        }

        /// <summary>
        /// Writes a list of jump start events to instance store
        /// </summary>
        /// <param name="entities">List of jump start events to write</param>
        public Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            var jumpStartEntities = entities.Select(e => new AzureTableOrchestrationJumpStartEntity(e));
            return this.tableClient.WriteJumpStartEntitiesAsync(jumpStartEntities);
        }

        /// <summary>
        /// Deletes a list of jump start events from instance store
        /// </summary>
        /// <param name="entities">List of jump start events to delete</param>
        public Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            var jumpStartEntities = entities.Select(e => new AzureTableOrchestrationJumpStartEntity(e));
            return this.tableClient.DeleteJumpStartEntitiesAsync(jumpStartEntities);
        }

        /// <summary>
        /// Get a list of jump start events from instance store
        /// </summary>
        /// <returns>List of jump start events</returns>
        public async Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitiesAsync(int top)
        {
            return (await this.tableClient.QueryJumpStartOrchestrationsAsync(
                        DateTime.UtcNow.AddDays(-AzureTableClient.JumpStartTableScanIntervalInDays),
                        DateTime.UtcNow,
                        top)).Select(e => e.OrchestrationJumpStartInstanceEntity);
        }

        AzureTableCompositeTableEntity HistoryEventToTableEntity(InstanceEntityBase historyEvent)
        {
            OrchestrationWorkItemInstanceEntity workItemEvent = null;
            OrchestrationStateInstanceEntity historyStateEvent = null;

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

        InstanceEntityBase TableEntityToHistoryEvent(AzureTableCompositeTableEntity entity)
        {
            AzureTableOrchestrationHistoryEventEntity workItemEntity = null;
            AzureTableOrchestrationStateEntity historyStateEntity = null;

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

        string SerializeTableContinuationToken(TableContinuationToken continuationToken)
        {
            if (continuationToken == null)
            {
                throw new ArgumentNullException(nameof(continuationToken));
            }

            string serializedToken = DataConverter.Serialize(continuationToken);
            return Convert.ToBase64String(Encoding.Unicode.GetBytes(serializedToken));
        }

        TableContinuationToken DeserializeTableContinuationToken(string serializedContinuationToken)
        {
            if (string.IsNullOrWhiteSpace(serializedContinuationToken))
            {
                throw new ArgumentException("Invalid serializedContinuationToken");
            }

            byte[] tokenBytes = Convert.FromBase64String(serializedContinuationToken);

            return DataConverter.Deserialize<TableContinuationToken>(Encoding.Unicode.GetString(tokenBytes));
        }
    }
}
