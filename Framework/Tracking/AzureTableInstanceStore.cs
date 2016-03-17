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

namespace DurableTask.Tracking
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Text;
    using DurableTask.History;
    using DurableTask.Serializing;
    using DurableTask.Tracing;
    using Microsoft.WindowsAzure.Storage.Table;

    public class AzureTableInstanceStore : IOrchestrationServiceInstanceStore
    {
        const int MaxDisplayStringLengthForAzureTableColumn = (1024 * 24) - 20;

        static readonly DataConverter DataConverter = new JsonDataConverter();

        readonly AzureTableClient tableClient;

        public AzureTableInstanceStore(string hubName, string tableConnectionString)
        {
            this.tableClient = new AzureTableClient(hubName, tableConnectionString);
        }

        public async Task InitializeStorageAsync(bool recreateStorage)
        {
            if (recreateStorage)
            {
                await this.tableClient.DeleteTableIfExistsAsync();
            }

            await this.tableClient.CreateTableIfNotExistsAsync();
        }

        /// <summary>
        /// Deletes instances storage
        /// </summary>
        public async Task DeleteStorageAsync()
        {
            await this.tableClient.DeleteTableIfExistsAsync();
        }
        
        public int MaxHistoryEntryLength => MaxDisplayStringLengthForAzureTableColumn;

        public async Task<object> WriteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities)
        {
            return await this.tableClient.WriteEntitesAsync(entities.Select(HistoryEventToTableEntity));
        }

        public async Task<object> DeleteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities)
        {
            return await this.tableClient.DeleteEntitesAsync(entities.Select(HistoryEventToTableEntity));
        }

        public async Task<IEnumerable<OrchestrationStateHistoryEvent>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            IEnumerable<AzureTableOrchestrationStateEntity> results = 
                (await this.tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter(instanceId)).ConfigureAwait(false));

            if (allInstances)
            {
                return results.Select(TableStateToStateEvent);
            }
            else
            {
                foreach (AzureTableOrchestrationStateEntity stateEntity in results)
                {
                    if (stateEntity.State.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                    {
                        return new List<OrchestrationStateHistoryEvent>() { TableStateToStateEvent(stateEntity) };
                    }
                }

                return null;
            }
        }

        public async Task<OrchestrationStateHistoryEvent> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            AzureTableOrchestrationStateEntity result = 
                (await this.tableClient.QueryOrchestrationStatesAsync(
                new OrchestrationStateQuery().AddInstanceFilter(instanceId, executionId)).ConfigureAwait(false)).FirstOrDefault();

            return (result != null) ? TableStateToStateEvent(result) : null;
        }

        public async Task<IEnumerable<OrchestrationWorkItemEvent>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
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
            return new List<OrchestrationState>(result.Select(stateEntity => stateEntity.State));
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

        public async Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            TableContinuationToken continuationToken = null;

            int purgeCount = 0;
            do
            {
                TableQuerySegment<AzureTableOrchestrationStateEntity> resultSegment =
                    (await tableClient.QueryOrchestrationStatesSegmentedAsync(
                        new OrchestrationStateQuery()
                            .AddTimeRangeFilter(DateTime.MinValue, thresholdDateTimeUtc, timeRangeFilterType),
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
                historyEventList => tableClient.DeleteEntitesAsync(historyEventList)).Cast<Task>().ToList();

            // need to serialize history deletes before the state deletes so we dont leave orphaned history events
            await Task.WhenAll(historyDeleteTasks).ConfigureAwait(false);
            await Task.WhenAll(tableClient.DeleteEntitesAsync(stateEntitiesToDelete)).ConfigureAwait(false);
        }

        AzureTableCompositeTableEntity HistoryEventToTableEntity(OrchestrationHistoryEvent historyEvent)
        {
            OrchestrationWorkItemEvent workItemEvent = null;
            OrchestrationStateHistoryEvent historyStateEvent = null;

            if ((workItemEvent = historyEvent as OrchestrationWorkItemEvent) != null)
            {
                return new AzureTableOrchestrationHistoryEventEntity(
                    workItemEvent.InstanceId,
                    workItemEvent.ExecutionId,
                    workItemEvent.SequenceNumber,
                    workItemEvent.EventTimestamp,
                    workItemEvent.HistoryEvent);
            }
            else if ((historyStateEvent = historyEvent as OrchestrationStateHistoryEvent) != null)
            {
                return new AzureTableOrchestrationStateEntity(historyStateEvent.State);
            }
            else
            {
                throw new InvalidOperationException($"Invalid history event type: {historyEvent.GetType()}");
            }
        }

        OrchestrationHistoryEvent TableEntityToHistoryEvent(AzureTableCompositeTableEntity entity)
        {
            AzureTableOrchestrationHistoryEventEntity workItemEntity = null;
            AzureTableOrchestrationStateEntity historyStateEntity = null;

            if ((workItemEntity = entity as AzureTableOrchestrationHistoryEventEntity) != null)
            {
                return new OrchestrationWorkItemEvent { 
                    InstanceId = workItemEntity.InstanceId,
                    ExecutionId = workItemEntity.ExecutionId,
                    SequenceNumber = workItemEntity.SequenceNumber,
                    EventTimestamp = workItemEntity.TaskTimeStamp,
                    HistoryEvent = workItemEntity.HistoryEvent};
            }
            else if ((historyStateEntity = entity as AzureTableOrchestrationStateEntity) != null)
            {
                return new OrchestrationStateHistoryEvent { State = historyStateEntity.State };
            }
            else
            {
                throw new InvalidOperationException($"Invalid entity event type: {entity.GetType()}");
            }
        }

        OrchestrationStateHistoryEvent TableStateToStateEvent(AzureTableOrchestrationStateEntity entity)
        {
            return new OrchestrationStateHistoryEvent { State = entity.State };
        }

        OrchestrationWorkItemEvent TableHistoryEntityToWorkItemEvent(AzureTableOrchestrationHistoryEventEntity entity)
        {
            return new OrchestrationWorkItemEvent
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
