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
    using DurableTask.History;
    using DurableTask.Tracing;
    using Microsoft.WindowsAzure.Storage.Table;

    public class AzureTableHistoryProvider : IOrchestrationServiceHistoryProvider
    {
        const int MaxDisplayStringLengthForAzureTableColumn = (1024 * 24) - 20;

        private readonly AzureTableClient tableClient;

        public AzureTableHistoryProvider(string hubName, string tableConnectionString)
        {
            this.tableClient = new AzureTableClient(hubName, tableConnectionString);
        }

        public async Task InitializeStorage(bool recreateStorage)
        {
            if (recreateStorage)
            {
                await this.tableClient.DeleteTableIfExistsAsync();
            }

            await this.tableClient.CreateTableIfNotExistsAsync();
        }

        public int MaxHistoryEntryLength()
        {
            return MaxDisplayStringLengthForAzureTableColumn;
        }

        public async Task<object> WriteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities)
        {
            return await this.tableClient.WriteEntitesAsync(entities.Select(HistoryEventToTableEntity));
        }

        public async Task<object> DeleteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities)
        {
            return await this.tableClient.DeleteEntitesAsync(entities.Select(HistoryEventToTableEntity));
        }

        public async Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            var entities = await this.tableClient.ReadOrchestrationHistoryEventsAsync(instanceId, executionId);
            return entities.Select(TableEntityToHistoryEvent);
        }

        public async Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(string instanceId, bool allInstances)
        {
            var entities = await this.tableClient.ReadOrchestrationHistoryEventsAsync(instanceId, string.Empty);
            return entities.Select(TableEntityToHistoryEvent);
        }

        public async Task PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            TableContinuationToken continuationToken = null;

            TraceHelper.Trace(TraceEventType.Information,
                () =>
                    "Purging orchestration instances before: " + thresholdDateTimeUtc + ", Type: " + timeRangeFilterType);

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

            TraceHelper.Trace(TraceEventType.Information, () => "Purged " + purgeCount + " orchestration histories");
        }

        private async Task PurgeOrchestrationHistorySegmentAsync(
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

        private AzureTableCompositeTableEntity HistoryEventToTableEntity(OrchestrationHistoryEvent historyEvent)
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

        private OrchestrationHistoryEvent TableEntityToHistoryEvent(AzureTableCompositeTableEntity entity)
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
    }
}
