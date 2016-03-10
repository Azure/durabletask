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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using DurableTask.Tracing;
using Microsoft.WindowsAzure.Storage.Table;

namespace DurableTask.Tracking
{
    public class AzureTableHistoryProvider : IOrchestrationServiceHistoryProvider
    {
        const int MaxDisplayStringLengthForAzureTableColumn = 1024 * 24;

        private AzureTableClient tableClient;

        public int MaxHistoryEntryLength()
        {
            return MaxDisplayStringLengthForAzureTableColumn;
        }

        public AzureTableHistoryProvider(string hubName, string tableConnectionString)
        {
            this.tableClient = new AzureTableClient(hubName, tableConnectionString);
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
            throw new NotImplementedException();
        }

        private OrchestrationHistoryEvent TableEntityToHistoryEvent(AzureTableCompositeTableEntity entity)
        {
            throw new NotImplementedException();
        }
    }
}
