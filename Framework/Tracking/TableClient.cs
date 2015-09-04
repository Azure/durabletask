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
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Xml;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using System.Text;
    using System.Collections.Concurrent;
    using History;

    internal class TableClient : IStateProvider
    {
        const int MaxRetries = 3;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);
        static readonly TimeSpan DeltaBackOff = TimeSpan.FromSeconds(5);

        readonly string hubName;
        readonly CloudTableClient tableClient;
        readonly object thisLock = new object();

        volatile CloudTable table;

        public TableClient(string hubName, string tableConnectionString)
        {
            if (string.IsNullOrEmpty(tableConnectionString))
            {
                throw new ArgumentException("Invalid connection string", "tableConnectionString");
            }

            if (string.IsNullOrEmpty(hubName))
            {
                throw new ArgumentException("Invalid hub name", "hubName");
            }

            tableClient = CloudStorageAccount.Parse(tableConnectionString).CreateCloudTableClient();
            tableClient.RetryPolicy = new ExponentialRetry(DeltaBackOff,
                MaxRetries);
            tableClient.MaximumExecutionTime = MaximumExecutionTime;

            this.hubName = hubName;
            table = tableClient.GetTableReference(TableName);
        }

        public string TableName
        {
            get { return TableConstants.InstanceHistoryTableNamePrefix + "00" + hubName; }
        }

        internal void CreateTableIfNotExists()
        {
            table = tableClient.GetTableReference(TableName);
            table.CreateIfNotExists();
        }

        internal void DeleteTableIfExists()
        {
            if (table != null)
            {
                table.DeleteIfExists();
            }
        }

        //public Task<IEnumerable<OrchestrationStateEntity>> QueryOrchestrationStatesAsync(
        //    OrchestrationStateQuery stateQuery)
        //{
        //    TableQuery<OrchestrationStateEntity> query = CreateQueryInternal(stateQuery, -1);
        //    return ReadAllEntitiesAsync(query);
        //}


        private Task<TableQuerySegment<OrchestrationStateEntity>> queryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, TableContinuationToken continuationToken, int count)
        {
            TableQuery<OrchestrationStateEntity> query = CreateQueryInternal(stateQuery, count);
            return table.ExecuteQuerySegmentedAsync(query, continuationToken);
        }

        public async Task<OrchestrationStateQuerySegment> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery, object continuationToken = null, int count = -1)
        {
            TableContinuationToken tokenObj = null;

            if (continuationToken != null)
            {
                tokenObj = DeserializeTableContinuationToken((string)continuationToken);
            }

   
            TableQuerySegment<OrchestrationStateEntity> results =
              await
                  this.queryOrchestrationStatesSegmentedAsync(stateQuery, tokenObj, count)
                      .ConfigureAwait(false);

            return new OrchestrationStateQuerySegment
            {
                Results = results.Results.Select(s => s.State),
                ContinuationToken = results.ContinuationToken == null
                    ? null
                    : SerializeTableContinuationToken(results.ContinuationToken)
            };
           
        }


        private string SerializeTableContinuationToken(TableContinuationToken continuationToken)
        {
            if (continuationToken == null)
            {
                throw new ArgumentNullException("continuationToken");
            }

            string serializedToken = JsonConvert.SerializeObject(continuationToken,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None });
            return Convert.ToBase64String(Encoding.Unicode.GetBytes(serializedToken));
        }

        private TableContinuationToken DeserializeTableContinuationToken(string serializedContinuationToken)
        {
            if (string.IsNullOrWhiteSpace(serializedContinuationToken))
            {
                throw new ArgumentException("Invalid serializedContinuationToken");
            }

            byte[] tokenBytes = Convert.FromBase64String(serializedContinuationToken);

            return JsonConvert.DeserializeObject<TableContinuationToken>(Encoding.Unicode.GetString(tokenBytes));
        }





        TableQuery<OrchestrationStateEntity> CreateQueryInternal(OrchestrationStateQuery stateQuery, int count)
        {
            OrchestrationStateQueryFilter primaryFilter = null;
            IEnumerable<OrchestrationStateQueryFilter> secondaryFilters = null;

            Tuple<OrchestrationStateQueryFilter, IEnumerable<OrchestrationStateQueryFilter>> filters =
                stateQuery.GetFilters();
            if (filters != null)
            {
                primaryFilter = filters.Item1;
                secondaryFilters = filters.Item2;
            }

            string filterExpression = GetPrimaryFilterExpression(primaryFilter);
            if (string.IsNullOrEmpty(filterExpression))
            {
                throw new InvalidOperationException("Invalid primary filter");
            }

            if (secondaryFilters != null)
            {
                filterExpression = secondaryFilters.Aggregate(filterExpression,
                    (current, filter) =>
                    {
                        string newFilter = current;
                        string secondaryFilter = GetSecondaryFilterExpression(filter);
                        if (!string.IsNullOrEmpty(secondaryFilter))
                        {
                            newFilter += " and " + GetSecondaryFilterExpression(filter);
                        }
                        return newFilter;
                    });
            }

            TableQuery<OrchestrationStateEntity> query =
                new TableQuery<OrchestrationStateEntity>().Where(filterExpression);
            if (count != -1)
            {
                query.TakeCount = count;
            }
            return query;
        }

        string GetPrimaryFilterExpression(OrchestrationStateQueryFilter filter)
        {
            string basicPrimaryFilter = string.Format(CultureInfo.InvariantCulture, TableConstants.PrimaryFilterTemplate);
            string filterExpression = string.Empty;
            if (filter != null)
            {
                if (filter is OrchestrationStateInstanceFilter)
                {
                    var typedFilter = filter as OrchestrationStateInstanceFilter;
                    if (typedFilter.StartsWith)
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.PrimaryInstanceQueryRangeTemplate,
                            typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(typedFilter.ExecutionId))
                        {
                            filterExpression = string.Format(CultureInfo.InvariantCulture,
                                TableConstants.PrimaryInstanceQueryRangeTemplate,
                                typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                        }
                        else
                        {
                            filterExpression = string.Format(CultureInfo.InvariantCulture,
                                TableConstants.PrimaryInstanceQueryExactTemplate,
                                typedFilter.InstanceId,
                                typedFilter.ExecutionId);
                        }
                    }
                }
                else
                {
                    // TODO : for now we don't have indexes for anything other than the
                    // instance id so all filters are 'secondary' filters
                    filterExpression = GetSecondaryFilterExpression(filter);
                }
            }
            return basicPrimaryFilter + (string.IsNullOrEmpty(filterExpression) ? 
                string.Empty : " and " + filterExpression);
        }

        string GetSecondaryFilterExpression(OrchestrationStateQueryFilter filter)
        {
            string filterExpression;

            if (filter is OrchestrationStateInstanceFilter)
            {
                var typedFilter = filter as OrchestrationStateInstanceFilter;
                if (typedFilter.StartsWith)
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        TableConstants.InstanceQuerySecondaryFilterRangeTemplate,
                        typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                }
                else
                {
                    if (string.IsNullOrEmpty(typedFilter.ExecutionId))
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.InstanceQuerySecondaryFilterTemplate, typedFilter.InstanceId);
                    }
                    else
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.InstanceQuerySecondaryFilterExactTemplate, typedFilter.InstanceId,
                            typedFilter.ExecutionId);
                    }
                }
            }
            else if (filter is OrchestrationStateNameVersionFilter)
            {
                var typedFilter = filter as OrchestrationStateNameVersionFilter;
                if (typedFilter.Version == null)
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        TableConstants.NameVersionQuerySecondaryFilterTemplate, typedFilter.Name);
                }
                else
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        TableConstants.NameVersionQuerySecondaryFilterExactTemplate, typedFilter.Name,
                        typedFilter.Version);
                }
            }
            else if (filter is OrchestrationStateStatusFilter)
            {
                var typedFilter = filter as OrchestrationStateStatusFilter;
                filterExpression = string.Format(CultureInfo.InvariantCulture,
                    TableConstants.StatusQuerySecondaryFilterTemplate, typedFilter.Status);
            }
            else if (filter is OrchestrationStateTimeRangeFilter)
            {
                var typedFilter = filter as OrchestrationStateTimeRangeFilter;
                typedFilter.StartTime = ClipStartTime(typedFilter.StartTime);
                typedFilter.EndTime = ClipEndTime(typedFilter.EndTime);

                string startTime = XmlConvert.ToString(typedFilter.StartTime, XmlDateTimeSerializationMode.RoundtripKind);
                string endTime = XmlConvert.ToString(typedFilter.EndTime, XmlDateTimeSerializationMode.RoundtripKind);

                switch (typedFilter.FilterType)
                {
                    case OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter:
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.CreatedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
                        break;
                    case OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter:
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.CompletedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
                        break;
                    case OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter:
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            TableConstants.LastUpdatedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
                        break;
                    default:
                        throw new InvalidOperationException("Unsupported filter type: " +
                                                            typedFilter.FilterType.GetType());
                }
            }
            else
            {
                throw new InvalidOperationException("Unsupported filter type: " + filter.GetType());
            }

            return filterExpression;
        }

        DateTime ClipStartTime(DateTime startTime)
        {
            DateTimeOffset offsetStartTime = startTime;
            if (offsetStartTime < Microsoft.WindowsAzure.Storage.Table.Protocol.TableConstants.MinDateTime)
            {
                startTime = Microsoft.WindowsAzure.Storage.Table.Protocol.TableConstants.MinDateTime.DateTime;
            }
            return startTime;
        }

        DateTime ClipEndTime(DateTime endTime)
        {
            if (endTime > Utils.DateTimeSafeMaxValue)
            {
                endTime = Utils.DateTimeSafeMaxValue;
            }
            return endTime;
        }

        public async Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(string instanceId,
            string executionId)
        {
            var results = fromHistoryEventEntities(await readHistoryEventsAsync(instanceId, executionId));

            return results;
        }

        /// <summary>
        /// Reads events from Table Storage
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="executionId"></param>
        /// <returns></returns>
        private Task<IEnumerable<OrchestrationHistoryEventEntity>> readHistoryEventsAsync(string instanceId,
         string executionId)
        {
            string partitionKey = TableConstants.InstanceHistoryEventPrefix +
                                  TableConstants.JoinDelimiter + instanceId;

            string rowKeyLower = TableConstants.InstanceHistoryEventRowPrefix +
                                 TableConstants.JoinDelimiter + executionId +
                                 TableConstants.JoinDelimiter;

            string rowKeyUpper = TableConstants.InstanceHistoryEventRowPrefix +
                                 TableConstants.JoinDelimiter + executionId +
                                 TableConstants.JoinDelimiterPlusOne;

            string filter = string.Format(CultureInfo.InvariantCulture, TableConstants.TableRangeQueryFormat,
                partitionKey, rowKeyLower, rowKeyUpper);

            TableQuery<OrchestrationHistoryEventEntity> query =
                new TableQuery<OrchestrationHistoryEventEntity>().Where(filter);

            return ReadAllEntitiesAsync(query);
        }

        async Task<IEnumerable<T>> ReadAllEntitiesAsync<T>(TableQuery<T> query)
            where T : ITableEntity, new()
        {
            var results = new List<T>();
            TableQuerySegment<T> resultSegment = await table.ExecuteQuerySegmentedAsync(query, null);
            if (resultSegment.Results != null)
            {
                results.AddRange(resultSegment.Results);
            }

            while (resultSegment.ContinuationToken != null)
            {
                resultSegment = await table.ExecuteQuerySegmentedAsync(query, resultSegment.ContinuationToken);
                if (resultSegment.Results != null)
                {
                    results.AddRange(resultSegment.Results);
                }
            }
            return results;
        }

        string ComputeNextKeyInRange(string key)
        {
            char newChar = key[key.Length - 1];
            newChar++;

            string newKey = key.Substring(0, key.Length - 1);
            newKey += newChar;
            return newKey;
        }

        private async Task<object> performBatchTableOperationAsync(string operationTag,
            IEnumerable<CompositeTableEntity> entities,
            Action<TableBatchOperation, ITableEntity> batchOperationFunc)
        {
            if (entities == null)
            {
                throw new ArgumentNullException("entities");
            }

            var batchOperation = new TableBatchOperation();
            int operationCounter = 0;

            foreach (CompositeTableEntity entity in entities)
            {
                foreach (ITableEntity e in entity.BuildDenormalizedEntities())
                {
                    batchOperationFunc(batchOperation, e);
                    if (++operationCounter == 100)
                    {
                        await ExecuteBatchOperationAsync(operationTag, batchOperation);
                        batchOperation = new TableBatchOperation();
                        operationCounter = 0;
                    }
                }
            }

            if (operationCounter > 0)
            {
                await ExecuteBatchOperationAsync(operationTag, batchOperation);
            }

            return null;
        }

        async Task ExecuteBatchOperationAsync(string operationTag, TableBatchOperation batchOperation)
        {
            if (batchOperation.Count == 0)
            {
                return;
            }

            IList<TableResult> results = await table.ExecuteBatchAsync(batchOperation);

            foreach (TableResult result in results)
            {
                if (result.HttpStatusCode < 200 || result.HttpStatusCode > 299)
                {
                    throw new OrchestrationFrameworkException("Failed to perform " + operationTag + " batch operation: " +
                                                              result.HttpStatusCode);
                }
            }
        }

        public async Task<object> WriteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities)
        {
            return await performBatchTableOperationAsync("Write Entities", 
                fromHistoryEvents(entities), (bo, te) => bo.InsertOrReplace(te));
        }

        public async Task<object> WriteStateAsync(IEnumerable<OrchestrationState> states)
        {           
            return await performBatchTableOperationAsync("Write Entities", fromStates(states),
                (bo, te) => bo.InsertOrReplace(te));
        }
        

        private IEnumerable<CompositeTableEntity> fromHistoryEvents(IEnumerable<OrchestrationHistoryEvent> historyEvents)
        {
            List<OrchestrationHistoryEventEntity> events = new List<OrchestrationHistoryEventEntity>();

            foreach (var item in historyEvents)
            {
                events.Add(new OrchestrationHistoryEventEntity(item));
            }

            return events.ToArray();
        }

        private IEnumerable<CompositeTableEntity> fromStates(IEnumerable<OrchestrationState> stateEvents)
        {
            List<OrchestrationStateEntity> events = new List<OrchestrationStateEntity>();

            foreach (var item in stateEvents)
            {
                events.Add(new OrchestrationStateEntity(item));
            }

            return events.ToArray();
        }


        private IEnumerable<OrchestrationHistoryEvent> fromHistoryEventEntities(IEnumerable<OrchestrationHistoryEventEntity> historyEvents)
        {
            List<OrchestrationHistoryEvent> events = new List<OrchestrationHistoryEvent>();

            foreach (var item in historyEvents)
            {
                events.Add(new OrchestrationHistoryEvent(item.InstanceId,
                    item.ExecutionId, item.SequenceNumber, item.TaskTimeStamp, item.HistoryEvent));
            }

            return events.ToArray();
        }


        private async Task<object> deleteEntitesAsync(IEnumerable<CompositeTableEntity> entities)
        {
            return await performBatchTableOperationAsync("Delete Entities", entities, (bo, te) =>
            {
                te.ETag = "*";
                bo.Delete(te);
            });
        }


        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        /// <returns></returns>
        public async Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
           
            TableContinuationToken continuationToken = null;

            //TraceHelper.Trace(TraceEventType.Information,
            //    () =>
            //        "Purging orchestration instances before: " + thresholdDateTimeUtc + ", Type: " + timeRangeFilterType);

            int purgeCount = 0;
            do
            {
                TableQuerySegment<OrchestrationStateEntity> resultSegment =
                    (await this.queryOrchestrationStatesSegmentedAsync(
                        new OrchestrationStateQuery()
                            .AddTimeRangeFilter(DateTime.MinValue, thresholdDateTimeUtc, timeRangeFilterType),
                        continuationToken, 100)
                        .ConfigureAwait(false));

                continuationToken = resultSegment.ContinuationToken;

                if (resultSegment.Results != null)
                {
                    await purgeOrchestrationHistorySegmentAsync(resultSegment).ConfigureAwait(false);
                    purgeCount += resultSegment.Results.Count;
                }
            } while (continuationToken != null);

           // TraceHelper.Trace(TraceEventType.Information, () => "Purged " + purgeCount + " orchestration histories");
        }

        private async Task purgeOrchestrationHistorySegmentAsync(
            TableQuerySegment<OrchestrationStateEntity> orchestrationStateEntitySegment)
        {
            var stateEntitiesToDelete = new List<OrchestrationStateEntity>(orchestrationStateEntitySegment.Results);

            var historyEntitiesToDelete = new ConcurrentBag<IEnumerable<OrchestrationHistoryEventEntity>>();
            await Task.WhenAll(orchestrationStateEntitySegment.Results.Select(
                entity => Task.Run(async () =>
                {
                    IEnumerable<OrchestrationHistoryEventEntity> historyEntities =
                        await
                            this.readHistoryEventsAsync(
                                entity.State.OrchestrationInstance.InstanceId,
                                entity.State.OrchestrationInstance.ExecutionId).ConfigureAwait(false);

                    historyEntitiesToDelete.Add(historyEntities);
                })));

            List<Task> historyDeleteTasks = historyEntitiesToDelete.Select(
                historyEventList => this.deleteEntitesAsync(historyEventList)).Cast<Task>().ToList();

            // need to serialize history deletes before the state deletes so we dont leave orphaned history events
            await Task.WhenAll(historyDeleteTasks).ConfigureAwait(false);
            await Task.WhenAll(this.deleteEntitesAsync(stateEntitiesToDelete)).ConfigureAwait(false);
        }

        public async Task DeleteStoreIfExistsAsync()
        {
            await this.table.DeleteIfExistsAsync();
        }

        public async Task CreateStoreIfNotExistsAsync()
        {
            await this.table.CreateIfNotExistsAsync();
        }

     
    }
}