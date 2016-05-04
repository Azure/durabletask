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

    internal class TableClient
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
            tableClient.DefaultRequestOptions = new TableRequestOptions
            {
                RetryPolicy = new ExponentialRetry(DeltaBackOff, MaxRetries),
                MaximumExecutionTime = MaximumExecutionTime
            };

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

        public Task<IEnumerable<OrchestrationStateEntity>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            TableQuery<OrchestrationStateEntity> query = CreateQueryInternal(stateQuery, -1);
            return ReadAllEntitiesAsync(query);
        }

        public Task<TableQuerySegment<OrchestrationStateEntity>> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, TableContinuationToken continuationToken, int count)
        {
            TableQuery<OrchestrationStateEntity> query = CreateQueryInternal(stateQuery, count);
            return table.ExecuteQuerySegmentedAsync(query, continuationToken);
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

        public Task<IEnumerable<OrchestrationHistoryEventEntity>> ReadOrchestrationHistoryEventsAsync(string instanceId,
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

        public async Task<object> PerformBatchTableOperationAsync(string operationTag,
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

        public async Task<object> WriteEntitesAsync(IEnumerable<CompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", entities, (bo, te) => bo.InsertOrReplace(te));
        }

        public async Task<object> DeleteEntitesAsync(IEnumerable<CompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Delete Entities", entities, (bo, te) =>
            {
                te.ETag = "*";
                bo.Delete(te);
            });
        }
    }
}