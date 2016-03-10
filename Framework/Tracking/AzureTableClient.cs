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
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Table;

    internal class AzureTableClient
    {
        const int MaxRetries = 3;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);
        static readonly TimeSpan DeltaBackOff = TimeSpan.FromSeconds(5);

        readonly string hubName;
        readonly CloudTableClient tableClient;
        readonly object thisLock = new object();

        volatile CloudTable table;

        public AzureTableClient(string hubName, string tableConnectionString)
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
            get { return AzureTableConstants.InstanceHistoryTableNamePrefix + "00" + hubName; }
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

        internal async Task CreateTableIfNotExistsAsync()
        {
            table = tableClient.GetTableReference(TableName);
            await table.CreateIfNotExistsAsync();
        }

        internal async Task DeleteTableIfExistsAsync()
        {
            if (table != null)
            {
                await table.DeleteIfExistsAsync();
            }
        }

        public Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, -1);
            return ReadAllEntitiesAsync(query);
        }

        public Task<TableQuerySegment<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, TableContinuationToken continuationToken, int count)
        {
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, count);
            return table.ExecuteQuerySegmentedAsync(query, continuationToken);
        }

        TableQuery<AzureTableOrchestrationStateEntity> CreateQueryInternal(OrchestrationStateQuery stateQuery, int count)
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

            TableQuery<AzureTableOrchestrationStateEntity> query =
                new TableQuery<AzureTableOrchestrationStateEntity>().Where(filterExpression);
            if (count != -1)
            {
                query.TakeCount = count;
            }
            return query;
        }

        string GetPrimaryFilterExpression(OrchestrationStateQueryFilter filter)
        {
            string basicPrimaryFilter = string.Format(CultureInfo.InvariantCulture, AzureTableConstants.PrimaryFilterTemplate);
            string filterExpression = string.Empty;
            if (filter != null)
            {
                if (filter is OrchestrationStateInstanceFilter)
                {
                    var typedFilter = filter as OrchestrationStateInstanceFilter;
                    if (typedFilter.StartsWith)
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.PrimaryInstanceQueryRangeTemplate,
                            typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(typedFilter.ExecutionId))
                        {
                            filterExpression = string.Format(CultureInfo.InvariantCulture,
                                AzureTableConstants.PrimaryInstanceQueryRangeTemplate,
                                typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                        }
                        else
                        {
                            filterExpression = string.Format(CultureInfo.InvariantCulture,
                                AzureTableConstants.PrimaryInstanceQueryExactTemplate,
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
                        AzureTableConstants.InstanceQuerySecondaryFilterRangeTemplate,
                        typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                }
                else
                {
                    if (string.IsNullOrEmpty(typedFilter.ExecutionId))
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.InstanceQuerySecondaryFilterTemplate, typedFilter.InstanceId);
                    }
                    else
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.InstanceQuerySecondaryFilterExactTemplate, typedFilter.InstanceId,
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
                        AzureTableConstants.NameVersionQuerySecondaryFilterTemplate, typedFilter.Name);
                }
                else
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        AzureTableConstants.NameVersionQuerySecondaryFilterExactTemplate, typedFilter.Name,
                        typedFilter.Version);
                }
            }
            else if (filter is OrchestrationStateStatusFilter)
            {
                var typedFilter = filter as OrchestrationStateStatusFilter;
                filterExpression = string.Format(CultureInfo.InvariantCulture,
                    AzureTableConstants.StatusQuerySecondaryFilterTemplate, typedFilter.Status);
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
                            AzureTableConstants.CreatedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
                        break;
                    case OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter:
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.CompletedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
                        break;
                    case OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter:
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.LastUpdatedTimeRangeQuerySecondaryFilterTemplate, startTime, endTime);
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

        public Task<IEnumerable<AzureTableOrchestrationHistoryEventEntity>> ReadOrchestrationHistoryEventsAsync(string instanceId,
            string executionId)
        {
            string partitionKey = AzureTableConstants.InstanceHistoryEventPrefix +
                                  AzureTableConstants.JoinDelimiter + instanceId;

            string rowKeyLower = AzureTableConstants.InstanceHistoryEventRowPrefix +
                                 AzureTableConstants.JoinDelimiter + executionId +
                                 AzureTableConstants.JoinDelimiter;

            string rowKeyUpper = AzureTableConstants.InstanceHistoryEventRowPrefix +
                                 AzureTableConstants.JoinDelimiter + executionId +
                                 AzureTableConstants.JoinDelimiterPlusOne;

            string filter = string.Format(CultureInfo.InvariantCulture, AzureTableConstants.TableRangeQueryFormat,
                partitionKey, rowKeyLower, rowKeyUpper);

            TableQuery<AzureTableOrchestrationHistoryEventEntity> query =
                new TableQuery<AzureTableOrchestrationHistoryEventEntity>().Where(filter);
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
            IEnumerable<AzureTableCompositeTableEntity> entities,
            Action<TableBatchOperation, ITableEntity> batchOperationFunc)
        {
            if (entities == null)
            {
                throw new ArgumentNullException("entities");
            }

            var batchOperation = new TableBatchOperation();
            int operationCounter = 0;

            foreach (AzureTableCompositeTableEntity entity in entities)
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

        public async Task<object> WriteEntitesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", entities, (bo, te) => bo.InsertOrReplace(te));
        }

        public async Task<object> DeleteEntitesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Delete Entities", entities, (bo, te) =>
            {
                te.ETag = "*";
                bo.Delete(te);
            });
        }
    }
}