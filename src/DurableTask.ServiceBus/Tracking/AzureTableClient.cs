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
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using System.Xml;
    using Azure;
    using Azure.Core;
    using Azure.Data.Tables;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Tracing;

    internal class AzureTableClient
    {
        public const int JumpStartTableScanIntervalInDays = 1;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);

        readonly string hubName;
        readonly TableServiceClient tableClient;

        static readonly IDictionary<FilterComparisonType, string> ComparisonOperatorMap 
            = new Dictionary<FilterComparisonType, string>
            {{ FilterComparisonType.Equals, AzureTableConstants.EqualityOperator},
            { FilterComparisonType.NotEquals, AzureTableConstants.InEqualityOperator}};

        volatile TableClient historyTableClient;
        volatile TableClient jumpStartTableClient;

        public AzureTableClient(string hubName, string tableConnectionString)
        {
            if(string.IsNullOrEmpty(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            if(string.IsNullOrEmpty(tableConnectionString))
            {
                throw new ArgumentException("Invalid table connection string", nameof(tableConnectionString));
            }

            this.tableClient = new TableServiceClient(tableConnectionString);
            this.hubName = hubName;
        }

        public AzureTableClient(string hubName, Uri endpoint, TokenCredential credential)
        {
            if (string.IsNullOrWhiteSpace(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            if (endpoint == null)
            {
                throw new ArgumentException("Invalid endpoint", nameof(endpoint));
            }

            if (credential == null)
            {
                throw new ArgumentException("Invalid credentials", nameof(credential));
            }

            this.tableClient = new TableServiceClient(endpoint, credential);
            this.hubName = hubName;
        }

        public string TableName => AzureTableConstants.InstanceHistoryTableNamePrefix + "00" + this.hubName;

        public string JumpStartTableName => AzureTableConstants.JumpStartTableNamePrefix + "00" + this.hubName;

        internal async Task CreateTableIfNotExistsAsync()
        {
            await this.tableClient.CreateTableIfNotExistsAsync(TableName);
            this.historyTableClient = tableClient.GetTableClient(TableName);
        }

        internal async Task CreateJumpStartTableIfNotExistsAsync()
        {
            await this.tableClient.CreateTableIfNotExistsAsync(JumpStartTableName);
            this.historyTableClient = tableClient.GetTableClient(JumpStartTableName);
        }

        internal async Task DeleteTableIfExistsAsync()
        {
            if (this.historyTableClient != null)
            {
                await this.tableClient.DeleteTableAsync(TableName);
            }
        }

        internal async Task DeleteJumpStartTableIfExistsAsync()
        {
            if (this.jumpStartTableClient != null)
            {
                await this.tableClient.DeleteTableAsync(JumpStartTableName);
            }
        }

        public Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            var query = CreateQueryInternal(stateQuery,false);
            return ReadAllEntitiesAsync<AzureTableOrchestrationStateEntity>(query, this.historyTableClient);
        }

        public async Task<Page<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken, int count)
        {
            var query = CreateQueryInternal(stateQuery, false);
            var pageableResults = this.historyTableClient.QueryAsync<AzureTableOrchestrationStateEntity>(filter: query, maxPerPage: count);
            await using var enumerator = pageableResults.AsPages(continuationToken, count).GetAsyncEnumerator();
            return await enumerator.MoveNextAsync() ? enumerator.Current : default;
        }

        public Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryJumpStartOrchestrationsAsync(OrchestrationStateQuery stateQuery)
        {
            // TODO: Enable segmented query for paging purpose
            var query = CreateQueryInternal(stateQuery, true);
            return ReadAllEntitiesAsync<AzureTableOrchestrationStateEntity>(query, this.jumpStartTableClient);
        }

        public async Task<Page<AzureTableOrchestrationStateEntity>> QueryJumpStartOrchestrationsSegmentedAsync(
            OrchestrationStateQuery stateQuery, string continuationToken, int count)
        {
            var query = CreateQueryInternal(stateQuery, true);
            var pageableResults = this.jumpStartTableClient.QueryAsync<AzureTableOrchestrationStateEntity>(filter: query, maxPerPage: count);
            await using var enumerator = pageableResults.AsPages(continuationToken, count).GetAsyncEnumerator();
            return await enumerator.MoveNextAsync() ? enumerator.Current : default;
        }

        public Task<IEnumerable<AzureTableOrchestrationJumpStartEntity>> QueryJumpStartOrchestrationsAsync(DateTime startTime, DateTime endTime, int count)
        {
            var query = CreateJumpStartQuery(startTime, endTime);
            return ReadAllEntitiesAsync<AzureTableOrchestrationJumpStartEntity>(query, this.jumpStartTableClient, count);
        }

        internal string CreateJumpStartQuery(DateTime startTime, DateTime endTime)
        {
            string query = string.Format(
                CultureInfo.InvariantCulture,
                AzureTableConstants.PrimaryTimeRangeTemplate,
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(startTime),
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(endTime));

            return query;
        }

        internal string CreateQueryInternal(OrchestrationStateQuery stateQuery, bool useTimeRangePrimaryFilter)
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

            string filterExpression = GetPrimaryFilterExpression(primaryFilter, useTimeRangePrimaryFilter);
            if (string.IsNullOrWhiteSpace(filterExpression))
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
                        if (!string.IsNullOrWhiteSpace(secondaryFilter))
                        {
                            newFilter += " and " + secondaryFilter;
                        }

                        return newFilter;
                    });
            }

            return filterExpression;
        }

        string GetPrimaryFilterExpression(OrchestrationStateQueryFilter filter, bool isJumpStartTable)
        {
            string basicPrimaryFilter;
            if (!isJumpStartTable)
            {
                basicPrimaryFilter = string.Format(CultureInfo.InvariantCulture, AzureTableConstants.PrimaryFilterTemplate);
            }
            else
            {
                basicPrimaryFilter = string.Format(
                    CultureInfo.InvariantCulture,
                    AzureTableConstants.PrimaryTimeRangeTemplate,
                    AzureTableOrchestrationJumpStartEntity.GetPartitionKey(DateTime.UtcNow.AddDays(-1)),
                    AzureTableOrchestrationJumpStartEntity.GetPartitionKey(DateTime.UtcNow));
            }

            string filterExpression = string.Empty;
            if (filter != null)
            {
                if (filter is OrchestrationStateInstanceFilter typedFilter)
                {
                    if (typedFilter.StartsWith)
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.PrimaryInstanceQueryRangeTemplate,
                            typedFilter.InstanceId, ComputeNextKeyInRange(typedFilter.InstanceId));
                    }
                    else
                    {
                        if (string.IsNullOrWhiteSpace(typedFilter.ExecutionId))
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

            return basicPrimaryFilter + (string.IsNullOrWhiteSpace(filterExpression) ?
                string.Empty : " and " + filterExpression);
        }

        string GetSecondaryFilterExpression(OrchestrationStateQueryFilter filter)
        {
            string filterExpression;

            if (filter is OrchestrationStateInstanceFilter orchestrationStateInstanceFilter)
            {
                if (orchestrationStateInstanceFilter.StartsWith)
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        AzureTableConstants.InstanceQuerySecondaryFilterRangeTemplate,
                        orchestrationStateInstanceFilter.InstanceId, ComputeNextKeyInRange(orchestrationStateInstanceFilter.InstanceId));
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(orchestrationStateInstanceFilter.ExecutionId))
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.InstanceQuerySecondaryFilterTemplate, orchestrationStateInstanceFilter.InstanceId);
                    }
                    else
                    {
                        filterExpression = string.Format(CultureInfo.InvariantCulture,
                            AzureTableConstants.InstanceQuerySecondaryFilterExactTemplate, orchestrationStateInstanceFilter.InstanceId,
                            orchestrationStateInstanceFilter.ExecutionId);
                    }
                }
            }
            else if (filter is OrchestrationStateNameVersionFilter orchestrationStateNameVersionFilter)
            {
                if (orchestrationStateNameVersionFilter.Version == null)
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        AzureTableConstants.NameVersionQuerySecondaryFilterTemplate, orchestrationStateNameVersionFilter.Name);
                }
                else
                {
                    filterExpression = string.Format(CultureInfo.InvariantCulture,
                        AzureTableConstants.NameVersionQuerySecondaryFilterExactTemplate, orchestrationStateNameVersionFilter.Name,
                        orchestrationStateNameVersionFilter.Version);
                }
            }
            else if (filter is OrchestrationStateStatusFilter orchestrationStateStatusFilter)
            {
                string template = AzureTableConstants.StatusQuerySecondaryFilterTemplate;
                filterExpression = string.Format(CultureInfo.InvariantCulture,
                    template, ComparisonOperatorMap[orchestrationStateStatusFilter.ComparisonType], orchestrationStateStatusFilter.Status);
            }
            else if (filter is OrchestrationStateTimeRangeFilter orchestrationStateTimeRangeFilter)
            {
                orchestrationStateTimeRangeFilter.StartTime = ClipStartTime(orchestrationStateTimeRangeFilter.StartTime);
                orchestrationStateTimeRangeFilter.EndTime = ClipEndTime(orchestrationStateTimeRangeFilter.EndTime);

                string startTime = XmlConvert.ToString(orchestrationStateTimeRangeFilter.StartTime, XmlDateTimeSerializationMode.RoundtripKind);
                string endTime = XmlConvert.ToString(orchestrationStateTimeRangeFilter.EndTime, XmlDateTimeSerializationMode.RoundtripKind);

                switch (orchestrationStateTimeRangeFilter.FilterType)
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
                        throw new InvalidOperationException("Unsupported filter type: " + orchestrationStateTimeRangeFilter.FilterType.GetType());
                }
            }
            else
            {
                throw new InvalidOperationException("Unsupported filter type: " + filter.GetType());
            }

            return filterExpression;
        }

        private static readonly DateTimeOffset MinDateTime = new DateTimeOffset(1601, 1, 1, 0, 0, 0, TimeSpan.Zero);
        DateTime ClipStartTime(DateTime startTime)
        {
            DateTimeOffset offsetStartTime = startTime;
            if (offsetStartTime < MinDateTime)
            {
                startTime = MinDateTime.DateTime;
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

        public async Task<IEnumerable<AzureTableOrchestrationHistoryEventEntity>> ReadOrchestrationHistoryEventsAsync(string instanceId,
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

            var pageableResults = historyTableClient.QueryAsync<AzureTableOrchestrationHistoryEventEntity>(filter);

            var results = new List<AzureTableOrchestrationHistoryEventEntity>();

            await foreach (var entity in pageableResults)
            {
                results.Add(entity);
            }

            return results;
        }

        async Task<IEnumerable<T>> ReadAllEntitiesAsync<T>(string filter, TableClient tableClient, int count = -1)
            where T : class, ITableEntity
        {
            var pageableResults = tableClient.QueryAsync<T>(filter: filter, 
                maxPerPage: count > 0? count: default);

            var results = new List<T>();

            await foreach (T entity in pageableResults)
            {
                results.Add(entity);

                if(count > 0 && results.Count >= count)
                {
                    break;
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

        public async Task<object> PerformBatchTableOperationAsync(
            string operationTag,
            TableClient tableClient,
            IEnumerable<AzureTableCompositeTableEntity> entities,
            Action<List<TableTransactionAction>, ITableEntity> batchOperationFunc)
        {
            if (entities == null)
            {
                throw new ArgumentNullException(nameof(entities));
            }

            var batchOperation = new List<TableTransactionAction>();
            var operationCounter = 0;

            foreach (AzureTableCompositeTableEntity entity in entities)
            {
                foreach (ITableEntity e in entity.BuildDenormalizedEntities())
                {
                    batchOperationFunc(batchOperation, e);
                    if (++operationCounter == 100)
                    {
                        await ExecuteBatchOperationAsync(operationTag, tableClient, batchOperation);
                        batchOperation = new List<TableTransactionAction>();
                        operationCounter = 0;
                    }
                }
            }

            if (operationCounter > 0)
            {
                await ExecuteBatchOperationAsync(operationTag, tableClient, batchOperation);
            }

            return null;
        }

        async Task ExecuteBatchOperationAsync(string operationTag, TableClient tableClient, List<TableTransactionAction> batchOperation)
        {
            if (batchOperation.Count == 0)
            {
                return;
            }

            var results = await tableClient.SubmitTransactionAsync(batchOperation);
            foreach (var result in results.Value)
            {
                if (result.Status < 200 || result.Status > 299)
                {
                    throw new OrchestrationFrameworkException("Failed to perform " + operationTag + " batch operation: " +
                                                              result.Status);
                }
            }
        }

        public async Task<object> WriteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.historyTableClient, entities, (bo, te) => bo.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, te)));
        }

        public async Task<object> WriteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.jumpStartTableClient, entities, (bo, te) => bo.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, te)));
        }

        public async Task<object> DeleteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Delete Entities", this.historyTableClient, entities, (bo, te) =>
            {
                te.ETag = new ETag("*");
                bo.Add(new TableTransactionAction(TableTransactionActionType.Delete, te));
            });
        }

        public async Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            try
            {
                return await PerformBatchTableOperationAsync("Delete Entities", this.jumpStartTableClient, entities, (bo, te) =>
                {
                    te.ETag = new ETag("*");
                    bo.Add(new TableTransactionAction(TableTransactionActionType.Delete, te));
                });
            }
            // This call could come in from multiple nodes at the same time so a not found exception is harmless
            catch (RequestFailedException e) when (e.Status == (int)HttpStatusCode.NotFound)
            {
                TraceHelper.Trace(TraceEventType.Information, "AzureTableClient-DeleteJumpStartEntities-NotFound", "DeleteJumpStartEntitiesAsync not found exception: {0}", e.Message);
                return Task.FromResult(false);
            }
        }
    }
}