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
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Tracing;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Table.Protocol;

    internal class AzureTableClient
    {
        public const int JumpStartTableScanIntervalInDays = 1;
        const int MaxRetries = 3;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);
        static readonly TimeSpan DeltaBackOff = TimeSpan.FromSeconds(5);

        readonly string hubName;
        readonly CloudTableClient tableClient;
        readonly object thisLock = new object();

        volatile CloudTable historyTable;
        volatile CloudTable jumpStartTable;

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

            this.tableClient = CloudStorageAccount.Parse(tableConnectionString).CreateCloudTableClient();
            this.tableClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry(DeltaBackOff,
                MaxRetries);
            this.tableClient.DefaultRequestOptions.MaximumExecutionTime = MaximumExecutionTime;

            this.hubName = hubName;
            this.historyTable = tableClient.GetTableReference(TableName);
            this.jumpStartTable = tableClient.GetTableReference(this.JumpStartTableName);
        }

        public string TableName
        {
            get { return AzureTableConstants.InstanceHistoryTableNamePrefix + "00" + this.hubName; }
        }

        public string JumpStartTableName
        {
            get { return AzureTableConstants.JumpStartTableNamePrefix + "00" + this.hubName; }
        }

        internal async Task CreateTableIfNotExistsAsync()
        {
            this.historyTable = this.tableClient.GetTableReference(TableName);
            await this.historyTable.CreateIfNotExistsAsync();
        }

        internal async Task CreateJumpStartTableIfNotExistsAsync()
        {
            jumpStartTable = this.tableClient.GetTableReference(this.JumpStartTableName);
            await this.jumpStartTable.CreateIfNotExistsAsync();
        }

        internal async Task DeleteTableIfExistsAsync()
        {
            if (this.historyTable != null)
            {
                await this.historyTable.DeleteIfExistsAsync();
            }
        }

        internal async Task DeleteJumpStartTableIfExistsAsync()
        {
            if (this.jumpStartTable != null)
            {
                await this.jumpStartTable.DeleteIfExistsAsync();
            }
        }

        public Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesAsync(
            OrchestrationStateQuery stateQuery)
        {
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, -JumpStartTableScanIntervalInDays, false);
            return ReadAllEntitiesAsync(query, this.historyTable);
        }

        public Task<TableQuerySegment<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesSegmentedAsync(
            OrchestrationStateQuery stateQuery, TableContinuationToken continuationToken, int count)
        {
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, count, false);
            return historyTable.ExecuteQuerySegmentedAsync(query, continuationToken);
        }

        public Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryJumpStartOrchestrationsAsync(OrchestrationStateQuery stateQuery)
        {
            // TODO: Enable segmented query for paging purpose
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, -1, true);
            return ReadAllEntitiesAsync(query, this.jumpStartTable);
        }

        public Task<TableQuerySegment<AzureTableOrchestrationStateEntity>> QueryJumpStartOrchestrationsSegmentedAsync(
            OrchestrationStateQuery stateQuery, TableContinuationToken continuationToken, int count)
        {
            TableQuery<AzureTableOrchestrationStateEntity> query = CreateQueryInternal(stateQuery, count, true);
            return this.jumpStartTable.ExecuteQuerySegmentedAsync(query, continuationToken);
        }

        public Task<IEnumerable<AzureTableOrchestrationJumpStartEntity>> QueryJumpStartOrchestrationsAsync(DateTime startTime, DateTime endTime, int count)
        {
            TableQuery<AzureTableOrchestrationJumpStartEntity> query = CreateJumpStartQuery(startTime, endTime, count);
            return ReadAllEntitiesAsync(query, this.jumpStartTable);
        }

        TableQuery<AzureTableOrchestrationJumpStartEntity> CreateJumpStartQuery(DateTime startTime, DateTime endTime, int count)
        {
            string filterExpression = string.Format(
                CultureInfo.InvariantCulture,
                AzureTableConstants.PrimaryTimeRangeTemplate,
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(startTime),
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(endTime));

            TableQuery<AzureTableOrchestrationJumpStartEntity> query =
                new TableQuery<AzureTableOrchestrationJumpStartEntity>().Where(filterExpression);
            if (count != -1)
            {
                query.TakeCount = count;
            }
            return query;
        }

        TableQuery<AzureTableOrchestrationStateEntity> CreateQueryInternal(OrchestrationStateQuery stateQuery, int count, bool useTimerangePrimaryFilter)
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

            string filterExpression = GetPrimaryFilterExpression(primaryFilter, useTimerangePrimaryFilter);
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
            if (offsetStartTime < TableConstants.MinDateTime)
            {
                startTime = TableConstants.MinDateTime.DateTime;
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
            return ReadAllEntitiesAsync(query, this.historyTable);
        }

        async Task<IEnumerable<T>> ReadAllEntitiesAsync<T>(TableQuery<T> query, CloudTable table)
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

        public async Task<object> PerformBatchTableOperationAsync(
            string operationTag,
            CloudTable table,
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
                        await ExecuteBatchOperationAsync(operationTag, table, batchOperation);
                        batchOperation = new TableBatchOperation();
                        operationCounter = 0;
                    }
                }
            }

            if (operationCounter > 0)
            {
                await ExecuteBatchOperationAsync(operationTag, table, batchOperation);
            }

            return null;
        }

        async Task ExecuteBatchOperationAsync(string operationTag, CloudTable table, TableBatchOperation batchOperation)
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

        public async Task<object> WriteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.historyTable, entities, (bo, te) => bo.InsertOrReplace(te));
        }

        public async Task<object> WriteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.jumpStartTable, entities, (bo, te) => bo.InsertOrReplace(te));
        }

        public async Task<object> DeleteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            return await PerformBatchTableOperationAsync("Delete Entities", this.historyTable, entities, (bo, te) =>
            {
                te.ETag = "*";
                bo.Delete(te);
            });
        }

        public async Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities)
        {
            try
            {
                return await PerformBatchTableOperationAsync("Delete Entities", this.jumpStartTable, entities, (bo, te) =>
                {
                    te.ETag = "*";
                    bo.Delete(te);
                });
            }
            // This call could come in from multiple nodes at the same time so a not found exception is harmless
            catch (StorageException e) when (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
            {
                TraceHelper.Trace(TraceEventType.Information, "AzureTableClient-DeleteJumpStartEntities-NotFound", "DeleteJumpStartEntitiesAsync not found exception: {0}", e.Message);
                return Task.FromResult(false);
            }
        }
    }
}