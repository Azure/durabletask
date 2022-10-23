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
    using System.Threading;
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
        const int MaxRetries = 3;
        static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromSeconds(30);
        static readonly TimeSpan DeltaBackOff = TimeSpan.FromSeconds(5);

        readonly string hubName;
        readonly TableServiceClient tableClient;
        readonly TableClient historyTable;
        readonly TableClient jumpStartTable;

        static readonly IDictionary<FilterComparisonType, string> ComparisonOperatorMap 
            = new Dictionary<FilterComparisonType, string>
            {{ FilterComparisonType.Equals, AzureTableConstants.EqualityOperator},
            { FilterComparisonType.NotEquals, AzureTableConstants.InEqualityOperator}};

        public AzureTableClient(string hubName, string tableConnectionString)
            : this(hubName, CreateDefaultClient(tableConnectionString))
        {

        }

        public AzureTableClient(string hubName, TableServiceClient client)
        {
            if (string.IsNullOrWhiteSpace(hubName))
            {
                throw new ArgumentException("Invalid hub name", nameof(hubName));
            }

            this.tableClient = client ?? throw new ArgumentNullException(nameof(client));
            this.hubName = hubName;
            this.historyTable = this.tableClient.GetTableClient(TableName);
            this.jumpStartTable = this.tableClient.GetTableClient(JumpStartTableName);
        }

        private static TableServiceClient CreateDefaultClient(string connectionString)
        {
            var options = new TableClientOptions();
            options.Retry.Delay = DeltaBackOff;
            options.Retry.MaxRetries = MaxRetries;
            options.Retry.Mode = RetryMode.Exponential;
            options.Retry.NetworkTimeout = MaximumExecutionTime;

            return new TableServiceClient(connectionString, options);
        }

        public string TableName => AzureTableConstants.InstanceHistoryTableNamePrefix + "00" + this.hubName;

        public string JumpStartTableName => AzureTableConstants.JumpStartTableNamePrefix + "00" + this.hubName;

        internal Task CreateTableIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.historyTable.CreateIfNotExistsAsync(cancellationToken);
        }

        internal Task CreateJumpStartTableIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.jumpStartTable.CreateIfNotExistsAsync(cancellationToken);
        }

        internal async Task DeleteTableIfExistsAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await this.historyTable.DeleteAsync(cancellationToken);
            }
            catch (RequestFailedException e) when (e.Status == (int)HttpStatusCode.NotFound)
            {
            }
        }

        internal async Task DeleteJumpStartTableIfExistsAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await this.jumpStartTable.DeleteAsync(cancellationToken);
            }
            catch (RequestFailedException e) when (e.Status == (int)HttpStatusCode.NotFound)
            {
            }
        }

        public AsyncPageable<AzureTableOrchestrationStateEntity> QueryOrchestrationStatesAsync(OrchestrationStateQuery stateQuery, CancellationToken cancellationToken = default)
        {
            return this.historyTable.QueryAsync<AzureTableOrchestrationStateEntity>(
                CreateODataFilter(stateQuery, false),
                cancellationToken: cancellationToken);
        }

        public AsyncPageable<AzureTableOrchestrationStateEntity> QueryJumpStartOrchestrationsAsync(OrchestrationStateQuery stateQuery, CancellationToken cancellationToken = default)
        {
            return this.jumpStartTable.QueryAsync<AzureTableOrchestrationStateEntity>(
                CreateODataFilter(stateQuery, true),
                cancellationToken: cancellationToken);
        }

        public AsyncPageable<AzureTableOrchestrationJumpStartEntity> QueryJumpStartOrchestrationsAsync(DateTime startTime, DateTime endTime, CancellationToken cancellationToken = default)
        {
            return this.jumpStartTable.QueryAsync<AzureTableOrchestrationJumpStartEntity>(
                CreateJumpStartODataFilter(startTime, endTime),
                cancellationToken: cancellationToken);
        }

        string CreateJumpStartODataFilter(DateTime startTime, DateTime endTime)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                AzureTableConstants.PrimaryTimeRangeTemplate,
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(startTime),
                AzureTableOrchestrationJumpStartEntity.GetPartitionKey(endTime));
        }

        internal string CreateODataFilter(OrchestrationStateQuery stateQuery, bool useTimeRangePrimaryFilter)
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

        public IAsyncEnumerable<AzureTableOrchestrationHistoryEventEntity> ReadOrchestrationHistoryEventsAsync(string instanceId, string executionId, CancellationToken cancellationToken = default)
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

            return this.historyTable.QueryAsync<AzureTableOrchestrationHistoryEventEntity>(filter, cancellationToken: cancellationToken);
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
            TableClient client,
            IEnumerable<AzureTableCompositeTableEntity> entities,
            Func<ITableEntity, TableTransactionAction> batchOperationFunc,
            CancellationToken cancellationToken = default)
        {
            const int BatchSize = 100;

            var batch = new List<TableTransactionAction>(BatchSize);
            foreach (ITableEntity entity in entities.SelectMany(x => x.BuildDenormalizedEntities()))
            {
                batch.Add(batchOperationFunc(entity));
                if (batch.Count == BatchSize)
                {
                    await ExecuteBatchOperationAsync(operationTag, client, batch, cancellationToken);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await ExecuteBatchOperationAsync(operationTag, client, batch, cancellationToken);
            }

            return null;
        }

        async Task ExecuteBatchOperationAsync(string operationTag, TableClient client, ICollection<TableTransactionAction> transaction, CancellationToken cancellationToken)
        {
            if (transaction.Count == 0)
            {
                return;
            }

            Response<IReadOnlyList<Response>> results = await client.SubmitTransactionAsync(transaction, cancellationToken);
            foreach (Response response in results.Value)
            {
                if (response.Status < 200 || response.Status > 299)
                {
                    throw new OrchestrationFrameworkException("Failed to perform " + operationTag + " batch operation: " +
                                                              response.Status);
                }
            }
        }

        public async Task<object> WriteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities, CancellationToken cancellationToken = default)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.historyTable, entities, item => new TableTransactionAction(TableTransactionActionType.UpsertReplace, item), cancellationToken);
        }

        public async Task<object> WriteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities, CancellationToken cancellationToken = default)
        {
            return await PerformBatchTableOperationAsync("Write Entities", this.jumpStartTable, entities, item => new TableTransactionAction(TableTransactionActionType.UpsertReplace, item), cancellationToken);
        }

        public async Task<object> DeleteEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities, CancellationToken cancellationToken = default)
        {
            return await PerformBatchTableOperationAsync("Delete Entities", this.historyTable, entities, item => new TableTransactionAction(TableTransactionActionType.Delete, item), cancellationToken);
        }

        public async Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<AzureTableCompositeTableEntity> entities, CancellationToken cancellationToken = default)
        {
            try
            {
                return await PerformBatchTableOperationAsync("Delete Entities", this.jumpStartTable, entities, item => new TableTransactionAction(TableTransactionActionType.Delete, item), cancellationToken);
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