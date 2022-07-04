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
#nullable enable
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using Azure.Data.Tables.Models;
    using DurableTask.AzureStorage.Monitoring;
    using Newtonsoft.Json;

    class Table
    {
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly TableServiceClient tableServiceClient;
        readonly TableClient tableClient;

        public Table(AzureStorageClient azureStorageClient, TableServiceClient tableServiceClient, string tableName)
        {
            this.azureStorageClient = azureStorageClient;
            this.Name = tableName;
            this.stats = this.azureStorageClient.Stats;

            this.tableServiceClient = tableServiceClient;
            this.tableClient = tableServiceClient.GetTableClient(tableName);
        }

        public string Name { get; }

        public Uri Uri => foo;

        public async Task<bool> CreateIfNotExistsAsync()
        {
            Response<TableItem> response = await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.CreateIfNotExistsAsync(cancellationToken),
                "Table Create");

            // If we received null, then the response must have been a 409 (Conflict) and the table must already exist
            return response != null;
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            Response response = await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.DeleteAsync(cancellationToken),
                "Table Delete");

            // If we received null, then the response must have been a 404 (NotFound) and the table must not exist
            return response != null;
        }

        public async Task<bool> ExistsAsync()
        {
            // TODO: Re-evaluate the use of an "Exists" method as it was intentional omitted from the client API
            List<TableItem> tables = await this.azureStorageClient
                .EnumerateTableStorageRequest<TableItem>(
                    cancellationToken => this.tableServiceClient.QueryAsync(filter: $"TableName eq '{tableClient.Name}'", cancellationToken: cancellationToken),
                    "Table Query Tables")
                .ToListAsync();

            return tables.Count > 0;
        }

        public async Task ReplaceAsync<T>(T tableEntity, ETag ifMatch) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Replace, cancellationToken),
                "Table Execute Replace");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task DeleteAsync<T>(T tableEntity, ETag ifMatch = default) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.DeleteEntityAsync(tableEntity.PartitionKey, tableEntity.RowKey, ifMatch, cancellationToken),
                "Table Execute Delete");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertAsync<T>(T tableEntity) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.AddEntityAsync(tableEntity, cancellationToken),
                "Table Execute Insert");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task MergeAsync<T>(T tableEntity, ETag ifMatch) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Merge, cancellationToken),
                "Table Execute Merge");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrMergeAsync<T>(T tableEntity) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge, cancellationToken),
                "Table Execute InsertOrMerge");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrReplaceAsync<T>(T tableEntity) where T : ITableEntity
        {
            await this.azureStorageClient.GetTableStorageRequestResponse(
                cancellationToken => this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace, cancellationToken),
                "Table Execute InsertOrReplace");

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task<TableResultResponseInfo> DeleteBatchAsync<T>(IList<T> entityBatch) where T : ITableEntity
        {
            return await this.ExecuteBatchAsync(entityBatch, "Delete", item => new TableTransactionAction(TableTransactionActionType.Delete, item));
        }

        public async Task<TableResultResponseInfo> InsertOrMergeBatchAsync<T>(IList<T> entityBatch) where T : ITableEntity
        {
            this.stats.TableEntitiesWritten.Increment(entityBatch.Count);
            return await this.ExecuteBatchAsync(entityBatch, "InsertOrMerge", item => new TableTransactionAction(TableTransactionActionType.UpsertMerge, item));
        }

        private async Task<TableResultResponseInfo> ExecuteBatchAsync<T>(
            IList<T> entityBatch,
            string batchType,
            Func<T, TableTransactionAction> batchOperation) where T : ITableEntity
        {
            List<Response> results = new List<Response>();
            long elapsedMilliseconds = 0;
            for (int i = 0; i < entityBatch.Count; i += 100)
            {
                // Note: Skip optimizes for IList<T> implementations
                IEnumerable<TableTransactionAction> batch = entityBatch
                    .Skip(i)
                    .Take(100)
                    .Select(batchOperation);

                TableResultResponseInfo batchResults = await this.ExecuteBatchAsync(batch, batchType);

                elapsedMilliseconds += batchResults.ElapsedMilliseconds;
                results.AddRange(batchResults.TableResults);
            }

            return new TableResultResponseInfo
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = entityBatch.Count,
                TableResults = results
            };
        }

        public async Task<TableResultResponseInfo> ExecuteBatchAsync(IEnumerable<TableTransactionAction> batchOperation, string batchType)
        {
            var stopwatch = new Stopwatch();
            long elapsedMilliseconds = 0;

            IReadOnlyList<Response> batchResults = await this.azureStorageClient.MakeTableStorageRequest(
                timeoutToken => this.tableClient.SubmitTransactionAsync(batchOperation, timeoutToken),
                "Table BatchExecute " + batchType);

            stopwatch.Stop();
            elapsedMilliseconds += stopwatch.ElapsedMilliseconds;

            this.stats.TableEntitiesWritten.Increment(batchResults.Count);

            return new TableResultResponseInfo
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = 1,
                TableResults = batchResults
            };
        }

        public async Task<TableEntitiesResponseInfo<T>> ExecuteQueryAsync<T>(Expression<Func<T, bool>> filter, CancellationToken callerCancellationToken) where T : class, ITableEntity, new()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            int pages = 0;
            List<T> results = await this.azureStorageClient
                .EnumerateTableStorageRequest(
                     cancellationToken => this.tableClient.QueryAsync(filter, cancellationToken: cancellationToken),
                     "Table Query",
                     onPageFetch: p => pages++)
                .ToListAsync();

            stopwatch.Stop();

            return new TableEntitiesResponseInfo<T>
            {
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
                RequestCount = pages,
                ReturnedEntities = results,
            };
        }

        public async Task<TableEntitiesResponseInfo<T>> ExecuteQueryAsync<T>(TableQuery<T> query) where T : ITableEntity, new()
        {
            return await this.ExecuteQueryAsync(query, CancellationToken.None);
        }

        public virtual async Task<TableEntitiesResponseInfo<T>> ExecuteQuerySegmentAsync<T>(
            TableQuery<T> query, 
            CancellationToken callerCancellationToken, 
            string? continuationToken = null) 
            where T : ITableEntity, new()
        {
            var results = new List<T>();
            TableContinuationToken? tableContinuationToken = null;

            if (!string.IsNullOrEmpty(continuationToken))
            {
                var tokenContent = Encoding.UTF8.GetString(Convert.FromBase64String(continuationToken));
                tableContinuationToken = JsonConvert.DeserializeObject<TableContinuationToken>(tokenContent);
            }

            var stopwatch = new Stopwatch();
            long elapsedMilliseconds = 0;

            stopwatch.Start();

            var segment = await this.azureStorageClient.MakeTableStorageRequest(
                async (context, timeoutCancellationToken) =>
                {
                    using (var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, timeoutCancellationToken))
                    {
                        return await this.cloudTable.ExecuteQuerySegmentedAsync(query, tableContinuationToken, null, context, finalLinkedCts.Token);
                    }
                },
                "Table ExecuteQuerySegmented");

            stopwatch.Stop();
            elapsedMilliseconds += stopwatch.ElapsedMilliseconds;
            this.stats.TableEntitiesRead.Increment(segment.Results.Count);

            results.AddRange(segment);

            string? newContinuationToken = null;
            if (segment.ContinuationToken != null)
            {
                string tokenJson = JsonConvert.SerializeObject(segment.ContinuationToken);
                newContinuationToken = Convert.ToBase64String(Encoding.UTF8.GetBytes(tokenJson));
            }

            return new TableEntitiesResponseInfo<T>
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = 1,
                ReturnedEntities = results,
                ContinuationToken = newContinuationToken,
            };
        }
    }
}
