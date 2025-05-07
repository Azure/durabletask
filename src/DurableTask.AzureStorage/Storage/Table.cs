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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using Azure.Data.Tables.Models;
    using DurableTask.AzureStorage.Monitoring;

    class Table
    {
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly TableServiceClient tableServiceClient;
        readonly TableClient tableClient;

        public Table(AzureStorageClient azureStorageClient, TableServiceClient tableServiceClient, string tableName)
        {
            this.azureStorageClient = azureStorageClient;
            this.stats = this.azureStorageClient.Stats;

            this.tableServiceClient = tableServiceClient;
            this.tableClient = tableServiceClient.GetTableClient(tableName);
        }

        public string Name => this.tableClient.Name;

        internal Uri Uri => this.tableClient.Uri;

        public async Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            // If we received null, then the response must have been a 409 (Conflict) and the table must already exist
            Response<TableItem> response = await this.tableClient.CreateIfNotExistsAsync(cancellationToken).DecorateFailure();
            return response != null;
        }

        public async Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            // If we received null, then the response must have been a 404 (NotFound) and the table must not exist
            Response response = await this.tableClient.DeleteAsync(cancellationToken).DecorateFailure();
            return response != null;
        }

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Re-evaluate the use of an "Exists" method as it was intentional omitted from the client API
            List<TableItem> tables = await this.tableServiceClient
                .QueryAsync(filter: $"TableName eq '{this.tableClient.Name}'", cancellationToken: cancellationToken)
                .DecorateFailure()
                .ToListAsync(cancellationToken);

            return tables.Count > 0;
        }

        public async Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            await this.tableClient.DeleteAsync(cancellationToken).DecorateFailure();
        }

        public async Task ReplaceEntityAsync<T>(T tableEntity, ETag ifMatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Replace, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task DeleteEntityAsync<T>(T tableEntity, ETag ifMatch = default, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.DeleteEntityAsync(tableEntity.PartitionKey, tableEntity.RowKey, ifMatch, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertEntityAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.AddEntityAsync(tableEntity, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task MergeEntityAsync<T>(T tableEntity, ETag ifMatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Merge, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrMergeEntityAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrReplaceEntityAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace, cancellationToken).DecorateFailure();
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task<TableTransactionResults> DeleteBatchAsync<T>(IEnumerable<T> entityBatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            return await this.ExecuteBatchAsync(entityBatch, item => new TableTransactionAction(TableTransactionActionType.Delete, item), cancellationToken: cancellationToken);
        }

        public async Task<TableTransactionResults> InsertOrMergeBatchAsync<T>(IEnumerable<T> entityBatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            TableTransactionResults results = await this.ExecuteBatchAsync(entityBatch, item => new TableTransactionAction(TableTransactionActionType.UpsertMerge, item), cancellationToken: cancellationToken);

            if (results.Responses.Count > 0)
            {
                this.stats.TableEntitiesWritten.Increment(results.Responses.Count);
            }

            return results;
        }

        async Task<TableTransactionResults> ExecuteBatchAsync<T>(
            IEnumerable<T> entityBatch,
            Func<T, TableTransactionAction> batchOperation,
            int batchSize = 100,
            CancellationToken cancellationToken = default) where T : ITableEntity
        {
            if (batchSize > 100)
            {
                throw new ArgumentOutOfRangeException(nameof(batchSize), "Table storage does not support batch sizes greater than 100.");
            }

            var resultsBuilder = new TableTransactionResultsBuilder();
            var batch = new List<TableTransactionAction>(batchSize);

            foreach (T entity in entityBatch)
            {
                batch.Add(batchOperation(entity));
                if (batch.Count == batchSize)
                {
                    resultsBuilder.Add(await this.ExecuteBatchAsync(batch, cancellationToken));
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                resultsBuilder.Add(await this.ExecuteBatchAsync(batch, cancellationToken));
            }

            return resultsBuilder.ToResults();
        }

        public async Task<TableTransactionResults> ExecuteBatchAsync(IEnumerable<TableTransactionAction> batchOperation, CancellationToken cancellationToken = default)
        {
            var stopwatch = new Stopwatch();

            Response<IReadOnlyList<Response>> response = await this.tableClient.SubmitTransactionAsync(batchOperation, cancellationToken).DecorateFailure();
            IReadOnlyList<Response> batchResults = response.Value;

            stopwatch.Stop();

            this.stats.TableEntitiesWritten.Increment(batchResults.Count);

            return new TableTransactionResults(batchResults, stopwatch.Elapsed);
        }

        public TableQueryResponse<T> ExecuteQueryAsync<T>(
            string? filter = null,
            int? maxPerPage = null,
            IEnumerable<string>? select = null,
            CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            return new TableQueryResponse<T>(
                this.tableClient.QueryAsync<T>(filter, maxPerPage, select, cancellationToken).DecorateFailure(),
                this.stats);
        }
    }
}
