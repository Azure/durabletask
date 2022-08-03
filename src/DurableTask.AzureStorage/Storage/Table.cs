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
            this.Name = tableName;
            this.stats = this.azureStorageClient.Stats;

            this.tableServiceClient = tableServiceClient;
            this.tableClient = tableServiceClient.GetTableClient(tableName);
        }

        public string Name { get; }

        internal Uri? Uri => this.tableClient.GetEndpoint();

        public async Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            // If we received null, then the response must have been a 409 (Conflict) and the table must already exist
            Response<TableItem> response = await this.tableClient.CreateIfNotExistsAsync(cancellationToken);
            return response != null;
        }

        public async Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            // If we received null, then the response must have been a 404 (NotFound) and the table must not exist
            Response response = await this.tableClient.DeleteAsync(cancellationToken);
            return response != null;
        }

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Re-evaluate the use of an "Exists" method as it was intentional omitted from the client API
            List<TableItem> tables = await this.tableServiceClient
                .QueryAsync(filter: $"TableName eq '{tableClient.Name}'", cancellationToken: cancellationToken)
                .ToListAsync(cancellationToken);

            return tables.Count > 0;
        }

        public async Task ReplaceAsync<T>(T tableEntity, ETag ifMatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Replace, cancellationToken);
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task DeleteAsync<T>(T tableEntity, ETag ifMatch = default, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.DeleteEntityAsync(tableEntity.PartitionKey, tableEntity.RowKey, ifMatch, cancellationToken);
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.AddEntityAsync(tableEntity, cancellationToken);
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task MergeAsync<T>(T tableEntity, ETag ifMatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpdateEntityAsync(tableEntity, ifMatch, TableUpdateMode.Merge, cancellationToken);
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrMergeAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge, cancellationToken);
            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task InsertOrReplaceAsync<T>(T tableEntity, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            await this.tableClient.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace, cancellationToken);
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
            TableTransactionResults response = TableTransactionResults.Empty;
            var batch = new List<TableTransactionAction>(batchSize);

            foreach (T entity in entityBatch)
            {
                batch.Add(batchOperation(entity));
                if (batch.Count == batchSize)
                {
                    response.Add(await this.ExecuteBatchAsync(batch, cancellationToken));
                }
            }

            if (batch.Count > 0)
            {
                response.Add(await this.ExecuteBatchAsync(batch, cancellationToken));
            }

            return response;
        }

        public async Task<TableTransactionResults> ExecuteBatchAsync(IEnumerable<TableTransactionAction> batchOperation, CancellationToken cancellationToken = default)
        {
            var stopwatch = new Stopwatch();

            Response<IReadOnlyList<Response>> response = await this.tableClient.SubmitTransactionAsync(batchOperation, cancellationToken);
            IReadOnlyList<Response> batchResults = response.Value;

            stopwatch.Stop();

            this.stats.TableEntitiesWritten.Increment(batchResults.Count);

            return new TableTransactionResults(batchResults, stopwatch.Elapsed);
        }

        public TableQueryResponse<T> ExecuteQueryAsync<T>(string? filter = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            return new TableQueryResponse<T>(this.tableClient.QueryAsync<T>(filter, select: select, cancellationToken: cancellationToken));
        }
    }
}
