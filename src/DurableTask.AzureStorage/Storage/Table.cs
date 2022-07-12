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

        public async Task<TableResultResponseInfo> DeleteBatchAsync<T>(IEnumerable<T> entityBatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            return await this.ExecuteBatchAsync(entityBatch, item => new TableTransactionAction(TableTransactionActionType.Delete, item), cancellationToken);
        }

        public async Task<TableResultResponseInfo> InsertOrMergeBatchAsync<T>(IEnumerable<T> entityBatch, CancellationToken cancellationToken = default) where T : ITableEntity
        {
            TableResultResponseInfo results = await this.ExecuteBatchAsync(entityBatch, item => new TableTransactionAction(TableTransactionActionType.UpsertMerge, item), cancellationToken);

            if (results.TableResults?.Count > 0)
            {
                this.stats.TableEntitiesWritten.Increment(results.TableResults.Count);
            }

            return results;
        }

        async Task<TableResultResponseInfo> ExecuteBatchAsync<T>(
            IEnumerable<T> entityBatch,
            Func<T, TableTransactionAction> batchOperation,
            CancellationToken cancellationToken = default) where T : ITableEntity
        {
            var results = new List<Response>();
            long elapsedMilliseconds = 0;
            int requestCount = 0;

            List<TableTransactionAction>? batch = null;
            IEnumerator<T> batchEnumerator = entityBatch.GetEnumerator();
            do
            {
                batch = GetNext(batchEnumerator, 100).Select(batchOperation).ToList();

                if (batch.Count > 0)
                {
                    TableResultResponseInfo batchResults = await this.ExecuteBatchAsync(batch, cancellationToken);

                    elapsedMilliseconds += batchResults.ElapsedMilliseconds;
                    requestCount++;
                    results.AddRange(batchResults.TableResults);
                }
            } while (batch.Count > 0);

            return new TableResultResponseInfo
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = requestCount,
                TableResults = results
            };

            IEnumerable<T> GetNext(IEnumerator<T> enumerator, int count)
            {
                for (int i = 0; i < count && enumerator.MoveNext(); i++)
                {
                    yield return enumerator.Current;
                }
            }
        }

        public async Task<TableResultResponseInfo> ExecuteBatchAsync(IEnumerable<TableTransactionAction> batchOperation, CancellationToken cancellationToken = default)
        {
            var stopwatch = new Stopwatch();
            long elapsedMilliseconds = 0;

            Response<IReadOnlyList<Response>> response = await this.tableClient.SubmitTransactionAsync(batchOperation, cancellationToken);
            IReadOnlyList<Response> batchResults = response.Value;

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

        public async Task<TableEntitiesResponseInfo<T>> ExecuteCompleteQueryAsync<T>(string? filter = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            int requests = 0;
            var results = new List<T>();
            await foreach (Page<T> page in this.tableClient.QueryAsync<T>(filter, select: select, cancellationToken: cancellationToken).AsPages())
            {
                requests++;
                results.AddRange(page.Values);
            }

            stopwatch.Stop();

            return new TableEntitiesResponseInfo<T>
            {
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
                RequestCount = requests,
                ReturnedEntities = results,
            };
        }

        public AsyncPageable<T> ExecuteQueryAsync<T>(string? filter = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default) where T : class, ITableEntity, new()
        {
            return this.tableClient.QueryAsync<T>(filter, select: select, cancellationToken: cancellationToken);
        }
    }
}
