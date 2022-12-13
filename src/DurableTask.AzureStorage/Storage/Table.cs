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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    class Table
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudTableClient tableClient;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly CloudTable cloudTable;

        public Table(AzureStorageClient azureStorageClient, CloudTableClient tableClient, string tableName)
        {
            this.azureStorageClient = azureStorageClient;
            this.tableClient = tableClient;
            this.Name = tableName;
            this.stats = this.azureStorageClient.Stats;

            this.cloudTable = this.tableClient.GetTableReference(this.Name);
        }

        public string Name { get; }

        public Uri Uri => this.cloudTable.Uri;

        public async Task<bool> CreateIfNotExistsAsync()
        {
            return await this.azureStorageClient.MakeTableStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.CreateIfNotExistsAsync(null, context, cancellationToken),
                "Table Create");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeTableStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.DeleteIfExistsAsync(null, context, cancellationToken),
                "Table Delete");
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeTableStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.ExistsAsync(null, context, cancellationToken),
                "Table Exists");
        }

        public async Task ReplaceAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Replace(tableEntity);

            await ExecuteAsync(tableOperation, "Replace");
        }

        public async Task DeleteAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Delete(tableEntity);

            await ExecuteAsync(tableOperation, "Delete");
        }

        public async Task InsertAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Insert(tableEntity);

            await ExecuteAsync(tableOperation, "Insert");
        }

        public async Task MergeAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Merge(tableEntity);

            await ExecuteAsync(tableOperation, "Merge");
        }

        public async Task InsertOrMergeAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.InsertOrMerge(tableEntity);

            await ExecuteAsync(tableOperation, "InsertOrMerge");
        }

        public async Task InsertOrReplaceAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.InsertOrReplace(tableEntity);

            await ExecuteAsync(tableOperation, "InsertOrReplace");
        }

        private async Task ExecuteAsync(TableOperation operation, string operationType)
        {
            var storageTableResult = await this.azureStorageClient.MakeTableStorageRequest<TableResult>(
                (context, cancellationToken) => this.cloudTable.ExecuteAsync(operation, null, context, cancellationToken),
                "Table Execute " + operationType);

            this.stats.TableEntitiesWritten.Increment();
        }

        public async Task<TableResultResponseInfo> DeleteBatchAsync(IList<DynamicTableEntity> entityBatch)
        {
            return await this.ExecuteBatchAsync(entityBatch, "Delete", (batch, item) => { batch.Delete(item); return batch; });
        }

        public async Task<TableResultResponseInfo> InsertOrMergeBatchAsync(IList<DynamicTableEntity> entityBatch)
        {
            this.stats.TableEntitiesWritten.Increment(entityBatch.Count);
            return await this.ExecuteBatchAsync(entityBatch, "InsertOrMerge", (batch, item) => { batch.InsertOrMerge(item); return batch; });
        }

        private async Task<TableResultResponseInfo> ExecuteBatchAsync(
            IList<DynamicTableEntity> entityBatch, 
            string batchType, 
            Func<TableBatchOperation, DynamicTableEntity, TableBatchOperation> batchOperation)
        {
            List<TableResult> results = new List<TableResult>();
            int requestCount = 0;
            long elapsedMilliseconds = 0;
            int pageOffset = 0;
            while (pageOffset < entityBatch.Count)
            {
                List<DynamicTableEntity> entitiesInBatch = entityBatch.Skip(pageOffset).Take(100).ToList();

                var batch = new TableBatchOperation();
                foreach (DynamicTableEntity item in entitiesInBatch)
                {
                    batch = batchOperation(batch, item);
                }

                var batchResults = await this.ExecuteBatchAsync(batch, batchType);

                elapsedMilliseconds += batchResults.ElapsedMilliseconds;
                requestCount += batchResults.RequestCount;
                results.AddRange(batchResults.TableResults);
                pageOffset += entitiesInBatch.Count;
            }


            return new TableResultResponseInfo
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = requestCount,
                TableResults = results
            };
        }

        public async Task<TableResultResponseInfo> ExecuteBatchAsync(TableBatchOperation batchOperation, string batchType)
        {
            var stopwatch = new Stopwatch();
            long elapsedMilliseconds = 0;

            var batchResults = await this.azureStorageClient.MakeTableStorageRequest(
                (context, timeoutToken) => this.cloudTable.ExecuteBatchAsync(batchOperation, null, context, timeoutToken),
                "Table BatchExecute " + batchType);

            stopwatch.Stop();
            elapsedMilliseconds += stopwatch.ElapsedMilliseconds;

            this.stats.TableEntitiesWritten.Increment(batchOperation.Count);

            return new TableResultResponseInfo
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = 1,
                TableResults = batchResults
            };
        }

        public async Task<TableEntitiesResponseInfo<T>> ExecuteQueryAsync<T>(TableQuery<T> query, CancellationToken callerCancellationToken) where T : ITableEntity, new()
        {
            var results = new List<T>();
            TableContinuationToken? tableContinuationToken = null;

            var stopwatch = new Stopwatch();
            int requestCount = 0;
            long elapsedMilliseconds = 0;

            while (true)
            {
                stopwatch.Start();

                TableQuerySegment<T> segment = await this.azureStorageClient.MakeTableStorageRequest(
                    async (context, timeoutCancellationToken) =>
                    {
                        using var finalLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerCancellationToken, timeoutCancellationToken);
                        return await this.cloudTable.ExecuteQuerySegmentedAsync(query, tableContinuationToken, null, context, finalLinkedCts.Token);
                    },
                    "Table ExecuteQuerySegmented");

                stopwatch.Stop();
                elapsedMilliseconds += stopwatch.ElapsedMilliseconds;
                this.stats.TableEntitiesRead.Increment(segment.Results.Count);
                requestCount++;

                results.AddRange(segment);
                if (query.TakeCount > 0 && results.Count >= query.TakeCount)
                {
                    break;
                }

                tableContinuationToken = segment.ContinuationToken;
                if (tableContinuationToken == null || callerCancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            return new TableEntitiesResponseInfo<T>
            {
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = requestCount,
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
                tableContinuationToken = Utils.DeserializeFromJson<TableContinuationToken>(tokenContent);
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
                string tokenJson = Utils.SerializeToJson(segment.ContinuationToken);
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
