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

namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    class Table
    {
        readonly AzureStorageClient azureStorageClient;
        readonly CloudTableClient tableClient;
        readonly TableRequestOptions tableRequestOptions;
        readonly CloudTable cloudTable;

        public string Name { get; }
        public string Uri { get; internal set; }

        public Table(AzureStorageClient azureStorageClient, CloudTableClient tableClient, string tableName)
        {
            this.azureStorageClient = azureStorageClient;
            this.tableClient = tableClient;
            this.Name = tableName;

            NameValidator.ValidateTableName(this.Name);
            this.cloudTable = this.tableClient.GetTableReference(this.Name);
        }

        public async Task<bool> CreateIfNotExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.CreateIfNotExistsAsync(null, context, cancellationToken),
                "Table Create");
        }

        public async Task<bool> DeleteIfExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.DeleteIfExistsAsync(null, context, cancellationToken),
                "Table Delete");
        }

        public async Task<bool> ExistsAsync()
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.ExistsAsync(null, context, cancellationToken),
                "Table Exists");
        }

        public async Task<TableResult> ReplaceAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Replace(tableEntity);

            return await ExecuteAsync(tableOperation);
        }

        public async Task<TableResult> DeleteAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Delete(tableEntity);

            return await ExecuteAsync(tableOperation);
        }

        public async Task<TableResult> InsertAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Insert(tableEntity);

            return await ExecuteAsync(tableOperation);
        }

        public async Task<TableResult> MergeAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.Merge(tableEntity);

            return await ExecuteAsync(tableOperation);
        }

        public async Task<TableResult> InsertOrMergeAsync(DynamicTableEntity tableEntity)
        {
            TableOperation tableOperation = TableOperation.InsertOrMerge(tableEntity);

            return await ExecuteAsync(tableOperation);
        }

        private async Task<TableResult> ExecuteAsync(TableOperation operation)
        {
            var storageTableResult = await this.azureStorageClient.MakeStorageRequest<Microsoft.WindowsAzure.Storage.Table.TableResult>(
                (context, cancellationToken) => this.cloudTable.ExecuteAsync(operation, null, context, cancellationToken),
            "Table Execute " + operation.OperationType);

            return new TableResult(storageTableResult);
        }

        public async Task<IList<Microsoft.WindowsAzure.Storage.Table.TableResult>> DeleteBatchAsync(IEnumerable<DynamicTableEntity> entityBatch)
        {
            var batch = new TableBatchOperation();
            foreach (DynamicTableEntity item in entityBatch)
            {
                batch.Delete(item);
            }

            return await this.ExecuteBatchAsync(batch, "Delete");
        }

        public async Task<IList<Microsoft.WindowsAzure.Storage.Table.TableResult>> InsertOrMergeBatchAsync(IEnumerable<DynamicTableEntity> entityBatch)
        {
            var batch = new TableBatchOperation();
            foreach (DynamicTableEntity item in entityBatch)
            {
                batch.InsertOrMerge(item);
            }

            return await this.ExecuteBatchAsync(batch, "InsertOrMerge");
        }

        private async Task<IList<Microsoft.WindowsAzure.Storage.Table.TableResult>> ExecuteBatchAsync(TableBatchOperation batch, string batchType)
        {
            return await this.azureStorageClient.MakeStorageRequest<IList<Microsoft.WindowsAzure.Storage.Table.TableResult>>(
                (context, timeoutToken) => this.cloudTable.ExecuteBatchAsync(batch, null, context, timeoutToken),
                "Table BatchExecute " + batchType);
        }

        public async Task<object> ExecuteQuerySegmentedAsync(TableQuery query, TableContinuationToken continuationToken, TableRequestOptions storageTableRequestOptions, OperationContext context, CancellationToken token)
        {
            return await this.azureStorageClient.MakeStorageRequest<bool>(
                (context, cancellationToken) => this.cloudTable.ExecuteQuerySegmentedAsync(null, context, cancellationToken),
                "Table ExecuteQuerySegmented");
        }
    }
}
