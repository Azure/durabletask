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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Monitoring;

    class AzureStorageClient
    {
        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);
        readonly BlobServiceClient blobClient;
        readonly QueueServiceClient queueClient;
        readonly TableServiceClient tableClient;
        readonly SemaphoreSlim requestThrottleSemaphore;

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings)
        {
            this.Settings = settings;
            this.Stats = new AzureStorageOrchestrationServiceStats();
            this.queueClient = settings.StorageAccountDetails.GetQueueServiceClient();

            var blobOptions = new BlobClientOptions();
            blobOptions.Retry.NetworkTimeout = StorageMaximumExecutionTime;
            this.blobClient = settings.StorageAccountDetails.GetBlobServiceClient(blobOptions);

            if (settings.HasTrackingStoreStorageAccount)
            {
                this.tableClient = settings.TrackingStoreStorageAccountDetails.GetTableServiceClient();
            }
            else
            {
                this.tableClient = settings.StorageAccountDetails.GetTableServiceClient();
            }

            this.requestThrottleSemaphore = new SemaphoreSlim(this.Settings.MaxStorageOperationConcurrency);
        }

        public AzureStorageOrchestrationServiceSettings Settings { get; }

        public AzureStorageOrchestrationServiceStats Stats { get; }

        public string BlobAccountName => this.blobClient.AccountName;

        public string QueueAccountName => this.queueClient.AccountName;

        public string TableAccountName => this.tableClient.AccountName;

        public Blob GetBlobReference(string container, string blobName) =>
            new Blob(this, this.blobClient, container, blobName);

        internal Blob GetBlobReference(Uri blobUri) =>
            new Blob(this, this.blobClient, blobUri);

        public BlobContainer GetBlobContainerReference(string container) =>
            new BlobContainer(this, this.blobClient, container);

        public Queue GetQueueReference(string queueName) =>
            new Queue(this, this.queueClient, queueName);

        public Table GetTableReference(string tableName) =>
            new Table(this, this.tableClient, tableName);

        public Task<T> GetBlobStorageRequestResponse<T>(Func<CancellationToken, Task<T>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(storageRequest, BlobAccountName, operationName, clientRequestId, force);

        public Task<T> GetQueueStorageRequestResponse<T>(Func<CancellationToken, Task<T>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(storageRequest, QueueAccountName, operationName, clientRequestId, force);

        public Task<T> MakeBlobStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.GetStorageResponseValue(storageRequest, BlobAccountName, operationName, clientRequestId, force);

        public Task<T> MakeQueueStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.GetStorageResponseValue(storageRequest, QueueAccountName, operationName, clientRequestId);

        public Task<T> MakeTableStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.GetStorageResponseValue(storageRequest, TableAccountName, operationName, clientRequestId);

        public Task<Page<T>?> MakePaginatedBlobStorageRequest<T>(Func<CancellationToken, AsyncPageable<T>> storageRequest, string operationName, string? continuationToken = null, string? clientRequestId = null)
            where T : notnull =>
            this.GetNextPage(storageRequest, BlobAccountName, operationName, continuationToken, clientRequestId);

        public Task MakeBlobStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(storageRequest, BlobAccountName, operationName, clientRequestId, force);

        public Task MakeQueueStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, QueueAccountName, operationName, clientRequestId);

        public Task MakeTableStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, TableAccountName, operationName, clientRequestId);

        private async Task<T> GetStorageResponseValue<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string accountName, string operationName, string? clientRequestId, bool force = false)
        {
            Response<T> response = await this.MakeStorageRequest(storageRequest, accountName, operationName, clientRequestId, force);
            return response.Value;
        }

        private Task<Page<T>?> GetNextPage<T>(Func<CancellationToken, AsyncPageable<T>> storageRequest, string accountName, string operationName, string? continuationToken, string? clientRequestId, bool force = false)
            where T : notnull =>
            this.MakeStorageRequest(t => storageRequest(t).GetPageAsync(continuationToken, cancellationToken: t), accountName, operationName, clientRequestId, force);

        private async Task<T> MakeStorageRequest<T>(Func<CancellationToken, Task<T>> storageRequest, string accountName, string operationName, string? clientRequestId = null, bool force = false)
        {
            if (!force)
            {
                await requestThrottleSemaphore.WaitAsync();
            }

            try
            {
                return await TimeoutHandler.ExecuteWithTimeout(operationName, accountName, this.Settings, storageRequest, this.Stats, clientRequestId);
            }
            catch (RequestFailedException ex)
            {
                throw new DurableTaskStorageException(ex);
            }
            finally
            {
                if (!force)
                {
                    requestThrottleSemaphore.Release();
                }
            }
        }
    }
}
