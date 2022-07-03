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
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
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

        public Blob GetBlobReference(string container, string blobName, string? blobDirectory = null) =>
            new Blob(this, this.blobClient, container, blobName, blobDirectory);

        internal Blob GetBlobReference(Uri blobUri) =>
            new Blob(this, this.blobClient, blobUri);

        public BlobContainer GetBlobContainerReference(string container) =>
            new BlobContainer(this, this.blobClient, container);

        public Queue GetQueueReference(string queueName) =>
            new Queue(this, this.queueClient, queueName);

        public Table GetTableReference(string tableName) =>
            new Table(this, this.tableClient, tableName);

        public Task<T> MakeBlobStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(storageRequest, BlobAccountName, operationName, clientRequestId, force);

        public Task<T> MakeQueueStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, QueueAccountName, operationName, clientRequestId);

        public Task<T> MakeTableStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, TableAccountName, operationName, clientRequestId);

        public Task<bool> MakeBlobStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(storageRequest, BlobAccountName, operationName, clientRequestId, force);

        public Task<bool> MakeQueueStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, QueueAccountName, operationName, clientRequestId);

        public Task<bool> MakeTableStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string operationName, string? clientRequestId = null) =>
            this.MakeStorageRequest(storageRequest, TableAccountName, operationName, clientRequestId);

        private async Task<T> MakeStorageRequest<T>(Func<CancellationToken, Task<Response<T>>> storageRequest, string accountName, string operationName, string? clientRequestId = null, bool force = false)
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

        private Task<bool> MakeStorageRequest(Func<CancellationToken, Task<Response>> storageRequest, string accountName, string operationName, string? clientRequestId = null, bool force = false) =>
            this.MakeStorageRequest(cancellationToken => WrapFunctionWithReturnType(storageRequest, cancellationToken), accountName, operationName, clientRequestId, force);

        private static async Task<Response<bool>> WrapFunctionWithReturnType(Func<CancellationToken, Task<Response>> storageRequest, CancellationToken cancellationToken)
        {
            Response response = await storageRequest(cancellationToken);
            return response != null ? Response.FromValue(true, response) : Response.FromValue(false, NullResponse.Value);
        }

        private sealed class NullResponse : Response
        {
            public static Response Value { get; } = new NullResponse();

            private NullResponse()
            { }

            #region Response Implementation

            public override int Status => throw new NotImplementedException();

            public override string ReasonPhrase => throw new NotImplementedException();

            public override Stream? ContentStream { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override string ClientRequestId { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            

            public override void Dispose() =>
                throw new NotImplementedException();

            protected override bool ContainsHeader(string name) =>
                throw new NotImplementedException();

            protected override IEnumerable<HttpHeader> EnumerateHeaders() =>
                throw new NotImplementedException();

            protected override bool TryGetHeader(string name, out string value) =>
                throw new NotImplementedException();

            protected override bool TryGetHeaderValues(string name, out IEnumerable<string> values) =>
                throw new NotImplementedException();

            #endregion
        }
    }
}
