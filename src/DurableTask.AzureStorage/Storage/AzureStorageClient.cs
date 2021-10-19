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
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;

    class AzureStorageClient
    {
        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);
        readonly CloudStorageAccount account;
        readonly CloudBlobClient blobClient;
        readonly CloudQueueClient queueClient;
        readonly CloudTableClient tableClient;

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings) : 
            this(settings.StorageAccountDetails == null ?
                CloudStorageAccount.Parse(settings.StorageConnectionString) : settings.StorageAccountDetails.ToCloudStorageAccount(),
                settings)
        { }

        public AzureStorageClient(CloudStorageAccount account, AzureStorageOrchestrationServiceSettings settings)
        {
            this.account = account;
            this.Settings = settings;

            this.StorageAccountName = account.Credentials.AccountName ?? settings.StorageAccountDetails.AccountName;
            this.Stats = new AzureStorageOrchestrationServiceStats();
            this.queueClient = account.CreateCloudQueueClient();
            this.queueClient.BufferManager = SimpleBufferManager.Shared;
            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.BufferManager = SimpleBufferManager.Shared;

            this.blobClient.DefaultRequestOptions.MaximumExecutionTime = StorageMaximumExecutionTime;

            if (settings.HasTrackingStoreStorageAccount)
            {
                var trackingStoreAccount = settings.TrackingStoreStorageAccountDetails.ToCloudStorageAccount();
                this.tableClient = trackingStoreAccount.CreateCloudTableClient();
            }
            else
            {
                this.tableClient = account.CreateCloudTableClient();
            }

            this.tableClient.BufferManager = SimpleBufferManager.Shared;
        }

        public AzureStorageOrchestrationServiceSettings Settings { get; }

        public AzureStorageOrchestrationServiceStats Stats { get; }

        public string StorageAccountName { get; }

        public Blob GetBlobReference(string container, string blobName, string? blobDirectory = null)
        {
            NameValidator.ValidateBlobName(blobName);
            return new Blob(this, this.blobClient, container, blobName, blobDirectory);
        }

        internal Blob GetBlobReference(Uri blobUri)
        {
            return new Blob(this, this.blobClient, blobUri);
        }

        public BlobContainer GetBlobContainerReference(string container)
        {
            NameValidator.ValidateContainerName(container);
            return new BlobContainer(this, this.blobClient, container);
        }

        public Queue GetQueueReference(string queueName)
        {
            NameValidator.ValidateQueueName(queueName);
            return new Queue(this, this.queueClient, queueName);
        }

        public Table GetTableReference(string tableName)
        {
            NameValidator.ValidateTableName(tableName);
            return new Table(this, this.tableClient, tableName);
        }


        public async Task<T> MakeStorageRequest<T>(Func<OperationContext, CancellationToken, Task<T>> storageRequest, string operationName, string? clientRequestId = null)
        {
            try
            {
                return await TimeoutHandler.ExecuteWithTimeout<T>(operationName, this.StorageAccountName, this.Settings, storageRequest, this.Stats, clientRequestId);
            }
            catch (StorageException ex)
            {
                throw new DurableTaskStorageException(ex);
            }
        }

        public async Task MakeStorageRequest(Func<OperationContext, CancellationToken, Task> storageRequest, string operationName, string? clientRequestId = null)
        {
            await this.MakeStorageRequest((context, cancellationToken) => WrapFunctionWithReturnType(storageRequest, context, cancellationToken), operationName, clientRequestId);
        }

        private static async Task<object?> WrapFunctionWithReturnType(Func<OperationContext, CancellationToken, Task> storageRequest, OperationContext context, CancellationToken cancellationToken)
        {
            await storageRequest(context, cancellationToken);
            return null;
        }
    }
}
