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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;

    class AzureStorageClient
    {
        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly CloudStorageAccount account;
        readonly CloudBlobClient blobClient;
        readonly CloudQueueClient queueClient;

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings, AzureStorageOrchestrationServiceStats stats, string storageAccountName)
        {
            this.Settings = settings;
            this.stats = stats;
            this.StorageAccountName = storageAccountName;

            this.account = settings.StorageAccountDetails == null
                ? CloudStorageAccount.Parse(settings.StorageConnectionString)
                : settings.StorageAccountDetails.ToCloudStorageAccount();

            this.StorageAccountName = account.Credentials.AccountName;
            this.stats = new AzureStorageOrchestrationServiceStats();
            this.queueClient = account.CreateCloudQueueClient();
            this.queueClient.BufferManager = SimpleBufferManager.Shared;
            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.BufferManager = SimpleBufferManager.Shared;

            this.blobClient.DefaultRequestOptions.MaximumExecutionTime = StorageMaximumExecutionTime;
        }

        public AzureStorageOrchestrationServiceSettings Settings { get; }

        public string StorageAccountName { get; }

        public Blob GetBlobReference(string container, string blobName, string blobDirectory = null)
        {
            return new Blob(this, this.blobClient, container, blobName, blobDirectory);
        }

        internal Blob GetBlobReference(Uri blobUri)
        {
            return new Blob(this, this.blobClient, blobUri);
        }

        public BlobContainer GetBlobContainerReference(string container)
        {
            return new BlobContainer(this, this.blobClient, container);
        }

        public async Task<T> MakeStorageRequest<T>(Func<OperationContext, CancellationToken, Task<T>> storageRequest, string operationName)
        {
            try
            {
                return await TimeoutHandler.ExecuteWithTimeout<T>(operationName, this.StorageAccountName, this.Settings, storageRequest, this.stats);
            }
            catch (StorageException ex)
            {
                throw new DurableTaskStorageException(ex);
            }
        }

        public async Task MakeStorageRequest(Func<OperationContext, CancellationToken, Task> storageRequest, string operationName)
        {
            await this.MakeStorageRequest((context, cancellationToken) => WrapFunctionWithReturnType(storageRequest, context, cancellationToken), operationName);
        }

        private static async Task<object> WrapFunctionWithReturnType(Func<OperationContext, CancellationToken, Task> storageRequest, OperationContext context, CancellationToken cancellationToken)
        {
            await storageRequest(context, cancellationToken);
            return null;
        }
    }
}
