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
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using DurableTask.AzureStorage.Http;
    using DurableTask.AzureStorage.Monitoring;

    class AzureStorageClient
    {
        readonly BlobServiceClient blobClient;
        readonly QueueServiceClient queueClient;
        readonly TableServiceClient tableClient;

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (settings.StorageAccountClientProvider == null)
            {
                throw new ArgumentException("Storage account client provider is not specified.", nameof(settings));
            }

            this.Settings = settings;
            this.Stats = new AzureStorageOrchestrationServiceStats();

            var throttlingPolicy = new ThrottlingHttpPipelinePolicy(this.Settings.MaxStorageOperationConcurrency);
            var timeoutPolicy = new LeaseTimeoutHttpPipelinePolicy(this.Settings.LeaseRenewInterval);
            var monitoringPolicy = new MonitoringHttpPipelinePolicy(this.Stats);

            this.blobClient = CreateClient(settings.StorageAccountClientProvider.Blob, ConfigureClientPolicies);
            this.queueClient = CreateClient(settings.StorageAccountClientProvider.Queue, ConfigureClientPolicies);
            this.tableClient = CreateClient(
                settings.HasTrackingStoreStorageAccount
                    ? settings.TrackingServiceClientProvider!
                    : settings.StorageAccountClientProvider.Table,
                ConfigureClientPolicies);

            void ConfigureClientPolicies<TClientOptions>(TClientOptions options) where TClientOptions : ClientOptions
            {
                options.AddPolicy(throttlingPolicy!, HttpPipelinePosition.PerCall);
                options.AddPolicy(timeoutPolicy!, HttpPipelinePosition.PerCall);
                options.AddPolicy(monitoringPolicy!, HttpPipelinePosition.PerRetry);
            }
        }

        public AzureStorageOrchestrationServiceSettings Settings { get; }

        public AzureStorageOrchestrationServiceStats Stats { get; }

        public string BlobAccountName => this.blobClient.AccountName;

        public string QueueAccountName => this.queueClient.AccountName;

        public string TableAccountName => this.tableClient.AccountName;

        public Blob GetBlobReference(string container, string blobName)
        {
            return new Blob(this.blobClient, container, blobName);
        }

        internal Blob GetBlobReference(Uri blobUri)
        {
            return new Blob(this.blobClient, blobUri);
        }

        public BlobContainer GetBlobContainerReference(string container)
        {
            return new BlobContainer(this, this.blobClient, container);
        }

        public Queue GetQueueReference(string queueName)
        {
            return new Queue(this, this.queueClient, queueName);
        }

        public Table GetTableReference(string tableName)
        {
            return new Table(this, this.tableClient, tableName);
        }

        static TClient CreateClient<TClient, TClientOptions>(
            IStorageServiceClientProvider<TClient, TClientOptions> storageProvider,
            Action<TClientOptions> configurePolicies)
            where TClientOptions : ClientOptions
        {
            TClientOptions options = storageProvider.CreateOptions();
            configurePolicies?.Invoke(options);

            return storageProvider.CreateClient(options);
        }
    }
}
