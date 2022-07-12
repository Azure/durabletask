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

            if (settings.StorageAccountDetails?.BlobClientProvider == null)
            {
                throw new ArgumentException("Blob client provider is not specified.", nameof(settings));
            }

            if (settings.StorageAccountDetails?.QueueClientProvider == null)
            {
                throw new ArgumentException("Queue client provider is not specified.", nameof(settings));
            }

            if (settings.StorageAccountDetails?.TableClientProvider == null)
            {
                throw new ArgumentException("Table client provider is not specified.", nameof(settings));
            }

            this.Settings = settings;
            this.Stats = new AzureStorageOrchestrationServiceStats();

            var exceptionPolicy = new ExceptionHttpPipelinePolicy();
            var throttlingPolicy = new ThrottlingHttpPipelinePolicy(this.Settings.MaxStorageOperationConcurrency);
            var monitoringPolicy = new MonitoringHttpPipelinePolicy(this.Stats);

            this.blobClient = settings.StorageAccountDetails.BlobClientProvider.Create(o => AddPolicies(o, exceptionPolicy, throttlingPolicy, monitoringPolicy));
            this.queueClient = settings.StorageAccountDetails.QueueClientProvider.Create(o => AddPolicies(o, exceptionPolicy, throttlingPolicy, monitoringPolicy));
            this.tableClient = settings.HasTrackingStoreStorageAccount
                ? settings.TrackingStoreClientProvider!.Create(o => AddPolicies(o, exceptionPolicy, throttlingPolicy, monitoringPolicy))
                : settings.StorageAccountDetails.TableClientProvider.Create(o => AddPolicies(o, exceptionPolicy, throttlingPolicy, monitoringPolicy));
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

        static void AddPolicies(ClientOptions options, ExceptionHttpPipelinePolicy exceptionPolicy, ThrottlingHttpPipelinePolicy throttlePolicy, MonitoringHttpPipelinePolicy monitoringPolicy)
        {
            options.AddPolicy(exceptionPolicy, HttpPipelinePosition.PerCall);
            options.AddPolicy(throttlePolicy, HttpPipelinePosition.PerCall);
            options.AddPolicy(monitoringPolicy, HttpPipelinePosition.PerRetry);
        }
    }
}
