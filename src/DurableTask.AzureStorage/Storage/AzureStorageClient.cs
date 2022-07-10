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
        // using IDisposable requestLifetime = HttpPipeline.CreateClientRequestIdScope(clientRequestId);

        readonly BlobServiceClient blobClient;
        readonly QueueServiceClient queueClient;
        readonly TableServiceClient tableClient;

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings)
        {
            this.Settings = settings;
            this.Stats = new AzureStorageOrchestrationServiceStats();

            var throttlingPolicy = new ThrottlingHttpPipelinePolicy(this.Settings.MaxStorageOperationConcurrency);
            var monitoringPolicy = new MonitoringHttpPipelinePolicy(this.Stats);

            this.blobClient = settings.BlobClientProvider.Create(o => AddPolicies(o, throttlingPolicy, monitoringPolicy));
            this.queueClient = settings.QueueClientProvider.Create(o => AddPolicies(o, throttlingPolicy, monitoringPolicy));
            this.tableClient = settings.TableClientProvider.Create(o => AddPolicies(o, throttlingPolicy, monitoringPolicy));
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

        static void AddPolicies(ClientOptions options, ThrottlingHttpPipelinePolicy throttlePolicy, MonitoringHttpPipelinePolicy monitoringPolicy)
        {
            options.AddPolicy(throttlePolicy, HttpPipelinePosition.PerCall);
            options.AddPolicy(monitoringPolicy, HttpPipelinePosition.PerRetry);
        }
    }
}
