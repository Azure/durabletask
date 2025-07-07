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
    using System.Text;
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

            this.queueClient = CreateClient(settings.StorageAccountClientProvider.Queue, ConfigureQueueClientPolicies);
            if (settings.HasTrackingStoreStorageAccount)
            {
                this.blobClient = CreateClient(settings.TrackingServiceClientProvider!.Blob, ConfigureClientPolicies);
                this.tableClient = CreateClient(settings.TrackingServiceClientProvider!.Table, ConfigureClientPolicies);
            }
            else
            {
                this.blobClient = CreateClient(settings.StorageAccountClientProvider.Blob, ConfigureClientPolicies);
                this.tableClient = CreateClient(settings.StorageAccountClientProvider.Table, ConfigureClientPolicies);
            }

            void ConfigureClientPolicies<TClientOptions>(TClientOptions options) where TClientOptions : ClientOptions
            {
                options.AddPolicy(throttlingPolicy!, HttpPipelinePosition.PerCall);
                options.AddPolicy(timeoutPolicy!, HttpPipelinePosition.PerCall);
                options.AddPolicy(monitoringPolicy!, HttpPipelinePosition.PerRetry);
            }

            void ConfigureQueueClientPolicies(QueueClientOptions options)
            {
                // Configure message encoding based on settings
                options.MessageEncoding = this.Settings.QueueClientMessageEncoding switch
                {
                    QueueClientMessageEncoding.UTF8 => QueueMessageEncoding.None,
                    QueueClientMessageEncoding.Base64 => QueueMessageEncoding.Base64,
                    _ => throw new ArgumentException($"Unsupported encoding strategy: {this.Settings.QueueClientMessageEncoding}")
                };

                // Base64-encoded clients will fail to decode messages sent in UTF-8 format.
                // This handler catches decoding failures and update the message with its the original content,
                // so that the client can successfully process it on the next attempt.
                if (this.Settings.QueueClientMessageEncoding == QueueClientMessageEncoding.Base64)
                {
                    options.MessageDecodingFailed += async (QueueMessageDecodingFailedEventArgs args) =>
                    {
                        this.Settings.Logger.GeneralWarning(
                            this.QueueAccountName,
                            this.Settings.TaskHubName,
                            $"Base64-encoded queue client failed to decode message with ID: {args.ReceivedMessage.MessageId}. " +
                            "The message appears to have been originally sent using UTF-8 encoding. " +
                            "Will attempt to re-encode the message content as Base64 and update the queue message in-place " +
                            "so it can be successfully processed on the next attempt."
                        );
                        
                        if (args.ReceivedMessage != null)
                        {
                            var queueMessage = args.ReceivedMessage;

                            try
                            {
                                // Get the raw message content and update the message.
                                string originalJson = Encoding.UTF8.GetString(queueMessage.Body.ToArray());

                                // Update the message in the queue with the Base64-encoded body
                                if (args.IsRunningSynchronously)
                                {
                                    args.Queue.UpdateMessage(
                                        queueMessage.MessageId,
                                        queueMessage.PopReceipt,
                                        originalJson,
                                        TimeSpan.FromSeconds(0));
                                }
                                else
                                {
                                    await args.Queue.UpdateMessageAsync(
                                        queueMessage.MessageId,
                                        queueMessage.PopReceipt,
                                        originalJson,
                                        TimeSpan.FromSeconds(0));
                                }
                            }
                            catch (Exception ex)
                            {
                                // If re-encoding or update fails, rethrow the error to be handled upstream
                                throw new InvalidOperationException(
                                    $"Failed to re-encode and update UTF-8 message as Base64. MessageId: {queueMessage.MessageId}", ex);
                            }
                        }
                    };
                }

                    ConfigureClientPolicies(options);
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

            // Disable distributed tracing by default to reduce the noise in the traces.
            options.Diagnostics.IsDistributedTracingEnabled = false;

            configurePolicies?.Invoke(options);

            return storageProvider.CreateClient(options);
        }
    }
}
