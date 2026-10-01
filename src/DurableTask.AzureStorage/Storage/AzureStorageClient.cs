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
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Specialized;
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
        readonly MigrationSasManager? migration;
        readonly bool isMigrationActive;
        readonly object? initializationLock;
        Task? migrationInitialization;
        readonly QueueClientOptions? sasQueueOptions;
        readonly TableClientOptions? sasTableOptions;
        readonly BlobClientOptions? sasBlobOptions;
        readonly ConcurrentDictionary<string, QueueClient>? sasQueues;
        readonly ConcurrentDictionary<string, TableClient>? sasTables;
        readonly ConcurrentDictionary<string, BlobContainerClient>? sasContainers;

        public event Action? MigrationEnding;
        public bool IsMigrationActive => this.isMigrationActive;
        public bool IsMigrationEnding => this.isMigrationActive && this.migration!.IsMigrationEnding;

        public async Task InitializeMigrationAsync(bool refresh = false)
        {
            if (!this.IsMigrationActive)
            {
                return;
            }
            Task initialization;
            bool refreshCachedInitialization;
            lock (this.initializationLock!)
            {
                // New or in-progress initialization already refreshes credentials and starts the loop.
                refreshCachedInitialization = refresh && this.migrationInitialization?.Status == TaskStatus.RanToCompletion;
                // Run on the thread pool because synchronous SDK wrapper properties also call this method.
                initialization = this.migrationInitialization ??= Task.Run(() => this.InitializeMigrationCoreAsync(createGateIfMissing: true));
            }
            try
            {
                await initialization.ConfigureAwait(false);
            }
            catch
            {
                lock (this.initializationLock!)
                {
                    // Do not clear a newer initialization started by another caller.
                    if (ReferenceEquals(this.migrationInitialization, initialization))
                    {
                        this.migrationInitialization = null;
                    }
                }
                throw;
            }
            if (refreshCachedInitialization)
            {
                // A completed initialization may belong to a stopped service; refresh and restart its loop.
                await this.InitializeMigrationCoreAsync(createGateIfMissing: false);
            }
        }

        public Task StopMigrationTokenRefreshAsync()
        {
            return this.migration?.StopRefreshing() ?? Task.CompletedTask;
        }

        async Task InitializeMigrationCoreAsync(bool createGateIfMissing)
        {
            await this.migration!.RefreshAsync(CancellationToken.None, createGateIfMissing).ConfigureAwait(false);
            _ = this.migration!.StartRefreshLoop();
        }

        public void EnsureAccess()
        {
            if (this.isMigrationActive)
            {
                this.InitializeMigrationAsync().GetAwaiter().GetResult();
                this.migration!.EnsureAccess();
            }
        }

        public void ThrowIfMigrationResourceDeletion()
        {
            this.EnsureAccess();
            if (this.isMigrationActive)
            {
                throw new OrchestrationServiceUnavailableException("Task hub resource deletion is not supported during migration.");
            }
        }

        public QueueClient GetQueueClient(QueueClient original)
        {
            this.EnsureAccess();
            return !this.isMigrationActive ? original : this.sasQueues!.GetOrAdd(original.Name,
                name => new QueueClient(original.Uri, this.migration!.GetCredential('q', name), this.sasQueueOptions));
        }

        public TableClient GetTableClient(TableClient original)
        {
            this.EnsureAccess();
            return !this.isMigrationActive ? original : this.sasTables!.GetOrAdd(original.Name,
                name => new TableClient(original.Uri, this.migration!.GetCredential('t', name), this.sasTableOptions));
        }

        public BlobContainerClient GetBlobContainerClient(BlobContainerClient original)
        {
            this.EnsureAccess();
            return !this.isMigrationActive ? original : this.sasContainers!.GetOrAdd(original.Name,
                name => new BlobContainerClient(original.Uri, this.migration!.GetCredential('b', name), this.sasBlobOptions));
        }

        public BlockBlobClient GetBlockBlobClient(BlockBlobClient original)
        {
            this.EnsureAccess();
            return !this.isMigrationActive ? original : this.GetBlobContainerClient(
                this.blobClient.GetBlobContainerClient(original.BlobContainerName)).GetBlockBlobClient(original.Name);
        }

        public AzureStorageClient(AzureStorageOrchestrationServiceSettings settings, bool isMigrationActive = false)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (settings.StorageAccountClientProvider == null)
            {
                throw new ArgumentException("Storage account client provider is not specified.", nameof(settings));
            }

            this.isMigrationActive = isMigrationActive;
            this.Settings = settings;
            this.Stats = new AzureStorageOrchestrationServiceStats();

            var throttlingPolicy = new ThrottlingHttpPipelinePolicy(this.Settings.MaxStorageOperationConcurrency);
            var timeoutPolicy = new LeaseTimeoutHttpPipelinePolicy(this.Settings.LeaseRenewInterval);
            var monitoringPolicy = new MonitoringHttpPipelinePolicy(this.Stats);

            QueueClientOptions? queueManagementOptions = null;
            BlobClientOptions? blobManagementOptions = null;
            TableClientOptions? tableManagementOptions = null;
            this.queueClient = CreateClient(settings.StorageAccountClientProvider.Queue, options =>
            {
                queueManagementOptions = options;
                ConfigureQueueClientPolicies(options);
            });
            if (settings.HasTrackingStoreStorageAccount)
            {
                this.blobClient = CreateClient(settings.TrackingServiceClientProvider!.Blob, options => { blobManagementOptions = options; ConfigureClientPolicies(options); });
                this.tableClient = CreateClient(settings.TrackingServiceClientProvider!.Table, options => { tableManagementOptions = options; options.Diagnostics.IsLoggingContentEnabled = false; ConfigureClientPolicies(options); });
            }
            else
            {
                this.blobClient = CreateClient(settings.StorageAccountClientProvider.Blob, options => { blobManagementOptions = options; ConfigureClientPolicies(options); });
                this.tableClient = CreateClient(settings.StorageAccountClientProvider.Table, options => { tableManagementOptions = options; options.Diagnostics.IsLoggingContentEnabled = false; ConfigureClientPolicies(options); });
            }

            if (this.isMigrationActive)
            {
                this.initializationLock = new object();
                this.sasQueues = new ConcurrentDictionary<string, QueueClient>();
                this.sasTables = new ConcurrentDictionary<string, TableClient>();
                this.sasContainers = new ConcurrentDictionary<string, BlobContainerClient>();

                this.migration = new MigrationSasManager(settings, this.blobClient, this.queueClient, this.tableClient,
                    (settings.TrackingServiceClientProvider ?? settings.StorageAccountClientProvider).Table,
                    () => this.MigrationEnding?.Invoke());

                // Never add the migration policy to provider-owned options: a provider may reuse those options for
                // another service instance or a management client. SAS pipelines must own their policy lists.
                this.sasQueueOptions = CopyTransportOptions(queueManagementOptions!, new QueueClientOptions
                {
                    Audience = queueManagementOptions!.Audience,
                    GeoRedundantSecondaryUri = queueManagementOptions.GeoRedundantSecondaryUri,
                });
                ConfigureQueueClientPolicies(this.sasQueueOptions);
                this.sasQueueOptions.AddPolicy(new MigrationRequestPolicy(this.migration, useDelegationVersion: true), HttpPipelinePosition.PerRetry);
                this.sasTableOptions = CopyTransportOptions(tableManagementOptions!, new TableClientOptions { Audience = tableManagementOptions!.Audience });
                ConfigureClientPolicies(this.sasTableOptions);
                this.sasTableOptions.AddPolicy(new MigrationRequestPolicy(this.migration, useDelegationVersion: true), HttpPipelinePosition.PerRetry);
                this.sasBlobOptions = CopyTransportOptions(blobManagementOptions!, new BlobClientOptions
                {
                    Audience = blobManagementOptions!.Audience,
                    GeoRedundantSecondaryUri = blobManagementOptions.GeoRedundantSecondaryUri,
                });
                ConfigureClientPolicies(this.sasBlobOptions);
                this.sasBlobOptions.AddPolicy(new MigrationRequestPolicy(this.migration), HttpPipelinePosition.PerRetry);
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
            return new Blob(this, this.blobClient, container, blobName);
        }

        internal Blob GetBlobReference(Uri blobUri)
        {
            return new Blob(this, this.blobClient, blobUri);
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

        static TOptions CopyTransportOptions<TOptions>(ClientOptions source, TOptions destination) where TOptions : ClientOptions
        {
            destination.Transport = source.Transport;
            destination.Retry.Mode = source.Retry.Mode;
            destination.Retry.Delay = source.Retry.Delay;
            destination.Retry.MaxDelay = source.Retry.MaxDelay;
            // These options are used only by migration SAS clients. Do not let SDK retries start another storage request.
            destination.Retry.MaxRetries = 0;
            destination.Retry.NetworkTimeout = source.Retry.NetworkTimeout;
            destination.Diagnostics.ApplicationId = source.Diagnostics.ApplicationId;
            destination.Diagnostics.IsLoggingEnabled = source.Diagnostics.IsLoggingEnabled;
            destination.Diagnostics.IsLoggingContentEnabled = source.Diagnostics.IsLoggingContentEnabled;
            destination.Diagnostics.IsDistributedTracingEnabled = false;
            return destination;
        }
    }
}
