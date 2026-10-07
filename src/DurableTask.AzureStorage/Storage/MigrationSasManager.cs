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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Data.Tables.Sas;
    using Azure.Storage.Blobs;
    using Azure.Storage.Sas;
    using Azure.Storage.Queues;
    using BlobKey = Azure.Storage.Blobs.Models.UserDelegationKey;
    using QueueKey = Azure.Storage.Queues.Models.UserDelegationKey;

    sealed class MigrationSasManager
    {
        internal static readonly TimeSpan SasLifetime = TimeSpan.FromSeconds(15);
        static readonly TimeSpan RenewBefore = TimeSpan.FromSeconds(11);
        readonly AzureStorageMigration migration;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly BlobServiceClient blobService;
        readonly QueueServiceClient queueService;
        readonly TableServiceClient tableService;
        readonly IStorageServiceClientProvider<TableServiceClient, TableClientOptions> tableProvider;
        readonly Action onMigrationEnding;
        readonly object sync = new();
        readonly SemaphoreSlim refreshLock = new(1, 1);
        readonly Dictionary<string, AzureSasCredential> credentials = new();
        BlobKey? blobKey;
        QueueKey? queueKey;
        TableUserDelegationKey? tableKey;
        DateTimeOffset expiresAt;
        DateTimeOffset sampledServerTime;
        long sampledAt;
        volatile bool isMigrationEnding;
        CancellationTokenSource? renewalSource;
        Task renewalTask = Task.CompletedTask;

        public MigrationSasManager(AzureStorageOrchestrationServiceSettings settings, BlobServiceClient blobService,
            QueueServiceClient queueService, TableServiceClient tableService,
            IStorageServiceClientProvider<TableServiceClient, TableClientOptions> tableProvider, Action onMigrationEnding)
        {
            this.settings = settings;
            this.blobService = blobService;
            this.queueService = queueService;
            this.tableService = tableService;
            this.tableProvider = tableProvider;
            this.onMigrationEnding = onMigrationEnding;
            this.migration = new AzureStorageMigration(tableService, settings.TaskHubName);
        }

        public bool IsMigrationEnding => this.isMigrationEnding;

        public async Task RefreshAsync(CancellationToken cancellationToken, bool createGateIfMissing = false)
        {
            // Serialize explicit refreshes and the renewal loop
            await this.refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (this.isMigrationEnding)
                {
                    return;
                }
                await this.RefreshCoreAsync(cancellationToken, createGateIfMissing).ConfigureAwait(false);
            }
            finally
            {
                this.refreshLock.Release();
            }
        }

        public Task StartRefreshLoop()
        {
            lock (this.sync)
            {
                if (!this.isMigrationEnding && (this.renewalSource == null || this.renewalTask.IsCompleted))
                {
                    this.renewalSource?.Dispose();
                    var source = new CancellationTokenSource();
                    this.renewalSource = source;
                    this.renewalTask = Task.Run(() => this.RunAsync(source.Token));
                }
                return this.renewalTask;
            }
        }

        public Task StopRefreshing()
        {
            lock (this.sync)
            {
                this.renewalSource?.Cancel();
                return this.renewalTask;
            }
        }

        async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && !this.isMigrationEnding)
            {
                try
                {
                    // Per-host jitter avoids synchronizing readers
                    await Task.Delay(TimeSpan.FromMilliseconds(1000 + (uint)this.settings.WorkerId.GetHashCode() % 1000), cancellationToken).ConfigureAwait(false);
                    await this.RefreshAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { break; }
                catch (Exception e)
                {
                    // Keep the last credentials, never extend them locally or use unrestricted authentication.
                    this.settings.Logger.GeneralWarning(this.queueService.AccountName, this.settings.TaskHubName,
                        $"Migration SAS renewal failed ({e.GetType().Name}). Source access will stop when its published credentials expire.");
                }
            }
        }

        async Task RefreshCoreAsync(CancellationToken cancellationToken, bool createGateIfMissing)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                AzureStorageMigration.Gate? gate = await this.migration.ReadAsync(cancellationToken).ConfigureAwait(false);
                long sampledAt = Stopwatch.GetTimestamp();
                if (gate == null)
                {
                    if (!createGateIfMissing)
                    {
                        throw new InvalidOperationException("Migration gate disappeared. Source access cannot be restored.");
                    }
                    // Only initial startup may create the gate. Read it again because another host
                    // may have created or ended migration in the meantime. Renewals must never recreate it.
                    await this.migration.CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
                    createGateIfMissing = false;
                    continue;
                }

                createGateIfMissing = false;

                // If the gate indicates migration is ending, exit
                if (gate.IsMigrationEnding) 
                {
                    this.SetMigrationEnding();
                    return; 
                }

                if (await this.RefreshKeysAsync(gate.ServerTime, cancellationToken).ConfigureAwait(false))
                {
                    // Key acquisition may be slow, so re-read the gate
                    continue;
                }

                // If the gate is about to expire, extend it
                if (gate.AccessExpiresAt <= gate.ServerTime + RenewBefore)
                {
                    gate.AccessExpiresAt = gate.ServerTime + SasLifetime;
                    // Another host must have updated the gate in the meantime. Re-read and try again.
                    if (!await this.migration.TryUpdateAsync(gate, cancellationToken).ConfigureAwait(false))
                    {
                        continue;
                    }
                }

                // Generate new SAS tokens with the new expiry time
                lock (this.sync)
                {
                    this.expiresAt = gate.AccessExpiresAt;
                    this.sampledServerTime = gate.ServerTime;
                    this.sampledAt = sampledAt;
                    foreach (KeyValuePair<string, AzureSasCredential> entry in this.credentials)
                    {
                        entry.Value.Update(this.Sign(entry.Key[0], entry.Key.Substring(2)));
                    }
                }
                // Some slowdown in the renewal path consumed the whole expiry interval. Re-read before returning
                if (this.EstimatedServerTime >= gate.AccessExpiresAt)
                {
                    continue;
                }
                return;
            }
        }

        async Task<bool> RefreshKeysAsync(DateTimeOffset serverTime, CancellationToken cancellationToken)
        {
            // Delegation keys are cached for a day; short-lived SAS renewal normally needs only the gate.
            bool refreshed = false;
            TokenCredential? credential = (this.tableProvider as IStorageTokenCredentialProvider)?.TokenCredential;
            if (credential != null && (this.tableKey == null || this.tableKey.ExpiresOn <= serverTime.AddMinutes(5)))
            {
                TableUserDelegationKey key = await TableUserDelegationKey.GetAsync(this.tableService.Uri, credential,
                    this.tableProvider.CreateOptions(), serverTime, cancellationToken).ConfigureAwait(false);
                lock (this.sync)
                {
                    this.tableKey = key;
                }
                refreshed = true;
            }
            if (!this.blobService.CanGenerateAccountSasUri && (this.blobKey == null || this.blobKey.SignedExpiresOn <= serverTime.AddMinutes(5)))
            {
                BlobKey key = await this.blobService.GetUserDelegationKeyAsync(
                    serverTime.AddMinutes(-15), serverTime.AddHours(24), cancellationToken).ConfigureAwait(false);
                lock (this.sync)
                {
                    this.blobKey = key;
                }
                refreshed = true;
            }
            if (!this.queueService.CanGenerateAccountSasUri && (this.queueKey == null || this.queueKey.SignedExpiresOn <= serverTime.AddMinutes(5)))
            {
                QueueKey key = await this.queueService.GetUserDelegationKeyAsync(
                    serverTime.AddMinutes(-15), serverTime.AddHours(24), cancellationToken).ConfigureAwait(false);
                lock (this.sync)
                {
                    this.queueKey = key;
                }
                refreshed = true;
            }
            return refreshed;
        }

        DateTimeOffset EstimatedServerTime => this.sampledServerTime + TimeSpan.FromSeconds((Stopwatch.GetTimestamp() - this.sampledAt) / (double)Stopwatch.Frequency);

        public void EnsureAccess()
        {
            lock (this.sync)
            {
                if (this.isMigrationEnding)
                {
                    throw new OrchestrationServiceUnavailableException("The source migration credentials have expired or migration is ending.");
                }
            }
        }

        public AzureSasCredential GetCredential(char service, string resource)
        {
            lock (this.sync)
            {
                this.EnsureAccess();
                string key = service + ":" + resource;
                if (!this.credentials.TryGetValue(key, out AzureSasCredential? credential))
                {
                    credential = new AzureSasCredential(this.Sign(service, resource));
                    this.credentials.Add(key, credential);
                }
                return credential;
            }
        }

        string Sign(char service, string resource)
        {
            switch (service)
            {
                case 'b':
                    var blob = new BlobSasBuilder { BlobContainerName = resource, Resource = "c", ExpiresOn = this.expiresAt };
                    blob.SetPermissions(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.Write | BlobContainerSasPermissions.Create | BlobContainerSasPermissions.Delete | BlobContainerSasPermissions.List);
                    if (this.blobService.CanGenerateAccountSasUri)
                        return this.blobService.GetBlobContainerClient(resource).GenerateSasUri(blob).Query.TrimStart('?');
                    blob.Protocol = SasProtocol.Https;
                    blob.StartsOn = this.blobKey!.SignedStartsOn;
                    return blob.ToSasQueryParameters(this.blobKey, this.blobService.AccountName).ToString();
                case 'q':
                    var queue = new QueueSasBuilder { QueueName = resource, ExpiresOn = this.expiresAt };
                    queue.SetPermissions(QueueSasPermissions.Read | QueueSasPermissions.Add | QueueSasPermissions.Update | QueueSasPermissions.Process);
                    if (this.queueService.CanGenerateAccountSasUri)
                        return this.queueService.GetQueueClient(resource).GenerateSasUri(queue).Query.TrimStart('?');
                    queue.Protocol = SasProtocol.Https;
                    queue.StartsOn = this.queueKey!.SignedStartsOn;
                    return queue.ToSasQueryParameters(this.queueKey, this.queueService.AccountName).ToString();
                case 't':
                    if (this.tableKey != null) return this.tableKey.Sign(this.tableService.AccountName, resource, this.expiresAt);
                    // Account-key connections also use bounded SAS. Queries and batches require container scope;
                    // individual entity operations require object scope. Resource deletion is blocked by our wrappers.
                    return this.tableService.GenerateSasUri(TableAccountSasPermissions.Read | TableAccountSasPermissions.Add
                        | TableAccountSasPermissions.Update | TableAccountSasPermissions.Delete,
                        TableAccountSasResourceTypes.Container | TableAccountSasResourceTypes.Object, this.expiresAt).Query.TrimStart('?');
                default: throw new ArgumentOutOfRangeException(nameof(service));
            }
        }

        void SetMigrationEnding()
        {
            this.isMigrationEnding = true;
            this.onMigrationEnding();
        }
    }
}
