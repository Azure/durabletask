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

namespace DurableTask.AzureStorage.Partitioning
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using DurableTask.AzureStorage.Storage;

    class SafePartitionManager : IPartitionManager
    {
        readonly AzureStorageOrchestrationService service;
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly OrchestrationSessionManager sessionManager;

        readonly BlobPartitionLeaseManager intentLeaseManager;
        readonly LeaseCollectionBalancer<BlobPartitionLease> intentLeaseCollectionManager;

        readonly BlobPartitionLeaseManager ownershipLeaseManager;
        readonly LeaseCollectionBalancer<BlobPartitionLease> ownershipLeaseCollectionManager;

        IDisposable intentLeaseSubscription;
        IDisposable ownershipLeaseSubscription;

        public SafePartitionManager(
            AzureStorageOrchestrationService service,
            AzureStorageClient azureStorageClient,
            OrchestrationSessionManager sessionManager)
        {
            this.service = service;
            this.azureStorageClient = azureStorageClient;
            this.settings = this.azureStorageClient.Settings;
            this.sessionManager = sessionManager;

            this.intentLeaseManager = AzureStorageOrchestrationService.GetBlobLeaseManager(
                this.azureStorageClient,
                "intent");

            this.intentLeaseCollectionManager = new LeaseCollectionBalancer<BlobPartitionLease>(
                "intent",
                settings,
                this.azureStorageClient.BlobAccountName,
                this.intentLeaseManager,
                new LeaseCollectionBalancerOptions
                {
                    AcquireInterval = settings.LeaseAcquireInterval,
                    RenewInterval = settings.LeaseRenewInterval,
                    LeaseInterval = settings.LeaseInterval,
                });

            var currentlyOwnedIntentLeases = this.intentLeaseCollectionManager.GetCurrentlyOwnedLeases();

            this.ownershipLeaseManager = AzureStorageOrchestrationService.GetBlobLeaseManager(
                this.azureStorageClient,
                "ownership");

            this.ownershipLeaseCollectionManager = new LeaseCollectionBalancer<BlobPartitionLease>(
                "ownership",
                this.settings,
                this.azureStorageClient.BlobAccountName,
                this.ownershipLeaseManager,
                new LeaseCollectionBalancerOptions
                {
                    AcquireInterval = TimeSpan.FromSeconds(5),
                    RenewInterval = TimeSpan.FromSeconds(10),
                    LeaseInterval = TimeSpan.FromSeconds(15),
                    ShouldStealLeases = false
                },
                shouldAquireLeaseDelegate: leaseKey => currentlyOwnedIntentLeases.ContainsKey(leaseKey),
                shouldRenewLeaseDelegate: leaseKey => currentlyOwnedIntentLeases.ContainsKey(leaseKey)
                                                      || this.sessionManager.IsControlQueueReceivingMessages(leaseKey)
                                                      || this.sessionManager.IsControlQueueProcessingMessages(leaseKey));
        }

        IAsyncEnumerable<BlobPartitionLease> IPartitionManager.GetOwnershipBlobLeasesAsync(CancellationToken cancellationToken)
        {
            return this.ownershipLeaseManager.ListLeasesAsync(cancellationToken);
        }

        Task IPartitionManager.CreateLeaseStore()
        {
            TaskHubInfo hubInfo = new TaskHubInfo(this.settings.TaskHubName, DateTime.UtcNow, this.settings.PartitionCount);
            return Task.WhenAll(
                // Only need to check if the lease store (i.e. the taskhub.json) is stale for one of the two
                // lease managers.
                this.intentLeaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo, checkIfStale: true),
                this.ownershipLeaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo, checkIfStale: false));
        }

        Task IPartitionManager.DeleteLeases()
        {
            return Task.WhenAll(
                this.intentLeaseManager.DeleteAllAsync(),
                this.ownershipLeaseManager.DeleteAllAsync()
                ).ContinueWith(t =>
            {
                if (t.Exception?.InnerExceptions?.Count > 0)
                {
                    foreach (Exception e in t.Exception.InnerExceptions)
                    {
                        RequestFailedException storageException = e as RequestFailedException;
                        if (storageException == null || storageException.Status != 404)
                        {
                            ExceptionDispatchInfo.Capture(e).Throw();
                        }
                    }
                }
            });
        }

        async Task IPartitionManager.StartAsync()
        {
            await this.intentLeaseCollectionManager.InitializeAsync();
            this.intentLeaseSubscription = await this.intentLeaseCollectionManager.SubscribeAsync(
                this.service.OnIntentLeaseAquiredAsync,
                this.service.OnIntentLeaseReleasedAsync);
            await this.intentLeaseCollectionManager.StartAsync();

            await this.ownershipLeaseCollectionManager.InitializeAsync();
            this.ownershipLeaseSubscription = await this.ownershipLeaseCollectionManager.SubscribeAsync(
                this.service.OnOwnershipLeaseAquiredAsync,
                this.service.OnOwnershipLeaseReleasedAsync);
            await this.ownershipLeaseCollectionManager.StartAsync();
        }

        async Task IPartitionManager.StopAsync()
        {
            await this.intentLeaseCollectionManager.StopAsync();
            this.intentLeaseSubscription?.Dispose();

            await this.ownershipLeaseCollectionManager.StopAsync();
            this.ownershipLeaseSubscription?.Dispose();
        }

        Task IPartitionManager.CreateLease(string leaseName)
        {
            return Task.WhenAll(
                this.intentLeaseManager.CreateLeaseIfNotExistAsync(leaseName),
                this.ownershipLeaseManager.CreateLeaseIfNotExistAsync(leaseName)
            );
        }
    }
}
