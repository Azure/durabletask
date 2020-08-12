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
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using System;
    using System.Collections.Generic;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;

    class SafePartitionManager : IPartitionManager
    {
        private readonly AzureStorageOrchestrationService service;
        private readonly AzureStorageOrchestrationServiceSettings settings;
        private readonly AzureStorageOrchestrationServiceStats stats;
        private readonly OrchestrationSessionManager sessionManager;

        private readonly BlobLeaseManager intentLeaseManager;
        private readonly LeaseCollectionBalancer<BlobLease> intentLeaseCollectionManager;

        private readonly BlobLeaseManager ownershipLeaseManager;
        private readonly LeaseCollectionBalancer<BlobLease> ownershipLeaseCollectionManager;

        public SafePartitionManager(
            AzureStorageOrchestrationService service,
            OrchestrationSessionManager sessionManager,
            AzureStorageOrchestrationServiceSettings settings,
            CloudStorageAccount account,
            AzureStorageOrchestrationServiceStats stats)
        {
            this.service = service;
            this.settings = settings;
            this.stats = stats;
            this.sessionManager = sessionManager;

            string storageAccountName = account.Credentials.AccountName;
            this.intentLeaseManager = new BlobLeaseManager(
                settings,
                settings.TaskHubName.ToLowerInvariant() + "-leases",
                string.Empty,
                "intent",
                account.CreateCloudBlobClient(),
                skipBlobContainerCreation: false,
                stats);

            this.intentLeaseCollectionManager = new LeaseCollectionBalancer<BlobLease>(
                "intent",
                settings,
                storageAccountName,
                this.intentLeaseManager,
                new LeaseCollectionBalancerOptions
                {
                    AcquireInterval = settings.LeaseAcquireInterval,
                    RenewInterval = settings.LeaseRenewInterval,
                    LeaseInterval = settings.LeaseInterval,
                });

            var currentlyOwnedIntentLeases = this.intentLeaseCollectionManager.GetCurrentlyOwnedLeases();

            this.ownershipLeaseManager = new BlobLeaseManager(
                settings,
                settings.TaskHubName.ToLowerInvariant() + "-leases",
                string.Empty,
                "ownership",
                account.CreateCloudBlobClient(),
                skipBlobContainerCreation: false,
                stats);

            this.ownershipLeaseCollectionManager = new LeaseCollectionBalancer<BlobLease>(
                "ownership",
                this.settings,
                storageAccountName,
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

        Task<IEnumerable<BlobLease>> IPartitionManager.GetOwnershipBlobLeases()
        {
            return this.ownershipLeaseManager.ListLeasesAsync();
        }

        Task IPartitionManager.CreateLeaseStore()
        {
            TaskHubInfo hubInfo = new TaskHubInfo(this.settings.TaskHubName, DateTime.UtcNow, this.settings.PartitionCount);
            return Task.WhenAll(
                this.intentLeaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo),
                this.ownershipLeaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo));
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
                        StorageException storageException = e as StorageException;
                        if (storageException == null || storageException.RequestInformation.HttpStatusCode != 404)
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
            await this.intentLeaseCollectionManager.SubscribeAsync(
                    this.service.OnIntentLeaseAquiredAsync,
                    this.service.OnIntentLeaseReleasedAsync);
            await this.intentLeaseCollectionManager.StartAsync();

            await this.ownershipLeaseCollectionManager.InitializeAsync();
            await this.ownershipLeaseCollectionManager.SubscribeAsync(
                    this.service.OnOwnershipLeaseAquiredAsync,
                    this.service.OnOwnershipLeaseReleasedAsync);
            await this.ownershipLeaseCollectionManager.StartAsync();
        }

        Task IPartitionManager.StopAsync()
        {
            return Task.WhenAll(
                this.intentLeaseCollectionManager.StopAsync(),
                this.ownershipLeaseCollectionManager.StopAsync()
            );
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
