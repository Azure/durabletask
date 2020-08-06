using DurableTask.AzureStorage.Monitoring;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Partitioning
{
    class SafePartitionManager : IPartitionManager
    {
        private readonly AzureStorageOrchestrationService service;
        private readonly AzureStorageOrchestrationServiceSettings settings;
        private readonly AzureStorageOrchestrationServiceStats stats;
        private readonly OrchestrationSessionManager sessionManager;

        private readonly BlobLeaseManager intentLeaseManager;
        private readonly LeaseCollectionManager<BlobLease> intentLeaseCollectionManager;

        private readonly BlobLeaseManager ownershipLeaseManager;
        private readonly LeaseCollectionManager<BlobLease> ownershipLeaseCollectionManager;

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

            this.intentLeaseCollectionManager = new LeaseCollectionManager<BlobLease>(
                "intent",
                settings,
                storageAccountName,
                this.intentLeaseManager,
                new LeaseCollectionManagerOptions
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

            this.ownershipLeaseCollectionManager = new LeaseCollectionManager<BlobLease>(
                "ownership",
                this.settings,
                storageAccountName,
                this.ownershipLeaseManager,
                new LeaseCollectionManagerOptions
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
            await Task.WhenAll(
                this.intentLeaseCollectionManager.InitializeAsync(),
                this.ownershipLeaseCollectionManager.InitializeAsync());

            await Task.WhenAll(
                this.intentLeaseCollectionManager.SubscribeAsync(
                    this.service.OnIntentLeaseAquiredAsync,
                    this.service.OnIntentLeaseReleasedAsync),
                this.ownershipLeaseCollectionManager.SubscribeAsync(
                    this.service.OnOwnershipLeaseAquiredAsync,
                    this.service.OnOwnershipLeaseReleasedAsync)
            );


            await Task.WhenAll(
                this.intentLeaseCollectionManager.StartAsync(),
                this.ownershipLeaseCollectionManager.StartAsync()
                );
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
