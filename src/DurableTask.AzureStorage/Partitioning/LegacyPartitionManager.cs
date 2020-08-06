using DurableTask.AzureStorage.Monitoring;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Partitioning
{
    class LegacyPartitionManager : IPartitionManager
    {
        private readonly AzureStorageOrchestrationService service;
        private readonly AzureStorageOrchestrationServiceSettings settings;
        private readonly AzureStorageOrchestrationServiceStats stats;

        private readonly BlobLeaseManager leaseManager;
        private readonly LeaseCollectionBalancer<BlobLease> leaseCollectionManager;

        public LegacyPartitionManager(
            AzureStorageOrchestrationService service,
            AzureStorageOrchestrationServiceSettings settings,
            CloudStorageAccount account,
            AzureStorageOrchestrationServiceStats stats)
        {
            this.service = service;
            this.settings = settings;
            this.stats = stats;
            this.leaseManager = new BlobLeaseManager(
                settings,
                settings.TaskHubName.ToLowerInvariant() + "-leases",
                string.Empty,
                "default",
                account.CreateCloudBlobClient(),
                skipBlobContainerCreation: false,
                stats);

            this.leaseCollectionManager = new LeaseCollectionBalancer<BlobLease>(
                "default",
                settings,
                account.Credentials.AccountName,
                leaseManager,
                new LeaseCollectionBalancerOptions
                {
                    AcquireInterval = settings.LeaseAcquireInterval,
                    RenewInterval = settings.LeaseRenewInterval,
                    LeaseInterval = settings.LeaseInterval,
                });
        }

        Task IPartitionManager.CreateLease(string leaseName)
        {
            this.stats.StorageRequests.Increment();
            return this.leaseManager.CreateLeaseIfNotExistAsync(leaseName);
        }

        Task IPartitionManager.CreateLeaseStore()
        {    
            TaskHubInfo hubInfo = new TaskHubInfo(this.settings.TaskHubName, DateTime.UtcNow, this.settings.PartitionCount);
            this.stats.StorageRequests.Increment();
            return this.leaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo);
        }

        Task IPartitionManager.DeleteLeases()
        {
            return this.leaseManager.DeleteAllAsync().ContinueWith(t =>
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

        Task<IEnumerable<BlobLease>> IPartitionManager.GetOwnershipBlobLeases()
        {
            return this.leaseManager.ListLeasesAsync();
        }

        async Task IPartitionManager.StartAsync()
        {
            await this.leaseCollectionManager.InitializeAsync();
            await this.leaseCollectionManager.SubscribeAsync(
                this.service.OnOwnershipLeaseAquiredAsync,
                this.service.OnOwnershipLeaseReleasedAsync);
            await this.leaseCollectionManager.StartAsync();
        }

        Task IPartitionManager.StopAsync()
        {
            return this.leaseCollectionManager.StopAsync();
        }
    }
}
