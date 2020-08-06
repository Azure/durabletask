using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Partitioning
{
    internal interface IPartitionManager
    {
        Task StartAsync();

        Task StopAsync();

        Task CreateLeaseStore();

        Task CreateLease(string leaseName);

        Task DeleteLeases();

        Task<IEnumerable<BlobLease>> GetOwnershipBlobLeases();
    }
}
