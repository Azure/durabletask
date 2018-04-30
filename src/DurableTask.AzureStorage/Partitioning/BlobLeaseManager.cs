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
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;
    using Newtonsoft.Json;

    sealed class BlobLeaseManager : ILeaseManager<BlobLease>
    {
        const string TaskHubInfoBlobName = "taskhub.json";
        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);

        readonly string storageAccountName;
        readonly string taskHubName;
        readonly string workerName;
        readonly string blobPrefix;
        readonly string leaseContainerName;
        readonly string consumerGroupName;
        readonly bool skipBlobContainerCreation;
        readonly TimeSpan leaseInterval;
        readonly TimeSpan renewInterval;
        readonly CloudBlobClient storageClient;
        readonly BlobRequestOptions renewRequestOptions;
        readonly AzureStorageOrchestrationServiceStats stats;

        CloudBlobContainer taskHubContainer;
        CloudBlobDirectory consumerGroupDirectory;
        CloudBlockBlob taskHubInfoBlob;

        public BlobLeaseManager(
            string taskHubName,
            string workerName,
            string leaseContainerName,
            string blobPrefix,
            string consumerGroupName,
            CloudBlobClient storageClient,
            TimeSpan leaseInterval,
            TimeSpan renewInterval,
            bool skipBlobContainerCreation,
            AzureStorageOrchestrationServiceStats stats)
        {
            this.storageAccountName = storageClient.Credentials.AccountName;
            this.taskHubName = taskHubName;
            this.workerName = workerName;
            this.leaseContainerName = leaseContainerName;
            this.blobPrefix = blobPrefix;
            this.consumerGroupName = consumerGroupName;
            this.storageClient = storageClient;
            this.leaseInterval = leaseInterval;
            this.renewInterval = renewInterval;
            this.skipBlobContainerCreation = skipBlobContainerCreation;
            this.renewRequestOptions = new BlobRequestOptions { ServerTimeout = renewInterval };
            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

            this.Initialize();
        }

        public async Task<bool> LeaseStoreExistsAsync()
        {
            try
            {
                return await this.taskHubContainer.ExistsAsync();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo)
        {
            bool result = false;
            if (!this.skipBlobContainerCreation)
            {
                result = await this.taskHubContainer.CreateIfNotExistsAsync();
                this.stats.StorageRequests.Increment();
            }

            await this.CreateTaskHubInfoIfNotExistAsync(eventHubInfo);

            return result;
        }

        public async Task<IEnumerable<BlobLease>> ListLeasesAsync()
        {
            var blobLeases = new List<BlobLease>();

            BlobContinuationToken continuationToken = null;
            do
            {
                BlobResultSegment segment = await this.consumerGroupDirectory.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = segment.ContinuationToken;

                var downloadTasks = new List<Task<BlobLease>>();
                foreach (IListBlobItem blob in segment.Results)
                {
                    CloudBlockBlob lease = blob as CloudBlockBlob;
                    if (lease != null)
                    {
                        downloadTasks.Add(this.DownloadLeaseBlob(lease));
                    }
                }

                await Task.WhenAll(downloadTasks);

                blobLeases.AddRange(downloadTasks.Select(t => t.Result));
            }
            while (continuationToken != null);

            return blobLeases;
        }

        public async Task CreateLeaseIfNotExistAsync(string paritionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(paritionId);
            BlobLease lease = new BlobLease(leaseBlob) { PartitionId = paritionId };
            string serializedLease = JsonConvert.SerializeObject(lease);
            try
            {
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.storageAccountName,
                    this.taskHubName,
                    this.workerName,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, consumerGroupName: {1}, partitionId: {2}. blobPrefix: {3}",
                        this.leaseContainerName,
                        this.consumerGroupName,
                        paritionId,
                        this.blobPrefix ?? string.Empty));

                await leaseBlob.UploadTextAsync(serializedLease, null, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null);
            }
            catch (StorageException se)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.storageAccountName,
                    this.taskHubName,
                    this.workerName,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, consumerGroupName: {1}, partitionId: {2}, blobPrefix: {3}, exception: {4}.",
                        this.leaseContainerName,
                        this.consumerGroupName,
                        paritionId,
                        this.blobPrefix ?? string.Empty,
                        se));
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task<BlobLease> GetLeaseAsync(string paritionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(paritionId);
            if (await leaseBlob.ExistsAsync())
            {
                return await this.DownloadLeaseBlob(leaseBlob);
            }

            return null;
        }

        public async Task<bool> RenewAsync(BlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            try
            {
                await leaseBlob.RenewLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token), options: this.renewRequestOptions, operationContext: null);
            }
            catch (StorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            return true;
        }

        public async Task<bool> AcquireAsync(BlobLease lease, string owner)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            try
            {
                await leaseBlob.FetchAttributesAsync();
                string newLeaseId = Guid.NewGuid().ToString("N");
                if (leaseBlob.Properties.LeaseState == LeaseState.Leased)
                {
                    lease.Token = await leaseBlob.ChangeLeaseAsync(newLeaseId, accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token));
                }
                else
                {
                    lease.Token = await leaseBlob.AcquireLeaseAsync(this.leaseInterval, newLeaseId);
                }

                this.stats.StorageRequests.Increment();
                lease.Owner = owner;
                // Increment Epoch each time lease is acquired or stolen by new host
                lease.Epoch += 1;
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
                this.stats.StorageRequests.Increment();
            }
            catch (StorageException storageException)
            {
                this.stats.StorageRequests.Increment();
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task<bool> ReleaseAsync(BlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            try
            {
                string leaseId = lease.Token;

                BlobLease copy = new BlobLease(lease);
                copy.Token = null;
                copy.Owner = null;
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(copy), null, AccessCondition.GenerateLeaseCondition(leaseId), null, null);
                this.stats.StorageRequests.Increment();
                await leaseBlob.ReleaseLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId));
                this.stats.StorageRequests.Increment();
            }
            catch (StorageException storageException)
            {
                this.stats.StorageRequests.Increment();
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task DeleteAsync(BlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            try
            {
                await leaseBlob.DeleteIfExistsAsync();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task DeleteAllAsync()
        {
            try
            {
                await this.taskHubContainer.DeleteIfExistsAsync();
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        public async Task<bool> UpdateAsync(BlobLease lease)
        {
            if (lease == null || string.IsNullOrWhiteSpace(lease.Token))
            {
                return false;
            }

            CloudBlockBlob leaseBlob = lease.Blob;
            try
            {
                // First renew the lease to make sure checkpoint will go through
                await leaseBlob.RenewLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(lease.Token));
            }
            catch (StorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            try
            {
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
            }
            catch (StorageException storageException)
            {
                throw HandleStorageException(lease, storageException, true);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            return true;
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
        {
            string serializedInfo = JsonConvert.SerializeObject(taskHubInfo);
            try
            {
                await this.taskHubInfoBlob.UploadTextAsync(serializedInfo, null, AccessCondition.GenerateIfNoneMatchCondition("*"), null, null);
            }
            catch (StorageException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }

        internal async Task<TaskHubInfo> GetOrCreateTaskHubInfoAsync(TaskHubInfo createdTaskHubInfo)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
            if (currentTaskHubInfo != null)
            {
                return currentTaskHubInfo;
            }

            await this.CreateTaskHubInfoIfNotExistAsync(createdTaskHubInfo);
            return createdTaskHubInfo;
        }

        internal async Task<bool> IsStaleLeaseStore(TaskHubInfo taskHubInfo)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
            if (currentTaskHubInfo != null)
            {
                if (!currentTaskHubInfo.TaskHubName.Equals(taskHubInfo.TaskHubName, StringComparison.OrdinalIgnoreCase)
                    || !currentTaskHubInfo.CreatedAt.Equals(taskHubInfo.CreatedAt)
                    || !currentTaskHubInfo.PartitionCount.Equals(taskHubInfo.PartitionCount))
                {
                    return true;
                }
            }

            return false;
        }

        void Initialize()
        {
            this.storageClient.DefaultRequestOptions.MaximumExecutionTime = StorageMaximumExecutionTime;

            this.taskHubContainer = this.storageClient.GetContainerReference(this.leaseContainerName);

            string consumerGroupDirectoryName = string.IsNullOrWhiteSpace(this.blobPrefix)
                ? this.consumerGroupName
                : this.blobPrefix + this.consumerGroupName;
            this.consumerGroupDirectory = this.taskHubContainer.GetDirectoryReference(consumerGroupDirectoryName);

            string taskHubInfoBlobFileName = string.IsNullOrWhiteSpace(this.blobPrefix)
                ? TaskHubInfoBlobName
                : this.blobPrefix + TaskHubInfoBlobName;

            this.taskHubInfoBlob = this.taskHubContainer.GetBlockBlobReference(taskHubInfoBlobFileName);
        }

        async Task<TaskHubInfo> GetTaskHubInfoAsync()
        {
            if (await this.taskHubInfoBlob.ExistsAsync())
            {
                await taskHubInfoBlob.FetchAttributesAsync();
                this.stats.StorageRequests.Increment();
                string serializedEventHubInfo = await this.taskHubInfoBlob.DownloadTextAsync();
                this.stats.StorageRequests.Increment();
                return JsonConvert.DeserializeObject<TaskHubInfo>(serializedEventHubInfo);
            }

            this.stats.StorageRequests.Increment();
            return null;
        }

        async Task<BlobLease> DownloadLeaseBlob(CloudBlockBlob blob)
        {
            string serializedLease = await blob.DownloadTextAsync();
            this.stats.StorageRequests.Increment();
            BlobLease deserializedLease = JsonConvert.DeserializeObject<BlobLease>(serializedLease);
            deserializedLease.Blob = blob;

            // Workaround: for some reason storage client reports incorrect blob properties after downloading the blob
            await blob.FetchAttributesAsync();
            this.stats.StorageRequests.Increment();
            return deserializedLease;
        }

        static Exception HandleStorageException(Lease lease, StorageException storageException, bool ignoreLeaseLost = false)
        {
            if (storageException.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict
                || storageException.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                // Don't throw LeaseLostException if caller chooses to ignore it.
                StorageExtendedErrorInformation extendedErrorInfo = storageException.RequestInformation.ExtendedErrorInformation;
                if (!ignoreLeaseLost || extendedErrorInfo == null || extendedErrorInfo.ErrorCode != BlobErrorCodeStrings.LeaseLost)
                {
                    return new LeaseLostException(lease, storageException);
                }
            }

            return storageException;
        }
    }
}
