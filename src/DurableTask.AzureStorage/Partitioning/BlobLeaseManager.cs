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
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;
    using Newtonsoft.Json;

    sealed class BlobLeaseManager : ILeaseManager<BlobLease>
    {
        const string TaskHubInfoBlobName = "taskhub.json";
        static readonly TimeSpan StorageMaximumExecutionTime = TimeSpan.FromMinutes(2);

        readonly string blobPrefix;
        readonly string leaseContainerName;
        readonly string consumerGroupName;
        readonly bool skipBlobContainerCreation;
        readonly TimeSpan leaseInterval;
        readonly TimeSpan renewInterval;
        readonly CloudBlobClient storageClient;
        readonly BlobRequestOptions renewRequestOptions;

        CloudBlobContainer taskHubContainer;
        CloudBlobDirectory consumerGroupDirectory;
        CloudBlockBlob taskHubInfoBlob;

        public BlobLeaseManager(string leaseContainerName, string blobPrefix, string consumerGroupName, CloudBlobClient storageClient, TimeSpan leaseInterval, TimeSpan renewInterval,
            bool skipBlobContainerCreation)
        {
            this.leaseContainerName = leaseContainerName;
            this.blobPrefix = blobPrefix;
            this.consumerGroupName = consumerGroupName;
            this.storageClient = storageClient;
            this.leaseInterval = leaseInterval;
            this.renewInterval = renewInterval;
            this.skipBlobContainerCreation = skipBlobContainerCreation;
            this.renewRequestOptions = new BlobRequestOptions { ServerTimeout = renewInterval };

            this.Initialize();
        }

        public async Task<bool> LeaseStoreExistsAsync()
        {
            return await this.taskHubContainer.ExistsAsync();
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo)
        {
            bool result = false;
            if (!this.skipBlobContainerCreation)
            {
                result = await this.taskHubContainer.CreateIfNotExistsAsync();
            }

            await this.CreateTaskHubInfoIfNotExistAsync(eventHubInfo);

            return result;
        }

        public IEnumerable<Task<BlobLease>> ListLeases()
        {
            foreach (IListBlobItem blob in this.consumerGroupDirectory.ListBlobs())
            {
                CloudBlockBlob lease = blob as CloudBlockBlob;
                if (lease != null)
                {
                    yield return DownloadLeaseBlob(lease);
                }
            }
        }

        public async Task CreateLeaseIfNotExistAsync(string paritionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(paritionId);
            BlobLease lease = new BlobLease(leaseBlob) { PartitionId = paritionId };
            string serializedLease = JsonConvert.SerializeObject(lease);
            try
            {
                AnalyticsEventSource.Log.LogPartitionInfo(
                    string.Format(CultureInfo.InvariantCulture,
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
                AnalyticsEventSource.Log.LogPartitionInfo(
                    string.Format(CultureInfo.InvariantCulture,
                    "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, consumerGroupName: {1}, partitionId: {2}, blobPrefix: {3}, exception: {4}.",
                    this.leaseContainerName,
                    this.consumerGroupName,
                    paritionId,
                    this.blobPrefix ?? string.Empty,
                    se));
            }
        }

        public Task<BlobLease> GetLeaseAsync(string paritionId)
        {
            CloudBlockBlob leaseBlob = this.consumerGroupDirectory.GetBlockBlobReference(paritionId);
            if (leaseBlob.Exists())
            {
                return DownloadLeaseBlob(leaseBlob);
            }

            return Task.FromResult<BlobLease>(null);
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
                lease.Owner = owner;
                // Increment Epoch each time lease is acquired or stolen by new host
                lease.Epoch += 1;
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
            }
            catch (StorageException storageException)
            {
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
                await leaseBlob.ReleaseLeaseAsync(accessCondition: AccessCondition.GenerateLeaseCondition(leaseId));
            }
            catch (StorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task DeleteAsync(BlobLease lease)
        {
            CloudBlockBlob leaseBlob = lease.Blob;
            await leaseBlob.DeleteIfExistsAsync();
        }

        public async Task DeleteAllAsync()
        {
            await this.taskHubContainer.DeleteIfExistsAsync();
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

            try
            {
                await leaseBlob.UploadTextAsync(JsonConvert.SerializeObject(lease), null, AccessCondition.GenerateLeaseCondition(lease.Token), null, null);
            }
            catch (StorageException storageException)
            {
                throw HandleStorageException(lease, storageException, true);
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
                string serializedEventHubInfo = await this.taskHubInfoBlob.DownloadTextAsync();
                return JsonConvert.DeserializeObject<TaskHubInfo>(serializedEventHubInfo);
            }

            return null;
        }

        static async Task<BlobLease> DownloadLeaseBlob(CloudBlockBlob blob)
        {
            string serializedLease = await blob.DownloadTextAsync();
            BlobLease deserializedLease = JsonConvert.DeserializeObject<BlobLease>(serializedLease);
            deserializedLease.Blob = blob;

            // Workaround: for some reason storage client reports incorrect blob properties after downloading the blob
            await blob.FetchAttributesAsync();
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
