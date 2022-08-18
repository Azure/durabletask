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
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Storage;
    using Newtonsoft.Json;

    sealed class BlobLeaseManager : ILeaseManager<BlobLease>
    {
        const string TaskHubInfoBlobName = "taskhub.json";

        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly string storageAccountName;
        readonly string taskHubName;
        readonly string workerName;
        readonly string leaseContainerName;
        readonly TimeSpan leaseInterval;

        BlobContainer taskHubContainer;
        string blobDirectoryName;
        Blob taskHubInfoBlob;

        public BlobLeaseManager(
            AzureStorageClient azureStorageClient,
            string leaseContainerName,
            string leaseType)
        {
            this.azureStorageClient = azureStorageClient;
            this.settings = this.azureStorageClient.Settings;
            this.storageAccountName = this.azureStorageClient.BlobAccountName;
            this.taskHubName = this.settings.TaskHubName;
            this.workerName = this.settings.WorkerId;
            this.leaseContainerName = leaseContainerName;
            this.blobDirectoryName = leaseType;
            this.leaseInterval = this.settings.LeaseInterval;

            this.Initialize();
        }

        public Task<bool> LeaseStoreExistsAsync()
        {
            return taskHubContainer.ExistsAsync();
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo, bool checkIfStale)
        {
            bool result = false;
            result = await taskHubContainer.CreateIfNotExistsAsync();

            await this.GetOrCreateTaskHubInfoAsync(eventHubInfo, checkIfStale: checkIfStale);

            return result;
        }

        public async Task<IEnumerable<BlobLease>> ListLeasesAsync()
        {
            var blobLeases = new List<BlobLease>();

            IEnumerable<Blob> blobs = await this.taskHubContainer.ListBlobsAsync(this.blobDirectoryName);

            var downloadTasks = new List<Task<BlobLease>>();
            foreach (Blob blob in blobs)
            {
                downloadTasks.Add(this.DownloadLeaseBlob(blob));
            }

            await Task.WhenAll(downloadTasks);

            blobLeases.AddRange(downloadTasks.Select(t => t.Result));

            return blobLeases;
        }

        public async Task CreateLeaseIfNotExistAsync(string partitionId)
        {
            Blob leaseBlob = this.taskHubContainer.GetBlobReference(partitionId, this.blobDirectoryName);
            BlobLease lease = new BlobLease(leaseBlob) { PartitionId = partitionId };
            string serializedLease = Utils.SerializeToJson(lease);
            try
            {
                this.settings.Logger.PartitionManagerInfo(
                    this.storageAccountName,
                    this.taskHubName,
                    this.workerName,
                    partitionId,
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, leaseType: {1}, partitionId: {2}",
                        this.leaseContainerName,
                        this.blobDirectoryName,
                        partitionId));

                await leaseBlob.UploadTextAsync(serializedLease, ifDoesntExist: true);
            }
            catch (DurableTaskStorageException se)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
                if (se.HttpStatusCode != 409)
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.storageAccountName,
                        this.taskHubName,
                        this.workerName,
                        partitionId,
                        string.Format(
                            CultureInfo.InvariantCulture,
                            "CreateLeaseIfNotExistAsync - leaseContainerName: {0}, leaseType: {1}, partitionId: {2}, exception: {3}",
                            this.leaseContainerName,
                            this.blobDirectoryName,
                            partitionId,
                            se.Message));
                }
            }
        }

        public async Task<BlobLease> GetLeaseAsync(string partitionId)
        {
            Blob leaseBlob = this.taskHubContainer.GetBlobReference(partitionId, this.blobDirectoryName);
            if (await leaseBlob.ExistsAsync())
            {
                return await this.DownloadLeaseBlob(leaseBlob);
            }

            return null;
        }

        public async Task<bool> RenewAsync(BlobLease lease)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                await leaseBlob.RenewLeaseAsync(lease.Token);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task<bool> AcquireAsync(BlobLease lease, string owner)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                string newLeaseId = Guid.NewGuid().ToString();
                if (leaseBlob.IsLeased)
                {
                    lease.Token = await leaseBlob.ChangeLeaseAsync(newLeaseId, currentLeaseId: lease.Token);
                }
                else
                {
                    lease.Token = await leaseBlob.AcquireLeaseAsync(this.leaseInterval, newLeaseId);
                }

                lease.Owner = owner;
                // Increment Epoch each time lease is acquired or stolen by new host
                lease.Epoch += 1;
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(lease), leaseId: lease.Token);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task<bool> ReleaseAsync(BlobLease lease)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                string leaseId = lease.Token;

                BlobLease copy = new BlobLease(lease);
                copy.Token = null;
                copy.Owner = null;
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(copy), leaseId);
                await leaseBlob.ReleaseLeaseAsync(leaseId);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task DeleteAsync(BlobLease lease)
        {
            await lease.Blob.DeleteIfExistsAsync();
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

            Blob leaseBlob = lease.Blob;
            try
            {
                // First renew the lease to make sure checkpoint will go through
                await leaseBlob.RenewLeaseAsync(lease.Token);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            try
            {
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(lease), lease.Token);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException, true);
            }

            return true;
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo)
        {
            string serializedInfo = Utils.SerializeToJson(taskHubInfo);
            try
            {
                await this.taskHubInfoBlob.UploadTextAsync(serializedInfo, ifDoesntExist: true);
            }
            catch (DurableTaskStorageException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
            }
        }

        internal async Task<TaskHubInfo> GetOrCreateTaskHubInfoAsync(TaskHubInfo newTaskHubInfo, bool checkIfStale)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync();
            if (currentTaskHubInfo != null)
            {
                if (checkIfStale && IsStale(currentTaskHubInfo, newTaskHubInfo))
                {
                    this.settings.Logger.PartitionManagerWarning(
                       this.storageAccountName,
                       this.taskHubName,
                       this.workerName,
                       string.Empty,
                       $"Partition count for the task hub {currentTaskHubInfo.TaskHubName} from {currentTaskHubInfo.PartitionCount} to {newTaskHubInfo.PartitionCount}. This could result in errors for in-flight orchestrations.");

                    try
                    {
                        string serializedInfo = Utils.SerializeToJson(newTaskHubInfo);
                        await this.taskHubInfoBlob.UploadTextAsync(serializedInfo);
                    } 
                    catch (DurableTaskStorageException)
                    {
                        // eat any storage exception as this is a best effort to
                        // to update the metadata used by Azure Functions scaling logic
                    }

                }
                return currentTaskHubInfo;
            }

            await this.CreateTaskHubInfoIfNotExistAsync(newTaskHubInfo);
            return newTaskHubInfo;
        }

        private bool IsStale(TaskHubInfo currentTaskHubInfo, TaskHubInfo newTaskHubInfo)
        {
            return !currentTaskHubInfo.TaskHubName.Equals(newTaskHubInfo.TaskHubName, StringComparison.OrdinalIgnoreCase)
                    || !currentTaskHubInfo.PartitionCount.Equals(newTaskHubInfo.PartitionCount);
        }

        void Initialize()
        {
            this.taskHubContainer = this.azureStorageClient.GetBlobContainerReference(this.leaseContainerName);

            this.taskHubInfoBlob = this.taskHubContainer.GetBlobReference(TaskHubInfoBlobName);
        }

        async Task<TaskHubInfo> GetTaskHubInfoAsync()
        {
            if (await this.taskHubInfoBlob.ExistsAsync())
            {
                await taskHubInfoBlob.FetchAttributesAsync();
                string serializedEventHubInfo = await this.taskHubInfoBlob.DownloadTextAsync();
                return Utils.DeserializeFromJson<TaskHubInfo>(serializedEventHubInfo);
            }

            return null;
        }

        async Task<BlobLease> DownloadLeaseBlob(Blob blob)
        {
            string serializedLease = null;
            var buffer = SimpleBufferManager.Shared.TakeBuffer(SimpleBufferManager.SmallBufferSize);
            try
            {
                using (var memoryStream = new MemoryStream(buffer))
                {
                    await blob.DownloadToStreamAsync(memoryStream);
                    serializedLease = Encoding.UTF8.GetString(buffer, 0, (int)memoryStream.Position);
                }
            }
            finally
            {
                SimpleBufferManager.Shared.ReturnBuffer(buffer);
            }

            BlobLease deserializedLease = Utils.DeserializeFromJson<BlobLease>(serializedLease);
            deserializedLease.Blob = blob;

            // Workaround: for some reason storage client reports incorrect blob properties after downloading the blob
            await blob.FetchAttributesAsync();
            return deserializedLease;
        }

        static Exception HandleStorageException(Lease lease, DurableTaskStorageException storageException, bool ignoreLeaseLost = false)
        {
            if (storageException.HttpStatusCode == (int)HttpStatusCode.Conflict
                || storageException.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                // Don't throw LeaseLostException if caller chooses to ignore it.
                if (!ignoreLeaseLost || storageException.LeaseLost)
                {
                    return new LeaseLostException(lease, storageException);
                }
            }

            return storageException;
        }
    }
}
