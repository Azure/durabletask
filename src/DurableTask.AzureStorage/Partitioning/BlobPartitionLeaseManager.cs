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
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs.Models;
    using DurableTask.AzureStorage.Storage;

    sealed class BlobPartitionLeaseManager : ILeaseManager<BlobPartitionLease>
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

        public BlobPartitionLeaseManager(
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

        public Task<bool> LeaseStoreExistsAsync(CancellationToken cancellationToken = default)
        {
            return taskHubContainer.ExistsAsync(cancellationToken);
        }

        public async Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo, bool checkIfStale, CancellationToken cancellationToken = default)
        {
            bool result = await taskHubContainer.CreateIfNotExistsAsync(cancellationToken);

            await this.GetOrCreateTaskHubInfoAsync(eventHubInfo, checkIfStale: checkIfStale, cancellationToken: cancellationToken);

            return result;
        }

        public async IAsyncEnumerable<BlobPartitionLease> ListLeasesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (Page<Blob> page in this.taskHubContainer.ListBlobsAsync(this.blobDirectoryName, cancellationToken: cancellationToken).AsPages())
            {
                // Start each of the Tasks in parallel
                Task<BlobPartitionLease>[] downloadTasks = page.Values.Select(b => DownloadLeaseBlob(b, cancellationToken)).ToArray();

                foreach (Task<BlobPartitionLease> downloadTask in downloadTasks)
                {
                    yield return await downloadTask;
                }
            }
        }

        public async Task CreateLeaseIfNotExistAsync(string partitionId, CancellationToken cancellationToken = default)
        {
            Blob leaseBlob = this.taskHubContainer.GetBlobReference(partitionId, this.blobDirectoryName);
            BlobPartitionLease lease = new BlobPartitionLease(leaseBlob) { PartitionId = partitionId };
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

                await leaseBlob.UploadTextAsync(serializedLease, ifDoesntExist: true, cancellationToken: cancellationToken);
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

        public async Task<BlobPartitionLease> GetLeaseAsync(string partitionId, CancellationToken cancellationToken = default)
        {
            Blob leaseBlob = this.taskHubContainer.GetBlobReference(partitionId, this.blobDirectoryName);
            if (await leaseBlob.ExistsAsync(cancellationToken))
            {
                return await DownloadLeaseBlob(leaseBlob, cancellationToken);
            }

            return null;
        }

        public async Task<bool> RenewAsync(BlobPartitionLease lease, CancellationToken cancellationToken = default)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                await leaseBlob.RenewLeaseAsync(lease.Token, cancellationToken);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task<bool> AcquireAsync(BlobPartitionLease lease, string owner, CancellationToken cancellationToken = default)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                string newLeaseId = Guid.NewGuid().ToString();
                if (await leaseBlob.IsLeasedAsync(cancellationToken))
                {
                    lease.Token = await leaseBlob.ChangeLeaseAsync(newLeaseId, currentLeaseId: lease.Token, cancellationToken: cancellationToken);
                }
                else
                {
                    lease.Token = await leaseBlob.AcquireLeaseAsync(this.leaseInterval, newLeaseId, cancellationToken: cancellationToken);
                }

                lease.Owner = owner;
                // Increment Epoch each time lease is acquired or stolen by new host
                lease.Epoch += 1;
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(lease), leaseId: lease.Token, cancellationToken: cancellationToken);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public async Task<bool> ReleaseAsync(BlobPartitionLease lease, CancellationToken cancellationToken = default)
        {
            Blob leaseBlob = lease.Blob;
            try
            {
                string leaseId = lease.Token;

                BlobPartitionLease copy = new BlobPartitionLease(lease);
                copy.Token = null;
                copy.Owner = null;
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(copy), leaseId, cancellationToken: cancellationToken);
                await leaseBlob.ReleaseLeaseAsync(leaseId, cancellationToken);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            return true;
        }

        public Task DeleteAsync(BlobPartitionLease lease, CancellationToken cancellationToken = default)
        {
            return lease.Blob.DeleteIfExistsAsync(cancellationToken);
        }

        public Task DeleteAllAsync(CancellationToken cancellationToken = default)
        {
            return this.taskHubContainer.DeleteIfExistsAsync(cancellationToken: cancellationToken);
        }

        public async Task<bool> UpdateAsync(BlobPartitionLease lease, CancellationToken cancellationToken = default)
        {
            if (lease == null || string.IsNullOrWhiteSpace(lease.Token))
            {
                return false;
            }

            Blob leaseBlob = lease.Blob;
            try
            {
                // First renew the lease to make sure checkpoint will go through
                await leaseBlob.RenewLeaseAsync(lease.Token, cancellationToken);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException);
            }

            try
            {
                await leaseBlob.UploadTextAsync(Utils.SerializeToJson(lease), lease.Token, cancellationToken: cancellationToken);
            }
            catch (DurableTaskStorageException storageException)
            {
                throw HandleStorageException(lease, storageException, true);
            }

            return true;
        }

        public async Task CreateTaskHubInfoIfNotExistAsync(TaskHubInfo taskHubInfo, CancellationToken cancellationToken = default)
        {
            string serializedInfo = Utils.SerializeToJson(taskHubInfo);
            try
            {
                await this.taskHubInfoBlob.UploadTextAsync(serializedInfo, ifDoesntExist: true, cancellationToken: cancellationToken);
            }
            catch (DurableTaskStorageException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
            }
        }

        internal async Task<TaskHubInfo> GetOrCreateTaskHubInfoAsync(TaskHubInfo newTaskHubInfo, bool checkIfStale, CancellationToken cancellationToken = default)
        {
            TaskHubInfo currentTaskHubInfo = await this.GetTaskHubInfoAsync(cancellationToken);
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
                        await this.taskHubInfoBlob.UploadTextAsync(serializedInfo, cancellationToken: cancellationToken);
                    } 
                    catch (DurableTaskStorageException)
                    {
                        // eat any storage exception as this is a best effort to
                        // to update the metadata used by Azure Functions scaling logic
                    }

                }
                return currentTaskHubInfo;
            }

            await this.CreateTaskHubInfoIfNotExistAsync(newTaskHubInfo, cancellationToken);
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

        async Task<TaskHubInfo> GetTaskHubInfoAsync(CancellationToken cancellationToken)
        {
            if (await this.taskHubInfoBlob.ExistsAsync(cancellationToken))
            {
                string serializedEventHubInfo = await this.taskHubInfoBlob.DownloadTextAsync(cancellationToken);
                return Utils.DeserializeFromJson<TaskHubInfo>(serializedEventHubInfo);
            }

            return null;
        }

        static async Task<BlobPartitionLease> DownloadLeaseBlob(Blob blob, CancellationToken cancellationToken)
        {
            using BlobDownloadStreamingResult result = await blob.DownloadStreamingAsync(cancellationToken);
            BlobPartitionLease deserializedLease = Utils.DeserializeFromJson<BlobPartitionLease>(result.Content);
            deserializedLease.Blob = blob;

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
