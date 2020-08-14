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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Security.Cryptography;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System.Runtime.ExceptionServices;

    sealed class AppLeaseManager
    {
        private readonly string accountName;
        private readonly string taskHub;
        private readonly string workerName;
        private readonly CloudBlobClient storageClient;
        private readonly string appLeaseContainerName;
        private readonly string appName;
        private readonly AppLeaseOptions options;
        private readonly AzureStorageOrchestrationServiceStats stats;
        readonly AzureStorageOrchestrationServiceSettings settings;

        private readonly CloudBlobContainer appLeaseContainer;
        private readonly string appLeaseId;

        private bool isLeaseOwner;
        private int isStarted;
        private bool shutdownComplete;
        private Task renewTask;
        private CancellationTokenSource leaseRenewerCancellationTokenSource;

        public AppLeaseManager(
            AzureStorageOrchestrationServiceSettings settings,
            string accountName, 
            CloudBlobClient storageClient, 
            string appLeaseContainerName, 
            AppLeaseOptions options, 
            AzureStorageOrchestrationServiceStats stats)
        {
            this.settings = settings;
            this.accountName = accountName;
            this.taskHub = settings.TaskHubName;
            this.workerName = settings.WorkerId;
            this.storageClient = storageClient;
            this.appLeaseContainerName = appLeaseContainerName;
            this.appName = settings.AppName;
            this.options = options;
            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

            this.isLeaseOwner = false;

            this.appLeaseContainer = this.storageClient.GetContainerReference(this.appLeaseContainerName);

            using (MD5 md5 = MD5.Create())
            {
                byte[] hash = md5.ComputeHash(Encoding.Default.GetBytes(this.appName));
               this.appLeaseId = new Guid(hash).ToString();
            }
        }

        public async Task<bool> CreateContainerIfNotExistsAsync()
        {
            bool result = await appLeaseContainer.CreateIfNotExistsAsync();
            return result;
        }

        public async Task StartAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 1, 0) != 0)
            {
                throw new InvalidOperationException("AppLeaseManager has already started");
            }

            this.shutdownComplete = false;
            this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

            this.renewTask = await Task.Factory.StartNew(() => this.LeaseRenewer());
        }

        public async Task StopAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 0, 1) != 1)
            {
                //idempotent
                return;
            }

            await this.ReleaseLeaseAsync();
            this.shutdownComplete = true;

            if (this.renewTask != null)
            {
                this.leaseRenewerCancellationTokenSource.Cancel();
                await this.renewTask;
            }

            this.leaseRenewerCancellationTokenSource?.Dispose();
            this.leaseRenewerCancellationTokenSource = null;
        }

        public async Task<bool> TryAquireAppLeaseAsync()
        {
            bool leaseAcquired;

            try
            {
                this.settings.Logger.LeaseAcquisitionStarted(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName);

                await appLeaseContainer.AcquireLeaseAsync(this.options.LeaseInterval, this.appLeaseId);
                leaseAcquired = true;
                this.isLeaseOwner = true;

                this.settings.Logger.LeaseAcquisitionSucceeded(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName);
            }
            catch (StorageException e)
            {
                leaseAcquired = false;

                this.settings.Logger.LeaseAcquisitionFailed(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName);

                this.settings.Logger.PartitionManagerWarning(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    $"Failed to acquire app lease with appLeaseId {this.appLeaseId}. Another app likely has the lease on this container. Exception: {e.Message}");
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            return leaseAcquired;
        }

        async Task LeaseRenewer()
        {
            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                $"Starting background renewal of app lease with interval: {this.options.RenewInterval}.");

            while (this.isStarted == 1 || !shutdownComplete)
            {
                try
                {
                    bool renewSucceeded = await RenewLeaseAsync();

                    if (!renewSucceeded)
                    {
                        break;
                    }

                    await Task.Delay(this.options.RenewInterval, this.leaseRenewerCancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    this.settings.Logger.PartitionManagerInfo(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        this.appLeaseContainerName,
                        "Background renewal task was canceled.");
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.accountName, 
                        this.taskHub, 
                        this.workerName,
                        this.appLeaseContainerName, 
                        $"App lease renewer task failed. AppLeaseId: {this.appLeaseId} Exception: {ex}");
                }
            }

            this.settings.Logger.PartitionManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                "Background app lease renewer task completed.");
        }

        async Task<bool> RenewLeaseAsync()
        {
            bool renewed;
            string errorMessage = string.Empty;

            try
            {
                this.settings.Logger.StartingLeaseRenewal(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    this.appLeaseId);

                AccessCondition accessCondition = new AccessCondition() { LeaseId = appLeaseId };
                await appLeaseContainer.RenewLeaseAsync(accessCondition);

                renewed = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;

                if (ex is LeaseLostException ||
                    ex is ArgumentException)
                {
                    renewed = false;
                    this.isLeaseOwner = false;

                    this.settings.Logger.LeaseRenewalFailed(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        this.appLeaseContainerName,
                        this.appLeaseId,
                        ex.Message);

                    this.settings.Logger.PartitionManagerError(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        this.appLeaseContainerName,
                        $"AppLeaseManager failed to renew lease. AppLeaseId: {this.appLeaseId} Exception: {ex}");
                }
                else
                {
                    // Eat any exceptions during renew and keep going.
                    // Consider the lease as renewed.  Maybe lease store outage is causing the lease to not get renewed.
                    renewed = true;
                }
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            this.settings.Logger.LeaseRenewalResult(
                this.accountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                renewed,
                this.appLeaseId,
                errorMessage);

            return renewed;
        }

        private async Task ReleaseLeaseAsync()
        {
            try
            {
                AccessCondition accessCondition = new AccessCondition() { LeaseId = this.appLeaseId };
                await this.appLeaseContainer.ReleaseLeaseAsync(accessCondition);

                this.isLeaseOwner = false;

                this.settings.Logger.LeaseRemoved(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    this.appLeaseId);
            }
            catch (Exception)
            {
                this.settings.Logger.LeaseRemovalFailed(
                    this.accountName, 
                    this.taskHub, 
                    this.workerName,
                    this.appLeaseContainerName, 
                    this.appLeaseId);
            }
        }

        public async Task DeleteContainerAsync()
        {
            try
            {
                if (this.isLeaseOwner)
                {
                    AccessCondition accessCondition = new AccessCondition() { LeaseId = appLeaseId };
                    await this.appLeaseContainer.DeleteIfExistsAsync(accessCondition, null, null);
                }
                else
                {
                    await this.appLeaseContainer.DeleteIfExistsAsync();
                }
            }
            catch (StorageException ex)
            { 
                if (ex.RequestInformation.HttpStatusCode != 404)
                {
                    throw;
                }
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }
        }
    }
}
