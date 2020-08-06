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

using System;
using System.Threading.Tasks;
using System.Threading;
using DurableTask.AzureStorage.Monitoring;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Security.Cryptography;
using System.Text;

namespace DurableTask.AzureStorage.Partitioning
{
    sealed class AppLeaseManager
    {
        private readonly string accountName;
        private readonly string taskHub;
        private readonly string workerName;
        private readonly CloudBlobClient storageClient;
        private readonly string taskHubContainerName;
        private readonly string appName;
        private readonly AppLeaseOptions options;
        private readonly AzureStorageOrchestrationServiceStats stats;

        private CloudBlobContainer taskHubContainer;
        private string appId;

        private int isStarted;
        private bool shutdownComplete;
        private Task renewTask;
        private CancellationTokenSource leaseRenewerCancellationTokenSource;

        public AppLeaseManager(string accountName, string taskHub, string workerName, CloudBlobClient storageClient, string taskHubContainerName, string appName, AppLeaseOptions options, AzureStorageOrchestrationServiceStats stats)
        {
            this.accountName = accountName;
            this.taskHub = taskHub;
            this.workerName = workerName;
            this.storageClient = storageClient;
            this.taskHubContainerName = taskHubContainerName;
            this.appName = appName;
            this.options = options;
            this.stats = stats ?? new AzureStorageOrchestrationServiceStats();

            Initialize();
        }

        void Initialize()
        {
            this.taskHubContainer = this.storageClient.GetContainerReference(this.taskHubContainerName);

            using (MD5 md5 = MD5.Create())
            {
                byte[] hash = md5.ComputeHash(Encoding.Default.GetBytes(appName));
                this.appId = new Guid(hash).ToString();
            }
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

            this.leaseRenewerCancellationTokenSource = null;
        }

        public async Task<bool> TryAquireAppLeaseAsync()
        {
            bool leaseAcquired;

            try
            {
                AnalyticsEventSource.Log.AppLeaseAcquisitionStarted(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    Utils.ExtensionVersion);

                appId = await taskHubContainer.AcquireLeaseAsync(this.options.LeaseInterval, appId);
                leaseAcquired = true;

                AnalyticsEventSource.Log.LeaseAcquisitionSucceeded(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    Utils.ExtensionVersion);
            }
            catch (StorageException)
            {
                leaseAcquired = false;

                AnalyticsEventSource.Log.AppLeaseAcquisitionFailed(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    Utils.ExtensionVersion);
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            return leaseAcquired;
        }

        async Task LeaseRenewer()
        {
            AnalyticsEventSource.Log.AppLeaseManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                this.appId,
                $"Starting background renewal of app lease with interval: {this.options.RenewInterval}.",
                Utils.ExtensionVersion);

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
                    AnalyticsEventSource.Log.AppLeaseManagerInfo(
                        this.accountName,
                        this.taskHub,
                        this.workerName,
                        this.appId,
                        "Background renewal task was canceled.",
                        Utils.ExtensionVersion);
                }
                catch (Exception ex)
                {
                    AnalyticsEventSource.Log.AppLeaseManagerError(this.accountName, this.taskHub, this.workerName, string.Empty, ex, Utils.ExtensionVersion);
                }
            }

            AnalyticsEventSource.Log.AppLeaseManagerInfo(
                this.accountName,
                this.taskHub,
                this.workerName,
                this.appId,
                "Background renewer task completed.",
                Utils.ExtensionVersion);
        }

        async Task<bool> RenewLeaseAsync()
        {
            bool renewed;
            string errorMessage = string.Empty;

            try
            {
                AnalyticsEventSource.Log.StartingAppLeaseRenewal(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    Utils.ExtensionVersion);

                AccessCondition accessCondition = new AccessCondition() { LeaseId = appId };
                await taskHubContainer.RenewLeaseAsync(accessCondition);

                renewed = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;

                if (ex is LeaseLostException ||
                    ex is ArgumentException)
                {
                    renewed = false;

                    AnalyticsEventSource.Log.AppLeaseRenewalFailed(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    errorMessage,
                    Utils.ExtensionVersion);
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

            AnalyticsEventSource.Log.AppLeaseRenewalResult(
                this.accountName,
                this.taskHub,
                this.workerName,
                renewed,
                this.appId,
                errorMessage,
                Utils.ExtensionVersion);

            return renewed;
        }

        private async Task ReleaseLeaseAsync()
        {
            try
            {
                AnalyticsEventSource.Log.ReleasingAppLease(
                    this.accountName,
                    this.taskHub,
                    this.workerName,
                    this.appId,
                    Utils.ExtensionVersion);

                AccessCondition accessCondition = new AccessCondition() { LeaseId = appId };
                await taskHubContainer.ReleaseLeaseAsync(accessCondition);
            }
            catch (Exception ex)
            {
                AnalyticsEventSource.Log.AppLeaseManagerError(this.accountName, this.taskHub, this.workerName, this.appId, ex, Utils.ExtensionVersion);
            }
        }
    }
}
