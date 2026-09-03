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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Storage;
    using Newtonsoft.Json;

    /// <summary>
    /// Class responsible for starting and stopping the partition manager. Also implements the app lease feature to ensure a single app's partition manager is started at a time.
    /// </summary>
    sealed class AppLeaseManager
    {
        const string LeaseType = "app";

        readonly AzureStorageClient azureStorageClient;
        readonly IPartitionManager partitionManager;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly string appLeaseContainerName;
        readonly string appLeaseInfoBlobName;
        readonly AppLeaseOptions options;
        readonly string storageAccountName;
        readonly string taskHub;
        readonly string workerName;
        readonly string appName;
        readonly bool appLeaseIsEnabled;
        readonly BlobContainer appLeaseContainer;
        readonly Blob appLeaseInfoBlob;
        readonly string appLeaseId;
        readonly AsyncManualResetEvent shutdownCompletedEvent;
        readonly AppLeaseOwnershipSignal ownershipSignal;

        bool isLeaseOwner;
        int appLeaseIsStarted;
        Task renewTask;
        Task acquireTask;
        CancellationTokenSource starterTokenSource;
        CancellationTokenSource leaseRenewerCancellationTokenSource;

        public AppLeaseManager(
            AzureStorageClient azureStorageClient,
            IPartitionManager partitionManager,
            string appLeaseContainerName,
            string appLeaseInfoBlobName,
            AppLeaseOptions options)
        {
            this.azureStorageClient = azureStorageClient;
            this.partitionManager = partitionManager;
            this.appLeaseContainerName = appLeaseContainerName;
            this.appLeaseInfoBlobName = appLeaseInfoBlobName;
            this.options = options;

            this.storageAccountName = this.azureStorageClient.BlobAccountName;
            this.settings = this.azureStorageClient.Settings;
            this.taskHub = settings.TaskHubName;
            this.workerName = settings.WorkerId;
            this.appName = settings.AppName;
            this.appLeaseIsEnabled = this.settings.UseAppLease;
            this.appLeaseContainer = this.azureStorageClient.GetBlobContainerReference(this.appLeaseContainerName);
            this.appLeaseInfoBlob = this.appLeaseContainer.GetBlobReference(this.appLeaseInfoBlobName);

            var appNameHashInBytes = BitConverter.GetBytes(Fnv1aHashHelper.ComputeHash(this.appName));
            Array.Resize(ref appNameHashInBytes, 16);
            this.appLeaseId = new Guid(appNameHashInBytes).ToString();

            this.isLeaseOwner = false;
            this.shutdownCompletedEvent = new AsyncManualResetEvent();
            this.ownershipSignal = new AppLeaseOwnershipSignal();
        }

        public Task<AppLeaseOwnershipSignal.AppLeaseOwnership> WaitForOwnershipAsync(
            CancellationToken cancellationToken)
        {
            return this.ownershipSignal.WaitAsync(cancellationToken);
        }

        public async Task StartAsync()
        {
            if (!this.appLeaseIsEnabled)
            {
                this.ownershipSignal.Set();
                this.starterTokenSource = new CancellationTokenSource();

                await Task.Factory.StartNew(() => this.PartitionManagerStarter(this.starterTokenSource.Token));
            }
            else
            {
                await RestartAppLeaseStarterTask();
            }
        }

        async Task PartitionManagerStarter(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await this.partitionManager.StartAsync();
                    break;
                }
                catch (Exception e)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.workerName,
                        this.appLeaseContainerName,
                        $"Error in PartitionManagerStarter task. Exception: {e}");
                }
            }
        }

        async Task RestartAppLeaseStarterTask()
        {
            if (this.starterTokenSource != null)
            {
                this.starterTokenSource.Cancel();
                this.starterTokenSource.Dispose();
            }

            if (this.acquireTask != null)
            {
                await this.acquireTask;
            }

            this.starterTokenSource = new CancellationTokenSource();
            this.acquireTask = await Task.Factory.StartNew(() => this.AppLeaseManagerStarter(this.starterTokenSource.Token));
        }

        async Task AppLeaseManagerStarter(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    while (!await this.TryAcquireAppLeaseAsync(cancellationToken))
                    {
                        await Task.Delay(this.settings.AppLeaseOptions.AcquireInterval, cancellationToken);
                    }

                    await this.StartAppLeaseAsync();

                    await this.shutdownCompletedEvent.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Catch OperationCanceledException to avoid logging an error if the Task.Delay was cancelled.
                }
                catch (Exception e)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        this.workerName,
                        this.appLeaseContainerName,
                        $"Error in AppLeaseStarter task. Exception: {e}");
                }
            }
        }

        public async Task StopAsync()
        {
            this.ownershipSignal.Reset();

            if (this.starterTokenSource != null)
            {
                this.starterTokenSource.Cancel();
                this.starterTokenSource.Dispose();
                this.starterTokenSource = null;
            }

            if (this.acquireTask != null)
            {
                await this.acquireTask;
            }

            if (this.appLeaseIsEnabled)
            {
                await this.StopAppLeaseAsync();
            }
            else
            {
                await this.partitionManager.StopAsync();
            }

        }

        public async Task ForceChangeAppLeaseAsync()
        {
            if (!this.appLeaseIsEnabled)
            {
                throw new InvalidOperationException("Cannot force change app lease. UseAppLease is not enabled.");
            }
            if (!this.isLeaseOwner)
            {
                await this.UpdateDesiredSwapAppIdToCurrentApp();

                await this.RestartAppLeaseStarterTask();
            }
        }

        public async Task<bool> CreateContainerIfNotExistsAsync()
        {
            bool result = await appLeaseContainer.CreateIfNotExistsAsync();

            await this.CreateAppLeaseInfoIfNotExistsAsync();

            return result;
        }

        public async Task DeleteContainerAsync()
        {
            try
            {
                if (this.isLeaseOwner)
                {
                    await this.appLeaseContainer.DeleteIfExistsAsync(appLeaseId);
                }
                else
                {
                    await this.appLeaseContainer.DeleteIfExistsAsync();
                }
            }
            catch (DurableTaskStorageException)
            {
                // If we cannot delete the existing app lease due to another app having a lease, just ignore it.
            }
        }

        async Task CreateAppLeaseInfoIfNotExistsAsync()
        {
            try
            {
                await this.appLeaseInfoBlob.UploadTextAsync("{}", ifDoesntExist: true);
            }
            catch (DurableTaskStorageException)
            {
                // eat any storage exception related to conflict
                // this means the blob already exist
            }
        }

        async Task StartAppLeaseAsync()
        {
            if (Interlocked.CompareExchange(ref this.appLeaseIsStarted, 1, 0) != 0)
            {
                throw new InvalidOperationException("AppLeaseManager has already started");
            }

            this.shutdownCompletedEvent.Reset();
            this.leaseRenewerCancellationTokenSource = new CancellationTokenSource();

            try
            {
                await this.partitionManager.StartAsync();
                this.ownershipSignal.Set();
                this.renewTask = await Task.Factory.StartNew(
                    () => this.LeaseRenewer(this.leaseRenewerCancellationTokenSource.Token));
            }
            catch
            {
                this.ownershipSignal.Reset();
                this.isLeaseOwner = false;
                Interlocked.Exchange(ref this.appLeaseIsStarted, 0);
                this.leaseRenewerCancellationTokenSource.Dispose();
                this.leaseRenewerCancellationTokenSource = null;
                this.shutdownCompletedEvent.Set();
                throw;
            }
        }

        async Task StopAppLeaseAsync(bool calledFromLeaseRenewer = false)
        {
            int previousState = Interlocked.CompareExchange(ref this.appLeaseIsStarted, 2, 1);
            if (previousState != 1)
            {
                if (previousState == 2 && !calledFromLeaseRenewer)
                {
                    await this.shutdownCompletedEvent.WaitAsync(
                        Timeout.InfiniteTimeSpan,
                        CancellationToken.None);
                }

                return;
            }

            CancellationTokenSource renewerCancellation = this.leaseRenewerCancellationTokenSource;
            Task renewer = this.renewTask;

            try
            {
                this.ownershipSignal.Reset();

                if (!calledFromLeaseRenewer && renewer != null)
                {
                    renewerCancellation.Cancel();
                    await renewer;
                }

                await this.partitionManager.StopAsync();
            }
            finally
            {
                this.isLeaseOwner = false;

                renewerCancellation?.Dispose();
                this.leaseRenewerCancellationTokenSource = null;
                this.renewTask = null;

                Interlocked.Exchange(ref this.appLeaseIsStarted, 0);
                this.shutdownCompletedEvent.Set();
            }
        }

        async Task<bool> TryAcquireAppLeaseAsync(CancellationToken cancellationToken)
        {
            AppLeaseInfo appLeaseInfo = await this.GetAppLeaseInfoAsync();

            bool leaseAcquired;
            if (appLeaseInfo.DesiredSwapId == this.appLeaseId)
            {
                leaseAcquired = await this.ChangeLeaseAsync(appLeaseInfo.OwnerId, cancellationToken);
            }
            else
            {
                leaseAcquired = await this.TryAcquireLeaseAsync();
            }

            if (leaseAcquired)
            {
                AppLeaseInfo currentAppLeaseInfo = await this.GetAppLeaseInfoAsync();
                TimeSpan transitionDelay =
                    currentAppLeaseInfo.TransitionUntilUtc.GetValueOrDefault() - DateTime.UtcNow;
                if (transitionDelay > TimeSpan.Zero)
                {
                    await Task.Delay(transitionDelay, cancellationToken);
                }
            }

            this.isLeaseOwner = leaseAcquired;

            return leaseAcquired;
        }

        async Task<bool> ChangeLeaseAsync(
            string currentLeaseId,
            CancellationToken cancellationToken)
        {
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                $"Attempting to change lease from current owner {currentLeaseId} to {this.appLeaseId}.");

            bool leaseAcquired;

            try
            {
                this.settings.Logger.LeaseAcquisitionStarted(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    LeaseType);

                await this.appLeaseContainer.ChangeLeaseAsync(this.appLeaseId, currentLeaseId);

                DateTime transitionUntilUtc = DateTime.UtcNow.Add(this.options.RenewInterval);
                var appLeaseInfo = new AppLeaseInfo()
                {
                    OwnerId = this.appLeaseId,
                    TransitionUntilUtc = transitionUntilUtc,
                };

                await this.UpdateAppLeaseInfoBlob(appLeaseInfo);

                // Give the previous app's renewers time to observe the changed lease and stop
                // dequeueing work before this app begins processing.
                await Task.Delay(this.options.RenewInterval, cancellationToken);
                leaseAcquired = true;

                this.settings.Logger.LeaseAcquisitionSucceeded(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    LeaseType);
            }
            catch (DurableTaskStorageException e)
            {
                leaseAcquired = false;

                this.settings.Logger.PartitionManagerWarning(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    $"Failed to change app lease from currentLeaseId {currentLeaseId} to {this.appLeaseId}. Exception: {e.Message}");
            }

            return leaseAcquired;
        }

        async Task<bool> TryAcquireLeaseAsync()
        {
            bool leaseAcquired;

            try
            {
                this.settings.Logger.LeaseAcquisitionStarted(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    LeaseType);

                await this.appLeaseContainer.AcquireLeaseAsync(this.options.LeaseInterval, this.appLeaseId);

                await this.UpdateOwnerAppIdToCurrentApp();
                leaseAcquired = true;

                this.settings.Logger.LeaseAcquisitionSucceeded(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    LeaseType);
            }
            catch (DurableTaskStorageException e)
            {
                leaseAcquired = false;

                this.settings.Logger.LeaseAcquisitionFailed(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    LeaseType);

                this.settings.Logger.PartitionManagerWarning(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    $"Failed to acquire app lease with appLeaseId {this.appLeaseId}. Another app likely has the lease on this container. Exception: {e.Message}");
            }

            return leaseAcquired;
        }

        async Task LeaseRenewer(CancellationToken cancellationToken)
        {
            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                $"Starting background renewal of app lease with interval: {this.options.RenewInterval}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    bool renewSucceeded = await RenewLeaseAsync();

                    if (!renewSucceeded)
                    {
                        break;
                    }

                    await Task.Delay(this.options.RenewInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Catch OperationCanceledException to avoid logging an error if the Task.Delay was cancelled.
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName, 
                        this.taskHub, 
                        this.workerName,
                        this.appLeaseContainerName,
                        $"App lease renewer task failed. AppLeaseId: {this.appLeaseId} Exception: {ex}");
                }
            }

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                "Background app lease renewer task completed.");

            this.settings.Logger.PartitionManagerInfo(
                this.storageAccountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                "Lease renewer task completing. Stopping AppLeaseManager.");

            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await this.StopAppLeaseAsync(calledFromLeaseRenewer: true);
                }
                catch (Exception ex)
                {
                    this.settings.Logger.PartitionManagerError(
                        this.storageAccountName,
                        this.taskHub,
                        this.workerName,
                        this.appLeaseContainerName,
                        $"Failed to stop AppLeaseManager after losing the app lease. AppLeaseId: {this.appLeaseId} Exception: {ex}");
                }
            }
        }

        async Task<bool> RenewLeaseAsync()
        {
            bool renewed;
            string errorMessage = string.Empty;

            try
            {
                this.settings.Logger.StartingLeaseRenewal(
                    this.storageAccountName,
                    this.taskHub,
                    this.workerName,
                    this.appLeaseContainerName,
                    this.appLeaseId,
                    LeaseType);

                await this.appLeaseContainer.RenewLeaseAsync(appLeaseId);

                renewed = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;

                if (ex is DurableTaskStorageException storageException
                    && (storageException.HttpStatusCode == (int)HttpStatusCode.Conflict
                        || storageException.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed))
                {
                    renewed = false;
                    this.isLeaseOwner = false;
                    this.ownershipSignal.Reset();

                    this.settings.Logger.LeaseRenewalFailed(
                        this.storageAccountName,
                        this.taskHub,
                        this.workerName,
                        this.appLeaseContainerName,
                        this.appLeaseId,
                        LeaseType,
                        ex.Message);

                    this.settings.Logger.PartitionManagerWarning(
                        this.storageAccountName,
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

            this.settings.Logger.LeaseRenewalResult(
                this.storageAccountName,
                this.taskHub,
                this.workerName,
                this.appLeaseContainerName,
                renewed,
                this.appLeaseId,
                LeaseType,
                errorMessage);

            return renewed;
        }

        async Task UpdateOwnerAppIdToCurrentApp()
        {
            var appLeaseInfo = await GetAppLeaseInfoAsync();
            if (appLeaseInfo.OwnerId != this.appLeaseId)
            {
                appLeaseInfo.OwnerId = this.appLeaseId;
                await UpdateAppLeaseInfoBlob(appLeaseInfo);
            }
        }

        async Task UpdateDesiredSwapAppIdToCurrentApp()
        {
            var appLeaseInfo = await GetAppLeaseInfoAsync();
            if (appLeaseInfo.DesiredSwapId != this.appLeaseId)
            {
                appLeaseInfo.DesiredSwapId = this.appLeaseId;
                await UpdateAppLeaseInfoBlob(appLeaseInfo);
            }
        }

        async Task UpdateAppLeaseInfoBlob(AppLeaseInfo appLeaseInfo)
        {
            string serializedInfo = Utils.SerializeToJson(appLeaseInfo);
            try
            {
                await this.appLeaseInfoBlob.UploadTextAsync(serializedInfo);
            }
            catch (DurableTaskStorageException)
            {
                // eat any storage exception related to conflict
            }
        }

        async Task<AppLeaseInfo> GetAppLeaseInfoAsync()
        {
            if (await this.appLeaseInfoBlob.ExistsAsync())
            {
                string serializedEventHubInfo = await this.appLeaseInfoBlob.DownloadTextAsync();
                return Utils.DeserializeFromJson<AppLeaseInfo>(serializedEventHubInfo);
            }

            return null;
        }

        private class AppLeaseInfo
        {
            public string OwnerId { get; set; }
            public string DesiredSwapId { get; set; }
            public DateTime? TransitionUntilUtc { get; set; }
        }
    }
}
