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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;

    internal class OrchestrationMemoryManager
    {
        private AzureStorageOrchestrationServiceSettings settings;
        private string storageAccountName;
        private string taskHub;
        private double messageVisibilityTimeout;
        private long totalMemoryBytes;
        private long memoryBufferBytes;
        private long? adjustedTotalMemory;
        private long pendingMemory;
        private CancellationTokenSource currentMemoryCancellationTokenSource;
        private Task currentMemoryUpdaterTask;
        private int isStarted;
        private long currentlyAllocatedMemory;
        private readonly object lockObject = new object();

        public OrchestrationMemoryManager(AzureStorageOrchestrationServiceSettings settings, string storageAccountName)
        {
            this.settings = settings;
            this.storageAccountName = storageAccountName;
            this.taskHub = settings.TaskHubName;
            this.messageVisibilityTimeout = settings.ControlQueueVisibilityTimeout.TotalMilliseconds;
            this.totalMemoryBytes = settings.TotalProcessMemoryMBytes * 1024 * 1024;
            this.memoryBufferBytes = settings.MemoryBufferMBytes * 1024 * 1024;
            this.adjustedTotalMemory = this.totalMemoryBytes - this.memoryBufferBytes;
            this.pendingMemory = 0;
        }

        public async Task StartAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 1, 0) != 0)
            {
                throw new InvalidOperationException($"{nameof(OrchestrationMemoryManager)} has already started");
            }

            this.currentMemoryCancellationTokenSource = new CancellationTokenSource();

            this.currentMemoryUpdaterTask = await Task.Factory.StartNew(() => this.CurrentMemoryUpdater());
        }

        public async Task StopAsync()
        {
            if (Interlocked.CompareExchange(ref this.isStarted, 0, 1) != 1)
            {
                //idempotent
                return;
            }

            if (this.currentMemoryUpdaterTask != null)
            {
                this.currentMemoryCancellationTokenSource.Cancel();
                await this.currentMemoryUpdaterTask;
            }

            this.currentMemoryCancellationTokenSource = null;
        }

        async Task CurrentMemoryUpdater()
        {
            this.settings.Logger.OrchestrationMemoryManagerInfo(
                this.storageAccountName,
                this.taskHub,
                $"Starting background currenly allocated memory updater.");

            while (this.isStarted == 1)
            {
                this.currentlyAllocatedMemory = Process.GetCurrentProcess().PrivateMemorySize64;

                await Task.Delay(10, this.currentMemoryCancellationTokenSource.Token);
            }

            this.settings.Logger.OrchestrationMemoryManagerInfo(
                this.storageAccountName,
                this.taskHub,
                $"Background updater for currenly allocated memory completed.");
        }

        public async Task<OrchestrationHistory> GetHistoryEventsAsync(ITrackingStore trackingStore, string instanceId, string executionId, CancellationToken cancellationToken)
        {
            bool memoryReserved = false;
            long memorySize = 0;

            try
            {
                if (this.settings.UseOrchestrationHistoryLoadThrottle)
                {
                    OrchestrationState state = await trackingStore.GetStateAsync(
                            instanceId,
                            executionId,
                            false);

                    memoryReserved = await this.WaitUntilMemoryReservedAsync(state, cancellationToken);
                    memorySize = state.Size;
                }

                OrchestrationHistory history = await trackingStore.GetHistoryEventsAsync(
                   instanceId,
                   executionId,
                   cancellationToken);

                return history;
            }
            finally
            {
                if (memoryReserved)
                {
                    this.FreeMemoryFromReserve(memorySize);
                }
            }
        }

        async Task<bool> WaitUntilMemoryReservedAsync(OrchestrationState state, CancellationToken cancellationToken)
        {
            long bytesNeeded = state.Size;
            string instanceId = state.OrchestrationInstance.InstanceId;
            string executonId = state.OrchestrationInstance.ExecutionId;

            int attemptNumber = 1;
            double elapsed = 0;
            while (elapsed < this.messageVisibilityTimeout && !cancellationToken.IsCancellationRequested)
            {
                lock (this.lockObject)
                {
                    if (bytesNeeded + this.currentlyAllocatedMemory + this.pendingMemory <= this.adjustedTotalMemory)
                    {
                        this.pendingMemory += bytesNeeded;
                        return true;
                    }
                }

                int delayInMs = 1000;

                this.settings.Logger.ThrottlingOrchestrationHistoryLoad(
                   this.storageAccountName,
                   this.taskHub,
                   instanceId,
                   executonId,
                   $"Not enough memory to load orchestrator history. Retrying in {delayInMs}ms. Total elapsed time: {elapsed}, Attempt number {attemptNumber}, Instance size: {bytesNeeded}, Currently allocated memory: {currentlyAllocatedMemory}, Pending orchestrator history memory: {this.pendingMemory}, MemoryLimitBytes: {this.totalMemoryBytes}");

                await Task.Delay(delayInMs, cancellationToken);
                elapsed += delayInMs;
                attemptNumber++;
            }

            return false;
        }

        void FreeMemoryFromReserve(long bytesToFree)
        {
            lock (this.lockObject)
            {
                this.pendingMemory -= bytesToFree;
            }
        }
    }
}
