﻿//  ----------------------------------------------------------------------------------
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

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.AzureStorage.Tracking;
using DurableTask.Core;

namespace DurableTask.AzureStorage
{
    class OrchestrationMemoryManager
    {
        private AzureStorageOrchestrationServiceSettings settings;
        private string storageAccountName;
        private double messageVisibilityTimeout;
        private long totalMemoryBytes;
        private long? adjustedTotalMemory;
        private long pendingMemory;
        private readonly object lockObject = new object();

        public OrchestrationMemoryManager(AzureStorageOrchestrationServiceSettings settings, string storageAccountName, long? memoryBufferBytes = null)
        {
            this.settings = settings;
            this.storageAccountName = storageAccountName;
            this.messageVisibilityTimeout = settings.ControlQueueVisibilityTimeout.TotalMilliseconds;
            this.totalMemoryBytes = settings.MemoryLimitBytes;
            this.adjustedTotalMemory = this.totalMemoryBytes - memoryBufferBytes;
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
                var currentlyAllocatedMemory = Process.GetCurrentProcess().PrivateMemorySize64;

                lock (this.lockObject)
                {
                    if (bytesNeeded + currentlyAllocatedMemory + this.pendingMemory <= this.adjustedTotalMemory)
                    {
                        this.pendingMemory += bytesNeeded;
                        return true;
                    }
                }
                //TODO: can make this an exponential backoff
                int delayInMs = 1000;

                this.settings.Logger.ThrottlingOrchestrationHistory(
                   this.storageAccountName,
                   this.settings.TaskHubName,
                   instanceId,
                   executonId,
                   bytesNeeded,
                   $"Not enough memory to load orchestrator history. Retrying in {delayInMs}ms. Total elapsed time: {elapsed}. Attempt number {attemptNumber}");

                await Task.Delay(delayInMs, cancellationToken);
                elapsed += delayInMs;
                attemptNumber++;
            }

            return false;
        }

        void FreeMemoryReserve(long bytesToFree)
        {
            lock (this.lockObject)
            {
                this.pendingMemory -= bytesToFree;
            }
        }

        public async Task<OrchestrationHistory> GetHistoryEventsAsync(ITrackingStore trackingStore, OrchestrationState state, CancellationToken cancellationToken)
        {
            bool memoryReserved = false;

            try
            {
                memoryReserved = await this.WaitUntilMemoryReservedAsync(state, cancellationToken);

                OrchestrationHistory history = await trackingStore.GetHistoryEventsAsync(
                   state.OrchestrationInstance.InstanceId,
                   state.OrchestrationInstance.ExecutionId,
                   cancellationToken);

                return history;
            }
            finally
            {
                if (memoryReserved)
                {
                    this.FreeMemoryReserve(state.Size);
                }
            }
        }
    }
}
