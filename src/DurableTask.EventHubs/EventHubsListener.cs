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

namespace DurableTask.EventHubs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.EventHubs.Monitoring;
    using DurableTask.EventHubs.Partitioning;
    using Microsoft.Azure.EventHubs;
    using Microsoft.WindowsAzure.Storage;

    class EventHubListener : IPartitionObserver<BlobLease>
    {
        readonly EventHubClient client;
        readonly EventHubsOrchestrationServiceSettings settings;
        readonly AsyncQueue<IEnumerable<EventData>> prefetchQueue;

        readonly ConcurrentDictionary<string, PartitionListener> listeners =
            new ConcurrentDictionary<string, PartitionListener>(StringComparer.OrdinalIgnoreCase);

        readonly ConcurrentDictionary<string, MessageSender> senders =
            new ConcurrentDictionary<string, MessageSender>(StringComparer.OrdinalIgnoreCase);

        readonly BlobLeaseManager leaseManager;
        readonly PartitionManager<BlobLease> partitionManager;

        EventHubRuntimeInformation runtimeInformation;

        public EventHubListener(
            EventHubsOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats)
        {
            if (stats == null)
            {
                throw new ArgumentNullException(nameof(stats));
            }

            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
            this.client = settings.CreateEventHubClient();
            this.prefetchQueue = new AsyncQueue<IEnumerable<EventData>>();

            CloudStorageAccount account = settings.GetStorageAccount();

            this.leaseManager = new BlobLeaseManager(
                settings.TaskHubName,
                settings.WorkerId,
                leaseContainerName: settings.TaskHubName.ToLowerInvariant() + "-leases",
                blobPrefix: string.Empty,
                consumerGroupName: settings.ConsumerGroupName,
                storageClient: account.CreateCloudBlobClient(),
                settings.LeaseInterval,
                settings.LeaseRenewInterval,
                skipBlobContainerCreation: false,
                stats: stats);

            this.partitionManager = new PartitionManager<BlobLease>(
                account.Credentials.AccountName,
                this.settings.TaskHubName,
                settings.WorkerId,
                this.leaseManager,
                new PartitionManagerOptions
                {
                    AcquireInterval = settings.LeaseAcquireInterval,
                    RenewInterval = settings.LeaseRenewInterval,
                    LeaseInterval = settings.LeaseInterval,
                });
        }

        public async Task CreateLeaseStoreAsync()
        {
            var hubInfo = new TaskHubInfo(
                this.settings.TaskHubName,
                DateTime.UtcNow,
                this.settings.PartitionCount);

            await this.leaseManager.CreateLeaseStoreIfNotExistsAsync(hubInfo);

            this.runtimeInformation = await this.client.GetRuntimeInformationAsync();

            await Task.WhenAll(
                this.runtimeInformation.PartitionIds.Select(
                    partitionId => this.leaseManager.CreateLeaseIfNotExistAsync(partitionId)));
        }

        public async Task StartAsync()
        {
            await this.partitionManager.InitializeAsync();
            await this.partitionManager.SubscribeAsync(this);
            await this.partitionManager.StartAsync();
        }

        public MessageSender GetOrCreatePartitionSender(string partitionKey)
        {
            // For orchestrations, the partitionKey is typically the instance ID
            int partitionIndex = (int)Fnv1aHashHelper.ComputeHash(partitionKey) 
                % this.runtimeInformation.PartitionCount;

            return this.GetOrCreatePartitionSender(partitionIndex);
        }

        public MessageSender GetOrCreatePartitionSender(int partitionIndex)
        {
            string partitionId = this.runtimeInformation.PartitionIds[partitionIndex];
            return this.senders.GetOrAdd(
                partitionId,
                id => new MessageSender(this.client.CreatePartitionSender(id)));
        }

        public async void StartListening(string partitionId)
        {
            if (this.listeners.ContainsKey(partitionId))
            {
                // This listener is already listening?
                return;
            }

            // TODO: Need to get the actual EventPosition from somewhere
            PartitionReceiver receiver = this.client.CreateReceiver(
                this.settings.ConsumerGroupName,
                partitionId,
                EventPosition.FromStart());

            var listener = new PartitionListener(receiver);
            this.listeners[partitionId] = listener;

            while (!listener.IsStopping)
            {
                try
                {
                    // TODO: Wait for threshold before receiving the next batch

                    IEnumerable<EventData> batch = await listener.Receiver.ReceiveAsync(
                        this.settings.MaxReceiveMessageCount);
                    if (batch != null)
                    {
                        // TODO: Trace batch received
                        Console.WriteLine($"[{partitionId}] Received {batch.Count()} messages.");

                        this.prefetchQueue.Enqueue(batch);
                    }
                }
                catch (Exception unexpectedError)
                {
                    // TODO: Trace unexpected exception
                    Console.Error.WriteLine("Unexpected error in receive loop: " + unexpectedError);
                }
            }

            try
            {
                await listener.Receiver.CloseAsync();
            }
            catch (Exception unexpectedError)
            {
                // TODO: Trace unexpected exception
                Console.Error.WriteLine("Unexpected error in receive loop: " + unexpectedError);
            }
        }

        public void StopListening(string partitionId)
        {
            if (this.listeners.TryRemove(partitionId, out PartitionListener listener))
            {
                listener.IsStopping = true;
            }
        }

        public Task<IEnumerable<EventData>> GetNextBatchAsync(CancellationToken token)
        {
            return this.prefetchQueue.DequeueAsync(token);
        }

        Task IPartitionObserver<BlobLease>.OnPartitionAcquiredAsync(BlobLease lease)
        {
            // TODO: Tracing
            this.StartListening(lease.PartitionId);
            return Task.CompletedTask;
        }

        Task IPartitionObserver<BlobLease>.OnPartitionReleasedAsync(BlobLease lease, CloseReason reason)
        {
            // TODO: Tracing
            this.StopListening(lease.PartitionId);
            return Task.CompletedTask;
        }

        public Task StopAsync()
        {
            return this.partitionManager.StopAsync();
        }

        public Task DeletePartitionsAsync()
        {
            return this.leaseManager.DeleteAllAsync();
        }

        class PartitionListener
        {
            public PartitionListener(PartitionReceiver receiver)
            {
                this.Receiver = receiver ?? throw new ArgumentNullException(nameof(receiver));
            }

            public PartitionReceiver Receiver { get; }
            public bool IsStopping { get; set; }
        }
    }
}
