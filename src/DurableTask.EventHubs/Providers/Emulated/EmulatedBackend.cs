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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class EmulatedBackend : Backend.ITaskHub
    {
        private readonly Backend.IHost host;
        private readonly EventHubsOrchestrationServiceSettings settings;

        private Dictionary<Guid, EmulatedQueue<ClientEvent>> clientQueues;
        private EmulatedQueue<PartitionEvent>[] partitionQueues;
        private CancellationTokenSource shutdownTokenSource;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(2);

        public EmulatedBackend(Backend.IHost host, EventHubsOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
        }

        async Task Backend.ITaskHub.CreateAsync()
        {
            var numberPartitions = settings.EmulatedPartitions;
            await Task.Delay(simulatedDelay);
            this.clientQueues = new Dictionary<Guid, EmulatedQueue<ClientEvent>>();
            this.partitionQueues = new EmulatedQueue<PartitionEvent>[numberPartitions];
        }

        async Task Backend.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay);
            this.clientQueues = null;
            this.partitionQueues = null;
        }

        async Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay);
            return this.partitionQueues != null;
        }

        async Task Backend.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            var numberPartitions = this.settings.EmulatedPartitions;
            this.host.NumberPartitions = numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            // create a client and start its receive loop
            var clientId = Guid.NewGuid();
            var clientQueue = new EmulatedQueue<ClientEvent>(simulatedDelay, this.shutdownTokenSource.Token);
            var clientSender = new SendWorker(this.shutdownTokenSource.Token);
            this.clientQueues[clientId] = clientQueue;
            var client = this.host.AddClient(clientId, clientSender);
            clientSender.SetHandler(list => SendEvents(client, list));
            var clientReceiveLoop = ClientReceiveLoop(client, clientQueue);

            // create all partitions and start their receive loops
            for (uint i = 0; i < this.settings.EmulatedPartitions; i++)
            {
                uint partitionId = i;
                var partitionQueue = new EmulatedQueue<PartitionEvent>(simulatedDelay, this.shutdownTokenSource.Token);
                var partitionSender = new SendWorker(this.shutdownTokenSource.Token);
                this.partitionQueues[i] = partitionQueue;
                var partition = this.host.AddPartition(i, partitionSender);
                partitionSender.SetHandler(list => SendEvents(partition, list));
                var partitionReceiveLoop = PartitionReceiveLoop(partition, partitionQueue);
            }

            for (uint i = 0; i < numberPartitions; i++)
            {
                var evt = new TaskhubCreated()
                {
                    PartitionId = i,
                    CreationTimestamp = creationTimestamp,
                    StartPositions = startPositions,
                };

                await this.partitionQueues[i].SendAsync(evt);
            }
        }

        async Task Backend.ITaskHub.StopAsync()
        {
            await Task.Delay(simulatedDelay);
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;
            }
        }

        private Task SendEvents(Backend.IClient client, List<Event> events)
        {
            try
            {
                return SendEvents(events);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                client.ReportError("Exception During Send", e);
                throw e;
            }
        }

        private Task SendEvents(Backend.IPartition partition, List<Event> events)
        {
            try
            {
                return SendEvents(events);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                partition.ReportError("Exception During Send", e);
                throw e;
            }
        }

        private async Task SendEvents(List<Event> events)
        {
            foreach (var evt in events)
            {
                if (evt is ClientEvent clientEvent)
                {
                    await this.clientQueues[clientEvent.ClientId].SendAsync(clientEvent);
                }
                else if (evt is PartitionEvent partitionEvent)
                {
                    await this.partitionQueues[partitionEvent.PartitionId].SendAsync(partitionEvent);
                }
            }
        }

        private async Task ClientReceiveLoop(Backend.IClient client, EmulatedQueue<ClientEvent> queue)
        {
            try
            {
                var token = this.shutdownTokenSource.Token;
                long position = 0;
                while (!token.IsCancellationRequested)
                {
                    var batch = await queue.ReceiveBatchAsync(position);
                    if (batch == null) break;
                    for (int i = 0; i < batch.Count; i++)
                    {
                        batch[i].QueuePosition = position + i;
                    }
                    client.Process(batch);
                    position = position + batch.Count;
                }
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                client.ReportError("Client Exception in receive loop", e);
            }
        }

        private async Task PartitionReceiveLoop(Backend.IPartition partition, EmulatedQueue<PartitionEvent> queue)
        {
            try
            {
                var token = this.shutdownTokenSource.Token;
                var position = await partition.StartAsync();
                while (!token.IsCancellationRequested)
                {
                    var batch = await queue.ReceiveBatchAsync(position);
                    if (batch == null) break;
                    for (int i = 0; i < batch.Count; i++)
                    {
                        batch[i].QueuePosition = position + i;
                    }
                    await partition.ProcessAsync(batch);
                    position = position + batch.Count;
                }
                await partition.StopAsync();
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                partition.ReportError("Exception in Receive Loop", e);
            }
        }
    }
}
