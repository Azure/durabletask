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

namespace DurableTask.ServiceFabric.Service
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.ServiceFabric.Tracing;
    using Microsoft.ServiceFabric.Services.Client;

    /// <inheritdoc/>
    public class FabricPartitionEndpointResolver : IPartitionEndpointResolver
    {
        readonly Uri service;
        FabricClient fabricClient;
        Int64RangePartitionInformation[] partitionInfos;
        ConcurrentDictionary<long, ResolvedServiceEndpoint> endPointsLookup;
        ServicePartitionResolver partitionResolver;
        IHasher<string> instanceIdHasher;
        SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Creates instance of <see cref="FabricPartitionEndpointResolver"/>.
        /// </summary>
        /// <param name="service">Fabric service uri</param>
        /// <param name="instanceIdHasher">Instance id hasher</param>
        public FabricPartitionEndpointResolver(Uri service, IHasher<string> instanceIdHasher)
        {
            this.service = service ?? throw new ArgumentNullException(nameof(service));
            this.instanceIdHasher = instanceIdHasher ?? throw new ArgumentNullException(nameof(instanceIdHasher));
            this.endPointsLookup = new ConcurrentDictionary<long, ResolvedServiceEndpoint>();
            this.partitionResolver = ServicePartitionResolver.GetDefault();

            this.InitializeFabricClient();
        }

        private void InitializeFabricClient()
        {
            if (this.fabricClient != null)
            {
                // unregister events
                this.fabricClient.ServiceManager.ServiceNotificationFilterMatched -= OnServiceNotificationFilterMatched;
                this.fabricClient.ClientDisconnected -= OnClientDisconnected;
            }

            this.fabricClient = new FabricClient();
            this.fabricClient.ServiceManager.ServiceNotificationFilterMatched += OnServiceNotificationFilterMatched;
            this.fabricClient.ClientDisconnected += OnClientDisconnected;
        }

        private void OnServiceNotificationFilterMatched(object sender, EventArgs e)
        {
            // Currently, code rebuilds entire partition table.
            BuildPartitionInfosAsync(true).ConfigureAwait(false);
        }

        private void OnClientDisconnected(object sender, EventArgs e)
        {
            // FabicClient is disconnected, recreate FabricClient.
            InitializeFabricClient();
            BuildPartitionInfosAsync(true).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<string> GetParitionEndpointAsync(string instanceId)
        {
            await BuildPartitionInfosAsync(false);
            var hash = await this.instanceIdHasher.GenerateHashCode(instanceId);
            var partition = this.BinarySearch(partitionInfos, hash);
            return endPointsLookup[partition.LowKey].Address;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPartitionEndpointsAsync()
        {
            await BuildPartitionInfosAsync(false);
            return this.endPointsLookup.Values.Select(x => x.Address);
        }

        private async Task BuildPartitionInfosAsync(bool force)
        {
            if (force || this.partitionInfos == null)
            {
                await this.semaphoreSlim.WaitAsync();
                try
                {
                    if (force || this.partitionInfos == null)
                    {
                        ProviderEventSource.Tracing.PartitionCacheBuildStart();
                        CancellationToken cancellationToken = new CancellationToken();
                        var partitionList = await fabricClient.QueryManager.GetPartitionListAsync(this.service);
                        List<Int64RangePartitionInformation> partitions = new List<Int64RangePartitionInformation>();
                        foreach (var partition in partitionList)
                        {
                            var info = partition.PartitionInformation as Int64RangePartitionInformation;
                            if (info == null)
                            {
                                throw new NotSupportedException($"{partition.PartitionInformation.Kind} is not supported");
                            }

                            partitions.Add(info);
                            var resolvedServicePartition = await this.partitionResolver.ResolveAsync(this.service, new ServicePartitionKey(info.LowKey), cancellationToken);
                            var endpoint = resolvedServicePartition.GetEndpoint();
                            this.endPointsLookup.AddOrUpdate(info.LowKey, endpoint, (_, __) => endpoint);
                        }

                        this.partitionInfos = partitions.OrderBy(x => x.LowKey).ToArray();
                        ProviderEventSource.Tracing.PartitionCacheBuildComplete();
                    }
                }
                finally
                {
                    this.semaphoreSlim.Release();
                }
            }
        }

        private Int64RangePartitionInformation BinarySearch(Int64RangePartitionInformation[] array, long hash)
        {
            int start = 0, end = array.Length - 1;

            do
            {
                int middle = (start + end) / 2;

                if (hash >= array[middle].LowKey && hash <= array[middle].HighKey)
                {
                    return array[middle];
                }

                else if (hash < array[middle].LowKey)
                {
                    end = middle - 1;
                }
                else
                {
                    start = middle + 1;
                }
            } while (start > end);

            throw new Exception($"{hash} is out of partition range");
        }
    }
}
