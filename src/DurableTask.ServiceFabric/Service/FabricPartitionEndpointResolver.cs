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

    using Microsoft.ServiceFabric.Services.Client;

    /// <inheritdoc/>
    public class FabricPartitionEndpointResolver : IPartitionEndpointResolver
    {
        readonly Uri service;
        readonly FabricClient fabricClient;
        Int64RangePartitionInformation[] partitionInfos;
        ConcurrentDictionary<long, ResolvedServiceEndpoint> endPointsLookup;
        ServicePartitionResolver partitionResolver;
        IHasher<string> instanceIdHasher;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="service"></param>
        /// <param name="instanceIdHasher"></param>
        public FabricPartitionEndpointResolver(Uri service, IHasher<string> instanceIdHasher)
        {
            this.service = service ?? throw new ArgumentNullException(nameof(service));
            this.instanceIdHasher = instanceIdHasher ?? throw new ArgumentNullException(nameof(instanceIdHasher));
            this.fabricClient = new FabricClient();
            this.endPointsLookup = new ConcurrentDictionary<long, ResolvedServiceEndpoint>();
            this.partitionResolver = ServicePartitionResolver.GetDefault();
        }

        /// <inheritdoc/>
        public async Task<Uri> GetParitionEndpointAsync(string instanceId)
        {
            await BuildPartitionInfos(false);
            var hash = this.instanceIdHasher.GetLongHashCode(instanceId);
            var partition = this.BinarySearch(partitionInfos, hash);
            return new Uri(endPointsLookup[partition.LowKey].Address);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<Uri>> GetPartitionEndpointsAsync()
        {
            await BuildPartitionInfos(false);
            return this.endPointsLookup.Values.Select(x => new Uri(x.Address));
        }

        private async Task BuildPartitionInfos(bool force)
        {
            if (force || this.partitionInfos == null)
            {
                CancellationToken cancellationToken = new CancellationToken();
                var partitionList = await fabricClient.QueryManager.GetPartitionListAsync(this.service);
                List<Int64RangePartitionInformation> partitions = new List<Int64RangePartitionInformation>();
                foreach (var partition in partitionList)
                {
                    var info = (Int64RangePartitionInformation)partition.PartitionInformation;
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
