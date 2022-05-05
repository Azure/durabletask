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

namespace DurableTask.AzureServiceFabric.Remote
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Fabric.Query;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Services.Client;

    /// <inheritdoc/>
    public class FabricPartitionEndpointResolver : IPartitionEndpointResolver
    {
        private readonly Uri serviceUri;
        private readonly ServicePartitionResolver partitionResolver;
        private readonly IPartitionHashing<string> instanceIdHasher;

        private TaskCompletionSource<Int64RangePartitionInformation[]> servicePartitionsTaskCompletionSource;
        private object tcsLockObject = new object();

        /// <summary>
        /// Creates instance of <see cref="FabricPartitionEndpointResolver"/>.
        /// </summary>
        /// <param name="serviceUri">Fabric service uri</param>
        /// <param name="instanceIdHasher">Instance id hasher</param>
        public FabricPartitionEndpointResolver(Uri serviceUri, IPartitionHashing<string> instanceIdHasher)
        {
            this.serviceUri = serviceUri ?? throw new ArgumentNullException(nameof(serviceUri));
            this.instanceIdHasher = instanceIdHasher ?? throw new ArgumentNullException(nameof(instanceIdHasher));
            this.partitionResolver = ServicePartitionResolver.GetDefault();
        }

        /// <inheritdoc/>
        public async Task<string> GetPartitionEndPointAsync(string instanceId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long hash = await this.instanceIdHasher.GeneratePartitionHashCodeAsync(instanceId, cancellationToken);
            ResolvedServicePartition partition = await this.partitionResolver.ResolveAsync(this.serviceUri, new ServicePartitionKey(hash), cancellationToken);
            return partition.GetEndpoint().Address;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPartitionEndpointsAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var endpoints = new List<string>();
            foreach (Int64RangePartitionInformation partitionInfo in await this.GetServicePartitionsListAsync())
            {
                var resolvedPartition = await this.partitionResolver.ResolveAsync(this.serviceUri, new ServicePartitionKey(partitionInfo.LowKey), cancellationToken);
                endpoints.Add(resolvedPartition.GetEndpoint().Address);
            }

            return endpoints;
        }

        /// <inheritdoc/>
        public async Task RefreshPartitionEndpointAsync(string instanceId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long hash = await this.instanceIdHasher.GeneratePartitionHashCodeAsync(instanceId, cancellationToken);

            /*
             * Service Fabric's ServicePartitionResolver class maintains an internal cache of ResolvedServicePartition objects for each partition
             * which is served by an internal reference of FabricClient. The cache is refreshed via 2 different processes:
             * - A notification protocol through which the FabricClient notifies resolver that a given value is stale and needs to be updated.
             * - A complaint based resolution, as part of which, the client passes in the previously retrieved ResolvedServicePartition object
             *   to the ResolveAsync() API call. This API call notifies the resolver that the previously provided object is stale and a new one
             *   should be fetched from the FabricClient
             * Service Fabric also states in their documentation that the notification mechanism cannot be completely relied upon since it is an
             * internal network call which is prone to failures. This is the one of the reasons for the existence of complaint based resolution.
             * In our current implementation, we were only relying on notifications and thus occasionally ran into failures due to non-existent/bad endpoints.
             * 
             * Retrieve the current ResolvedServicePartition object from the cache and then pass in the same object
             * to the ResolveAsync() API call. This operation notifies the ServicePartitionResolver that the current
             * object is most likely stale and that it needs to refresh its own cache.
             * Further calls to ResolveAsync() would return the newer version if the cache was refreshed.
             */
            var previousRsp = await this.partitionResolver.ResolveAsync(this.serviceUri, new ServicePartitionKey(hash), cancellationToken);
            await this.partitionResolver.ResolveAsync(previousRsp, cancellationToken);
        }

        private async Task<Int64RangePartitionInformation[]> GetServicePartitionsListAsync()
        {
            if (this.servicePartitionsTaskCompletionSource == null)
            {
                bool isNewTaskCompletionSourceCreated = false;
                lock (tcsLockObject)
                {
                    if (this.servicePartitionsTaskCompletionSource == null)
                    {
                        this.servicePartitionsTaskCompletionSource = new TaskCompletionSource<Int64RangePartitionInformation[]>();
                        isNewTaskCompletionSourceCreated = true;
                    }
                }

                if (isNewTaskCompletionSourceCreated)
                {
                    using (var fabricClient = new FabricClient())
                    {
                        ServicePartitionList partitionList = await fabricClient.QueryManager.GetPartitionListAsync(this.serviceUri);
                        Int64RangePartitionInformation[] orderedPartitions = partitionList.Select(x => x.PartitionInformation)
                                                                                .OfType<Int64RangePartitionInformation>()
                                                                                .OrderBy(x => x.LowKey).ToArray();
                        if (partitionList.Count != orderedPartitions.Length)
                        {
                            throw new NotSupportedException("FabricPartitionEndpointResolver doesn't support non Int64RangePartitionInformation partitions");
                        }

                        this.servicePartitionsTaskCompletionSource.SetResult(orderedPartitions);
                    }
                }
            }

            return this.servicePartitionsTaskCompletionSource.Task.Result;
        }
    }
}