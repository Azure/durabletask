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

namespace DurableTask.AzureServiceFabric.Service
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