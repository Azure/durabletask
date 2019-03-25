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
    using System.Fabric.Description;
    using System.Fabric.Query;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Services.Client;


    /// <inheritdoc/>
    public class FabricPartitionEndpointResolver : IPartitionEndpointResolver, IDisposable
    {
        private readonly Uri serviceUri;
        private FabricClient fabricClient;
        private Partition[] servicePartitions;
        private ConcurrentDictionary<Guid, TaskCompletionSource<ResolvedServiceEndpoint>> endPointsLookup;
        private readonly ServicePartitionResolver partitionResolver;
        private readonly IHasher<string> instanceIdHasher;

        /// <summary>
        /// Creates instance of <see cref="FabricPartitionEndpointResolver"/>.
        /// </summary>
        /// <param name="serviceUri">Fabric service uri</param>
        /// <param name="instanceIdHasher">Instance id hasher</param>
        public FabricPartitionEndpointResolver(Uri serviceUri, IHasher<string> instanceIdHasher)
        {
            this.serviceUri = serviceUri ?? throw new ArgumentNullException(nameof(serviceUri));
            this.instanceIdHasher = instanceIdHasher ?? throw new ArgumentNullException(nameof(instanceIdHasher));
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

            this.endPointsLookup = new ConcurrentDictionary<Guid, TaskCompletionSource<ResolvedServiceEndpoint>>();
            this.fabricClient = new FabricClient();
            this.fabricClient.ServiceManager.ServiceNotificationFilterMatched += OnServiceNotificationFilterMatched;
            this.fabricClient.ClientDisconnected += OnClientDisconnected;
            var serviceNotificationFilterDescription = new ServiceNotificationFilterDescription()
            {
                Name = this.serviceUri,
                MatchNamePrefix = true,
                MatchPrimaryChangeOnly = true
            };

            this.fabricClient.ServiceManager.RegisterServiceNotificationFilterAsync(serviceNotificationFilterDescription, TimeSpan.FromSeconds(10), CancellationToken.None);

            this.BuildServicePartitions().GetAwaiter().GetResult();
        }

        private async Task BuildServicePartitions()
        {
            var concurrentDictionary = new ConcurrentDictionary<Partition, TaskCompletionSource<ResolvedServiceEndpoint>>();
            var partitionList = await fabricClient.QueryManager.GetPartitionListAsync(this.serviceUri);
            this.servicePartitions = partitionList.OrderBy(x => ((Int64RangePartitionInformation)x.PartitionInformation).LowKey).ToArray();
        }

        private void OnServiceNotificationFilterMatched(object sender, EventArgs e)
        {
            var notification = ((FabricClient.ServiceManagementClient.ServiceNotificationEventArgs)e).Notification;
            var partitionInformation = (Int64RangePartitionInformation)notification.PartitionInfo;

            OnServiceNotificationFilterMatchedAsync(partitionInformation).GetAwaiter().GetResult();
        }

        private async Task OnServiceNotificationFilterMatchedAsync(Int64RangePartitionInformation partitionInformation)
        {
            var completionSource = new TaskCompletionSource<ResolvedServiceEndpoint>();
            this.endPointsLookup.AddOrUpdate(partitionInformation.Id, completionSource, (_, __) => completionSource);

            var endPoint = await this.partitionResolver.ResolveAsync(this.serviceUri, new ServicePartitionKey(partitionInformation.LowKey), CancellationToken.None);
            completionSource.SetResult(endPoint.GetEndpoint());
        }

        private void OnClientDisconnected(object sender, EventArgs e)
        {
            // FabicClient is disconnected, recreate FabricClient.
            InitializeFabricClient();
        }

        /// <inheritdoc/>
        public async Task<string> GetPartitionEndPointAsync(string instanceId)
        {
            var hash = await this.instanceIdHasher.GenerateHashCode(instanceId);
            var partition = this.BinarySearch(this.servicePartitions, hash);
            return await GetPartitionEndPointAsync(partition);
        }

        private async Task<string> GetPartitionEndPointAsync(Partition partition)
        {
            var newCompletionSource = new TaskCompletionSource<ResolvedServiceEndpoint>();
            var completionSource = this.endPointsLookup.GetOrAdd(partition.PartitionInformation.Id, newCompletionSource);
            if (completionSource == newCompletionSource)
            {
                var partitionInfo = (Int64RangePartitionInformation)partition.PartitionInformation;
                var endPoint = await this.partitionResolver.ResolveAsync(this.serviceUri, new ServicePartitionKey(partitionInfo.LowKey), CancellationToken.None);
                newCompletionSource.SetResult(endPoint.GetEndpoint());
            }

            return completionSource.Task.Result.Address;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPartitionEndpointsAsync()
        {
            var endpoints = new List<string>();
            foreach (var partition in this.servicePartitions)
            {
                endpoints.Add(await GetPartitionEndPointAsync(partition));
            }

            return endpoints;
        }

        private Partition BinarySearch(Partition[] array, long hash)
        {
            int start = 0, end = array.Length - 1;

            do
            {
                var middle = (start + end) / 2;
                var partitionInformation = (Int64RangePartitionInformation)array[middle].PartitionInformation;
                if (hash >= partitionInformation.LowKey && hash <= partitionInformation.HighKey)
                {
                    return array[middle];
                }
                else if (hash < partitionInformation.LowKey)
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

        #region IDisposable Support

        private bool disposedValue = false; // To detect redundant calls

        /// <inheritdoc />
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.servicePartitions = null;
                    this.endPointsLookup = null;
                    this.fabricClient.Dispose();
                }

                disposedValue = true;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion IDisposable Support
    }
}