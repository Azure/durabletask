//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express oTrace.TraceInformationr implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace DurableTask.EventHubs
{
    internal class EventHubsConnections
    {
        private readonly string connectionString;

        private object thisLock = new object();

        public EventHubClient _partitionsClient;
        public Dictionary<uint, EventHubClient> _clientBucketClients = new Dictionary<uint, EventHubClient>();
        public Dictionary<uint, PartitionSender> _partitionSenders = new Dictionary<uint, PartitionSender>();
        public Dictionary<Guid, PartitionSender> _clientBucketSenders = new Dictionary<Guid, PartitionSender>();

        public PartitionReceiver ClientReceiver { get; private set; }

        public EventHubsConnections(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public EventHubClient GetPartitionsClient()
        {
            lock (thisLock)
            {
                if (_partitionsClient == null)
                {
                    var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                    {
                        EntityPath = "Partitions"
                    };
                    _partitionsClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                    System.Diagnostics.Trace.TraceInformation($"Created Partitions Client {_partitionsClient.ClientId}");
                }
                return _partitionsClient;
            }
        }

        public EventHubClient GetClientBucketClient(uint clientBucket)
        {
            lock (_clientBucketClients)
            {
                var instance = clientBucket / 4;
                if (!_clientBucketClients.TryGetValue(instance, out var client))
                {
                    var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                    {
                        EntityPath = $"Clients{instance}"
                    };
                    _clientBucketClients[instance] = client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                    System.Diagnostics.Trace.TraceInformation($"Created EventHub Client {client.ClientId}");

                }
                return client;
            }
        }

        public PartitionReceiver GetClientReceiver(Guid clientId)
        {
            uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % 128;
            var client = GetClientBucketClient(clientBucket);
            return ClientReceiver = client.CreateReceiver("$Default", (clientBucket % 32).ToString(), EventPosition.FromEnd());
        }
      
        public PartitionSender GetProcessSender(uint partitionId)
        {
            lock (_partitionSenders)
            {
                if (!_partitionSenders.TryGetValue(partitionId, out var sender))
                {
                    var client = GetPartitionsClient();
                    _partitionSenders[partitionId] = sender = client.CreatePartitionSender(partitionId.ToString());
                    System.Diagnostics.Trace.TraceInformation($"Created PartitionSender {sender.ClientId} from {client.ClientId}");
                }
                return sender;
            }
        }

        public PartitionSender GetClientBucketSender(Guid clientId)
        {
            lock (_clientBucketSenders)
            {
                if (!_clientBucketSenders.TryGetValue(clientId, out var sender))
                {
                    uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % 128;
                    var client = GetClientBucketClient(clientBucket);
                    _clientBucketSenders[clientId] = sender = client.CreatePartitionSender((clientBucket % 32).ToString());
                    System.Diagnostics.Trace.TraceInformation($"Created ResponseSender {sender.ClientId} from {client.ClientId}");
                }
                return sender;
            }
        }

        public async Task Close()
        {
            if (ClientReceiver != null)
            {
                System.Diagnostics.Trace.TraceInformation($"Closing Client Receiver");
                await ClientReceiver.CloseAsync();
            }

            System.Diagnostics.Trace.TraceInformation($"Closing Client Bucket Clients");
            await Task.WhenAll(_clientBucketClients.Values.Select(s => s.CloseAsync()).ToList());

            if (_partitionsClient != null)
            {
                System.Diagnostics.Trace.TraceInformation($"Closing Partitions Client {_partitionsClient.ClientId}");
                await _partitionsClient.CloseAsync();
            }
        }
    }
}
