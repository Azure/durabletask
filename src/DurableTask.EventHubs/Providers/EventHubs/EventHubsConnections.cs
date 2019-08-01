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
        private readonly Backend.IHost host;
        private readonly string connectionString;

        private object thisLock = new object();

        public EventHubClient _partitionEventHubsClient;
        public Dictionary<uint, EventHubClient> _clientEventHubsClients = new Dictionary<uint, EventHubClient>();
        public Dictionary<uint, EventSender<PartitionEvent>> _partitionSenders = new Dictionary<uint, EventSender<PartitionEvent>>();
        public Dictionary<Guid, EventSender<ClientEvent>> _clientSenders = new Dictionary<Guid, EventSender<ClientEvent>>();

        public PartitionReceiver ClientReceiver { get; private set; }

        public EventHubsConnections(Backend.IHost host, string connectionString)
        {
            this.host = host;
            this.connectionString = connectionString;
        }

        public const string PartitionsPath = "partitions";

        public string GetClientsPath(uint instance) { return $"clients{instance}"; }

        public const uint NumClientBuckets = 128;
        public const uint NumPartitionsPerClientPath = 32;
        public uint NumClientPaths => NumClientBuckets / NumPartitionsPerClientPath;

        public const string PartitionsConsumerGroup = "$Default";
        public const string ClientsConsumerGroup = "$Default";

        public EventHubClient GetPartitionEventHubsClient()
        {
            lock (thisLock)
            {
                if (_partitionEventHubsClient == null)
                {
                    var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                    {
                        EntityPath = PartitionsPath
                    };
                    _partitionEventHubsClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                    //System.Diagnostics.Trace.TraceInformation($"Created Partitions Client {_partitionEventHubsClient.ClientId}");
                }
                return _partitionEventHubsClient;
            }
        }

        public EventHubClient GetClientBucketEventHubsClient(uint clientBucket)
        {
            lock (_clientEventHubsClients)
            {
                var clientPath = clientBucket / NumPartitionsPerClientPath;
                if (!_clientEventHubsClients.TryGetValue(clientPath, out var client))
                {
                    var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                    {
                        EntityPath = GetClientsPath(clientPath)
                    };
                    _clientEventHubsClients[clientPath] = client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                    //System.Diagnostics.Trace.TraceInformation($"Created EventHub Client {client.ClientId}");

                }
                return client;
            }
        }

        public PartitionReceiver GetClientReceiver(Guid clientId)
        {
            uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % NumClientBuckets;
            var client = GetClientBucketEventHubsClient(clientBucket);
            return ClientReceiver = client.CreateReceiver(ClientsConsumerGroup, (clientBucket % NumPartitionsPerClientPath).ToString(), EventPosition.FromEnd());
        }
      
        public EventSender<PartitionEvent> GetPartitionSender(uint partitionId)
        {
            lock (_partitionSenders)
            {
                if (!_partitionSenders.TryGetValue(partitionId, out var sender))
                {
                    var client = GetPartitionEventHubsClient();
                    var partitionSender = client.CreatePartitionSender(partitionId.ToString());
                    _partitionSenders[partitionId] = sender = new EventSender<PartitionEvent>(host, partitionSender);
                    //System.Diagnostics.Trace.TraceInformation($"Created PartitionSender {partitionSender.ClientId} from {client.ClientId}");
                }
                return sender;
            }
        }

        public EventSender<ClientEvent> GetClientSender(Guid clientId)
        {
            lock (_clientSenders)
            {
                if (!_clientSenders.TryGetValue(clientId, out var sender))
                {
                    uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % NumClientBuckets;
                    var client = GetClientBucketEventHubsClient(clientBucket);
                    var partitionSender = client.CreatePartitionSender((clientBucket % NumPartitionsPerClientPath).ToString());
                    _clientSenders[clientId] = sender = new EventSender<ClientEvent>(host, partitionSender);
                    //System.Diagnostics.Trace.TraceInformation($"Created ResponseSender {partitionSender.ClientId} from {client.ClientId}");
                }
                return sender;
            }
        }

        public async Task Close()
        {
            if (ClientReceiver != null)
            {
                //ystem.Diagnostics.Trace.TraceInformation($"Closing Client Receiver");
                await ClientReceiver.CloseAsync();
            }

            //System.Diagnostics.Trace.TraceInformation($"Closing Client Bucket Clients");
            await Task.WhenAll(_clientEventHubsClients.Values.Select(s => s.CloseAsync()).ToList());

            if (_partitionEventHubsClient != null)
            {
                //System.Diagnostics.Trace.TraceInformation($"Closing Partitions Client {_partitionEventHubsClient.ClientId}");
                await _partitionEventHubsClient.CloseAsync();
            }
        }
    }
}
