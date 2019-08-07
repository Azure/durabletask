using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableTask.EventHubs
{
    internal class EventHubsBackend :
        Backend.ITaskHub,
        IEventProcessorFactory,
        Backend.ISender
    {
        private readonly Backend.IHost host;
        private readonly string HostId;
        private readonly EventHubsOrchestrationServiceSettings settings;
        private readonly EventHubsConnections connections;

        private EventProcessorHost eventProcessorHost;
        private Backend.IClient client;

        private CancellationTokenSource shutdownSource;

        private CloudBlobContainer cloudBlobContainer;
        private CloudBlockBlob taskhubParameters;

        private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

        public Guid ClientId { get; private set; }

        public EventHubsBackend(Backend.IHost host, EventHubsOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
            this.HostId = $"{Environment.MachineName}-{DateTime.UtcNow:o}";
            this.ClientId = Guid.NewGuid();
            this.connections = new EventHubsConnections(host, settings.EventHubsConnectionString);
            var blobContainerName = $"{settings.EventHubsNamespaceName}-processors";
            var cloudStorageAccount = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");
        }

        Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            return this.taskhubParameters.ExistsAsync();
        }

        async Task Backend.ITaskHub.CreateAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync();

            // get runtime information from the eventhubs
            var partitionEventHubsClient = this.connections.GetPartitionEventHubsClient();
            var info = await partitionEventHubsClient.GetRuntimeInformationAsync();
            var numberPartitions = info.PartitionCount;

            // check the current queue position in all the partitions 
            var infoTasks = new Task<EventHubPartitionRuntimeInformation>[numberPartitions];
            var startPositions = new long[numberPartitions];
            for (uint i = 0; i < numberPartitions; i++)
            {
                infoTasks[i] = partitionEventHubsClient.GetPartitionRuntimeInformationAsync(i.ToString());
            }
            for (uint i = 0; i < numberPartitions; i++)
            {
                var queueInfo = await infoTasks[i];
                startPositions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
            }

            var taskHubParameters = new
            {
                CreationTimestamp = DateTime.UtcNow,
                StartPositions = startPositions
            };

            // save the taskhub parameters in a blob
            var jsonText = JsonConvert.SerializeObject(
                taskHubParameters,
                Formatting.Indented,
                new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });
            await this.taskhubParameters.UploadTextAsync(jsonText);

            // add a start up event to all partitions
            var ackCounter = new AckCounter(numberPartitions);
            for (uint i = 0; i < numberPartitions; i++)
            {
                var evt = new TaskhubCreated()
                {
                    PartitionId = i,
                    CreationTimestamp = taskHubParameters.CreationTimestamp,
                    StartPositions = taskHubParameters.StartPositions,
                };

                var partitionSender = this.connections.GetPartitionSender(i);
                partitionSender.Add(evt, ackCounter);
            }

            await ackCounter.WaitAsync();
        }

        [DataContract]
        public class TaskhubParameters
        {
            [DataMember]
            public DateTime CreationTimestamp { get; set; }

            [DataMember]
            public long[] StartPositions { get; set; }
        }

        private class AckCounter : Backend.ISendConfirmationListener
        {
            private readonly TaskCompletionSource<object> tcs = new TaskCompletionSource<object>();
            private int count;
            public AckCounter(int numberAcks)
            {
                count = numberAcks;
            }
            public void ConfirmDurablySent(Event evt)
            {
                var val = Interlocked.Decrement(ref count);
                if (val == 0)
                {
                    tcs.TrySetResult(null);
                }
            }
            public void ReportSenderException(Event evt, Exception e)
            {
                // not applicable for this type of event
                throw new NotImplementedException();
            }
            public Task WaitAsync()
            {
                return tcs.Task;
            }
        }

        Task Backend.ITaskHub.DeleteAsync()
        {
            return this.taskhubParameters.DeleteIfExistsAsync();
        }

        async Task Backend.ITaskHub.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            // load the taskhub parameters
            var jsonText = await this.taskhubParameters.DownloadTextAsync();
            var evt = JsonConvert.DeserializeObject<TaskhubCreated>(jsonText);

            this.host.NumberPartitions = (uint) evt.StartPositions.Length;

            this.client = host.AddClient(this.ClientId, this);

            var clientEventLoopTask = this.ClientEventLoop();

            this.eventProcessorHost = new EventProcessorHost(
                 EventHubsConnections.PartitionsPath,
                 EventHubsConnections.PartitionsConsumerGroup,
                 settings.EventHubsConnectionString,
                 settings.StorageConnectionString,
                 cloudBlobContainer.Name);

            var processorOptions = new EventProcessorOptions()
            {
                InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(evt.StartPositions[int.Parse(s)] - 1),
                MaxBatchSize = MaxReceiveBatchSize,
            };

            await eventProcessorHost.RegisterEventProcessorFactoryAsync(this, processorOptions);
        }

        async Task Backend.ITaskHub.StopAsync()
        {
            System.Diagnostics.Trace.TraceInformation("Shutting down EventHubsBackend");
            this.shutdownSource.Cancel();
            await this.eventProcessorHost.UnregisterEventProcessorAsync();
            await this.connections.Close();
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext context)
        {
            var processor = new EventProcessor(this.host, this);
            return processor;
        }

        void Backend.ISender.Submit(Event evt, Backend.ISendConfirmationListener listener)
        {
            if (evt is ClientEvent clientEvent)
            {
                var clientId = clientEvent.ClientId;
                var sender = this.connections.GetClientSender(clientEvent.ClientId);
                sender.Add(clientEvent, listener);
            }
            else if (evt is PartitionEvent partitionEvent)
            {
                var partitionId = partitionEvent.PartitionId;
                var sender = this.connections.GetPartitionSender(partitionId);
                sender.Add(partitionEvent, listener);
            }
        }

        private async Task ClientEventLoop()
        {
            var clientReceiver = this.connections.GetClientReceiver(this.ClientId);
            var receivedEvents = new List<ClientEvent>();

            while (!this.shutdownSource.IsCancellationRequested)
            {
                IEnumerable<EventData> eventData = await clientReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromMinutes(1));

                if (eventData != null)
                {
                    foreach (var ed in eventData)
                    {
                        var clientEvent = (ClientEvent)Serializer.DeserializeEvent(ed.Body);

                        if (clientEvent.ClientId == this.ClientId)
                        {
                            receivedEvents.Add(clientEvent);
                        }
                    }

                    if (receivedEvents.Count > 0)
                    {
                        client.Process(receivedEvents);
                        receivedEvents.Clear();
                    }
                }
            }
        }
    }
}
