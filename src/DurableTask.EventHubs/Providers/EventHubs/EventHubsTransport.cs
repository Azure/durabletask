using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventHubs
{
    internal class EventHubsTransport : 
        Backend.ITaskHub, 
        IEventProcessorFactory, 
        Backend.ISender<Event>
    {
        private readonly Backend.IHost host;
        private readonly string HostId;
        private readonly EventHubsOrchestrationServiceSettings settings;
        private readonly Serializer serializer;
        private readonly EventHubsConnections connections;

        private EventProcessorHost eventProcessorHost;
        private Backend.IClient client;

        const string ConsumerGroupName = "dfpartitions";
        const string PartitionLeaseContainerName = "dfleases";

        public Guid ClientId { get; private set; }

        public EventHubsTransport(Backend.IHost host, EventHubsOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
            this.HostId = $"{Environment.MachineName}-{DateTime.UtcNow:o}";
            this.ClientId = Guid.NewGuid();
            this.serializer = new Serializer();
            this.connections = new EventHubsConnections(settings.EventHubsConnectionString);
        }

        Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            return Task.FromResult(true); //TODO
        }

        Task Backend.ITaskHub.CreateAsync()
        {
            return Task.FromResult(true); //TODO
        }

        Task Backend.ITaskHub.DeleteAsync()
        {
            return Task.FromResult(true); //TODO
        }

        Task Backend.ITaskHub.StartAsync()
        {
            this.client = host.AddClient(this.ClientId, this);

            var clientReceiver = this.connections.GetClientReceiver(this.ClientId);
            //TODO: connect client

            this.eventProcessorHost = new EventProcessorHost(
                 settings.EventHubName,
                 ConsumerGroupName,
                 settings.EventHubsConnectionString,
                 settings.StorageConnectionString,
                 PartitionLeaseContainerName);

            var processorOptions = new EventProcessorOptions()
            {
                InitialOffsetProvider = (s) => EventPosition.FromStart(), //TODO
                MaxBatchSize = 10000,
            };

            return eventProcessorHost.RegisterEventProcessorFactoryAsync(this, processorOptions);
        }

        async Task Backend.ITaskHub.StopAsync()
        {
            await this.eventProcessorHost.UnregisterEventProcessorAsync();

            await this.connections.Close();
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext context)
        {
            var processor = new EventProcessor(this.host, this, this.serializer);
            return processor;
        }

        void Backend.ISender<Event>.Submit(Event element)
        {
            throw new NotImplementedException(); // TODO
        }

        void Backend.ISender<Event>.Submit(IEnumerable<Event> batch)
        {
            throw new NotImplementedException(); // TODO
        }
    }
}
