using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventHubs
{
    internal class EventProcessor : IEventProcessor
    {
        private readonly Backend.IHost host;
        private readonly Backend.ISender<Event> sender;
        private readonly Func<EventData, PartitionEvent> parseFunction;

        private Backend.IPartition partition;

        public EventProcessor(Backend.IHost host, Backend.ISender<Event> sender, Serializer serializer)
        {
            this.host = host;
            this.sender = sender;

            this.parseFunction = (EventData eventData) =>
            {
                var evt = serializer.DeserializeEvent(eventData.Body);
                evt.QueuePosition = eventData.SystemProperties.SequenceNumber;
                return (PartitionEvent) evt;
            };
            
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            uint partitionId = uint.Parse(context.PartitionId);
            this.partition = host.AddPartition(partitionId, this.sender);
            return this.partition.StartAsync();
        }

        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            return this.partition.StopAsync();
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            System.Diagnostics.Trace.TraceError($"exception in EventProcessor: {error}");
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            return this.partition.ProcessAsync(messages.Select(parseFunction));   
        }    
    }
}
