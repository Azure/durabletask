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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor
    {
        private readonly Backend.IHost host;
        private readonly Backend.ISender sender;

        private Backend.IPartition partition;

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(Backend.IHost host, Backend.ISender sender)
        {
            this.host = host;
            this.sender = sender;
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            uint partitionId = uint.Parse(context.PartitionId);
            this.partition = host.AddPartition(partitionId, new MemoryStorage(), this.sender);
            return this.partition.StartAsync();
        }

        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            return this.partition.StopAsync();
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            partition.ReportError("Exception in EventProcessor", error);
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach(var eventData in messages)
            {
                var evt = Serializer.DeserializeEvent(eventData.Body);
                evt.QueuePosition = eventData.SystemProperties.SequenceNumber;
                await this.partition.ProcessAsync((PartitionEvent) evt);
            }
        }    
    }
}
