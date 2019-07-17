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

namespace DurableTask.EventHubs
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Azure.EventHubs;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    // TODO: Split this into an abstract base class so that it can be unit tested
    class MessageSender
    {
        readonly PartitionSender innerSender;

        public MessageSender(PartitionSender innerSender)
        {
            this.innerSender = innerSender ?? throw new ArgumentNullException(nameof(innerSender));
        }

        public Task SendSingleMessageAsync(TaskMessage message)
        {
            EventData eventData = this.CreateEventData(message);
            return this.innerSender.SendAsync(eventData);
        }

        EventData CreateEventData(TaskMessage message)
        {
            // CONSIDER: Use reflection over public HistoryEvent properties
            //           to populate the EventData properties, making this code
            //           more future-proof.
            EventData eventData;
            switch (message.Event)
            {
                case ContinueAsNewEvent @event:
                    eventData = CreateEventData(@event.Result);
                    // TODO: Headers
                    break;
                case EventRaisedEvent @event:
                    eventData = CreateEventData(@event.Input);
                    eventData.Properties["DTFx.Name"] = @event.Name;
                    break;
                case EventSentEvent @event:
                    eventData = CreateEventData(@event.Input);
                    break;
                case ExecutionCompletedEvent @event:
                    eventData = CreateEventData(@event.Result);
                    break;
                case ExecutionStartedEvent @event:
                    eventData = CreateEventData(@event.Input);
                    eventData.Properties["DTFx.Name"] = @event.Name;
                    eventData.Properties["DTFx.Version"] = @event.Version;
                    eventData.Properties["DTFx.Tags"] = @event.Tags;
                    if (@event.ParentInstance != null)
                    {
                        // TODO: Populate the parent instance info
                    }

                    break;
                case ExecutionTerminatedEvent @event:
                    eventData = CreateEventData(@event.Input);
                    break;
                // TODO: More events
                default:
                    throw new NotImplementedException($"Event type {message.Event.EventType} is not yet supported.");
            }

            // Common headers
            eventData.Properties["DTFx.EventType"] = message.Event.EventType.ToString();
            eventData.Properties["DTFx.InstanceId"] = message.OrchestrationInstance.InstanceId;
            eventData.Properties["DTFx.ExecutionId"] = message.OrchestrationInstance.ExecutionId;
            eventData.Properties["DTFx.EventId"] = Utils.GetTaskEventId(message.Event);
            eventData.Properties["DTFx.Timestamp"] = message.Event.Timestamp;
            return eventData;
        }

        static EventData CreateEventData(string payload)
        {
            // Most calls to this method should not involve any byte[] allocations
            int byteCount = Encoding.UTF8.GetByteCount(payload);
            byte[] buffer = SimpleBufferManager.Shared.TakeBuffer(byteCount);
            Encoding.UTF8.GetBytes(payload, 0, payload.Length, buffer, 0);
            return new EventData(new ArraySegment<byte>(buffer, 0, byteCount));
        }
    }
}
