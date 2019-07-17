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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;

    class OrchestrationWorkItemProducer : IProducerConsumerCollection
    {
        readonly AsyncQueue<WorkItem> workItems;

        public OrchestrationWorkItemProducer()
        {
            this.workItems = new AsyncQueue<WorkItem>();
        }

        public async Task<TaskOrchestrationWorkItem> TakeAsync(
            CancellationToken cancellationToken)
        {
            return await this.workItems.DequeueAsync(cancellationToken);
        }

        class OrchestrationEventProcessor : IEventProcessor
        {
            readonly KeyedAsyncQueue<string, TaskOrchestrationWorkItem> pendingQueue;
            readonly AsyncQueue<TaskOrchestrationWorkItem> readyQueue;

            public OrchestrationEventProcessor(AsyncQueue<TaskOrchestrationWorkItem> readyQueue)
            {
                this.readyQueue = readyQueue;
                this.pendingQueue = new KeyedAsyncQueue<string, TaskOrchestrationWorkItem>(
                    item => item.InstanceId,
                    StringComparer.OrdinalIgnoreCase);
            }

            public Task CloseAsync(PartitionContext context, CloseReason reason)
            {
                return Task.CompletedTask;
            }

            public Task OpenAsync(PartitionContext context)
            {
                return Task.CompletedTask;
            }

            public Task ProcessErrorAsync(PartitionContext context, Exception error)
            {
                // TODO: Tracing
                return Task.CompletedTask;
            }

            public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> batch)
            {
                foreach (EventData data in batch)
                {
                    TaskMessage message = ToTaskMessage(data);
                    if (this.TryFindWorkItem(message, out TaskOrchestrationWorkItem workItem))
                    {
                        // This is the fast path: the work item has already been loaded into
                        // memory and we can just add a message to it.
                        workItem.NewMessages.Add(message);
                    }
                    else
                    {
                        // This is the slow path: the work item hasn't yet been loaded.
                        // We need to create one and then fetch the history for the corresponding instance.
                        this.CreateAndInitializeWorkItemInBackground(message);
                    }
                }

                return Task.CompletedTask;
            }

            bool TryFindWorkItem(TaskMessage message, out TaskOrchestrationWorkItem workItem)
            {
                string instanceId = message.OrchestrationInstance.InstanceId;
                return this.pendingQueue.TryGetValue(instanceId, out workItem);
            }

            static bool IsMatch(TaskMessage message, WorkItem workItem)
            {
                OrchestrationInstance instance = message.OrchestrationInstance;

                // TODO: How do we safely determine the execution ID for a work item?
            }

            void CreateAndInitializeWorkItemInBackground(TaskMessage message)
            {
                // Check to see whether a fetch is already happening for this
            }

            // TODO: Unit testing
            TaskMessage ToTaskMessage(EventData eventData)
            {
                OrchestrationInstance instance;
                HistoryEvent historyEvent;
                
                return new TaskMessage
                {
                    OrchestrationInstance = instance,
                    Event = historyEvent,
                    SequenceNumber = eventData.SystemProperties.SequenceNumber,
                };
            }


        }

        class MessageBatch
        {
            readonly List<EventHubTaskMessage> messages = new List<EventHubTaskMessage>();
            OrchestrationRuntimeState runtimeState;

            public void Append(EventHubTaskMessage message)
            {
                this.messages.Add(message);
            }

            public TaskOrchestrationWorkItem ToWorkItem()
            {
                // Filter out any messages which do not apply 
                IList<TaskMessage> newMessages = this.GetFinalMessageList().ToList<TaskMessage>();

                return new TaskOrchestrationWorkItem
                {
                    InstanceId = this.runtimeState.OrchestrationInstance.InstanceId,
                    OrchestrationRuntimeState = this.runtimeState,
                    NewMessages = newMessages,
                };
            }

            IEnumerable<EventHubTaskMessage> GetFinalMessageList()
            {
                OrchestrationInstance currentInstance = this.runtimeState.OrchestrationInstance;

                // Remove any messages which don't belong with this instance.
                // TODO: Add messages for auto-instance creation.
                // CONSIDER: Other filters, such as de-dup
                return this.messages.Where(message => IsMatch(message, currentInstance));
            }

            static bool IsMatch(TaskMessage message, OrchestrationInstance currentInstance)
            {
                OrchestrationInstance targetInstance = message.OrchestrationInstance;
                if (string.Equals(currentInstance.InstanceId, targetInstance.InstanceId, StringComparison.OrdinalIgnoreCase))
                {
                    return
                        string.IsNullOrEmpty(targetInstance.ExecutionId) ||
                        string.Equals(currentInstance.ExecutionId, targetInstance.ExecutionId, StringComparison.OrdinalIgnoreCase);
                }

                return false;
            }
        }

        class EventHubTaskMessage : TaskMessage
        {
            readonly EventData eventData;

            public EventHubTaskMessage(EventData eventData)
            {
                this.eventData = eventData ?? throw new ArgumentNullException(nameof(eventData));

                this.OrchestrationInstance = GetOrchestrationInstance(eventData);
                this.Event = GetHistoryEvent(eventData);
                this.SequenceNumber = eventData.SystemProperties.SequenceNumber;
            }

            // TODO: Unit testing
            static OrchestrationInstance GetOrchestrationInstance(EventData eventData)
            {

            }

            // TODO: Unit testing
            static HistoryEvent GetHistoryEvent(EventData eventData)
            {
                // TODO: How do we determine the history type if this is an
                //       externally generated eventData?
            }
        }

        class EventHubsWorkItem : TaskOrchestrationWorkItem
        {
            public void AddEvents
        }
    }
}
