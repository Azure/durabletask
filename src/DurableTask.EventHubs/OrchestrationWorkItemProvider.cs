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
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.EventHubs.Monitoring;
    using DurableTask.EventHubs.Tracking;
    using Microsoft.Azure.EventHubs;

    class OrchestrationWorkItemProvider : IEqualityComparer<OrchestrationInstance>
    {
        readonly KeyedAsyncQueue<OrchestrationInstance, TaskOrchestrationWorkItem> readyQueue;

        readonly EventHubListener listener;
        readonly ITrackingStore trackingStore;

        public OrchestrationWorkItemProvider(
            EventHubListener listener,
            ITrackingStore trackingStore,
            EventHubsOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats)
        {
            this.listener = listener;
            this.trackingStore = trackingStore;
        }

        async Task ListenLoop(CancellationToken cancellation)
        {
            while (!cancellation.IsCancellationRequested)
            {
                IEnumerable<EventData> batch = await this.listener.GetNextBatchAsync(cancellation);
                // TODO: Convert EventData messages into TaskMessage objects
                // TODO: Sort them into orchestration instance groups
                // TODO: Load or
            }
        }

        async void PrepareWorkItem(TaskMessage message)
        {
            TaskOrchestrationWorkItem workItem = await this.LoadStateAsync(ref message.OrchestrationInstance);
            workItem.NewMessages.Add(message);
            this.SetReady(workItem);
        }

        // NOTE: This method mutates the OrchestrationInstance parameter
        Task<TaskOrchestrationWorkItem> LoadStateAsync(ref OrchestrationInstance instance)
        {
            return this.readyQueue.GetOrAddAsync(
                instance,
                addCallback: async (instanceKey) =>
                {
                    OrchestrationHistory history = await this.trackingStore.GetHistoryEventsAsync(
                        instanceKey.InstanceId,
                        instanceKey.ExecutionId,
                        CancellationToken.None); // TODO: Wire up cancellation

                    var state = new OrchestrationRuntimeState(history.Events);
                    if (state.ExecutionStartedEvent != null)
                    {
                        // Some events (like EventRaisedEvent) don't have an execution ID but need to be 
                        // assigned to a specific orchestration that has an execution ID.
                        instanceKey.ExecutionId = state.OrchestrationInstance.ExecutionId;
                    }

                    // Create a work item with the fetched history and cache it in the ready queue.
                    // The caller will take care of adding messages to the work item until it gets
                    // processed.
                    return new TaskOrchestrationWorkItem
                    {
                        InstanceId = instanceKey.InstanceId,
                        OrchestrationRuntimeState = state,
                    };
                });
        }

        void SetReady(TaskOrchestrationWorkItem workItem)
        {

        }

        static TaskMessage ToTaskMessage(EventData data)
        {
            HistoryEvent historyEvent;
            if (data.Properties.TryGetValue("DTFx.EventType", out EventType type))
            {
                int eventId;
                if (!data.Properties.TryGetValue("DTFx.EventId", out eventId))
                {
                    eventId = -1;
                }

                switch (type)
                {
                    case EventType.ExecutionStarted:
                        string payload = Encoding.UTF8.GetString(
                            data.Body.Array,
                            data.Body.Offset,
                            data.Body.Count);
                        historyEvent = new ExecutionStartedEvent(eventId, payload);
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported event type {type}.")
                }
            }
            else
            {
                // If the event type isn't specified, we interpret it as a RaiseEvent.
                // TODO: Support non-strings
                string payload = Encoding.UTF8.GetString(
                    data.Body.Array,
                    data.Body.Offset,
                    data.Body.Count);
                historyEvent = new EventRaisedEvent(-1, payload);
            }

            return new TaskMessage
            {
                Event = historyEvent,
                OrchestrationInstance = null, // TODO
            };
        }

        bool IEqualityComparer<OrchestrationInstance>.Equals(
            OrchestrationInstance x,
            OrchestrationInstance y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (!string.Equals(x.InstanceId, y.InstanceId, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (string.IsNullOrEmpty(x.ExecutionId) || string.IsNullOrEmpty(y.ExecutionId))
            {
                return true;
            }

            return string.Equals(x.ExecutionId, y.ExecutionId, StringComparison.OrdinalIgnoreCase);
        }

        int IEqualityComparer<OrchestrationInstance>.GetHashCode(OrchestrationInstance instance)
        {
            // The InstanceId is never expected to change, which is why we use it and
            // not the ExecutionId for the GetHashCode implementation.
            return instance.InstanceId.GetHashCode();
        }
    }
}
