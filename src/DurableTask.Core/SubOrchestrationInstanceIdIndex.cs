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

#nullable enable
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core.History;

    internal sealed class SubOrchestrationInstanceIdIndex
    {
        Dictionary<int, PendingChild>? pendingTasks;
        Dictionary<string, PendingChild>? pendingInstances;
        bool hadConcurrentChildren;

        internal static SubOrchestrationInstanceIdIndex FromHistory(IEnumerable<HistoryEvent> history)
        {
            // Reduce completed history before allocating nodes: cold sequential replay needs
            // only a small temporary map, not one linked node for every historical child.
            Dictionary<int, string>? pending = null;
            foreach (HistoryEvent historyEvent in history)
            {
                switch (historyEvent)
                {
                    case SubOrchestrationInstanceCreatedEvent created
                        when created.InstanceId != null && !OrchestrationTags.IsTaggedAsFireAndForget(created.Tags):
                        pending ??= new Dictionary<int, string>();
                        pending[created.EventId] = created.InstanceId;
                        break;
                    case SubOrchestrationInstanceCompletedEvent completed:
                        pending?.Remove(completed.TaskScheduledId);
                        break;
                    case SubOrchestrationInstanceFailedEvent failed:
                        pending?.Remove(failed.TaskScheduledId);
                        break;
                }
            }

            var index = new SubOrchestrationInstanceIdIndex();
            if (pending != null)
            {
                foreach (KeyValuePair<int, string> child in pending)
                {
                    index.AddPending(child.Key, child.Value);
                }
            }

            return index;
        }

        internal bool TryGetPendingTaskId(string instanceId, out int taskId)
        {
            if (this.pendingInstances != null && this.pendingInstances.TryGetValue(instanceId, out PendingChild child))
            {
                taskId = child.TaskId;
                return true;
            }

            taskId = default;
            return false;
        }

        internal void AddEvent(HistoryEvent historyEvent)
        {
            switch (historyEvent)
            {
                case SubOrchestrationInstanceCreatedEvent created
                    when created.InstanceId != null && !OrchestrationTags.IsTaggedAsFireAndForget(created.Tags):
                    this.AddPending(created.EventId, created.InstanceId);
                    break;
                case SubOrchestrationInstanceCompletedEvent completed:
                    this.Remove(completed.TaskScheduledId);
                    break;
                case SubOrchestrationInstanceFailedEvent failed:
                    this.Remove(failed.TaskScheduledId);
                    break;
            }
        }

        void AddPending(int taskId, string instanceId)
        {
            this.Remove(taskId);
            this.pendingTasks ??= new Dictionary<int, PendingChild>();
            this.pendingInstances ??= new Dictionary<string, PendingChild>(StringComparer.Ordinal);
            this.pendingInstances.TryGetValue(instanceId, out PendingChild previous);
            var child = new PendingChild(taskId, instanceId) { Next = previous };
            if (previous != null)
            {
                previous.Previous = child;
            }

            this.pendingTasks.Add(child.TaskId, child);
            this.pendingInstances[child.InstanceId] = child;
            this.hadConcurrentChildren |= this.pendingTasks.Count > 1;
        }

        void Remove(int taskId)
        {
            if (this.pendingTasks == null || !this.pendingTasks.TryGetValue(taskId, out PendingChild child))
            {
                return;
            }

            if (child.Previous != null)
            {
                child.Previous.Next = child.Next;
            }
            else if (child.Next != null)
            {
                this.pendingInstances![child.InstanceId] = child.Next;
            }
            else
            {
                this.pendingInstances!.Remove(child.InstanceId);
            }

            if (child.Next != null)
            {
                child.Next.Previous = child.Previous;
            }

            this.pendingTasks.Remove(taskId);
            if (this.pendingTasks.Count == 0 && this.hadConcurrentChildren)
            {
                // Release drained fan-out capacity, but reuse the small buffers for sequential children.
                this.pendingTasks = null;
                this.pendingInstances = null;
                this.hadConcurrentChildren = false;
            }
        }

        // Legacy histories can contain several pending task IDs for one instance ID.
        // Links allow removal of any matching task in O(1), without a HashSet per child.
        sealed class PendingChild
        {
            internal PendingChild(int taskId, string instanceId)
            {
                this.TaskId = taskId;
                this.InstanceId = instanceId;
            }

            internal int TaskId { get; }

            internal string InstanceId { get; }

            internal PendingChild? Previous { get; set; }

            internal PendingChild? Next { get; set; }
        }
    }
}
