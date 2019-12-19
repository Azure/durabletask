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

namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    sealed class OrchestrationSession : SessionBase, IOrchestrationSession
    {
        readonly TimeSpan idleTimeout;

        readonly AsyncAutoResetEvent messagesAvailableEvent;
        readonly MessageCollection nextMessageBatch;

        public OrchestrationSession(
            string storageAccountName,
            string taskHubName,
            OrchestrationInstance orchestrationInstance,
            ControlQueue controlQueue,
            List<MessageData> initialMessageBatch,
            OrchestrationRuntimeState runtimeState,
            string eTag,
            DateTime lastCheckpointTime,
            TimeSpan idleTimeout,
            Guid traceActivityId)
            : base(storageAccountName, taskHubName, orchestrationInstance, traceActivityId)
        {
            this.idleTimeout = idleTimeout;
            this.ControlQueue = controlQueue ?? throw new ArgumentNullException(nameof(controlQueue));
            this.CurrentMessageBatch = initialMessageBatch ?? throw new ArgumentNullException(nameof(initialMessageBatch));
            this.RuntimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
            this.ETag = eTag;
            this.LastCheckpointTime = lastCheckpointTime;

            this.messagesAvailableEvent = new AsyncAutoResetEvent(signaled: false);
            this.nextMessageBatch = new MessageCollection();
        }

        public ControlQueue ControlQueue { get; }

        public List<MessageData> CurrentMessageBatch { get; private set; }

        public MessageCollection DeferredMessages { get; } = new MessageCollection();

        public MessageCollection DiscardedMessages { get; } = new MessageCollection();

        public OrchestrationRuntimeState RuntimeState { get; private set; }

        public string ETag { get; set; }

        public DateTime LastCheckpointTime { get; }

        public IReadOnlyList<MessageData> PendingMessages => this.nextMessageBatch;

        public override int GetCurrentEpisode()
        {
            // RuntimeState is mutable, so we cannot cache the current episode number.
            return Utils.GetEpisodeNumber(this.RuntimeState);
        }

        public void AddOrReplaceMessages(IEnumerable<MessageData> messages)
        {
            lock (this.nextMessageBatch)
            {
                foreach (MessageData message in messages)
                {
                    this.nextMessageBatch.AddOrReplace(message);
                }

                // Force running asynchronously to avoid blocking the main dispatch thread.
                Task.Run(() => this.messagesAvailableEvent.Set());
            }
        }

        // Called by the DTFx dispatcher thread
        public async Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(
            TaskOrchestrationWorkItem workItem)
        {
            if (!await this.messagesAvailableEvent.WaitAsync(this.idleTimeout))
            {
                return null; // timed-out
            }

            this.StartNewLogicalTraceScope();

            lock (this.nextMessageBatch)
            {
                this.CurrentMessageBatch = this.nextMessageBatch.ToList();
                this.nextMessageBatch.Clear();
            }

            var messages = new List<TaskMessage>(this.CurrentMessageBatch.Count);
            foreach (MessageData msg in this.CurrentMessageBatch)
            {
                this.TraceProcessingMessage(msg, isExtendedSession: true);
                messages.Add(msg.TaskMessage);
            }

            return messages;
        }

        public void UpdateRuntimeState(OrchestrationRuntimeState runtimeState)
        {
            this.RuntimeState = runtimeState;
            this.Instance = runtimeState.OrchestrationInstance;
        }

        internal void DeferMessage(MessageData message)
        {
            this.DeferredMessages.AddOrReplace(message);
        }

        internal void DiscardMessage(MessageData data)
        {
            this.DiscardedMessages.AddOrReplace(data);
        }

        internal bool IsOutOfOrderMessage(MessageData message)
        {
            if (this.IsNonexistantInstance() && message.OriginalQueueMessage.DequeueCount > 5)
            {
                // The first five times a message for a nonexistant instance is dequeued, give the message the benefit
                // of the doubt and assume that the instance hasn't had its history table populated yet. After the 
                // fifth execution, ~30 seconds have passed and the most likely scenario is that this is a zombie event. 
                // This means the history table for the message's orchestration no longer exists, either due to an explicit 
                // PurgeHistory request or due to a ContinueAsNew call cleaning the old execution's history.
                return false;
            }

            if (message.Sender?.InstanceId != this.Instance.InstanceId)
            {
                // Only messages sent and received within an orchestration instance require ordering.
                // Message sender metadata was added only after v1.6.4 and may be null for data created
                // in earlier versions.
                return false;
            }

            if (this.LastCheckpointTime > message.TaskMessage.Event.Timestamp)
            {
                // LastCheckpointTime represents the time at which the most recent history checkpoint completed.
                // The checkpoint is written to the history table only *after* all queue messages are sent.
                // A message is out of order when its timestamp *preceeds* the most recent checkpoint timestamp.
                // In this case, we see that the checkpoint came *after* the message, so there is no out-of-order
                // concern. Note that this logic only applies for messages sent by orchestrations to themselves.
                // The next check considers the other cases (activities, sub-orchestrations, etc.).
                // Orchestration checkpoint time information was added only after v1.6.4.
                return false;
            }

            if (Utils.TryGetTaskScheduledId(message.TaskMessage.Event, out int taskScheduledId))
            {
                // This message is a response to a task. Search the history to make sure that we've recorded the fact that
                // this task was scheduled.
                HistoryEvent mostRecentTaskEvent = this.RuntimeState.Events.LastOrDefault(e => e.EventId == taskScheduledId);
                if (mostRecentTaskEvent != null)
                {
                    return false;
                }
            }

            // The message is out of order and cannot be handled by the current session.
            return true;
        }

        bool IsNonexistantInstance()
        {
            return this.RuntimeState.Events.Count == 0 || this.RuntimeState.ExecutionStartedEvent == null;
        }
    }
}
