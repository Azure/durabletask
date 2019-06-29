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
        readonly AsyncManualResetEvent sessionReleasedEvent;
        readonly MessageCollection nextMessageBatch;

        public OrchestrationSession(
            string storageAccountName,
            string taskHubName,
            OrchestrationInstance orchestrationInstance,
            ControlQueue controlQueue,
            IList<MessageData> initialMessageBatch,
            OrchestrationRuntimeState runtimeState,
            string eTag,
            TimeSpan idleTimeout,
            Guid traceActivityId)
            : base(storageAccountName, taskHubName, orchestrationInstance, traceActivityId)
        {
            this.idleTimeout = idleTimeout;
            this.ControlQueue = controlQueue ?? throw new ArgumentNullException(nameof(controlQueue));
            this.CurrentMessageBatch = initialMessageBatch ?? throw new ArgumentNullException(nameof(initialMessageBatch));
            this.RuntimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
            this.ETag = eTag;

            this.messagesAvailableEvent = new AsyncAutoResetEvent(signaled: false);
            this.sessionReleasedEvent = new AsyncManualResetEvent();
            this.nextMessageBatch = new MessageCollection();
        }

        public ControlQueue ControlQueue { get; }

        public IList<MessageData> CurrentMessageBatch { get; private set; }

        public OrchestrationRuntimeState RuntimeState { get; private set; }

        public string ETag { get; set; }

        public IReadOnlyList<MessageData> PendingMessages => this.nextMessageBatch;

        public override int GetCurrentEpisode()
        {
            // RuntimeState is mutable, so we cannot cache the current episode number.
            return Utils.GetEpisodeNumber(this.RuntimeState);
        }

        public IEnumerable<MessageData> AddOrReplaceMessages(IEnumerable<MessageData> messages)
        {
            lock (this.nextMessageBatch)
            {
                List<MessageData> replacingMessages = null;
                foreach (MessageData message in messages)
                {
                    if (!this.nextMessageBatch.AddOrReplace(message))
                    {
                        if (replacingMessages == null)
                        {
                            replacingMessages = new List<MessageData>();
                        }

                        replacingMessages.Add(message);
                    }
                }

                // Force running asynchronously to avoid blocking the main dispatch thread.
                Task.Run(() => this.messagesAvailableEvent.Set());

                return replacingMessages ?? Enumerable.Empty<MessageData>();
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

            lock (this.nextMessageBatch)
            {
                this.CurrentMessageBatch = this.nextMessageBatch.ToArray();
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

            int taskScheduledId = Utils.GetTaskEventId(message.TaskMessage.Event);
            if (taskScheduledId < 0)
            {
                // This message does not require ordering (RaiseEvent, ExecutionStarted, Terminate, etc.).
                return false;
            }

            // This message is a response to a task. Search the history to make sure that we've recorded the fact that
            // this task was scheduled. We don't have the luxery of transactions between queues and tables, so queue
            // messages are always written before we update the history in table storage. This means that in some
            // cases the response message could get picked up before we're ready for it.
            HistoryEvent mostRecentTaskEvent = this.RuntimeState.Events.LastOrDefault(e => e.EventId == taskScheduledId);
            if (mostRecentTaskEvent != null)
            {
                return false;
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
