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
            IReadOnlyList<MessageData> initialMessageBatch,
            OrchestrationRuntimeState runtimeState,
            string eTag,
            TimeSpan idleTimeout,
            Guid traceActivityId)
            : base(storageAccountName, taskHubName, orchestrationInstance, traceActivityId)
        {
            this.idleTimeout = idleTimeout;
            this.CurrentMessageBatch = initialMessageBatch ?? throw new ArgumentNullException(nameof(initialMessageBatch));
            this.RuntimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
            this.ETag = eTag;

            this.messagesAvailableEvent = new AsyncAutoResetEvent(signaled: false);
            this.sessionReleasedEvent = new AsyncManualResetEvent();
            this.nextMessageBatch = new MessageCollection();
        }

        public IReadOnlyList<MessageData> CurrentMessageBatch { get; private set; }

        public OrchestrationRuntimeState RuntimeState { get; }

        public string ETag { get; set; }

        public IReadOnlyList<MessageData> PendingMessages => this.nextMessageBatch;

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
    }
}
