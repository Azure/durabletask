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
    using System.Threading.Tasks;
    using DurableTask.Core;

    sealed class OrchestrationSession : SessionBase, IOrchestrationSession
    {
        readonly TimeSpan idleTimeout;

        readonly Func<OrchestrationInstance, List<MessageData>> fetchMessagesCallback;
        readonly AsyncAutoResetEvent messagesAvailableEvent;

        public OrchestrationSession(
            string storageAccountName,
            string taskHubName,
            OrchestrationInstance orchestrationInstance,
            IReadOnlyList<MessageData> initialMessageBatch,
            Func<OrchestrationInstance, List<MessageData>> fetchMessagesCallback,
            TimeSpan idleTimeout,
            Guid traceActivityId)
            : base(storageAccountName, taskHubName, orchestrationInstance, traceActivityId)
        {
            this.idleTimeout = idleTimeout;
            this.CurrentMessageBatch = initialMessageBatch;
            this.fetchMessagesCallback = fetchMessagesCallback;
            this.messagesAvailableEvent = new AsyncAutoResetEvent(signaled: false);
        }

        public IReadOnlyList<MessageData> CurrentMessageBatch { get; private set; }

        public void Notify()
        {
            // Force running asynchronously to avoid blocking the main dispatch thread.
            Task.Run(() => this.messagesAvailableEvent.Set());
        }

        public async Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(
            TaskOrchestrationWorkItem workItem)
        {
            if (!await this.messagesAvailableEvent.WaitAsync(this.idleTimeout))
            {
                return null;
            }

            this.CurrentMessageBatch = this.fetchMessagesCallback.Invoke(this.Instance);
            if (this.CurrentMessageBatch == null)
            {
                return null;
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
