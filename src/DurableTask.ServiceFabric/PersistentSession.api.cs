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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using DurableTask.History;

    public sealed partial class PersistentSession
    {
        // Todo: Can optimize in a few other ways, for now, something that works
        public PersistentSession FireScheduledMessages()
        {
            var messagesBuilder = this.Messages.ToBuilder();
            var remainingScheduledMessagesBuilder = ImmutableList<ReceivableTaskMessage>.Empty.ToBuilder();

            bool changed = false;
            var currentTime = DateTime.UtcNow;

            foreach (var scheduledMessage in this.ScheduledMessages)
            {
                var timerEvent = scheduledMessage.TaskMessage.Event as TimerFiredEvent;

                if (timerEvent == null)
                {
                    //Should not happen with the current assumptions
                    throw new Exception("Internal server error.");
                }

                if (timerEvent.FireAt <= currentTime)
                {
                    messagesBuilder.Add(scheduledMessage);
                    changed = true;
                }
                else
                {
                    remainingScheduledMessagesBuilder.Add(scheduledMessage);
                }
            }

            if (changed)
            {
                return Create(this.SessionId, this.SessionState, messagesBuilder.ToImmutableList(), remainingScheduledMessagesBuilder.ToImmutableList());
            }

            return this;
        }

        public PersistentSession ReceiveMessages()
        {
            var newMessages = this.Messages.ToBuilder().ConvertAll(m => ReceivableTaskMessage.Create(m.TaskMessage, isReceived: true));
            return Create(this.SessionId, this.SessionState, newMessages, this.ScheduledMessages);
        }

        public PersistentSession CompleteMessages(OrchestrationRuntimeState newState, IList<TaskMessage> addedScheduledMessages)
        {
            var newMessages = this.Messages.RemoveAll(m => m.IsReceived);
            var newSessionState = newState?.Events.ToImmutableList();
            var newScheduledMessages = this.ScheduledMessages;
            if (addedScheduledMessages?.Count > 0)
            {
                newScheduledMessages = this.ScheduledMessages.AddRange(addedScheduledMessages.Select(tm => ReceivableTaskMessage.Create(tm)));
            }
            return Create(this.SessionId, newSessionState, newMessages, newScheduledMessages);
        }

        public PersistentSession AppendMessage(TaskMessage message)
        {
            var newMessages = this.Messages.Add(ReceivableTaskMessage.Create(message));
            return Create(this.SessionId, this.SessionState, newMessages, this.ScheduledMessages);
        }

        public PersistentSession AppendMessageBatch(IEnumerable<TaskMessage> addedMessages)
        {
            var newMessages = this.Messages.AddRange(addedMessages.Select(m => ReceivableTaskMessage.Create(m)));
            return Create(this.SessionId, this.SessionState, newMessages, this.ScheduledMessages);
        }

        public static PersistentSession CreateWithNewMessage(string sessionId, TaskMessage newMessage)
        {
            var newMessages = ImmutableList<ReceivableTaskMessage>.Empty.Add(ReceivableTaskMessage.Create(newMessage));
            return Create(sessionId, sessionState: null, messages: newMessages, scheduledMessages: null);
        }

        public static PersistentSession CreateWithNewMessages(string sessionId, IEnumerable<TaskMessage> allMessages)
        {
            var newMessages = ImmutableList<ReceivableTaskMessage>.Empty.AddRange(allMessages.Select(newMessage => ReceivableTaskMessage.Create(newMessage)));
            return Create(sessionId, sessionState: null, messages: newMessages, scheduledMessages: null);
        }
    }
}
