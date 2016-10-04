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
        public PersistentSession FireScheduledMessages()
        {
            var messagesBuilder = this.Messages.ToBuilder();
            var scheduledMessagesBuilder = this.ScheduledMessages.ToBuilder();

            bool changed = false;
            var currentTime = DateTime.UtcNow;

            while (scheduledMessagesBuilder.Count > 0)
            {
                var firstPendingMessage = scheduledMessagesBuilder.Min;
                var timerEvent = firstPendingMessage.Event as TimerFiredEvent;

                if (timerEvent == null)
                {
                    //Should not happen with the current assumptions
                    throw new Exception("Internal server error.");
                }

                if (timerEvent.FireAt <= currentTime)
                {
                    messagesBuilder.Add(ReceivableTaskMessage.Create(firstPendingMessage));
                    scheduledMessagesBuilder.Remove(firstPendingMessage);
                    changed = true;
                }
                else
                {
                    break;
                }
            }

            if (changed)
            {
                return Create(this.SessionId, this.SessionState, messagesBuilder.ToImmutableList(), scheduledMessagesBuilder.ToImmutableSortedSet());
            }

            return this;
        }

        public bool HasScheduledMessagesDue(DateTime threshHoldTime)
        {
            if (this.ScheduledMessages.Count > 0)
            {
                var firstPendingMessage = this.ScheduledMessages.Min;
                var timerEvent = firstPendingMessage.Event as TimerFiredEvent;

                if (timerEvent != null && timerEvent.FireAt <= threshHoldTime)
                {
                    return true;
                }
            }

            return false;
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
            var newScheduledMessagesBuilder = this.ScheduledMessages.ToBuilder();
            if (addedScheduledMessages?.Count > 0)
            {
                foreach (var addedScheduledMessage in addedScheduledMessages)
                {
                    newScheduledMessagesBuilder.Add(addedScheduledMessage);
                }
            }
            return Create(this.SessionId, newSessionState, newMessages, newScheduledMessagesBuilder.ToImmutableSortedSet());
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
