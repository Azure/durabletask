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
            var messagesBuilder = this.Messages.ToImmutableList().ToBuilder();
            var remainingScheduledMessagesBuilder = ImmutableList<ReceivableTaskMessage>.Empty.ToBuilder();

            bool changed = false;
            var currentTime = DateTime.UtcNow;

            foreach (var scheduledMessage in this.ScheduledMessages)
            {
                var timerEvent = scheduledMessage.TaskMessage.Event as TimerFiredEvent;

                if (timerEvent == null)
                {
                    //Should not happen with the current assumptions
                    throw new Exception("Internal server errors");
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
                return Create(this.SessionId, this.SessionState.ToImmutableList(), messagesBuilder.ToImmutableList(), remainingScheduledMessagesBuilder.ToImmutableList(), this.IsLocked);
            }

            return this;
        }

        public PersistentSession ReceiveMessages()
        {
            var messages = this.Messages.ToImmutableList().ToBuilder().ConvertAll(m => ReceivableTaskMessage.Create(m.TaskMessage, isReceived: true));
            return Create(this.SessionId, this.SessionState.ToImmutableList(), messages, this.ScheduledMessages.ToImmutableList(), isLocked: true);
        }

        public PersistentSession CompleteMessages(OrchestrationRuntimeState newState, IList<TaskMessage> newScheduledMessages)
        {
            var messages = this.Messages.ToImmutableList().RemoveAll(m => m.IsReceived);
            var sessionState = newState?.Events.ToImmutableList();
            var scheduledMessages = this.ScheduledMessages;
            if (newScheduledMessages?.Count > 0)
            {
                scheduledMessages = this.ScheduledMessages.ToImmutableList().AddRange(newScheduledMessages.Select(tm => ReceivableTaskMessage.Create(tm)));
            }
            return Create(this.SessionId, sessionState, messages, scheduledMessages.ToImmutableList(), isLocked: false);
        }

        public PersistentSession AppendMessage(TaskMessage message)
        {
            var messages = this.Messages.ToImmutableList().Add(ReceivableTaskMessage.Create(message));
            return Create(this.SessionId, this.SessionState.ToImmutableList(), messages, this.ScheduledMessages.ToImmutableList(), this.IsLocked);
        }

        public PersistentSession AppendMessageBatch(IEnumerable<TaskMessage> newMessages)
        {
            var messages = this.Messages.ToImmutableList().AddRange(newMessages.Select(m => ReceivableTaskMessage.Create(m)));
            return Create(this.SessionId, this.SessionState.ToImmutableList(), messages, this.ScheduledMessages.ToImmutableList(), this.IsLocked);
        }

        void UnlockSession(Builder builder)
        {
            builder.IsLocked = false;
            builder.ShouldAddToQueue = builder.Messages.Any();
        }

        void LockSession(Builder builder)
        {
            builder.IsLocked = true;
            builder.ShouldAddToQueue = false;
        }

        void SetShouldAddToQueue(Builder builder)
        {
            if (!builder.IsLocked)
            {
                if (builder.ShouldAddToQueue)
                {
                    builder.ShouldAddToQueue = false;
                }
                else
                {
                    builder.ShouldAddToQueue = builder.Messages.Any();
                }
            }
        }

        public static PersistentSession CreateWithNewMessage(string sessionId, TaskMessage newMessage)
        {
            var messages = ImmutableList<ReceivableTaskMessage>.Empty.Add(ReceivableTaskMessage.Create(newMessage));
            return PersistentSession.Create(sessionId, sessionState: null, messages: messages, scheduledMessages: null, isLocked: false);
        }

        public static PersistentSession CreateWithNewMessages(string sessionId, IEnumerable<TaskMessage> newMessages)
        {
            var messages = ImmutableList<ReceivableTaskMessage>.Empty.AddRange(newMessages.Select(newMessage => ReceivableTaskMessage.Create(newMessage)));
            return PersistentSession.Create(sessionId, sessionState: null, messages: messages, scheduledMessages: null, isLocked: false);
        }
    }
}
