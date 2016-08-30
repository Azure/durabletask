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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using DurableTask.History;

namespace DurableTask.ServiceFabric
{
    public sealed partial class PersistentSession2
    {
        // Todo: Can optimize in a few other ways, for now, something that works
        public PersistentSession2 FireScheduledMessages()
        {
            var builder = this.ToBuilder();
            var messagesBuilder = builder.Messages.ToBuilder();
            var scheduledMessagesBuilder = builder.ScheduledMessages.ToBuilder();

            var currentTime = DateTime.UtcNow;

            foreach (var scheduledMessage in builder.ScheduledMessages)
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
                    scheduledMessagesBuilder.Remove(scheduledMessage);
                }
            }

            builder.Messages = messagesBuilder.ToImmutable();
            builder.ScheduledMessages = scheduledMessagesBuilder.ToImmutable();

            return builder.ToImmutable();
        }

        public PersistentSession2 ReceiveMessages()
        {
            var builder = this.ToBuilder();
            //Todo: Experiment (measure memory for ConvertAll on builder vs SelectMany on immutable
            builder.Messages = builder.Messages.ToBuilder().ConvertAll(m => m.With(isReceived: true));
            return builder.ToImmutable();
        }

        public PersistentSession2 CompleteMessages(OrchestrationRuntimeState newState, IList<TaskMessage> newScheduledMessages)
        {
            var builder = this.ToBuilder();
            builder.Messages = builder.Messages.RemoveAll(m => m.IsReceived);
            builder.SessionState = newState?.Events.ToImmutableList();
            if (newScheduledMessages?.Count > 0)
            {
                builder.ScheduledMessages = builder.ScheduledMessages.AddRange(newScheduledMessages.Select(tm => ReceivableTaskMessage.Create(tm)));
            }
            return builder.ToImmutable();
        }

        public PersistentSession2 AppendMessage(TaskMessage message)
        {
            var builder = this.ToBuilder();
            builder.Messages = builder.Messages.Add(ReceivableTaskMessage.Create(message));
            return builder.ToImmutable();
        }

        public PersistentSession2 AppendMessageBatch(IEnumerable<TaskMessage> newMessages)
        {
            var builder = this.ToBuilder();
            builder.Messages = builder.Messages.AddRange(newMessages.Select(m => ReceivableTaskMessage.Create(m)));
            return builder.ToImmutable();
        }

        public static PersistentSession2 CreateWithNewMessage(string sessionId, TaskMessage newMessage)
        {
            var messages = ImmutableList<ReceivableTaskMessage>.Empty.Add(ReceivableTaskMessage.Create(newMessage));
            return PersistentSession2.Create(sessionId, messages: messages);
        }

        public static PersistentSession2 CreateWithNewMessages(string sessionId, IEnumerable<TaskMessage> newMessages)
        {
            var messages = ImmutableList<ReceivableTaskMessage>.Empty.AddRange(newMessages.Select(newMessage => ReceivableTaskMessage.Create(newMessage)));
            return PersistentSession2.Create(sessionId, messages: messages);
        }
    }
}
