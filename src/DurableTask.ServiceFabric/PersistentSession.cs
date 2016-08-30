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
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.History;
using DurableTask.Serializing;
using Microsoft.ServiceFabric.Data;
using Newtonsoft.Json;

namespace DurableTask.ServiceFabric
{
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;

    // Todo: Write a builder class for this immutable instead of creating intermediate objects for each operation for ex like in
    // SessionsProvider.CompleteAndUpdateSession.
    [DataContract]
    public sealed class PersistentSession
    {
        static readonly IEnumerable<LockableTaskMessage> NoMessages = ImmutableList<LockableTaskMessage>.Empty;
        static readonly DataConverter DatConverter = new JsonDataConverter();

        public PersistentSession(string sessionId)
            : this(sessionId, null, null, null)
        {
        }

        public PersistentSession(string sessionId, TaskMessage newMessage)
            : this(sessionId, new LockableTaskMessage(newMessage), null)
        {
        }

        public PersistentSession(string sessionId, LockableTaskMessage message, OrchestrationRuntimeState sessionState)
            : this(sessionId, sessionState, new LockableTaskMessage[] {message}, null)
        {
        }

        public PersistentSession(string sessionId,
            OrchestrationRuntimeState sessionState,
            IEnumerable<LockableTaskMessage> messages,
            IEnumerable<LockableTaskMessage> scheduledMessages)
        {
            this.SessionId = sessionId;
            this.SessionState = sessionState ?? new OrchestrationRuntimeState();
            this.Messages = messages != null ? messages.ToImmutableList() : NoMessages;
            this.ScheduledMessages = scheduledMessages != null ? scheduledMessages.ToImmutableList() : NoMessages;
        }

        [DataMember]
        public string SessionId { get; private set; }

        public OrchestrationRuntimeState SessionState { get; private set; }

        //Todo: Is this performant? Perhaps consider json serialization instead?
        [DataMember]
        private IList<HistoryEvent> SerializedState { get; set; }

        [DataMember]
        public IEnumerable<LockableTaskMessage> Messages { get; private set; }

        [DataMember]
        public IEnumerable<LockableTaskMessage> ScheduledMessages { get; private set; }

        public PersistentSession SetSessionState(OrchestrationRuntimeState newState)
        {
            return new PersistentSession(this.SessionId, newState, this.Messages, this.ScheduledMessages);
        }

        public PersistentSession AppendMessage(TaskMessage message)
        {
            return new PersistentSession(this.SessionId,
                this.SessionState,
                this.Messages.ToImmutableList().Add(new LockableTaskMessage(message)),
                this.ScheduledMessages);
        }

        public PersistentSession AppendMessageBatch(IEnumerable<TaskMessage> messages)
        {
            return new PersistentSession(this.SessionId,
                this.SessionState,
                this.Messages.ToImmutableList().AddRange(messages.Select(m => new LockableTaskMessage(m))),
                this.ScheduledMessages);
        }

        public PersistentSession AppendScheduledMessagesBatch(IEnumerable<TaskMessage> newMessages)
        {
            var scheduledMessages = this.ScheduledMessages.ToList();
            foreach (var message in newMessages)
            {
                if (!(message.Event is TimerFiredEvent))
                {
                    throw new ArgumentException("Expecting a scheduled message");
                }
                scheduledMessages.Add(new LockableTaskMessage(message));
            }
            return new PersistentSession(this.SessionId, this.SessionState, this.Messages, scheduledMessages);
        }

        // Todo: Can optimize in a few other ways, for now, something that works
        public PersistentSession FireScheduledMessages()
        {
            if (!this.ScheduledMessages.Any())
            {
                return this;
            }

            bool changed = false;
            var currentTime = DateTime.UtcNow;
            var newMessages = this.Messages.ToList();
            var remainingScheduledMessages = new List<LockableTaskMessage>();

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
                    newMessages.Add(scheduledMessage);
                    changed = true;
                }
                else
                {
                    remainingScheduledMessages.Add(scheduledMessage);
                }
            }

            if (changed)
            {
                return new PersistentSession(this.SessionId, this.SessionState, newMessages, remainingScheduledMessages);
            }

            return this;
        }

        public List<TaskMessage> ReceiveMessages()
        {
            // Todo: Need to be thread-safe?
            var result = new List<TaskMessage>();
            foreach (var m in this.Messages)
            {
                m.Received = true;
                result.Add(m.TaskMessage);
            }
            return result;
        }

        public PersistentSession CompleteMessages()
        {
            //Todo: Need to be thread-safe?
            var newMessages = this.Messages.Where(m => !m.Received);
            return new PersistentSession(this.SessionId, this.SessionState, newMessages, this.ScheduledMessages);
        }

        [OnDeserialized]
        void OnDeserialized(StreamingContext context)
        {
            this.Messages = this.Messages.ToImmutableList();
            this.SessionState = new OrchestrationRuntimeState(this.SerializedState);
        }

        [OnSerializing]
        void OnSerializing(StreamingContext context)
        {
            this.SerializedState = this.SessionState?.Events;
        }
    }

    [DataContract]
    public class LockableTaskMessage
    {
        public LockableTaskMessage(TaskMessage message)
        {
            this.TaskMessage = message;
        }

        [DataMember]
        public TaskMessage TaskMessage { get; private set; }

        public bool Received; //not serialized, default value false
    }
}
