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
using Microsoft.ServiceFabric.Data;
using Newtonsoft.Json;

namespace DurableTask.ServiceFabric
{
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;

    // Todo: Write a builder class for this immutable instead of creating intermediate objects for each operation for ex like in
    // SessionsProvider.CompleteAndUpdateSession
    [DataContract]
    public sealed class PersistentSession
    {
        static readonly IEnumerable<LockableTaskMessage> NoMessages = ImmutableList<LockableTaskMessage>.Empty;

        public PersistentSession(string sessionId, OrchestrationRuntimeState sessionState, LockableTaskMessage message)
            : this(sessionId, sessionState, new LockableTaskMessage[] {message}, null)
        {
        }

        public PersistentSession(string sessionId,
            OrchestrationRuntimeState sessionState,
            IEnumerable<LockableTaskMessage> messages,
            IEnumerable<LockableTaskMessage> scheduledMessages)
        {
            this.SessionId = sessionId;
            this.SessionState = sessionState;
            this.Messages = messages != null ? messages.ToImmutableList() : NoMessages;
            this.ScheduledMessages = scheduledMessages != null ? scheduledMessages.ToImmutableList() : NoMessages;
        }

        [DataMember]
        public string SessionId { get; private set; }

        public OrchestrationRuntimeState SessionState { get; private set; }

        [DataMember]
        private byte[] SerializedState { get; set; }

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
                this.Messages.ToImmutableList().Add(new LockableTaskMessage() {TaskMessage = message}),
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
                scheduledMessages.Add(new LockableTaskMessage() { TaskMessage = message });
            }
            return new PersistentSession(this.SessionId, this.SessionState, this.Messages, scheduledMessages);
        }

        // Todo: Can optimize in a few other ways, for now, something that works
        public bool CheckScheduledMessages(out PersistentSession newValue)
        {
            newValue = this;
            if (!this.ScheduledMessages.Any())
            {
                return false;
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
                newValue = new PersistentSession(this.SessionId, this.SessionState, newMessages, remainingScheduledMessages);
            }
            return changed;
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
            this.SessionState = DeserializeOrchestrationRuntimeState(this.SerializedState);
        }

        [OnSerializing]
        void OnSerializing(StreamingContext context)
        {
            this.SerializedState = SerializeOrchestrationRuntimeState(this.SessionState);
        }

        byte[] SerializeOrchestrationRuntimeState(OrchestrationRuntimeState runtimeState)
        {
            if (runtimeState == null)
            {
                return null;
            }

            string serializeState = JsonConvert.SerializeObject(runtimeState.Events,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
            return Encoding.UTF8.GetBytes(serializeState);
        }

        OrchestrationRuntimeState DeserializeOrchestrationRuntimeState(byte[] stateBytes)
        {
            if (stateBytes == null || stateBytes.Length == 0)
            {
                return null;
            }

            string serializedState = Encoding.UTF8.GetString(stateBytes);
            IList<HistoryEvent> events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedState, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
            return new OrchestrationRuntimeState(events);
        }
    }

    [DataContract]
    public class LockableTaskMessage
    {
        [DataMember]
        public TaskMessage TaskMessage;

        public bool Received; //not serialized, default value false
    }
}
