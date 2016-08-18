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

using System.Runtime.Serialization;
using System.Text;
using DurableTask.History;
using Newtonsoft.Json;

namespace DurableTask.ServiceFabric
{
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;

    [DataContract]
    public sealed class PersistentSession
    {
        static readonly IEnumerable<LockableTaskMessage> NoMessages = ImmutableList<LockableTaskMessage>.Empty;

        public PersistentSession(string sessionId, OrchestrationRuntimeState sessionState, LockableTaskMessage message)
            : this(sessionId, sessionState, new LockableTaskMessage[] { message})
        {
        }

        public PersistentSession(string sessionId, OrchestrationRuntimeState sessionState, IEnumerable<LockableTaskMessage> messages)
        {
            this.SessionId = sessionId;
            this.SessionState = sessionState;
            this.Messages = messages != null ? messages.ToImmutableList() : NoMessages;
        }

        [DataMember]
        public string SessionId { get; private set; }

        public OrchestrationRuntimeState SessionState { get; private set; }

        [DataMember]
        private byte[] SerializedState { get; set; }

        [DataMember]
        public IEnumerable<LockableTaskMessage> Messages { get; private set; }

        public PersistentSession SetSessionState(OrchestrationRuntimeState newState)
        {
            return new PersistentSession(this.SessionId, newState, this.Messages);
        }

        public PersistentSession AppendMessage(TaskMessage message)
        {
            return new PersistentSession(this.SessionId, this.SessionState, this.Messages.ToImmutableList().Add(new LockableTaskMessage() {TaskMessage = message}));
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
            return new PersistentSession(this.SessionId, this.SessionState, newMessages);
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
