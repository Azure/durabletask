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
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;

    public class PersistentSession
    {
        public PersistentSession(string sessionId, OrchestrationRuntimeState sessionState, IImmutableList<LockableTaskMessage> messages)
        {
            this.SessionId = sessionId;
            this.SessionState = sessionState;
            this.Messages = messages ?? ImmutableList<LockableTaskMessage>.Empty;
        }

        public string SessionId { get; }

        public OrchestrationRuntimeState SessionState { get; }

        public IImmutableList<LockableTaskMessage> Messages { get; }

        public PersistentSession SetSessionState(OrchestrationRuntimeState newState)
        {
            return new PersistentSession(this.SessionId, newState, this.Messages);
        }

        public PersistentSession AppendMessage(TaskMessage message)
        {
            return new PersistentSession(this.SessionId, this.SessionState, this.Messages.Add(new LockableTaskMessage() {TaskMessage = message}));
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
            var newMessages = this.Messages.Where(m => !m.Received).ToImmutableList();
            return new PersistentSession(this.SessionId, this.SessionState, newMessages);
        }
    }

    public class LockableTaskMessage
    {
        public TaskMessage TaskMessage; //serialized
        public bool Received; //not serialized, default value false
    }
}
