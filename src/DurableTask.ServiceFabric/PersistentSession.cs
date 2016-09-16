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
using System.Runtime.Serialization;
using DurableTask.History;
using ImmutableObjectGraph.Generation;

namespace DurableTask.ServiceFabric
{
    //[GenerateImmutable(GenerateBuilder = true)]
    [DataContract]
    public sealed partial class PersistentSession
    {
        [DataMember]
        public string SessionId { get; private set; }

        //Todo: Is this performant? Perhaps consider json serialization instead?
        [DataMember]
        public IList<HistoryEvent> SessionState { get; private set; }

        [DataMember]
        public IEnumerable<ReceivableTaskMessage> Messages { get; private set; }

        [DataMember]
        public IEnumerable<ReceivableTaskMessage> ScheduledMessages { get; private set; }

        public bool IsLocked { get; private set; }

        [OnDeserialized]
        private void OnDeserialized(StreamingContext context)
        {
            this.SessionState = this.SessionState.ToImmutableList();
            this.Messages = this.Messages.ToImmutableList();
            this.ScheduledMessages = this.ScheduledMessages.ToImmutableList();
        }

        private PersistentSession(string sessionId, IList<HistoryEvent> sessionState, IEnumerable<ReceivableTaskMessage> messages, IEnumerable<ReceivableTaskMessage> scheduledMessages, bool isLocked)
        {
            this.SessionId = sessionId;
            this.SessionState = sessionState ?? ImmutableList<HistoryEvent>.Empty;
            this.Messages = messages ?? ImmutableList<ReceivableTaskMessage>.Empty;
            this.ScheduledMessages = scheduledMessages ?? ImmutableList<ReceivableTaskMessage>.Empty;
            this.IsLocked = isLocked;
        }

        public static PersistentSession Create(string sessionId, IList<HistoryEvent> sessionState, IEnumerable<ReceivableTaskMessage> messages, IEnumerable<ReceivableTaskMessage> scheduledMessages, bool isLocked)
        {
            return new PersistentSession(sessionId, sessionState, messages, scheduledMessages, isLocked);
        }
    }

    //[GenerateImmutable(GenerateBuilder = true)]
    [DataContract]
    public sealed partial class ReceivableTaskMessage
    {
        [DataMember]
        public TaskMessage TaskMessage { get; private set; }

        public bool IsReceived { get; private set; }

        private ReceivableTaskMessage(TaskMessage taskMessage, bool isReceived)
        {
            this.TaskMessage = taskMessage;
            this.IsReceived = isReceived;
        }

        public static ReceivableTaskMessage Create(TaskMessage taskMessage, bool isReceived = false)
        {
            return new ReceivableTaskMessage(taskMessage, isReceived);
        }
    }
}
