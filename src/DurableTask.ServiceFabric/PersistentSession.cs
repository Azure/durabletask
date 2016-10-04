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
    using System.Runtime.Serialization;
    using DurableTask.History;

    [DataContract]
    public sealed partial class PersistentSession
    {
        // Note : Ideally all the properties in this class should be readonly because this
        // class is designed to be immutable class. We use private settable properties
        // for DataContract serialization to work - the private setters should not be used
        // in the code within this class as that would violate the immutable design. Any
        // method that mutates the state should return a new instance instead.
        [DataMember]
        public string SessionId { get; private set; }

        // Note: The properties below are marked IEnumerable but not
        // IImmutableList because DataContract serialization cannot deserialize the latter.
        // Except for the constructor and serialization methods, rest of the code in this class
        // should use only the public immutable list members.

        /// <summary>
        /// Do not use except in the constructor or serialization methods, use <see cref="SessionState"/> property instead.
        /// </summary>
        [DataMember]
        IEnumerable<HistoryEvent> sessionState { get; set; }

        /// <summary>
        /// Do not use except in the constructor or serialization methods, use <see cref="Messages"/> property instead.
        /// </summary>
        [DataMember]
        IEnumerable<ReceivableTaskMessage> messages { get; set; }

        [OnDeserialized]
        private void OnDeserialized(StreamingContext context)
        {
            this.sessionState = this.sessionState.ToImmutableList();
            this.messages = this.messages.ToImmutableList();
        }

        private PersistentSession(string sessionId, IImmutableList<HistoryEvent> sessionState, IImmutableList<ReceivableTaskMessage> messages)
        {
            this.SessionId = sessionId;
            this.sessionState = sessionState ?? ImmutableList<HistoryEvent>.Empty;
            this.messages = messages ?? ImmutableList<ReceivableTaskMessage>.Empty;
        }

        public static PersistentSession Create(string sessionId, IImmutableList<HistoryEvent> sessionState, IImmutableList<ReceivableTaskMessage> messages)
        {
            return new PersistentSession(sessionId, sessionState, messages);
        }

        public ImmutableList<HistoryEvent> SessionState => this.sessionState.ToImmutableList();
        public ImmutableList<ReceivableTaskMessage> Messages => this.messages.ToImmutableList();
    }

    [DataContract]
    public sealed class ReceivableTaskMessage
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
