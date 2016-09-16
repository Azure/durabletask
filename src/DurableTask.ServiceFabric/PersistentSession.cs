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
        readonly string sessionId;

        //readonly ImmutableList<HistoryEvent> sessionState;
        private ImmutableList<HistoryEvent> sessionState;

        //readonly ImmutableList<ReceivableTaskMessage> messages;
        private ImmutableList<ReceivableTaskMessage> messages;

        //readonly ImmutableList<ReceivableTaskMessage> scheduledMessages;
        private ImmutableList<ReceivableTaskMessage> scheduledMessages;

        //Todo: Is this performant? Perhaps consider json serialization instead?
        [DataMember]
        private IEnumerable<HistoryEvent> SessionStateWrapper { get; set; }

        [DataMember]
        private IEnumerable<ReceivableTaskMessage> MessagesWrapper { get; set; }

        [DataMember]
        private IEnumerable<ReceivableTaskMessage> ScheduledMessagesWrapper { get; set; }

        static partial void CreateDefaultTemplate(ref Template template)
        {
            template.Messages = ImmutableList<ReceivableTaskMessage>.Empty;
            template.ScheduledMessages = ImmutableList<ReceivableTaskMessage>.Empty;
            template.SessionState = ImmutableList<HistoryEvent>.Empty;
        }

        [OnDeserialized]
        private void OnDeserialized(StreamingContext context)
        {
            this.sessionState = this.SessionStateWrapper.ToImmutableList();
            this.messages = this.MessagesWrapper.ToImmutableList();
            this.scheduledMessages = this.ScheduledMessagesWrapper.ToImmutableList();
        }

        [OnSerializing]
        private void OnSerializing(StreamingContext context)
        {
            this.SessionStateWrapper = this.sessionState;
            this.MessagesWrapper = this.messages;
            this.ScheduledMessagesWrapper = this.scheduledMessages;
        }
    }

    //[GenerateImmutable(GenerateBuilder = true)]
    [DataContract]
    public sealed partial class ReceivableTaskMessage
    {
        [DataMember]
        readonly TaskMessage taskMessage;

        readonly bool isReceived;
    }
}
