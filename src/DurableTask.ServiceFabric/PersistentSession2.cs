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
using System.Collections.Immutable;
using System.Runtime.Serialization;
using DurableTask.History;
using ImmutableObjectGraph.Generation;

namespace DurableTask.ServiceFabric
{
    [GenerateImmutable(GenerateBuilder = true)]
    [DataContract]
    public sealed partial class PersistentSession2
    {
        [DataMember]
        readonly string sessionId;

        [DataMember]
        readonly ImmutableList<HistoryEvent> sessionState;

        [DataMember]
        readonly ImmutableList<ReceivableTaskMessage> messages;

        [DataMember]
        readonly ImmutableList<ReceivableTaskMessage> scheduledMessages;

        static partial void CreateDefaultTemplate(ref Template template)
        {
            template.Messages = ImmutableList<ReceivableTaskMessage>.Empty;
            template.ScheduledMessages = ImmutableList<ReceivableTaskMessage>.Empty;
            template.SessionState = ImmutableList<HistoryEvent>.Empty;
        }
    }

    [GenerateImmutable(GenerateBuilder = true)]
    [DataContract]
    public sealed partial class ReceivableTaskMessage
    {
        [DataMember]
        readonly TaskMessage taskMessage;

        //Todo: Should we really persist this? If not what does it mean to commit after ReceiveMessages?
        [DataMember]
        readonly bool isReceived;
    }
}
