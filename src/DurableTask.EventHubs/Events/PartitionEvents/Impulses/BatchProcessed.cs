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
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class BatchProcessed : PartitionEvent
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public long BatchStartPosition { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> OrchestratorMessages { get; set; }

        [DataMember]
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        public override TrackedObject StartProcessingOnObject(Storage.IPartitionState state)
        {
            return state.Sessions;
        }

        protected override void AddExtraInformation(StringBuilder s)
        {
            s.Append($" S{SessionId:D6}:{BatchStartPosition}");

            if (State != null)
            {
                s.Append(' ');
                s.Append(State.OrchestrationStatus);
            }

            s.Append(' ');
            s.Append(this.InstanceId);
        }
    }

}
