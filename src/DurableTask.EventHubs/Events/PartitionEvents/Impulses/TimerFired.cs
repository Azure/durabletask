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
    internal class TimerFired : PartitionEvent
    {
        [DataMember]
        public long TimerId { get; set; }

        [DataMember]
        public TaskMessage TimerFiredMessage { get; set; }

        [IgnoreDataMember]
        public TimerFiredEvent TimerFiredEvent => (TimerFiredMessage.Event as TimerFiredEvent);

        public override TrackedObject StartProcessingOnObject(Storage.IPartitionState state)
        {
            return state.Timers;
        }

        protected override void AddExtraInformation(StringBuilder s)
        {
            s.Append(" Id=");
            s.Append(TimerId);
        }
    }
}
