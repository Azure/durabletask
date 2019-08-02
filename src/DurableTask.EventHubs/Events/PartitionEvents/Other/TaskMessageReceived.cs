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
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class TaskMessageReceived : PartitionEvent
    {
        [DataMember]
        public List<TaskMessage> TaskMessages { get; set; }

        [DataMember]
        public uint OriginPartition { get; set; }

        [DataMember]
        public long OriginPosition { get; set; }

        public override TrackedObject StartProcessingOnObject(Storage.IPartitionState state)
        {
            return state.Dedup;
        }

        protected override void TraceInformation(StringBuilder s)
        {
            s.Append(' ');
            if (TaskMessages.Count == 1)
            {
                s.Append(TaskMessages[0].Event.EventType);
            }
            else
            {
                s.Append('[');
                s.Append(TaskMessages.Count);
                s.Append(']');
            }
            s.Append($" from: Part{OriginPartition:D2}.{OriginPosition:D7}");
        }
    }
}