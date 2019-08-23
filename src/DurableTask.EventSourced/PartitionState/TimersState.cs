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

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class TimersState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingTimers { get; private set; } = new Dictionary<long, TaskMessage>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override string Key => "Timers";

        protected override void Restore()
        {
            // restore the pending timers
            foreach (var kvp in PendingTimers)
            {
                var expirationEvent = new TimerFired()
                {
                    PartitionId = this.Partition.PartitionId,
                    TimerId = kvp.Key,
                    TimerFiredMessage = kvp.Value,
                };

                Partition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
            }
        }

        public void Process(TimerFired evt, EffectTracker effect)
        {
            if (PendingTimers.ContainsKey(evt.TimerId))
            {
                effect.ApplyTo(State.Sessions);
                effect.ApplyTo(this);
            }
        }

        public void Apply(TimerFired evt)
        {
            PendingTimers.Remove(evt.TimerId);
        }

        public void Apply(BatchProcessed evt)
        {
            foreach(var t in evt.TimerMessages)
            {
                var timerId = SequenceNumber++;
                PendingTimers.Add(timerId, t);

                var expirationEvent = new TimerFired()
                {
                    PartitionId = this.Partition.PartitionId,
                    TimerId = timerId,
                    TimerFiredMessage = t,
                };

                Partition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
            }
        }
    }
}
