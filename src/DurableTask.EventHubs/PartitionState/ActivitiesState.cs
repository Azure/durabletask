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
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingActivities { get; private set; } = new Dictionary<long, TaskMessage>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override string Key => "Activities";


        protected override void Restore()
        {
            // reschedule work items
            foreach (var pending in PendingActivities)
            {
                Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, pending.Key, pending.Value));
            }
        }

        // *************  event processing *****************

        public void Process(ActivityCompleted evt, EffectTracker effect)
        {
            if (PendingActivities.ContainsKey(evt.ActivityId))
            {
                effect.ApplyTo(State.Sessions);
                effect.ApplyTo(this);
            }
        }

        public void Apply(ActivityCompleted evt)
        {
            PendingActivities.Remove(evt.ActivityId);
        }

        public void Apply(BatchProcessed evt)
        {
            foreach (var msg in evt.ActivityMessages)
            {
                var activityId = SequenceNumber++;
                PendingActivities.Add(activityId, msg);

                Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityId, msg));
            }
        }
    }
}
