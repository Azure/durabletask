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
using System.Threading;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal abstract class TrackedObject
    {
        [IgnoreDataMember]
        protected Partition Partition;

        [DataMember]
        long LastProcessed { get; set; } = -1;

        [IgnoreDataMember]
        public abstract string Key { get; }

        [IgnoreDataMember]
        protected Storage.IPartitionState State => Partition.State;

        // protects conflicts between the event processor and local tasks
        internal object Lock { get; private set; } = new object();

        // call after deserialization to fill in non-serialized fields
        public long Restore(Partition Partition)
        {
            this.Partition = Partition;
            this.Restore();
            return LastProcessed;
        }

        protected virtual void Restore()
        {
            // subclasses override this if there is work they need to do here
        }

        public void Process(Event processorEvent, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            // start with reading this object only, to determine the scope
            if (processorEvent.QueuePosition > this.LastProcessed)
            {
                var scopeStartPos = scope.Count;
                var applyStartPos = apply.Count;

                System.Diagnostics.Trace.TraceInformation($"Part{this.Partition.PartitionId:D2}.{processorEvent.QueuePosition:D7}     Read [{this.Key}]");

                dynamic dynamicThis = this;
                dynamic dynamicProcessorEvent = processorEvent;
                dynamicThis.Scope(dynamicProcessorEvent, scope, apply);

                if (scope.Count > scopeStartPos)
                {
                    for (int i = scopeStartPos; i < scope.Count; i++)
                    {
                        scope[i].Process(processorEvent, scope, apply);
                    }
                }

                if (apply.Count > applyStartPos)
                {
                    for (int i = applyStartPos; i < apply.Count; i++)
                    {
                        var target = apply[i];
                        if (target.LastProcessed < processorEvent.QueuePosition)
                        {
                            lock (target.Lock)
                            {
                                System.Diagnostics.Trace.TraceInformation($"Part{this.Partition.PartitionId:D2}.{processorEvent.QueuePosition:D7}     Update [{target.Key}]");

                                dynamic dynamicTarget = target;
                                dynamicTarget.Apply(dynamicProcessorEvent);
                                target.LastProcessed = processorEvent.QueuePosition;
                            }
                        }
                    }
                }
            }
        }
    }
}
