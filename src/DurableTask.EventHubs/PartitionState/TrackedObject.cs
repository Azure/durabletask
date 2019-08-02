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

using System.Collections.Generic;
using System.Runtime.Serialization;

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

        public void Apply(PartitionEvent e)
        {
            // the default apply for an object does not update any state.
        }

        public virtual void Process(PartitionEventFragment e, EffectTracker effect)
        {
            // the default scope for a reassembled event applies that event
            dynamic dynamicThis = this;
            dynamic dynamicPartitionEvent = e.ReassembledEvent;
            dynamicThis.Process(dynamicPartitionEvent, effect);
        }

        public virtual void Apply(PartitionEventFragment e)
        {
            // the default apply for a reassembled event applies that event
            dynamic dynamicThis = this;
            dynamic dynamicPartitionEvent = e.ReassembledEvent;
            dynamicThis.Apply(dynamicPartitionEvent);
        }

        public class EffectTracker
        {
            public List<TrackedObject> ObjectsToProcessOn = new List<TrackedObject>();
            public List<TrackedObject> ObjectsToApplyTo = new List<TrackedObject>();

            public void ProcessOn(TrackedObject o)
            {
                ObjectsToProcessOn.Add(o);
            }

            public void ApplyTo(TrackedObject o)
            {
                ObjectsToApplyTo.Add(o);
            }

            public void Clear()
            {
                ObjectsToProcessOn.Clear();
                ObjectsToApplyTo.Clear();
            }
        }
        
        public void ProcessRecursively(PartitionEvent evt, EffectTracker effect)
        {
            if (evt.QueuePosition > this.LastProcessed)
            {
                this.Partition.Trace($"Process on [{this.Key}]");

                // remember the initial position of the lists so we can tell
                // which elements were added by this frame, and remove them at the end.

                var processOnStartPos = effect.ObjectsToProcessOn.Count;
                var applyToStartPos = effect.ObjectsToApplyTo.Count;

                // start with processing the event on this object, determining effect
                dynamic dynamicThis = this;
                dynamic dynamicPartitionEvent = evt;
                dynamicThis.Process(dynamicPartitionEvent, effect);

                var numObjectsToProcessOn = effect.ObjectsToProcessOn.Count - processOnStartPos;
                var numObjectsToApplyTo = effect.ObjectsToApplyTo.Count - applyToStartPos;

                // recursively process all objects as determined by effect tracker
                if (numObjectsToProcessOn > 0)
                {
                    for (int i = 0; i < numObjectsToProcessOn; i++)
                    {
                        effect.ObjectsToProcessOn[processOnStartPos + i].ProcessRecursively(evt, effect);
                    }
                }

                // apply all objects  as determined by effect tracker
                if (numObjectsToApplyTo > 0)
                {
                    for (int i = 0; i < numObjectsToApplyTo; i++)
                    {
                        var target = effect.ObjectsToApplyTo[applyToStartPos + i];
                        if (target.LastProcessed < evt.QueuePosition)
                        {
                            lock (target.Lock)
                            {
                                this.Partition.Trace($"Apply to [{target.Key}]");

                                dynamic dynamicTarget = target;
                                dynamicTarget.Apply(dynamicPartitionEvent);
                                target.LastProcessed = evt.QueuePosition;
                            }
                        }
                    }
                }

                // remove the elements that were added in this frame
                effect.ObjectsToProcessOn.RemoveRange(processOnStartPos, numObjectsToProcessOn);
                effect.ObjectsToApplyTo.RemoveRange(applyToStartPos, numObjectsToApplyTo);
            }
        }
    }
}
