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
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class ClocksState : TrackedObject
    {
        [IgnoreDataMember]
        public override string Key => "Clocks";

        // TaskhubCreated is always the first event, we use it to initialize the deduplication logic

        public void Process(TaskhubCreated evt, EffectTracker effect)
        {
            effect.ApplyTo(this);
        }

        public void Apply(TaskhubCreated evt)
        {

        }

        // TaskMessageReceived goes to session, if not a duplicate

        public void Process(TaskMessageReceived evt, EffectTracker effect)
        {
            if (!this.Deduplicate(evt))
            {
                effect.ApplyTo(State.Sessions);
                effect.ApplyTo(this);
            }
        }

        private bool Deduplicate(TaskMessageReceived taskMessageReceived)
        {
            // TODO check against vector clock
            return false;
        }

        public void Apply(TaskMessageReceived evt)
        {
            // TODO update clocks
        }

    }
}
