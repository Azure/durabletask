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

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class ClientsState : TrackedObject
    {
        [IgnoreDataMember]
        public override string Key => "Clients";

        // this is where we would add client-tracking state if we need it at some point
        // for now this just dispatches the processing, but keeps no state

        public void Process(ClientTaskMessagesReceived evt, EffectTracker effect)
        {
            effect.ApplyTo(Partition.State.Sessions);
        }

        public void Process(CreationRequestReceived evt, EffectTracker effect)
        {
            effect.ProcessOn(Partition.State.GetInstance(evt.InstanceId));
        }

        public void Process(StateRequestReceived evt, EffectTracker effect)
        {
            var task = Partition.HandleAsync(evt);
        }

        public void Process(WaitRequestReceived evt, EffectTracker effect)
        {
            var task = Partition.HandleAsync(evt);
        }
    }
}
