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

        private bool Deduplicate(TaskMessageReceived taskMessageReceived)
        {
            // TODO check against vector clock
            return false;
        }

        private bool Deduplicate(ClientRequestEvent clientRequestReceived)
        {
            // TODO check against vector clock
            return false;
        }

        public void Apply(ClientRequestEvent evt)
        {
            // TODO update clocks
        }

        public void Apply(TaskMessageReceived evt)
        {
            // TODO update clocks
        }

        // TaskMessageReceived goes to session

        public void Scope(TaskMessageReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (!this.Deduplicate(evt))
            {
                apply.Add(State.Sessions);
                apply.Add(this);
            }
        }

        // ClientTaskMessageReceived goes to session

        public void Scope(ClientTaskMessagesReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (!this.Deduplicate(evt))
            {
                apply.Add(State.Sessions);
                apply.Add(this);
            }
        }

        // CreationRequestReceived goes to instance

        public void Scope(CreationRequestReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (!this.Deduplicate(evt))
            {
                scope.Add(State.GetInstance(evt.InstanceId));
                apply.Add(this);
            }
        }

        // WaitRequestReceived starts an asychronous waiter 

        public void Scope(WaitRequestReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (!this.Deduplicate(evt))
            {
                var waitTask = this.Partition.HandleAsync(evt);
                apply.Add(this);
            }
        }

        // StateRequestReceived starts an asychronous read 

        public void Scope(StateRequestReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (!this.Deduplicate(evt))
            {
                var readTask = this.Partition.HandleAsync(evt);
                apply.Add(this);
            }
        }

    }
}
