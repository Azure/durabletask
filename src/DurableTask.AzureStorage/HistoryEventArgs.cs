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

namespace DurableTask.AzureStorage
{
    using System;
    using DurableTask.Core;
    using DurableTask.Core.History;

    public class HistoryEventArgs
    {
        internal HistoryEventArgs(HistoryEvent newHistoryEvent, OrchestrationRuntimeState state)
        {
            if (newHistoryEvent == null)
            {
                throw new ArgumentNullException(nameof(newHistoryEvent));
            }

            if (state == null)
            {
                throw new ArgumentNullException(nameof(state));
            }

            this.NewHistoryEvent = newHistoryEvent;
            this.InstanceId = state.OrchestrationInstance.InstanceId;
            this.State = state;
        }

        internal HistoryEventArgs(HistoryEvent newHistoryEvent, string instanceId)
        {
            if (newHistoryEvent == null)
            {
                throw new ArgumentNullException(nameof(newHistoryEvent));
            }

            if (string.IsNullOrEmpty(instanceId))
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            this.NewHistoryEvent = newHistoryEvent;
            this.InstanceId = instanceId;
        }

        public HistoryEvent NewHistoryEvent { get; private set; }

        public string InstanceId { get; private set; }

        public OrchestrationRuntimeState State { get; private set; }
    }
}
