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

namespace DurableTask
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    class ServiceBusOrchestrationService : IOrchestrationService
    {
        public int MaxMessageCount
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem, OrchestrationRuntimeState newOrchestrationRuntimeState, IList<TaskMessage> outboundMessages, IList<TaskMessage> orchestratorMessages, IList<TaskMessage> timerMessages)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem()
        {
            throw new NotImplementedException();
        }

        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync()
        {
            throw new NotImplementedException();
        }

        public Task RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task TerminateTaskOrchestrationAsync(TaskOrchestrationWorkItem workItem, bool force)
        {
            throw new NotImplementedException();
        }
    }
}