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
    using System.Collections.Generic;
    using System.Threading.Tasks;

    // AFFANDAR : TODO : MASTER
    //      + move public classes to separate files
    //      + rethink method names?
    //      + change TaskOrchestrationDispatcher2 to use this
    //      + implement ServiceBusOrchestrationService
    //      + add TaskActivityDispatcher2
    //      + build trackingdispatcher2 inside the serivce bus layer
    //      + add instance store methods to IOrchestrationService
    //      + replumb taskhubclient on top of IOrchestrationService
    //
    interface IOrchestrationService
    {
        int MaxMessageCount { get;  }

        Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync();

        Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);

        Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages);

        Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        Task TerminateTaskOrchestrationAsync(TaskOrchestrationWorkItem workItem, bool force);

        Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem();

        Task RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);

        Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage);

        Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem);

        // AFFANDAR : TODO : add instance store methods.
    }

    internal class TaskActivityWorkItem
    {
        public string Id;
        public TaskMessage TaskMessage;
    }

    internal class TrackingWorkItem
    {
        public string Id;
        public IList<TaskMessage> messages;
    }

    internal class TaskOrchestrationWorkItem
    {
        public OrchestrationInstance OrchestrationInstance;
        public OrchestrationRuntimeState OrchestrationRuntimeState;
        public IList<TaskMessage> NewMessages;
    }
}