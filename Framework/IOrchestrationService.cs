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
    using System.Threading;
    using System.Threading.Tasks;

    // AFFANDAR : TODO : MASTER
    //      + implement LocalOrchestrationService and LocalOchestrationServiceClient
    //      + test checkpoint
    //      + move public classes to separate files
    //      + rethink method names?
    //      + fix up all tests
    //      + make dispatcher start/stop methods async
    //      + task hub description
    //      + change TaskOrchestrationDispatcher2 to use this
    //      + implement ServiceBusOrchestrationService
    //      + build trackingdispatcher2 inside the serivce bus layer
    //      + add instance store methods to IOrchestrationService
    //      + replumb taskhubclient on top of IOrchestrationService
    //      + clean up XML doc comments in public classes
    //
    //  DONE:
    //      + fix up taskhubworker
    //      + add TaskActivityDispatcher2
    //      

    public interface IOrchestrationServiceClient
    {
        Task CreateTaskOrchestrationAsync(TaskMessage creationMessage);

        Task SendTaskOrchestrationMessage(TaskMessage message);

        Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId, 
            string executionId,
            TimeSpan timeout, 
            CancellationToken cancellationToken);

        Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions);

        Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId);

        Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId);

        Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }

    public interface IOrchestrationService
    {
        // Service management and lifecycle operations
        Task StartAsync();

        Task StopAsync();

        Task CreateAsync();

        Task CreateIfNotExistsAsync();

        Task DeleteAsync();

        // TaskOrchestrationDispatcher methods
        bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState);

        Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        Task<TaskOrchestrationWorkItem> RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);

        Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages);

        Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        Task TerminateTaskOrchestrationAsync(TaskOrchestrationWorkItem workItem, bool force);

        // TaskActiviryDispatcher methods
        Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);

        Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage);

        Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem);

        // AFFANDAR : TODO : add instance store methods.
    }

    public class TaskActivityWorkItem
    {
        public string Id;
        public DateTime LockedUntilUtc;
        public TaskMessage TaskMessage;
    }

    public class TaskOrchestrationWorkItem
    {
        public OrchestrationInstance OrchestrationInstance;
        public OrchestrationRuntimeState OrchestrationRuntimeState;
        DateTime LockedUntilUtc;
        public IList<TaskMessage> NewMessages;
    }
}