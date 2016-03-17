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
    //      + implement batched message receive
    //      + proper exception model for orchestration service providers
    //      + rethink method names?
    //      + rename xxx2 classes to xxx and remove old ones
    //      + clean up XML doc comments in public classes
    //
    //  DONE:
    //      + write tests for terminate, raise event, suborch and exception passing
    //      + write tests for generations
    //      + implement LocalOrchestrationService and LocalOrchestrationServiceClient
    //      + fix up taskhubworker
    //      + add TaskActivityDispatcher2
    //      + test checkpoint
    //      + move public classes to separate files
    //      + add instance store methods to IOrchestrationService
    //      + replumb taskhubclient on top of IOrchestrationService
    //      + sbus provider: force terminate if session size is greater than supported
    //      + make dispatcher start/stop methods async
    //      + task hub description
    //      + implement ServiceBusOrchestrationService
    //      + fix up all tests to use the new APIs
    //      + change TaskOrchestrationDispatcher2 to use this
    //      + build trackingdispatcher2 inside the service bus layer
    //      

    public interface IOrchestrationService
    {
        // Service management and lifecycle operations
        Task StartAsync();

        Task StopAsync();

        Task StopAsync(bool isForced);

        Task CreateAsync();

        Task CreateAsync(bool recreateInstanceStore);

        Task CreateIfNotExistsAsync();

        Task CreateIfNotExistsAsync(bool recreateInstanceStore);

        Task DeleteAsync();

        Task DeleteAsync(bool deleteInstanceStore);

        // TaskOrchestrationDispatcher methods
        bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState);

        int GetDelayInSecondsAfterOnProcessException(Exception exception);

        int GetDelayInSecondsAfterOnFetchException(Exception exception);

        int MaxConcurrentTaskOrchestrationWorkItems { get; }

        Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);

        Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState);

        Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        // TaskActivityDispatcher methods
        int MaxConcurrentTaskActivityWorkItems { get; }

        Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);

        Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage);

        Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem);
    }
}