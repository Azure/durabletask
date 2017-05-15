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

namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    // AFFANDAR : TODO : MASTER
    //      + implement batched message receive
    //      + proper exception model for orchestration service providers

    /// <summary>
    /// Orchestration Service interface for performing task hub management operations 
    /// and handling orchestrations and work items' state
    /// </summary>
    public interface IOrchestrationService
    {
        // Service management and lifecycle operations
        
        /// <summary>
        /// Starts the service initializing the required resources
        /// </summary>
        Task StartAsync();

        /// <summary>
        /// Stops the orchestration service gracefully
        /// </summary>
        Task StopAsync();

        /// <summary>
        /// Stops the orchestration service with optional forced flag
        /// </summary>
        Task StopAsync(bool isForced);

        /// <summary>
        /// Deletes and Creates the neccesary resources for the orchestration service and the instance store
        /// </summary>
        Task CreateAsync();

        /// <summary>
        /// Deletes and Creates the neccesary resources for the orchestration service and optionally the instance store
        /// </summary>
        Task CreateAsync(bool recreateInstanceStore);

        /// <summary>
        /// Creates the neccesary resources for the orchestration service and the instance store
        /// </summary>
        Task CreateIfNotExistsAsync();

        /// <summary>
        /// Deletes the resources for the orchestration service and the instance store
        /// </summary>
        Task DeleteAsync();

        /// <summary>
        /// Deletes the resources for the orchestration service and optionally the instance store
        /// </summary>
        Task DeleteAsync(bool deleteInstanceStore);

        // TaskOrchestrationDispatcher methods

        /// <summary>
        ///     Checks the message count against the threshold to see if a limit is being exceeded
        /// </summary>
        bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState);

        /// <summary>
        /// Inspects an exception to get a custom delay based on the exception (e.g. transient) properties for a process exception
        /// </summary>
        int GetDelayInSecondsAfterOnProcessException(Exception exception);

        /// <summary>
        /// Inspects an exception to get a custom delay based on the exception (e.g. transient) properties for a fetch exception
        /// </summary>
        int GetDelayInSecondsAfterOnFetchException(Exception exception);

        /// <summary>
        /// Gets the number of task orchestration dispatchers
        /// </summary>
        int TaskOrchestrationDispatcherCount { get; }

        /// <summary>
        /// Gets the maximum number of concurrent task orchestration items
        /// </summary>
        int MaxConcurrentTaskOrchestrationWorkItems { get; }

        /// <summary>
        ///     Wait for the next orchestration work item and return the orchestration work item
        /// </summary>
        Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        /// <summary>
        ///     Renew the lock on an orchestration
        /// </summary>
        Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);

        /// <summary>
        ///     Complete an orchestation, send any outbound messages and completes the session for all current messages
        /// </summary>
        Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState);

        /// <summary>
        ///     Abandon an orchestation, this abandons ownership/locking of all messages for an orchestation and it's session
        /// </summary>
        Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        /// <summary>
        ///     Release the lock on an orchestration, releases the session, decoupled from CompleteTaskOrchestrationWorkItemAsync to handle nested orchestrations
        /// </summary>
        Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);

        // TaskActivityDispatcher methods

        /// <summary>
        /// Gets the number of task activity dispatchers
        /// </summary>
        int TaskActivityDispatcherCount { get; }

        /// <summary>
        /// Gets the maximum number of concurrent task activity items
        /// </summary>
        int MaxConcurrentTaskActivityWorkItems { get; }

        /// <summary>
        ///    Wait for an lock the next task activity to be processed 
        /// </summary>
        Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken);

        /// <summary>
        ///    Renew the lock on a still processing work item
        /// </summary>
        Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);

        /// <summary>
        ///    Atomically complete a work item and send the response messages
        /// </summary>
        Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage);

        /// <summary>
        ///    Abandons a single work item and releases the lock on it
        /// </summary>
        Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem);
    }
}