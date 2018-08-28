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

    /// <summary>
    /// Interface to allow creation of new task orchestrations and query their status.
    /// </summary>
    public interface IOrchestrationServiceClient
    {
        /// <summary>
        /// Creates a new orchestration
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <returns></returns>
        Task CreateTaskOrchestrationAsync(TaskMessage creationMessage);

        /// <summary>
        /// Send a new message for an orchestration
        /// </summary>
        /// <param name="message">Message to send</param>
        /// <returns></returns>
        Task SendTaskOrchestrationMessageAsync(TaskMessage message);

        /// <summary>
        /// Send a new set of messages for an orchestration
        /// </summary>
        /// <param name="messages">Messages to send</param>
        /// <returns></returns>
        Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages);

        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <param name="timeout">Maximum amount of time to wait</param>
        /// <param name="cancellationToken">Task cancellation token</param>
        Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId, 
            string executionId,
            TimeSpan timeout, 
            CancellationToken cancellationToken);

        /// <summary>
        /// Forcefully terminate the specified orchestration instance
        /// </summary>
        /// <param name="instanceId">Instance to terminate</param>
        /// <param name="reason">Reason for termination</param>
        Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason);

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="allExecutions">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>List of OrchestrationState objects that represents the list of orchestrations in the instance store</returns>
        Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions);

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId);

        /// <summary>
        /// Get a string dump of the execution history of the specified orchestration instance specified execution (generation) of the specified instance
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Exectuion id</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId);

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// Also purges the blob storage.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }
}