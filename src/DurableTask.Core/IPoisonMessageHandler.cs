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
#nullable enable
namespace DurableTask.Core
{
    using System.Threading.Tasks;
    using DurableTask.Core.History;

    /// <summary>
    /// Provides extensibility points for detecting and handling "poison" messages and invalid work items
    /// in the task dispatchers.
    /// </summary>
    public interface IPoisonMessageHandler
    {
        /// <summary>
        /// Determines whether the given <see cref="HistoryEvent"/> is a poison message.
        /// </summary>
        /// <param name="historyEvent">The history event being dispatched.</param>
        /// <param name="reason">Why the message is considered poisoned.</param>
        /// <returns><c>true</c> if the message should be treated as poisoned; otherwise <c>false</c>.</returns>
        public bool IsPoisonMessage(HistoryEvent historyEvent, out string? reason);

        /// <summary>
        /// Invoked to handle a poison message in the case that a message cannot necessarily
        /// be "failed" by the dispatchers, so  the <see cref="IPoisonMessageHandler"/> must
        /// decide what to do.
        /// </summary>
        /// <param name="orchestrationInstance">The orchestration instance the event was sent to, or null
        /// if this information is not available.</param>
        /// <param name="historyEvent">The "poisoned" history event.</param>
        /// <param name="reason">The reason the event is "poisoned".</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandlePoisonMessageAsync(OrchestrationInstance? orchestrationInstance, HistoryEvent historyEvent, string reason);

        /// <summary>
        /// Invoked to handle a work item that is invalid and cannot be processed at all.
        /// </summary>
        /// <param name="workItem">The work item that could not be processed.</param>
        /// <param name="reason">Why the work item is invalid.</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandleInvalidWorkItemAsync(TaskOrchestrationWorkItem workItem, string reason);

        /// <summary>
        /// Invoked to handle a work item that is invalid and cannot be processed at all.
        /// </summary>
        /// <param name="workItem">The work item that could not be processed.</param>
        /// <param name="reason">Why the work item is invalid.</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandleInvalidWorkItemAsync(TaskActivityWorkItem workItem, string reason);
    }
}
