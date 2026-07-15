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
    /// Categorizes why a message or work item was considered "poisoned" so that an
    /// <see cref="IPoisonMessageHandler"/> can decide how to handle it without parsing human-readable strings.
    /// </summary>
    public enum PoisonMessageReason
    {
        /// <summary>
        /// A message or event could not be deserialized
        /// </summary>
        DeserializationError,

        /// <summary>
        /// A message was dispatched for processing more than <see cref="IPoisonMessageHandler.MaxDispatchCount"/>
        /// times and is therefore considered "poisoned".
        /// </summary>
        DispatchCount,

        /// <summary>
        /// A message or work item did not contain the orchestration instance information required to process it.
        /// </summary>
        MissingOrchestrationInstanceId,

        /// <summary>
        /// The orchestration runtime state reconstructed from history was invalid (for example, corrupted or
        /// partially deleted history) and could not be processed.
        /// </summary>
        InvalidRuntimeState,

        /// <summary>
        /// The orchestration history did not contain the required <see cref="History.ExecutionStartedEvent"/> and
        /// did not receive one as part of its new messages.
        /// </summary>
        MissingExecutionStartedEvent,

        /// <summary>
        /// A rewind request was invalid, for example because additional messages were delivered alongside the rewind
        /// request to an instance attempting to rewind from a terminal state.
        /// </summary>
        InvalidRewindRequest,

        /// <summary>
        /// A work item contained an event whose type is not supported by the dispatcher that received it.
        /// </summary>
        WrongEventType,

        /// <summary>
        /// An activity work item's <see cref="History.TaskScheduledEvent"/> did not specify an activity name, so the
        /// activity could not be dispatched.
        /// </summary>
        MissingActivityName,
    }

    /// <summary>
    /// Provides extensibility points for detecting and handling "poison" messages and invalid work items
    /// in the task dispatchers.
    /// </summary>
    public interface IPoisonMessageHandler
    {
        /// <summary>
        /// The maximum dispatch count after which a message should be considered "poisoned" if it is dispatched again.
        /// </summary>
        public int MaxDispatchCount { get; }

        /// <summary>
        /// Invoked to handle a poison entity message in the case that it cannot necessarily
        /// be "failed" by the dispatchers, so the <see cref="IPoisonMessageHandler"/> must
        /// decide what to do.
        /// </summary>
        /// <remarks>
        /// If this method returns false, the dispatcher should fall back to the default behavior
        /// followed when poison message handling is not enabled.
        /// </remarks>
        /// <param name="entityInstance">The entity instance the event was sent to, or null
        /// if this information is not available.</param>
        /// <param name="historyEvent">The "poisoned" history event.</param>
        /// <param name="reason">The category describing why the event is "poisoned".</param>
        /// <param name="details">A human-readable description of why the event is "poisoned".</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandlePoisonEntityMessageAsync(OrchestrationInstance? entityInstance, HistoryEvent historyEvent, PoisonMessageReason reason, string details);

        /// <summary>
        /// Invoked to handle a work item that is invalid and cannot be processed at all.
        /// </summary>
        /// <remarks>
        /// If this method returns false, the dispatcher should fall back to the default behavior
        /// followed in the case of an invalid work item.
        /// </remarks>
        /// <param name="workItem">The work item that could not be processed.</param>
        /// <param name="reason">The category describing why the work item is invalid.</param>
        /// <param name="details">A human-readable description of why the work item is invalid.</param>
        /// <param name="isEntity">Indicates whether the work item is for an entity.</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandleInvalidWorkItemAsync(TaskOrchestrationWorkItem workItem, PoisonMessageReason reason, string details, bool isEntity);

        /// <summary>
        /// Invoked to handle a work item that is invalid and cannot be processed at all.
        /// </summary>
        /// <remarks>
        /// If this method returns false, the dispatcher should fall back to the default behavior
        /// followed in the case of an invalid work item.
        /// </remarks>
        /// <param name="workItem">The work item that could not be processed.</param>
        /// <param name="reason">The category describing why the work item is invalid.</param>
        /// <param name="details">A human-readable description of why the work item is invalid.</param>
        /// <returns>True if the poison message was successfully handled, otherwise false.</returns>
        public Task<bool> HandleInvalidWorkItemAsync(TaskActivityWorkItem workItem, PoisonMessageReason reason, string details);
    }
}
