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
namespace DurableTask.Core.Entities
{
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.EventFormat;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json;
    using System;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Utility functions for clients that interact with entities, either by sending events or by accessing the entity state directly in storage
    /// </summary>
    public static class ClientEntityHelpers
    {
        /// <summary>
        /// Create an event to represent an entity signal.
        /// </summary>
        /// <param name="targetInstance">The target instance.</param>
        /// <param name="requestId">A unique identifier for the request.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="input">The serialized input for the operation.</param>
        /// <param name="scheduledTimeUtc">The time to schedule this signal, or null if not a scheduled signal</param>
        /// <returns>The event to send.</returns>
        public static EntityMessageEvent EmitOperationSignal(OrchestrationInstance targetInstance, Guid requestId, string operationName, string? input, (DateTime Original, DateTime Capped)? scheduledTimeUtc)
        {
            return EmitOperationSignal(targetInstance, requestId, operationName, input, scheduledTimeUtc, parentTraceContext: null, requestTime: null, createTrace: false);
        }

        /// <summary>
        /// Create an event to represent an entity signal.
        /// </summary>
        /// <param name="targetInstance">The target instance.</param>
        /// <param name="requestId">A unique identifier for the request.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="input">The serialized input for the operation.</param>
        /// <param name="scheduledTimeUtc">The time to schedule this signal, or null if not a scheduled signal</param>
        /// <param name="parentTraceContext">The parent trace context for this operation.</param>
        /// <param name="requestTime">The time at which the request was made.</param>
        /// <param name="createTrace">Whether to create a trace for this signal operation.</param>
        /// <returns>The event to send.</returns>
        public static EntityMessageEvent EmitOperationSignal(OrchestrationInstance targetInstance, Guid requestId, string operationName, string? input, (DateTime Original, DateTime Capped)? scheduledTimeUtc, DistributedTraceContext? parentTraceContext = null, DateTimeOffset? requestTime = null, bool createTrace = false)
        {
            var request = new RequestMessage()
            {
                ParentInstanceId = null, // means this was sent by a client
                ParentExecutionId = null,
                Id = requestId,
                IsSignal = true,
                Operation = operationName,
                ScheduledTime = scheduledTimeUtc?.Original,
                Input = input,
                ParentTraceContext = parentTraceContext,
                RequestTime = requestTime,
                CreateTrace = createTrace,
            };

            var eventName = scheduledTimeUtc.HasValue
                ? EntityMessageEventNames.ScheduledRequestMessageEventName(scheduledTimeUtc.Value.Capped)
                : EntityMessageEventNames.RequestMessageEventName;

            return new EntityMessageEvent(eventName, request, targetInstance);
        }

        /// <summary>
        /// Create an event to represent an entity unlock, which is called by clients to fix orphaned locks.
        /// </summary>
        /// <param name="targetInstance">The target instance.</param>
        /// <param name="lockOwnerInstanceId">The instance id of the entity to be unlocked.</param>
        /// <returns>The event to send.</returns>
        public static EntityMessageEvent EmitUnlockForOrphanedLock(OrchestrationInstance targetInstance, string lockOwnerInstanceId)
        {
            var message = new ReleaseMessage()
            {
                ParentInstanceId = lockOwnerInstanceId,
                Id = "fix-orphaned-lock", // we don't know the original id but it does not matter
            };

            return new EntityMessageEvent(EntityMessageEventNames.ReleaseMessageEventName, message, targetInstance);
        }

        /// <summary>
        /// Extracts the user-defined entity state from the serialized scheduler state. The result is the serialized state,
        /// or null if the entity has no state.
        /// </summary>
        public static string? GetEntityState(string? serializedSchedulerState)
        {
            if (serializedSchedulerState == null)
            {
                return null;
            }
           
            var schedulerState = JsonConvert.DeserializeObject<SchedulerState>(serializedSchedulerState, Serializer.InternalSerializerSettings)!;
            return schedulerState.EntityState;
        }

        /// <summary>
        /// Gets the entity status from the serialized custom status of the orchestration.
        /// or null if the entity has no state.
        /// </summary>
        public static EntityStatus? GetEntityStatus(string? orchestrationCustomStatus)
        {
            if (orchestrationCustomStatus == null)
            {
                return null;
            }

            return JsonConvert.DeserializeObject<EntityStatus>(orchestrationCustomStatus, Serializer.InternalSerializerSettings)!;
        }
    }
}