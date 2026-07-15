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
        /// Attempts to create a failure response event that can be sent back to the orchestration that called an entity,
        /// in the case where the entity operation request could not be processed.
        /// </summary>
        /// <remarks>
        /// A response is only produced for genuine two-way entity operation calls, which include lock requests and calls.
        /// Signals, releases, and self-continue messages have no caller awaiting a response, so this method returns
        /// <see langword="null"/> for those (and for any request that cannot be decoded well enough to identify the
        /// caller).
        /// </remarks>
        /// <param name="entityRequestInput">The serialized input of the poison entity request (the <c>Input</c> of the
        /// <see cref="History.EventRaisedEvent"/>).</param>
        /// <param name="errorType">The error type to record in the failure response.</param>
        /// <param name="errorMessage">The human-readable reason for the failure.</param>
        /// <returns>An <see cref="EntityMessageEvent"/> targeting the calling instance, or <see langword="null"/> if no
        /// caller can or should be notified.</returns>
        public static EntityMessageEvent? TryCreateEntityOperationFailedResponse(string? entityRequestInput, string errorType, string errorMessage)
        {
            if (string.IsNullOrEmpty(entityRequestInput))
            {
                return null;
            }

            RequestMessage? requestMessage = TryDecodeEntityMessage<RequestMessage>(entityRequestInput!);

            // If we cannot recover the caller info, there is no one to notify. We also require a valid request id,
            // since the response event is named after it so the caller can correlate the response with its outstanding
            // call. A default (empty) id means the id could not be recovered from the (malformed) request.
            if (requestMessage == null
                || string.IsNullOrEmpty(requestMessage.ParentInstanceId)
                || requestMessage.Id == Guid.Empty)
            {
                return null;
            }

            var responseMessage = new ResponseMessage()
            {
                FailureDetails = new FailureDetails(
                    errorType: errorType,
                    errorMessage: errorMessage,
                    stackTrace: null,
                    innerFailure: null,
                    isNonRetriable: true),
            };

            if (requestMessage.IsLockRequest)
            {
                responseMessage.Result = ResponseMessage.LockAcquisitionCompletion;
            }

            var destination = new OrchestrationInstance()
            {
                InstanceId = requestMessage.ParentInstanceId!,
                ExecutionId = requestMessage.ParentExecutionId,
            };

            return new EntityMessageEvent(
                EntityMessageEventNames.ResponseMessageEventName(requestMessage.Id),
                responseMessage,
                destination);
        }

        /// <summary>
        /// Attempts to create a lock release event that can be sent to an entity to release an orphaned lock, in the
        /// case where an incoming lock release message could not be processed.
        /// </summary>
        /// <remarks>
        /// This method attempts to decode the original release message (falling back to a lenient decode) in order to
        /// recover the instance id of the lock owner. If that id cannot be recovered, this method returns
        /// <see langword="null"/> since there is no way to know which lock to release.
        /// </remarks>
        /// <param name="releaseMessageInput">The serialized input of the original release message.</param>
        /// <param name="entityInstance">The entity instance whose lock should be released.</param>
        /// <param name="parentInstanceId">The parent instance ID of the lock owner, or <see langword="null"/> if it cannot be determined.</param>
        /// <returns>An <see cref="EntityMessageEvent"/> targeting the entity, or <see langword="null"/> if the lock
        /// owner cannot be identified.</returns>
        public static EntityMessageEvent? TryRecreateEntityUnlock(string? releaseMessageInput, OrchestrationInstance entityInstance, out string? parentInstanceId)
        {
            parentInstanceId = null;

            if (string.IsNullOrEmpty(releaseMessageInput))
            {
                return null;
            }

            ReleaseMessage? releaseMessage = TryDecodeEntityMessage<ReleaseMessage>(releaseMessageInput!);

            // If we cannot recover the lock owner, there is no way to know which lock to release.
            if (releaseMessage == null || string.IsNullOrEmpty(releaseMessage.ParentInstanceId))
            {
                return null;
            }

            parentInstanceId = releaseMessage.ParentInstanceId;
            return EmitUnlockForOrphanedLock(entityInstance, releaseMessage.ParentInstanceId!);
        }

        static T? TryDecodeEntityMessage<T>(string input) where T : EntityMessage, new()
        {
            // First attempt a strict deserialization.
            try
            {
                var message = new T();
                JsonConvert.PopulateObject(input, message, Serializer.InternalSerializerSettings);
                return message;
            }
            catch (Exception)
            {
                // Best-effort recovery: parse with an error-handling reader so any fields before the malformed
                // segment (notably the caller/lock-owner IDs) remain available.
            }

            try
            {
                var lenientSettings = new JsonSerializerSettings
                {
                    Error = (_, args) => args.ErrorContext.Handled = true,
                };

                var message = new T();
                JsonConvert.PopulateObject(input, message, lenientSettings);
                return message;
            }
            catch (Exception)
            {
                return null;
            }
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