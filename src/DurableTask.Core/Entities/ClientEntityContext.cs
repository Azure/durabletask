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
namespace DurableTask.Core.Entities
{
    using DurableTask.Core.Entities.EventFormat;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Entities.StateFormat;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json;
    using System;

    /// <summary>
    /// Utility functions for clients that interact with entities, either by sending events or by accessing the entity state directly in storage
    /// </summary>
    public static class ClientEntityContext
    {
        /// <summary>
        /// Create an event to represent an entity signal.
        /// </summary>
        /// <param name="requestId">A unique identifier for the request.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="input">The serialized input for the operation.</param>
        /// <param name="scheduledTimeUtc">The time to schedule this signal, or null if not a scheduled signal</param>
        /// <returns></returns>
        public static (string eventName, object eventContent) EmitOperationSignal(Guid requestId, string operationName, string input, (DateTime original, DateTime capped)? scheduledTimeUtc)
        {
            var request = new RequestMessage()
            {
                ParentInstanceId = null, // means this was sent by a client
                ParentExecutionId = null,
                Id = requestId,
                IsSignal = true,
                Operation = operationName,
                ScheduledTime = scheduledTimeUtc?.original,
                Input = input,
            };

            var jrequest = JToken.FromObject(request, Serializer.InternalSerializer);

            var eventName = scheduledTimeUtc.HasValue
                ? EntityMessageEventNames.ScheduledRequestMessageEventName(scheduledTimeUtc.Value.capped)
                : EntityMessageEventNames.RequestMessageEventName;

            return (eventName, jrequest);
        }

        /// <summary>
        /// Create an event to represent an entity unlock, which is called by clients to fix orphaned locks.
        /// </summary>
        /// <param name="lockOwnerInstanceId">The instance id of the entity to be unlocked.</param>
        /// <returns></returns>
        public static (string eventName, object eventContent) EmitUnlockForOrphanedLock(string lockOwnerInstanceId)
        {
            var message = new ReleaseMessage()
            {
                ParentInstanceId = lockOwnerInstanceId,
                Id = "fix-orphaned-lock", // we don't know the original id but it does not matter
            };

            var jmessage = JToken.FromObject(message, Serializer.InternalSerializer);

            return (EntityMessageEventNames.ReleaseMessageEventName, jmessage);
        }

        /// <summary>
        /// Extracts the user-defined entity state (as a serialized string) from the scheduler state (also a serialized string).
        /// </summary>
        /// <param name="serializedSchedulerState">The state of the scheduler, as a serialized string.</param>
        /// <param name="entityState">The entity state</param>
        /// <returns>True if the entity exists, or false otherwise</returns>
        public static bool TryGetEntityStateFromSerializedSchedulerState(string serializedSchedulerState, out string entityState)
        {
            var schedulerState = JsonConvert.DeserializeObject<SchedulerState>(serializedSchedulerState, Serializer.InternalSerializerSettings);
            entityState = schedulerState.EntityState;
            return schedulerState.EntityExists;
        }
    }
}