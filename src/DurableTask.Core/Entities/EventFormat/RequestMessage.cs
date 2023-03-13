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
namespace DurableTask.Core.Entities.EventFormat
{
    using System;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A message sent to an entity, such as operation, signal, lock, or continue messages.
    /// </summary>
    internal class RequestMessage
    {
        /// <summary>
        /// The name of the operation being called (if this is an operation message) or <c>null</c>
        /// (if this is a lock request).
        /// </summary>
        [JsonProperty(PropertyName = "op")]
        public string Operation { get; set; }

        /// <summary>
        /// Whether or not this is a one-way message.
        /// </summary>
        [JsonProperty(PropertyName = "signal", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool IsSignal { get; set; }

        /// <summary>
        /// The operation input.
        /// </summary>
        [JsonProperty(PropertyName = "input", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string Input { get; set; }

        /// <summary>
        /// A unique identifier for this operation.
        /// </summary>
        [JsonProperty(PropertyName = "id", Required = Required.Always)]
        public Guid Id { get; set; }

        /// <summary>
        /// The parent instance that called this operation.
        /// </summary>
        [JsonProperty(PropertyName = "parent", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string ParentInstanceId { get; set; }

        /// <summary>
        /// The parent instance that called this operation.
        /// </summary>
        [JsonProperty(PropertyName = "parentExecution", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string ParentExecutionId { get; set; }

        /// <summary>
        /// Optionally, a scheduled time at which to start the operation.
        /// </summary>
        [JsonProperty(PropertyName = "due", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public DateTime? ScheduledTime { get; set; }

        /// <summary>
        /// A timestamp for this request.
        /// Used for duplicate filtering and in-order delivery.
        /// </summary>
        [JsonProperty]
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// A timestamp for the predecessor request in the stream, or DateTime.MinValue if none.
        /// Used for duplicate filtering and in-order delivery.
        /// </summary>
        [JsonProperty]
        public DateTime Predecessor { get; set; }

        /// <summary>
        /// For lock requests, the set of locks being acquired. Is sorted,
        /// contains at least one element, and has no repetitions.
        /// </summary>
        [JsonProperty(PropertyName = "lockset", DefaultValueHandling = DefaultValueHandling.Ignore, TypeNameHandling = TypeNameHandling.None)]
        public EntityId[] LockSet { get; set; }

        /// <summary>
        /// For lock requests involving multiple locks, the message number.
        /// </summary>
        [JsonProperty(PropertyName = "pos", DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int Position { get; set; }

        /// <summary>
        /// whether this message is a lock request
        /// </summary>
        [JsonIgnore]
        public bool IsLockRequest => LockSet != null;

        /// <inheritdoc/>
        public override string ToString()
        {
            if (IsLockRequest)
            {
                return $"[Request lock {Id} by {ParentInstanceId} {ParentExecutionId}, position {Position}]";
            }
            else
            {
                return $"[{(IsSignal ? "Signal" : "Call")} '{Operation}' operation {Id} by {ParentInstanceId} {ParentExecutionId}]";
            }
        }
    }
}
