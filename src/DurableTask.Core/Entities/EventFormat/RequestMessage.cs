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
namespace DurableTask.Core.Entities.EventFormat
{
    using DurableTask.Core.Tracing;
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// A message sent to an entity, such as operation, signal, lock, or continue messages.
    /// </summary>
    [DataContract]
    internal class RequestMessage : EntityMessage
    {
        /// <summary>
        /// The name of the operation being called (if this is an operation message) or <c>null</c>
        /// (if this is a lock request).
        /// </summary>
        [DataMember(Name = "op")]
        public string? Operation { get; set; }

        /// <summary>
        /// Whether or not this is a one-way message.
        /// </summary>
        [DataMember(Name = "signal", EmitDefaultValue = false)]
        public bool IsSignal { get; set; }

        /// <summary>
        /// The operation input.
        /// </summary>
        [DataMember(Name = "input", EmitDefaultValue = false)]
        public string? Input { get; set; }

        /// <summary>
        /// A unique identifier for this operation.
        /// </summary>
        [DataMember(Name = "id", IsRequired = true)]
        public Guid Id { get; set; }

        /// <summary>
        /// The parent instance that called this operation.
        /// </summary>
        [DataMember(Name = "parent", EmitDefaultValue = false)]
        public string? ParentInstanceId { get; set; }

        /// <summary>
        /// The parent instance that called this operation.
        /// </summary>
        [DataMember(Name = "parentExecution", EmitDefaultValue = false)]
        public string? ParentExecutionId { get; set; }

        /// <summary>
        /// Optionally, a scheduled time at which to start the operation.
        /// </summary>
        [DataMember(Name = "due", EmitDefaultValue = false)]
        public DateTime? ScheduledTime { get; set; }

        /// <summary>
        /// A timestamp for this request.
        /// Used for duplicate filtering and in-order delivery.
        /// </summary>
        [DataMember]
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// A timestamp for the predecessor request in the stream, or DateTime.MinValue if none.
        /// Used for duplicate filtering and in-order delivery.
        /// </summary>
        [DataMember]
        public DateTime Predecessor { get; set; }

        /// <summary>
        /// For lock requests, the set of locks being acquired. Is sorted,
        /// contains at least one element, and has no repetitions.
        /// </summary>
        [DataMember(Name = "lockset", EmitDefaultValue = false)]
        public EntityId[]? LockSet { get; set; }

        /// <summary>
        /// For lock requests involving multiple locks, the message number.
        /// </summary>
        [DataMember(Name = "pos", EmitDefaultValue = false)]
        public int Position { get; set; }

        /// <summary>
        /// whether this message is a lock request
        /// </summary>
        [DataMember]
        public bool IsLockRequest => LockSet != null;

        /// <summary>
        /// Parent trace context of this request message.
        /// </summary>
        [DataMember(Name = "parentTraceContext", EmitDefaultValue = false)]
        public DistributedTraceContext? ParentTraceContext { get; set; }

        /// <summary>
        /// Whether or not to create an entity-specific trace for this request message
        /// </summary>
        [DataMember(Name = "createTrace")]
        public bool CreateTrace { get; set; }

        /// <summary>
        /// The time the request was generated.
        /// </summary>
        [DataMember(Name = "requestTime", EmitDefaultValue = false)]
        public DateTimeOffset? RequestTime { get; set; }

        /// <summary>
        /// The client span ID of this request.
        /// </summary>
        public string? ClientSpanId { get; set; }

        /// <inheritdoc/>
        public override string GetShortDescription()
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
