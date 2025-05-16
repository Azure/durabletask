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
namespace DurableTask.Core.Entities.OperationFormat
{
    using DurableTask.Core.Tracing;
    using System;

    /// <summary>
    /// Operation action for sending a signal.
    /// </summary>
    public class SendSignalOperationAction : OperationAction
    {
        /// <inheritdoc/>
        public override OperationActionType OperationActionType => OperationActionType.SendSignal;

        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The destination entity for the signal.
        /// </summary>
        public string? InstanceId { get; set; }

        /// <summary>
        /// The name of the operation being signaled.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// The input of the operation being signaled.
        /// </summary>
        public string? Input { get; set; }

        /// <summary>
        /// Optionally, a scheduled delivery time for the signal.
        /// </summary>
        public DateTime? ScheduledTime { get; set; }

        /// <summary>
        /// The time the signal request was generated.
        /// </summary>
        public DateTimeOffset? RequestTime { get; set; }

        /// <summary>
        /// The parent trace context for the signal, if any.
        /// </summary>
        public DistributedTraceContext? ParentTraceContext { get; set; }
    }
}