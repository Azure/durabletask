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
    /// A request message sent to an entity when calling or signaling the entity.
    /// </summary>
    public class OperationRequest
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The name of the operation.
        /// </summary>
        public string? Operation { get; set; }

        /// <summary>
        /// The unique GUID of the operation.
        /// </summary>
        public Guid Id { get; set; }

        /// <summary>
        /// The input for the operation. Can be null if no input was given.
        /// </summary>
        public string? Input { get; set; }

        /// <summary>
        /// The trace context for the operation, if any.
        /// </summary>
        public DistributedTraceContext? TraceContext { get; set; }
    }
}