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
    using System.Collections.Generic;

    /// <summary>
    /// Entity operation action for creating sub-orchestrations.
    /// </summary>
    public class StartNewOrchestrationOperationAction : OperationAction
    {
         /// <inheritdoc/>
        public override OperationActionType OperationActionType => OperationActionType.StartNewOrchestration;

        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// Gets or sets the name of the sub-orchestrator to start.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Gets or sets the version of the sub-orchestrator to start.
        /// </summary>
        public string? Version { get; set; }

        /// <summary>
        /// Gets or sets the instance ID of the created sub-orchestration.
        /// </summary>
        public string? InstanceId { get; set; }

        /// <summary>
        /// Gets or sets the input of the sub-orchestration.
        /// </summary>
        public string? Input { get; set; }

        /// <summary>
        /// Gets or sets when to start the orchestration, or null if the orchestration should be started immediately.
        /// </summary>
        public DateTime? ScheduledStartTime { get; set; }

        /// <summary>
        /// The time of the new orchestration request creation.
        /// </summary>
        public DateTimeOffset? RequestTime { get; set; }

        /// <summary>
        /// The parent trace context for the operation, if any.
        /// </summary>
        public DistributedTraceContext? ParentTraceContext { get; set; }

    }
}