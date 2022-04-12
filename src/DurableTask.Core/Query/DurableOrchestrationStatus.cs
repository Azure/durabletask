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

using System;
using Newtonsoft.Json.Linq;

#nullable enable

namespace DurableTask.Core.Query
{
    /// <summary>
    /// Represents the status of a durable orchestration instance.
    /// </summary>
    public class DurableOrchestrationStatus
    {
        /// <summary>
        /// Gets the name of the queried orchestrator function.
        /// </summary>
        /// <value>
        /// The orchestrator function name.
        /// </value>
        public string? Name { get; set; }

        /// <summary>
        /// Gets the ID of the queried orchestration instance.
        /// </summary>
        /// <remarks>
        /// The instance ID is generated and fixed when the orchestrator function is scheduled. It can be either
        /// auto-generated, in which case it is formatted as a GUID, or it can be user-specified with any format.
        /// </remarks>
        /// <value>
        /// The unique ID of the instance.
        /// </value>
        public string? InstanceId { get; set; }

        /// <summary>
        /// Gets the time at which the orchestration instance was created.
        /// </summary>
        /// <value>
        /// The instance creation time in UTC.
        /// </value>
        public DateTime? CreatedTime { get; set; }

        /// <summary>
        /// Gets the time at which the orchestration instance last updated its execution history.
        /// </summary>
        /// <value>
        /// The last-updated time in UTC.
        /// </value>
        public DateTime? LastUpdatedTime { get; set; }

        /// <summary>
        /// Gets the input of the orchestrator function instance.
        /// </summary>
        /// <value>
        /// The input as either a <c>JToken</c> or <c>null</c> if no input was provided.
        /// </value>
        public JToken? Input { get; set; }

        /// <summary>
        /// Gets the output of the queried orchestration instance.
        /// </summary>
        /// <value>
        /// The output as either a <c>JToken</c> object or <c>null</c> if it has not yet completed.
        /// </value>
        public JToken? Output { get; set; }

        /// <summary>
        /// Gets the runtime status of the queried orchestration instance.
        /// </summary>
        /// <value>
        /// Expected values include `Running`, `Pending`, `Failed`, `Canceled`, `Terminated`, `Completed`.
        /// </value>
        public OrchestrationRuntimeStatus RuntimeStatus { get; set; }

        /// <summary>
        /// Gets the custom status payload (if any) that was set by the orchestrator function.
        /// </summary>
        /// <value>
        /// The custom status as either a <c>JToken</c> object or <c>null</c> if no custom status has been set.
        /// </value>
        public JToken? CustomStatus { get; set; }

        /// <summary>
        /// Gets the execution history of the orchestration instance.
        /// </summary>
        /// <value>
        /// The output as a <c>JArray</c> object or <c>null</c>.
        /// </value>
        public JArray? History { get; set; }
    }
}
