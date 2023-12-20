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
    using System;
    using DurableTask.Core.Serializing;

    /// <summary>
    /// Settings that determine how a task orchestrator interacts with entities.
    /// </summary>
    public class TaskOrchestrationEntityParameters
    {
        /// <summary>
        /// The time window within which entity messages should be deduplicated and reordered.
        /// This is zero for providers that already guarantee exactly-once and ordered delivery.
        /// </summary>
        public TimeSpan EntityMessageReorderWindow { get; set; }

        /// <summary>
        /// Construct a <see cref="TaskOrchestrationEntityParameters"/> based on the given backend properties.
        /// </summary>
        /// <param name="properties">The backend properties.</param>
        /// <returns>The constructed object, or null if <paramref name="properties"/> is null.</returns>
        public static TaskOrchestrationEntityParameters? FromEntityBackendProperties(EntityBackendProperties? properties)
        {
            if (properties == null)
            {
                return null;
            }

            return new TaskOrchestrationEntityParameters()
            {
                EntityMessageReorderWindow = properties.EntityMessageReorderWindow,
            };
        }
    }
}