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
namespace DurableTask.Core
{
    /// <summary>
    /// Task context
    /// </summary>
    public class TaskContext
    {
        /// <summary>
        /// Creates a new TaskContext with the supplied OrchestrationInstance
        /// </summary>
        /// <param name="orchestrationInstance"></param>
        public TaskContext(OrchestrationInstance orchestrationInstance)
            : this(orchestrationInstance, string.Empty, null, -1)
        {
        }

        /// <summary>
        /// Creates a new TaskContext with the supplied OrchestrationInstance and taskId
        /// </summary>
        public TaskContext(OrchestrationInstance orchestrationInstance, string name, string? version, int taskId)
        {
            OrchestrationInstance = orchestrationInstance;
            Name = name;
            Version = version;
            TaskId = taskId;
        }

        /// <summary>
        /// Gets the OrchestrationInstance for this task context
        /// </summary>
        public OrchestrationInstance OrchestrationInstance { get; private set; }

        /// <summary>
        /// Gets the name of the task
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the version of the task, if any.
        /// </summary>
        public string? Version { get; }

        /// <summary>
        /// Gets the ID of the task, which is a sequential number unique to the orchestration instance.
        /// </summary>
        public int TaskId { get; }

        /// <summary>
        /// Gets or sets a value indicating how to propagate unhandled exception metadata.
        /// </summary>
        internal ErrorPropagationMode ErrorPropagationMode { get; set; }
    }
}