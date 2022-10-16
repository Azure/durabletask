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
    using System;
    using System.Collections.Generic;
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using Newtonsoft.Json;

    /// <summary>
    /// The result of an orchestration execution.
    /// </summary>
    public class OrchestratorExecutionResult
    {
        /// <summary>
        /// The list of actions resulting from the orchestrator execution.
        /// </summary>
        [JsonProperty("actions")]
        public IEnumerable<OrchestratorAction> Actions { get; set; } = Array.Empty<OrchestratorAction>();

        /// <summary>
        /// The custom status, if any, of the orchestrator.
        /// </summary>
        [JsonProperty("customStatus")]
        public string? CustomStatus { get; set; }

        /// <summary>
        /// Creates an orchestrator failure result with a specified message and exception.
        /// </summary>
        /// <param name="message">The simple failure message.</param>
        /// <param name="e">The exception that triggered the failure.</param>
        /// <returns>The orchestrator failure result.</returns>
        public static OrchestratorExecutionResult ForFailure(string message, Exception e)
        {
            return ForFailure(message, e.ToString());
        }

        /// <summary>
        /// Creates an orchestrator failure result with a specified message and details.
        /// </summary>
        /// <param name="message">The simple failure message.</param>
        /// <param name="details">The failure details that give more information about what triggered the failure.</param>
        /// <returns>The orchestrator failure result.</returns>
        public static OrchestratorExecutionResult ForFailure(string message, string? details)
        {
            return new OrchestratorExecutionResult
            {
                Actions = new List<OrchestratorAction>
                {
                    new OrchestrationCompleteOrchestratorAction
                    {
                        OrchestrationStatus = OrchestrationStatus.Failed,
                        Result = Utils.SerializeToJson((new { message, details })),
                    },
                },
            };
        }
    }
}