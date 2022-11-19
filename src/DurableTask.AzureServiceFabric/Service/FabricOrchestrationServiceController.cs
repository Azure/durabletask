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

namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.AzureServiceFabric.Models;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Microsoft.AspNetCore.Mvc;

    /// <summary>
    /// A Web Api controller that provides TaskHubClient operations.
    /// </summary>
    public class FabricOrchestrationServiceController : ControllerBase
    {
        private readonly IOrchestrationServiceClient orchestrationServiceClient;

        /// <summary>
        /// Creates an instance of FabricOrchestrationServiceController for given OrchestrationServiceClient
        /// </summary>
        /// <param name="orchestrationServiceClient">IOrchestrationServiceClient instance</param>
        public FabricOrchestrationServiceController(IOrchestrationServiceClient orchestrationServiceClient)
        {
            this.orchestrationServiceClient = orchestrationServiceClient;
        }

        /// <summary>
        /// Creates a task orchestration.
        /// </summary>
        /// <param name="orchestrationId">Orchestration Id</param>
        /// <param name="parameters">Parameters for creating task orchestration.</param>
        /// <exception cref="OrchestrationAlreadyExistsException">Will throw an OrchestrationAlreadyExistsException exception
        /// If any orchestration with the same instance Id exists in the instance store and it has a status specified in dedupeStatuses.</exception>
        /// <returns> <see cref="IActionResult"/> object. </returns>
        [HttpPut]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<IActionResult> CreateTaskOrchestration([FromRoute] string orchestrationId, [FromBody] CreateTaskOrchestrationParameters parameters)
        {
            parameters.TaskMessage.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            if (!orchestrationId.Equals(parameters.TaskMessage.OrchestrationInstance.InstanceId))
            {
                return BadRequest($"OrchestrationId from Uri {orchestrationId} doesn't match with the one from body {parameters.TaskMessage.OrchestrationInstance.InstanceId}");
            }

            try
            {
                if (parameters.DedupeStatuses == null)
                {
                    await this.orchestrationServiceClient.CreateTaskOrchestrationAsync(parameters.TaskMessage);
                }
                else
                {
                    await this.orchestrationServiceClient.CreateTaskOrchestrationAsync(parameters.TaskMessage, parameters.DedupeStatuses);
                }

                return Ok();
            }
            catch (OrchestrationAlreadyExistsException ex)
            {
                return BadRequest(ex.ToString());
            }
            catch (NotSupportedException ex)
            {
                return BadRequest(ex.ToString());
            }
        }

        /// <summary>
        /// Sends an orchestration message to TaskHubClient.
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="message">Message to send</param>
        /// <returns> <see cref="IActionResult"/> object. </returns>
        [HttpPost]
        [Route("messages/{messageId}")]
        public async Task<IActionResult> SendTaskOrchestrationMessage([FromRoute] long messageId, [FromBody] TaskMessage message)
        {
            if (messageId != message.SequenceNumber)
            {
                return Conflict();
            }

            message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            await this.orchestrationServiceClient.SendTaskOrchestrationMessageAsync(message);
            return Ok();
        }

        /// <summary>
        /// Sends an array of orchestration messages to TaskHubClient.
        /// </summary>
        /// <param name="messages">Message to send</param>
        /// <returns> <see cref="IActionResult"/> object. </returns>
        [HttpPost]
        [Route("messages")]
        public async Task<IActionResult> SendTaskOrchestrationMessageBatch([FromBody] TaskMessage[] messages)
        {
            await this.orchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(messages);
            return Ok();
        }

        /// <summary>
        /// Gets the state of orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <returns> <see cref="OrchestrationState" /> object. </returns>
        [HttpGet]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<OrchestrationState> GetOrchestrationState([FromRoute] string orchestrationId, string executionId)
        {
            orchestrationId.EnsureValidInstanceId();
            var state = await this.orchestrationServiceClient.GetOrchestrationStateAsync(orchestrationId, executionId);
            return state;
        }

        /// <summary>
        /// Gets the state of orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="allExecutions">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>List of <see cref="OrchestrationState"/>. </returns>
        [HttpGet]
        [Route("orchestrationsAll/{orchestrationId}")]
        public async Task<IList<OrchestrationState>> GetOrchestrationState([FromRoute] string orchestrationId, bool allExecutions)
        {
            orchestrationId.EnsureValidInstanceId();
            var state = await this.orchestrationServiceClient.GetOrchestrationStateAsync(orchestrationId, allExecutions);
            return state;
        }

        /// <summary>
        /// Terminates an orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="reason">Execution id of the orchestration</param>
        /// <returns> <see cref="IActionResult"/> object. </returns>
        [HttpDelete]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<IActionResult> ForceTerminateTaskOrchestration([FromRoute] string orchestrationId, string reason)
        {
            orchestrationId.EnsureValidInstanceId();
            await this.orchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(orchestrationId, reason);
            return Ok();
        }

        /// <summary>
        /// Gets the history of orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <returns>Orchestration history</returns>
        [HttpGet]
        [Route("history/{orchestrationId}")]
        public async Task<string> GetOrchestrationHistory([FromRoute] string orchestrationId, string executionId)
        {
            orchestrationId.EnsureValidInstanceId();
            var result = await this.orchestrationServiceClient.GetOrchestrationHistoryAsync(orchestrationId, executionId);
            return result;
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// </summary>
        /// <param name="purgeParameters">Purge history parameters</param>
        /// <returns> <see cref="IActionResult"/> object. </returns>
        [HttpPost]
        [Route("history")]
        public async Task<IActionResult> PurgeOrchestrationHistory([FromBody] PurgeOrchestrationHistoryParameters purgeParameters)
        {
            await this.orchestrationServiceClient.PurgeOrchestrationHistoryAsync(purgeParameters.ThresholdDateTimeUtc, purgeParameters.TimeRangeFilterType);
            return Ok();
        }
    }
}
