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

namespace DurableTask.AzureServiceFabric
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Web.Http;
    using System.Web.Http.Results;

    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.AzureServiceFabric.Models;
    using DurableTask.AzureServiceFabric.Tracing;

    /// <summary>
    /// A Web Api controller that provides TaskHubClient operations.
    /// </summary>
    public class FabricOrchestrationServiceController : ApiController
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
        /// <returns> <see cref="IHttpActionResult"/> object. </returns>
        [HttpPut]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<IHttpActionResult> CreateTaskOrchestration([FromUri] string orchestrationId, [FromBody] CreateTaskOrchestrationParameters parameters)
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

                return new OkResult(this);
            }
            catch (OrchestrationAlreadyExistsException)
            {
                return Conflict();
            }
        }

        /// <summary>
        /// Sends an orchestration message to TaskHubClient.
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="message">Message to send</param>
        /// <returns> <see cref="IHttpActionResult"/> object. </returns>
        [HttpPost]
        [Route("messages/{messageId}")]
        public async Task<IHttpActionResult> SendTaskOrchestrationMessage([FromUri]long messageId,[FromBody] TaskMessage message)
        {
            if (messageId != message.SequenceNumber)
            {
                return Conflict();
            }

            message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            await this.orchestrationServiceClient.SendTaskOrchestrationMessageAsync(message);
            return new OkResult(this);
        }

        /// <summary>
        /// Sends an array of orchestration messages to TaskHubClient.
        /// </summary>
        /// <param name="messages">Message to send</param>
        /// <returns> <see cref="IHttpActionResult"/> object. </returns>
        [HttpPost]
        [Route("messages")]
        public async Task<IHttpActionResult> SendTaskOrchestrationMessageBatch([FromBody] TaskMessage[] messages)
        {
            await this.orchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(messages);
            return new OkResult(this);
        }

        /// <summary>
        /// Gets the state of orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <returns> <see cref="OrchestrationState" /> object. </returns>
        [HttpGet]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<OrchestrationState> GetOrchestrationState([FromUri]string orchestrationId, string executionId)
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
        [Route("orchestrations/{orchestrationId}")]
        public async Task<IList<OrchestrationState>> GetOrchestrationState([FromUri]string orchestrationId, bool allExecutions)
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
        /// <returns> <see cref="IHttpActionResult"/> object. </returns>
        [HttpDelete]
        [Route("orchestrations/{orchestrationId}")]
        public async Task<IHttpActionResult> ForceTerminateTaskOrchestration([FromUri]string orchestrationId, string reason)
        {
            orchestrationId.EnsureValidInstanceId();
            await this.orchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(orchestrationId, reason);
            return new OkResult(this);
        }

        /// <summary>
        /// Gets the history of orchestration.
        /// </summary>
        /// <param name="orchestrationId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <returns>Orchestration history</returns>
        [HttpGet]
        [Route("history/{orchestrationId}")]
        public async Task<string> GetOrchestrationHistory([FromUri]string orchestrationId, string executionId)
        {
            orchestrationId.EnsureValidInstanceId();
            var result = await this.orchestrationServiceClient.GetOrchestrationHistoryAsync(orchestrationId, executionId);
            return result;
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// </summary>
        /// <param name="purgeParameters">Purge history parameters</param>
        /// <returns> <see cref="IHttpActionResult"/> object. </returns>
        [HttpPost]
        [Route("history")]
        public async Task<IHttpActionResult> PurgeOrchestrationHistory([FromBody]PurgeOrchestrationHistoryParameters purgeParameters)
        {
            await this.orchestrationServiceClient.PurgeOrchestrationHistoryAsync(purgeParameters.ThresholdDateTimeUtc, purgeParameters.TimeRangeFilterType);
            return new OkResult(this);
        }
    }
}
