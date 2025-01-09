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

namespace DurableTask.AzureServiceFabric.Remote
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Formatting;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Web;

    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.AzureServiceFabric.Exceptions;
    using DurableTask.AzureServiceFabric.Models;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Web.Http.Results;

    /// <summary>
    /// Allows to interact with a remote IOrchestrationServiceClient
    /// </summary>
    public class RemoteOrchestrationServiceClient : IOrchestrationServiceClient, IDisposable
    {
        private const int PartitionResolutionRetryCount = 3;
        private static readonly TimeSpan PartitionResolutionRetryDelay = TimeSpan.FromSeconds(3);

        private readonly IPartitionEndpointResolver partitionProvider;
        private readonly int maxPollDelayInSecs = 10;
        private HttpClient httpClient;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public RemoteOrchestrationServiceClient(IPartitionEndpointResolver partitionProvider)
        {
            this.partitionProvider = partitionProvider;
            this.httpClient = new HttpClient();
        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public RemoteOrchestrationServiceClient(IPartitionEndpointResolver partitionProvider, HttpClientHandler httpClientHandler = null)
        {
            this.partitionProvider = partitionProvider;
            this.httpClient = new HttpClient(httpClientHandler);
        }

        /// <summary>
        /// HttpClient for making REST calls.
        /// </summary>
        public HttpClient HttpClient
        {
            get
            {
                return this.httpClient;
            }
            set
            {
                this.httpClient = value;
            }
        }

        /// <summary>
        /// Creates a new orchestration
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <exception cref="OrchestrationAlreadyExistsException">Will throw an OrchestrationAlreadyExistsException exception If any orchestration with the same instance Id exists in the instance store.</exception>
        /// <returns></returns>
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return this.CreateTaskOrchestrationAsync(creationMessage, null);
        }

        /// <summary>
        /// Creates a new orchestration and specifies a subset of states which should be de duplicated on in the client side
        /// </summary>
        /// <param name="creationMessage">Orchestration creation message</param>
        /// <param name="dedupeStatuses">States of previous orchestration executions to be considered while de-duping new orchestrations on the client</param>
        /// <exception cref="OrchestrationAlreadyExistsException">Will throw an OrchestrationAlreadyExistsException exception If any orchestration with the same instance Id exists in the instance store and it has a status specified in dedupeStatuses.</exception>
        /// <returns></returns>
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            creationMessage.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            await this.PutJsonAsync(
                creationMessage.OrchestrationInstance.InstanceId,
                this.GetOrchestrationFragment(creationMessage.OrchestrationInstance.InstanceId),
                new CreateTaskOrchestrationParameters() { TaskMessage = creationMessage, DedupeStatuses = dedupeStatuses },
                CancellationToken.None);
        }

        /// <summary>
        /// Forcefully terminate the specified orchestration instance
        /// </summary>
        /// <param name="instanceId">Instance to terminate</param>
        /// <param name="reason">Reason for termination</param>
        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            instanceId.EnsureValidInstanceId();

            var fragment = $"{this.GetOrchestrationFragment(instanceId)}?reason={HttpUtility.UrlEncode(reason)}";
            using (var response = await this.ExecuteRequestWithRetriesAsync(
                instanceId,
                async (baseUri) => await this.HttpClient.DeleteAsync(new Uri(baseUri, fragment)),
                CancellationToken.None))
            {
                if (!response.IsSuccessStatusCode)
                {
                    var message = await response.Content.ReadAsStringAsync();
                    throw new RemoteServiceException($"Unable to terminate task instance. Error: {response.StatusCode}:{message}", response.StatusCode);
                }
            }
        }

        /// <summary>
        /// Get a string dump of the execution history of the specified orchestration instance specified execution (generation) of the specified instance
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Execution id</param>
        /// <returns>String with formatted JSON representing the execution history</returns>
        public async Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            instanceId.EnsureValidInstanceId();

            var fragment = $"{this.GetOrchestrationFragment(instanceId)}?executionId={executionId}";
            var history = await this.GetStringResponseAsync(instanceId, fragment, CancellationToken.None);
            return history;
        }

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the most current execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="allExecutions">True if method should fetch all executions of the instance, false if the method should only fetch the most recent execution</param>
        /// <returns>List of OrchestrationState objects that represents the list of orchestrations in the instance store</returns>
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            instanceId.EnsureValidInstanceId();

            var fragment = $"{this.GetOrchestrationFragment(instanceId)}?allExecutions={allExecutions}";
            var stateString = await this.GetStringResponseAsync(instanceId, fragment, CancellationToken.None);
            var states = JsonConvert.DeserializeObject<IList<OrchestrationState>>(stateString);
            return states;
        }

        /// <summary>
        /// Get a list of orchestration states from the instance storage for the specified execution (generation) of the specified instance.
        /// </summary>
        /// <param name="instanceId">Instance id</param>
        /// <param name="executionId">Execution id</param>
        /// <returns>The OrchestrationState of the specified instanceId or null if not found</returns>
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            if (string.IsNullOrWhiteSpace(executionId))
            {
                return (await this.GetOrchestrationStateAsync(instanceId, false)).FirstOrDefault();
            }

            instanceId.EnsureValidInstanceId();

            var fragment = $"{this.GetOrchestrationFragment(instanceId)}?executionId={executionId}";
            var stateString = await this.GetStringResponseAsync(instanceId, fragment, CancellationToken.None);
            var state = JsonConvert.DeserializeObject<OrchestrationState>(stateString);
            return state;
        }

        /// <summary>
        /// Purges orchestration instance state and history for orchestrations older than the specified threshold time.
        /// Also purges the blob storage.
        /// </summary>
        /// <param name="thresholdDateTimeUtc">Threshold date time in UTC</param>
        /// <param name="timeRangeFilterType">What to compare the threshold date time against</param>
        public async Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            List<Task<HttpResponseMessage>> allTasks = new List<Task<HttpResponseMessage>>();
            foreach (var endpoint in await this.GetAllEndpointsAsync(CancellationToken.None))
            {
                var uri = $"{endpoint.ToString()}/{GetHistoryFragment()}";
                var task = this.HttpClient.PostAsJsonAsync(uri, new PurgeOrchestrationHistoryParameters { ThresholdDateTimeUtc = thresholdDateTimeUtc, TimeRangeFilterType = timeRangeFilterType });
                allTasks.Add(task);
            }

            var responses = await Task.WhenAll(allTasks.ToArray());
            foreach (var response in responses)
            {
                response.EnsureSuccessStatusCode();
            }
        }

        /// <summary>
        /// Send a new message for an orchestration
        /// </summary>
        /// <param name="message">Message to send</param>
        /// <returns></returns>
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
            await this.PutJsonAsync(message.OrchestrationInstance.InstanceId, this.GetMessageFragment(message.SequenceNumber), message, CancellationToken.None);
        }

        /// <summary>
        /// Send a new set of messages for an orchestration
        /// </summary>
        /// <param name="messages">Messages to send</param>
        /// <returns></returns>
        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            foreach (var message in messages)
            {
                message.OrchestrationInstance.InstanceId.EnsureValidInstanceId();
                await SendTaskOrchestrationMessageAsync(message);
            }
        }

        /// <summary>
        /// Wait for an orchestration to reach any terminal state within the given timeout
        /// </summary>
        /// <param name="instanceId">Instance id of the orchestration</param>
        /// <param name="executionId">Execution id of the orchestration</param>
        /// <param name="timeout">Maximum amount of time to wait</param>
        /// <param name="cancellationToken">Task cancellation token</param>
        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            instanceId.EnsureValidInstanceId();
            var maxTime = DateTime.Now.Add(timeout);
            OrchestrationState state = null;
            double pollDelayInSecs = maxPollDelayInSecs;
            if (timeout.TotalSeconds < pollDelayInSecs)
            {
                pollDelayInSecs = timeout.TotalSeconds;
            }

            state = await this.GetOrchestrationStateAsync(instanceId, executionId);
            if (state != null && state.OrchestrationStatus.IsTerminalState())
            {
                return state;
            }

            do
            {
                await Task.Delay(TimeSpan.FromSeconds(pollDelayInSecs), cancellationToken);
                state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                if (state != null && state.OrchestrationStatus.IsTerminalState())
                {
                    return state;
                }

            } while (!cancellationToken.IsCancellationRequested && DateTime.Now < maxTime);

            // Either cancellation was requested or timedout, return the last known state.
            return state;
        }

        #region Prepare url fragments for the requests

        private string GetHistoryFragment() => "history";

        private string GetHistoryFragment(string orchestrationId) => $"history/{orchestrationId}";

        private string GetOrchestrationFragment() => "orchestrations";

        private string GetOrchestrationFragment(string orchestrationId) => $"orchestrations/{orchestrationId}";

        private string GetMessageFragment() => "messages";

        private string GetMessageFragment(long messageId) => $"messages/{messageId}";

        #endregion Prepare url fragments for the requests

        private async Task<string> GetStringResponseAsync(string instanceId, string fragment, CancellationToken cancellationToken)
        {
            using (var response = await this.ExecuteRequestWithRetriesAsync(
                instanceId,
                async (baseUri) => await this.HttpClient.GetAsync(new Uri(baseUri, fragment)),
                cancellationToken))
            {
                string content = await response.Content.ReadAsStringAsync();
                if (response.IsSuccessStatusCode)
                {
                    return content;
                }

                throw new HttpRequestException($"Request failed with status code '{response.StatusCode}' and content '{content}'");
            }
        }

        private async Task PutJsonAsync(string instanceId, string fragment, object @object, CancellationToken cancellationToken)
        {
            var mediaFormatter = new JsonMediaTypeFormatter()
            {
                SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All }
            };

            using (var result = await this.ExecuteRequestWithRetriesAsync(
                instanceId,
                async (baseUri) => await this.HttpClient.PutAsync(new Uri(baseUri, fragment), @object, mediaFormatter),
                cancellationToken))
            {

                // TODO: Improve exception handling
                if (result.StatusCode == HttpStatusCode.Conflict)
                {
                    throw await (result.Content?.ReadAsAsync<OrchestrationAlreadyExistsException>() ?? Task.FromResult(new OrchestrationAlreadyExistsException()));
                }

                if (!result.IsSuccessStatusCode)
                {
                    var content = await (result.Content?.ReadAsStringAsync() ?? Task.FromResult<string>(null));
                    throw new RemoteServiceException($"CreateTaskOrchestrationAsync failed with status code {result.StatusCode}: {content}", result.StatusCode);
                }
            }
        }

        private async Task<IEnumerable<Uri>> GetAllEndpointsAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IEnumerable<string> endpoints = await this.partitionProvider.GetPartitionEndpointsAsync(cancellationToken);
            return endpoints.Select(GetDefaultEndPoint).Select(x => new Uri(x));
        }

        private string GetDefaultEndPoint(string endpoint)
        {
            // sample endpoint - {"Endpoints":{"":"http:\/\/10.91.42.35:30001"}}
            var jObject = JObject.Parse(endpoint);
            var defaultEndPoint = jObject["Endpoints"][Constants.TaskHubProxyServiceName].ToString();
            return defaultEndPoint;
        }

        private async Task<HttpResponseMessage> ExecuteRequestWithRetriesAsync(string instanceId, Func<Uri, Task<HttpResponseMessage>> requestAsync, CancellationToken cancellationToken)
        {
            instanceId.EnsureValidInstanceId();

            int retryAttempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                HttpResponseMessage response = null;
                Exception exception = null;
                try
                {
                    var endpointJson = await this.partitionProvider.GetPartitionEndPointAsync(instanceId, cancellationToken);
                    var endpoint = this.GetDefaultEndPoint(endpointJson);

                    response = await requestAsync(new Uri(endpoint));
                    if (response.IsSuccessStatusCode || response.Headers.Contains(Constants.ActivityIdHeaderName))
                    {
                        return response;
                    }

                    // We will end up with an incorrect endpoint and a valid response without the ActivityId header when all of these conditions are true:
                    // a) Service Fabric internal cache fails to receive notification for a replica move (and the subsequent endpoint change).
                    // b) The http.sys URLACL either failed to clean up or was reused by another replica on the same machine.
                    // c) The server side is yet to be upgraded to return ActivityId header with each request.
                    // The HTTP status code 404 or 503 will depend upon the state the new replica and the active URLACLs.
                    // HTTP 404 - We'll end up with this error if the port was reused by another replica belonging to a different partition.
                    //            This will make the partition id component of the URL different resulting in http.sys returning a 404.
                    // HTTP 503 - We'll end up with this error if the URLACL fails to get cleaned up and we have no no process listening
                    //            on this port. This error code is returned by http.sys itself.
                    if (response.StatusCode != HttpStatusCode.NotFound && response.StatusCode != HttpStatusCode.ServiceUnavailable)
                    {
                        return response;
                    }
                }
                catch (Exception ex) when (ex is SocketException || ex is WebException || ex is HttpRequestException)
                {
                    exception = ex;
                }

                if (++retryAttempt < PartitionResolutionRetryCount)
                {
                    response?.Dispose();

                    await Task.Delay(PartitionResolutionRetryDelay);
                    await this.partitionProvider.RefreshPartitionEndpointAsync(instanceId, cancellationToken);
                    continue;
                }

                if (exception != null)
                {
                    throw exception;
                }

                return response;
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        /// <inheritdoc />
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    this.httpClient.Dispose();
                }

                disposedValue = true;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
