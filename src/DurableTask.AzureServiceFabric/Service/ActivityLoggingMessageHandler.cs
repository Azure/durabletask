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
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.Tracing;

    internal class ActivityLoggingMessageHandler : DelegatingHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string requestMethod = request.Method.ToString();
            string requestUri = request.RequestUri.AbsolutePath;
            ServiceFabricProviderEventSource.Tracing.LogProxyServiceRequestInformation($"Proxy service incoming request {requestUri} with method {requestMethod}");
            HttpResponseMessage response = null;
            try
            {
                response = await base.SendAsync(request, cancellationToken);
                ServiceFabricProviderEventSource.Tracing.LogProxyServiceRequestInformation($"Proxy service responding request {requestUri} with method {requestMethod}");
            }
            catch (Exception exception)
            {
                ServiceFabricProviderEventSource.Tracing.LogProxyServiceError(requestUri, requestMethod, exception);
                response = request.CreateErrorResponse(HttpStatusCode.InternalServerError, exception.Message);
            }

            return response;
        }
    }
}
