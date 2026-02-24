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

#if !NETFRAMEWORK
namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Net;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;

    /// <summary>
    /// ASP.NET Core middleware for activity logging of proxy service requests.
    /// </summary>
    internal class ActivityLoggingMiddleware
    {
        private readonly RequestDelegate next;

        public ActivityLoggingMiddleware(RequestDelegate next)
        {
            this.next = next;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            string requestMethod = context.Request.Method;
            string requestPath = context.Request.Path.ToString();
            string activityId = Guid.NewGuid().ToString("D");

            ServiceFabricProviderEventSource.Tracing.LogProxyServiceRequestInformation(
                $"{activityId} : Proxy service incoming request {requestPath} with method {requestMethod}");

            try
            {
                await this.next(context);
                ServiceFabricProviderEventSource.Tracing.LogProxyServiceRequestInformation(
                    $"{activityId} : Proxy service responding request {requestPath} with method {requestMethod} with status code {context.Response.StatusCode}");
            }
            catch (Exception exception)
            {
                ServiceFabricProviderEventSource.Tracing.LogProxyServiceError(activityId, requestPath, requestMethod, exception);
                context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsync(JsonConvert.SerializeObject(exception));
            }

            context.Response.Headers[Constants.ActivityIdHeaderName] = activityId;
        }
    }
}
#endif
