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
    using System.Net;
    using System.Net.Http;
    using System.Web.Http.ExceptionHandling;
    using System.Web.Http.Results;

    using DurableTask.AzureServiceFabric.Tracing;

    internal class ProxyServiceExceptionHandler : ExceptionHandler
    {
        public override void Handle(ExceptionHandlerContext context)
        {
            ServiceFabricProviderEventSource.Tracing.LogProxyServiceError(context.Request.Method.ToString(),
                                                                          context.Request.RequestUri.AbsolutePath,
                                                                          context.Exception);

            HttpResponseMessage response = context.Request.CreateResponse(HttpStatusCode.InternalServerError, context.Exception);
            context.Result = new ResponseMessageResult(response);
        }
    }
}
