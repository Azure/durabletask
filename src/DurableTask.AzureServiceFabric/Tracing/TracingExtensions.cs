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

namespace DurableTask.AzureServiceFabric.Tracing
{
    using System;

    using Microsoft.ServiceFabric.Services.Runtime;

    internal static class TracingExtensions
    {
        internal static void LogFabricServiceInformation(this ServiceFabricProviderEventSource eventSource, StatefulService service, string message, params object[] args)
        {
            string formattedMessage = string.Format(message, args);
            eventSource.LogFabricServiceInformation(
                service.Context.ServiceName.ToString(),
                service.Context.ServiceTypeName,
                service.Context.ReplicaId,
                service.Context.PartitionId,
                service.Context.CodePackageActivationContext.ApplicationName,
                service.Context.CodePackageActivationContext.ApplicationTypeName,
                service.Context.NodeContext.NodeName,
                formattedMessage);
        }

        internal static void LogProxyServiceError(this ServiceFabricProviderEventSource eventSource, string requestUri, string requestMethod, Exception exception)
        {
            string exceptionDetails = $"Type: {exception.GetType()}, Message: {exception.Message}, StackTrace: {exception.StackTrace}, InnerException: {exception.InnerException}";
            string logMessage = $"Proxy service request {requestUri} with method {requestMethod} resulted in error. Exception Details - {exceptionDetails}";

            eventSource.LogProxyServiceError(logMessage);
        }
    }
}
