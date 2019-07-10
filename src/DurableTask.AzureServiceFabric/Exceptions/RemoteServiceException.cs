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

namespace DurableTask.AzureServiceFabric.Exceptions
{
    using System;
    using System.Net;

    using DurableTask.AzureServiceFabric.Remote;


    /// <summary>
    /// The exception that is thrown by <see cref="RemoteOrchestrationServiceClient"/> when proxy service returns non successful message.
    /// </summary>
    public class RemoteServiceException : Exception
    {
        private HttpStatusCode statusCode;

        /// <summary>
        /// Creates an instance of <see cref="RemoteServiceException"/>.
        /// </summary>
        /// <param name="message">Exception message.</param>
        /// <param name="statusCode">Http response message</param>
        public RemoteServiceException(string message, HttpStatusCode statusCode) : base(message)
        {
            this.statusCode = statusCode;
        }

        /// <summary>
        /// Instace of <see cref="HttpStatusCode"/> sent by proxy service.
        /// </summary>
        public HttpStatusCode HttpStatusCode => this.statusCode;
    }
}
