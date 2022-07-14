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

using System.Runtime.Serialization;
using System.Security;

namespace DurableTask.AzureServiceFabric.Exceptions;

using System;
using System.Net;

using DurableTask.AzureServiceFabric.Remote;


/// <summary>
/// The exception that is thrown by <see cref="RemoteOrchestrationServiceClient"/> when proxy service returns non successful message.
/// </summary>
public class RemoteServiceException : Exception
{
    private readonly HttpStatusCode statusCode;

    /// <summary>
    /// Creates an instance of <see cref="RemoteServiceException"/>.
    /// </summary>
    /// <param name="message">Exception message.</param>
    /// <param name="statusCode">Http response message</param>
    public RemoteServiceException(string message, HttpStatusCode statusCode) : base(message)
     => this.statusCode = statusCode;

    /// <summary>Initializes a new instance of the <see cref="RemoteServiceException" /> class.</summary>
    private RemoteServiceException() { }

    /// <summary>Initializes a new instance of the <see cref="RemoteServiceException" /> class with a specified error message.</summary>
    /// <param name="message">The message that describes the error.</param>
    private RemoteServiceException(string message) : base(message) { }

    /// <summary>Initializes a new instance of the <see cref="RemoteServiceException" /> class with serialized data.</summary>
    /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo" /> that holds the serialized object data about the exception being thrown.</param>
    /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext" /> that contains contextual information about the source or destination.</param>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="info" /> is <see langword="null" />.</exception>
    /// <exception cref="System.Runtime.Serialization.SerializationException">The class name is <see langword="null" /> or <see cref="System.Exception.HResult" /> is zero (0).</exception>
    [SecuritySafeCritical]
    protected RemoteServiceException(SerializationInfo info, StreamingContext context) : base(info, context) { }

    private RemoteServiceException(string message, Exception innerException) : base(message, innerException) { }

    /// <summary>
    /// Instace of <see cref="HttpStatusCode"/> sent by proxy service.
    /// </summary>
    public HttpStatusCode HttpStatusCode => this.statusCode;
}
