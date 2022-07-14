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

/// <summary>
/// Exception representing that instanceId is not Valid.
/// </summary>
[Serializable]
public class InvalidInstanceIdException : Exception
{
    /// <summary>
    /// Creates an instance of <see cref="InvalidInstanceIdException"/>
    /// </summary>
    /// <param name="instanceId">Orchestration instance id</param>
    public InvalidInstanceIdException(string instanceId)
        : base("Not a valid instanceId: " + instanceId) { }

    /// <summary>Initializes a new instance of the <see cref="InvalidInstanceIdException" /> class.</summary>
    private InvalidInstanceIdException() { }

    /// <summary>Initializes a new instance of the <see cref="InvalidInstanceIdException" /> class with a specified error message and a reference to the inner exception that is the cause of this exception.</summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or a null reference (<see langword="Nothing" /> in Visual Basic) if no inner exception is specified.</param>
    private InvalidInstanceIdException(string message, Exception innerException) : base(message, innerException) { }

    /// <summary>Initializes a new instance of the <see cref="InvalidInstanceIdException" /> class with serialized data.</summary>
    /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo" /> that holds the serialized object data about the exception being thrown.</param>
    /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext" /> that contains contextual information about the source or destination.</param>
    /// <exception cref="System.ArgumentNullException">
    /// <paramref name="info" /> is <see langword="null" />.</exception>
    /// <exception cref="System.Runtime.Serialization.SerializationException">The class name is <see langword="null" /> or <see cref="System.Exception.HResult" /> is zero (0).</exception>
    [SecuritySafeCritical]
    protected InvalidInstanceIdException(SerializationInfo info, StreamingContext context) : base(info, context) { }
}
