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

namespace DurableTask.Core.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents errors created during deserialization
    /// </summary>
    [Serializable]
    public class TaskFailedExceptionDeserializationException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailedExceptionDeserializationException"/> class.
        /// </summary>
        public TaskFailedExceptionDeserializationException()
        {
        }

        /// <summary>
        /// Initializes an new instance of the TaskFailedExceptionDeserializationException class with a specified error message
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TaskFailedExceptionDeserializationException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes an new instance of the TaskFailedExceptionDeserializationException class with a specified error message
        ///    and a reference to the deserialization exception that is the cause.
        /// </summary>
        /// <param name="details">The message that describes the error.</param>
        /// <param name="deserializationException">The deserialization exception that is the cause of the current exception.</param>
        public TaskFailedExceptionDeserializationException(string details, Exception deserializationException)
            : base("Failed to deserialize exception from TaskActivity: " + details, deserializationException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the TaskFailedExceptionDeserializationException class with serialized data.
        /// </summary>
        /// <param name="info">The System.Runtime.Serialization.SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The System.Runtime.Serialization.StreamingContext that contains contextual information about the source or destination.</param>
        protected TaskFailedExceptionDeserializationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}