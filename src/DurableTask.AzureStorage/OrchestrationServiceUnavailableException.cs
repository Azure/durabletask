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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Thrown when a client request is rejected because the Azure Storage backend is unavailable, for example while a
    /// live migration is ending and requests should be directed to the new backend.
    /// </summary>
    [Serializable]
    public class OrchestrationServiceUnavailableException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationServiceUnavailableException"/> class.
        /// </summary>
        public OrchestrationServiceUnavailableException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationServiceUnavailableException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public OrchestrationServiceUnavailableException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationServiceUnavailableException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public OrchestrationServiceUnavailableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationServiceUnavailableException"/> class with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data.</param>
        /// <param name="context">The contextual information about the source or destination.</param>
        protected OrchestrationServiceUnavailableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
