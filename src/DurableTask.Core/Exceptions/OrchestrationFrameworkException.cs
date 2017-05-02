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

    /// <summary>
    /// Represents errors created by the orchestration framework
    /// </summary>
    [Serializable]
    public class OrchestrationFrameworkException : Exception
    {
        /// <summary>
        /// Initializes an new instance of the OrchestrationFrameworkException class with a specified error message
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public OrchestrationFrameworkException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes an new instance of the OrchestrationFrameworkException class with a specified inner exception
        /// </summary>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is specified.</param>
        public OrchestrationFrameworkException(Exception innerException)
            : base(innerException.Message, innerException)
        {
        }

        /// <summary>
        /// Initializes an new instance of the OrchestrationFrameworkException class with a specified error message
        ///    and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is specified.</param>
        public OrchestrationFrameworkException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}