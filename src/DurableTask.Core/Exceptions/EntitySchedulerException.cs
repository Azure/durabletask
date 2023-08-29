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
#nullable enable
namespace DurableTask.Core.Exceptions
{
    using System;
    using System.Runtime.Serialization;
 
    /// <summary>
    /// Exception used to describe various issues encountered by the entity scheduler.
    /// </summary>
    [Serializable]
    public class EntitySchedulerException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EntitySchedulerException"/> class.
        /// </summary>
        public EntitySchedulerException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntitySchedulerException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public EntitySchedulerException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes an new instance of the <see cref="EntitySchedulerException"/> class.
        /// </summary>
        /// <param name="errorMessage">The message that describes the error.</param>
        /// <param name="innerException">The exception that was caught.</param>
        public EntitySchedulerException(string errorMessage, Exception innerException)
            : base(errorMessage, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EntitySchedulerException"/> class with serialized data.
        /// </summary>
        /// <param name="info">The System.Runtime.Serialization.SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The System.Runtime.Serialization.StreamingContext that contains contextual information about the source or destination.</param>
        protected EntitySchedulerException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}