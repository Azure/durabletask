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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.Core.Exceptions
{
    /// <summary>
    /// The exception thrown by when a storage update detects a conflict, such as an e-tag mismatch.
    /// </summary>
    [Serializable]
    public class StorageConflictException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="StorageConflictException" /> class using default values.</summary>
        public StorageConflictException()
            : base()
        {
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="StorageConflictException" /> class using specified error message.</summary> 
        /// <param name="message">The message associated with the error.</param>
        public StorageConflictException(string message)
            : base(message)
        {
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="StorageConflictException" /> class using specified error message.</summary> 
        /// <param name="message">The message associated with the error.</param>
        /// <param name="innerException">The error that caused the exception.</param>
        public StorageConflictException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageConflictException" />  class with serialized data.
        /// </summary>
        /// <param name="info">The System.Runtime.Serialization.SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The System.Runtime.Serialization.StreamingContext that contains contextual information about the source or destination.</param>
        protected StorageConflictException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
