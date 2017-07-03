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

namespace DurableTask.AzureStorage.Partitioning
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>Represents an exception that occurs when the service lease has been lost.</summary>
    [Serializable]
    class LeaseLostException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using default values.</summary>
        public LeaseLostException()
            : base()
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using specified lease.</summary>
        /// <param name="lease">The messaging lease.</param>
        public LeaseLostException(Lease lease)
            : base()
        {
            this.Lease = lease;
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using specified lease and the error that caused the exception.</summary> 
        /// <param name="lease">The messaging lease.</param>
        /// <param name="innerException">The error that caused the exception.</param>
        public LeaseLostException(Lease lease, Exception innerException)
            : base(null, innerException)
        {
            this.Lease = lease;
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using specified error message.</summary> 
        /// <param name="message">The message associated with the error.</param>
        public LeaseLostException(string message)
            : base(message)
        {
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using specified error message and inner exception.</summary> 
        /// <param name="message">The message associated with the error.</param>
        /// <param name="innerException">The error that caused the exception.</param>
        public LeaseLostException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>Initializes a new instance of the 
        /// <see cref="DurableTask.AzureStorage.Partitioning.LeaseLostException" /> class using specified information and context.</summary> 
        /// <param name="info">The serialized information about the exception.</param>
        /// <param name="context">The contextual information about the source or destination.</param>
        protected LeaseLostException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
            this.Lease = (Lease)info.GetValue("Lease", typeof(Lease));
        }

        /// <summary>Gets or sets the service lease.</summary>
        /// <value>The service lease.</value>
        public Lease Lease { get; private set; }

        /// <summary>Populates a <see cref="System.Runtime.Serialization.SerializationInfo" /> with the data needed to serialize the target object.</summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo" /> object to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (Lease != null)
            {
                info.AddValue("Lease", this.Lease);
            }
        }
    }
}
