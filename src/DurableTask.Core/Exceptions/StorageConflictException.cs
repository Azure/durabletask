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
    }
}
