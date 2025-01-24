// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Exceptions
{
    using System;

    /// <summary>
    /// Represents a work item that is poisoned and should not be retried.
    /// </summary>
    public class WorkItemPoisonedException : Exception
    {
        /// <summary>
        /// Represents a work item that is poisoned and should not be retried.
        /// </summary>
        public WorkItemPoisonedException(
            string message = "Work item is poisoned",
            Exception innerException = null
        ) : base(message, innerException)
        {
        }
    }
}
