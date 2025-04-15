using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Core
{
    /// <summary>
    /// Tags to be used when sending external events.
    /// </summary>
    public static class EventTags
    {
        /// <summary>
        /// Whether or not to create a trace for this entity request event
        /// </summary>
        public const string CreateEntityRequestEventTrace = "CreateSignalEntityEventTrace";

        /// <summary>
        /// Whether or not to create a trace for this entity response event
        /// </summary>
        public const string CreateEntityResponseEventTrace = "CreateEntityResponseEventTrace";
    }
}
