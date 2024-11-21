using System;

namespace DurableTask.Core
{
    /// <summary>
    /// Base class for Work Items.
    /// </summary>
    public abstract class WorkItemBase
    {
        /// <summary>
        /// The datetime this work item is locked until
        /// </summary>
        public DateTime LockedUntilUtc;

        /// <summary>
        /// The trace context used for correlation.
        /// </summary>
        public TraceContextBase TraceContext;
    }
}
