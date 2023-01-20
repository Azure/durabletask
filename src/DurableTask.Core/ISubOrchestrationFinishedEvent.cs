using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core.Tracing;

namespace DurableTask.Core
{
    internal interface ISubOrchestrationFinishedEvent
    {
        /// <summary>
        /// The name of the sub-orchestration.
        /// </summary>
        int TaskScheduledId { get; }

        /// <summary>
        /// The W3C trace context associated with this event.
        /// </summary>
        DistributedTraceContext ParentTraceContext { get; set; }
    }
}
