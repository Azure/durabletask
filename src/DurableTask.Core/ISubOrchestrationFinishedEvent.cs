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
        string Name { get; set; }

        /// <summary>
        /// The W3C trace context associated with this event.
        /// </summary>
        DistributedTraceContext ParentTraceContext { get; set; }

        /// <summary>
        /// The start time of the sub-orchestration.
        /// </summary>
        DateTime StartTime { get; set; }

        /// <summary>
        /// Checks if the properties needed for distributed tracing are set. This is used to check
        /// whether a span should be created or not.
        /// </summary>
        bool DistributedTracingPropertiesAreSet();
    }
}
