using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
namespace DurableTask.Core
{
    /// <summary>
    /// TraceContext keep the correlation value.
    /// </summary>
    public class TraceContext
    {
        /// <summary>
        /// Default constructor 
        /// </summary>
        public TraceContext()
        {
            OrchestrationTraceContexts = new Stack<TraceContext>();
        }

        /// <summary>
        /// ParentId for backward compatibility
        /// </summary>
        public string ParentId { get; set; }

        /// <summary>
        /// W3C TraceContext: Traceparent
        /// </summary>
        public string Traceparent { get; set; }

        /// <summary>
        /// W3C TraceContext: Tracestate
        /// </summary>
        public string Tracestate { get; set; }

        /// <summary>
        /// W3C TraceContext: ParentSpanId
        /// </summary>
        public string ParentSpanId { get; set; }

        /// <summary>
        /// OrchestrationState save the state of the 
        /// </summary>
        public Stack<TraceContext> OrchestrationTraceContexts { get; set; }
    }
}