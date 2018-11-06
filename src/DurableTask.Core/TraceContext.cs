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

namespace DurableTask.Core
{
    using System.Collections.Generic;

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
