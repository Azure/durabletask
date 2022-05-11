﻿//  ----------------------------------------------------------------------------------
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
#nullable enable
namespace DurableTask.Core.Tracing
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// W3C-compliant distributed trace context.
    /// Spec: https://www.w3.org/TR/trace-context/.
    /// </summary>
    public class DistributedTraceContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DistributedTraceContext"/> class.
        /// </summary>
        /// <param name="traceParent">The W3C traceparent ID.</param>
        /// <param name="traceState">The optional W3C tracestate data.</param>
        public DistributedTraceContext(string traceParent, string? traceState = null)
        {
            this.TraceParent = traceParent;

            // The W3C spec allows vendors to truncate the trace state if it exceeds 513 characters,
            // but it has very specific requirements on HOW trace state can be modified, including
            // removing whole values, starting with the largest values, and preserving ordering.
            // Rather than implementing these complex requirements, we take the lazy path of just
            // truncating the whole thing.
            this.TraceState = traceState?.Length <= 513 ? traceState : null;
        }

        /// <summary>
        /// The W3C traceparent data: https://www.w3.org/TR/trace-context/#traceparent-header
        /// </summary>
        [DataMember]
        public string TraceParent { get; }

        /// <summary>
        /// The optional W3C tracestate parameter: https://www.w3.org/TR/trace-context/#tracestate-header
        /// </summary>
        [DataMember]
        public string? TraceState { get; set; }

        /// <summary>
        /// The Activity's Id value that is used to restore an Activity during replays.
        /// </summary>
        [DataMember]
        public string? Id { get; set; }

        /// <summary>
        /// The Activity's SpanId value that is used to restore an Activity during replays.
        /// </summary>
        [DataMember]
        public string? SpanId { get; set; }

        /// <summary>
        /// The Activity's start time value that is used to restore an Activity during replays.
        /// </summary>
        [DataMember]
        public DateTimeOffset? ActivityStartTime { get; set; }
    }
}
