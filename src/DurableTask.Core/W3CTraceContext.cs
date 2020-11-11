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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;

    /// <summary>
    /// W3CTraceContext keep the correlation value with W3C TraceContext protocol
    /// </summary>
    public class W3CTraceContext : TraceContextBase
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        public W3CTraceContext() : base() { }

        /// <summary>
        /// W3C TraceContext: Traceparent
        /// </summary>
        public string TraceParent { get; set; }

        /// <summary>
        /// W3C TraceContext: Tracestate
        /// </summary>
        public string TraceState { get; set; }

        /// <summary>
        /// W3C TraceContext: ParentSpanId
        /// </summary>
        public string ParentSpanId { get; set; }

        /// <inheritdoc />
        public override TimeSpan Duration => CurrentActivity?.Duration ?? DateTimeOffset.UtcNow - StartTime;

        /// <inheritdoc />
        public override string TelemetryId
        {
            get
            {
                if (CurrentActivity == null)
                {
                    var traceParent = TraceParentObject.Create(TraceParent);
                    return traceParent.SpanId;
                }
                else
                {
                    return CurrentActivity.SpanId.ToHexString();
                }
            }
        }

        /// <inheritdoc />
        public override string TelemetryContextOperationId => CurrentActivity?.RootId ??
                    TraceParentObject.Create(TraceParent).TraceId;

        /// <inheritdoc />
        public override string TelemetryContextOperationParentId {
            get
            {
                if (CurrentActivity == null)
                {
                    return ParentSpanId;
                }
                else
                {
                    return CurrentActivity.ParentSpanId.ToHexString();
                }
            }
        }

        /// <inheritdoc />
        public override void SetParentAndStart(TraceContextBase parentTraceContext)
        {
            if (CurrentActivity == null)
            {
                CurrentActivity = new Activity(this.OperationName);
                CurrentActivity.SetIdFormat(ActivityIdFormat.W3C);
            }

            if (parentTraceContext is W3CTraceContext)
            {
                var context = (W3CTraceContext)parentTraceContext;
                CurrentActivity.SetParentId(context.TraceParent);
                CurrentActivity.TraceStateString = context.TraceState;
                OrchestrationTraceContexts = context.OrchestrationTraceContexts.Clone();
            } 

            CurrentActivity.Start();

            StartTime = CurrentActivity.StartTimeUtc;
            TraceParent = CurrentActivity.Id;
            TraceState = CurrentActivity.TraceStateString;
            ParentSpanId = CurrentActivity.ParentSpanId.ToHexString();

            CorrelationTraceContext.Current = this;
        }

        /// <inheritdoc />
        public override void StartAsNew()
        {
            CurrentActivity = new Activity(this.OperationName);
            CurrentActivity.SetIdFormat(ActivityIdFormat.W3C);
            CurrentActivity.Start();

            StartTime = CurrentActivity.StartTimeUtc;

            TraceParent = CurrentActivity.Id;

            CurrentActivity.TraceStateString = TraceState;
            TraceState = CurrentActivity.TraceStateString;
            ParentSpanId = CurrentActivity.ParentSpanId.ToHexString();

            CorrelationTraceContext.Current = this;
        }
    }

    internal class TraceParentObject
    {
        public string Version { get; set; }

        public string TraceId { get; set; }

        public string SpanId { get; set; }

        public string TraceFlags { get; set; }

        public static TraceParentObject Create(string traceParent)
        {
            if (!string.IsNullOrEmpty(traceParent))
            {
                var substrings = traceParent.Split('-');
                if (substrings.Length != 4)
                {
                    throw new ArgumentException($"Traceparent doesn't respect the spec. {traceParent}");
                }

                return new TraceParentObject
                {
                    Version = substrings[0],
                    TraceId = substrings[1],
                    SpanId = substrings[2],
                    TraceFlags = substrings[3]
                };
            }

            return new TraceParentObject();
        }
    }
}
