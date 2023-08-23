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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using DurableTask.Core.Tracing;

namespace DurableTask.Core
{
    /// <summary>
    /// Interface to store the Trace Context for events. Used for Distributed Tracing.
    /// </summary>
    public interface ISupportsDurableTraceContext
    {
        /// <summary>
        /// The trace context associated with an event.
        /// </summary>
        public DistributedTraceContext ParentTraceContext { get; set; }
    }

    internal static class DurableTraceContextWrapperExtensions
    {
        internal static bool TryGetParentTraceContext(this ISupportsDurableTraceContext wrapper, out ActivityContext parentTraceContext)
        {
            if (wrapper.ParentTraceContext?.TraceParent == null)
            {
                parentTraceContext = default;
                return false;
            }

            return ActivityContext.TryParse(
                wrapper.ParentTraceContext.TraceParent,
                wrapper.ParentTraceContext.TraceState,
                out parentTraceContext);
        }

        internal static void SetParentTraceContext(this ISupportsDurableTraceContext wrapper, Activity activity)
        {
            if (activity != null)
            {
                wrapper.ParentTraceContext = new DistributedTraceContext(
                    activity.Id,
                    activity.TraceStateString);
            }
        }

        internal static void SetParentTraceContext(this ISupportsDurableTraceContext wrapper, ActivityContext activityContext)
        {
            if (activityContext != null)
            {
                // TODO: update trace flags casting to handle 2 digits
                wrapper.ParentTraceContext = new DistributedTraceContext(
                    $"00-{activityContext.TraceId}-{activityContext.SpanId}-0{activityContext.TraceFlags:d}",
                    activityContext.TraceState);
            }
        }
    }
}
