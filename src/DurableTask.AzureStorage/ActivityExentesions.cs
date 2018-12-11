using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using DurableTask.Core;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.W3C;
#pragma warning disable 618
namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Extension methods for Activity
    /// </summary>
    public static class ActivityExtensions
    {
        /// <summary>
        /// Set the <see cref="TraceContext"/>  as a Parent and Start Activity.
        /// This method support Both the HTTPCorrelation protocol and the W3C TraceContext.
        /// </summary>
        /// <param name="activity">For Extension methods</param>
        /// <param name="context">TraceContext instance.</param>
        /// <returns><see cref="Activity"/></returns>
        public static Activity SetParentAndStartActivity(this Activity activity, TraceContext context)
        {
            activity.SetParentId(context.ParentId);
            activity.SetTraceparent(context.Traceparent);
            activity.SetTracestate(context.Tracestate);
            activity.Start();
            return activity;
        }

        /// <summary>
        /// Set the <see cref="TraceContext"/>  as a Parent and Start Activity.
        /// This method support Both the HTTPCorrelation protocol and the W3C TraceContext.
        /// </summary>
        /// <param name="activity">For Extension methods</param>
        /// <param name="parent">TraceContext instance.</param>
        /// <returns><see cref="Activity"/></returns>
        public static Activity SetParentAndStartActivity(this Activity activity, Activity parent)
        {
            activity.SetParentId(parent.Id);
            activity.SetTraceparent(parent.GetTraceparent());
            activity.SetTracestate(parent.GetTracestate());
            activity.Start();
            return activity;
        }

        /// <summary>
        /// Create TraceContext from Activity and parent TraceContext.
        /// This method copy <see cref="TraceContext"/>.OrchestrationTraceContexts from TraceContext.
        /// </summary>
        /// <param name="activity">Activity which has already started.</param>
        /// <param name="parentTraceContext">Parent TraceContext</param>
        /// <returns></returns>
        public static TraceContext CreateTraceContext(this Activity activity, TraceContext parentTraceContext)
        {
            var context = new TraceContext()
            {
                Traceparent = activity.GetTraceparent(),
                Tracestate = activity.GetTracestate(),
                ParentSpanId = activity.GetParentSpanId(),
                ParentId = activity.Id,
                OrchestrationTraceContexts = parentTraceContext.OrchestrationTraceContexts
            };
            return context;
        }

        /// <summary>
        /// Create TraceContext from Activity and parent TraceContext.
        /// This method copy <see cref="TraceContext"/>.OrchestrationTraceContexts from TraceContext.
        /// </summary>
        /// <param name="activity">Activity which has already started.</param>
        /// <returns></returns>
        public static TraceContext CreateTraceContext(this Activity activity)
        {
            var context = new TraceContext()
            {
                Traceparent = activity.GetTraceparent(),
                Tracestate = activity.GetTracestate(),
                ParentSpanId = activity.GetParentSpanId(),
                ParentId = activity.Id
            };
            return context;
        }

        /// <summary>
        /// Create RequestTelemetry from the Activity.
        /// Currently W3C Trace context is supported. 
        /// </summary>
        /// <param name="activity"></param>
        /// <returns></returns>
        public static RequestTelemetry CreateRequestTelemetry(this Activity activity)
        {
            var telemetry = new RequestTelemetry { Name = activity.OperationName };
            telemetry.Id = $"|{activity.GetTraceId()}.{activity.GetSpanId()}";
            telemetry.Context.Operation.Id = activity.GetTraceId();
            telemetry.Context.Operation.ParentId = $"|{activity.GetTraceId()}.{activity.GetParentSpanId()}";
            return telemetry;
        }

        /// <summary>
        /// Create DependencyTelemetry from the Activity.
        /// Currently W3C Trace context is supported.
        /// </summary>
        /// <param name="activity"></param>
        /// <returns></returns>
        public static DependencyTelemetry CreateDependencyTelemetry(this Activity activity)
        {
            var telemetry = new DependencyTelemetry { Name = activity.OperationName };
            // TODO HttpCorrelation protocol

            telemetry.Id = $"|{activity.GetTraceId()}.{activity.GetSpanId()}";
            telemetry.Context.Operation.Id = activity.GetTraceId();
            telemetry.Context.Operation.ParentId = $"|{activity.GetTraceId()}.{activity.GetParentSpanId()}";

            return telemetry;
        }
    }
}