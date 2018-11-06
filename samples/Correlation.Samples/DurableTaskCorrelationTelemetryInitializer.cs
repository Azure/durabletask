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

namespace Correlation.Samples
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using DurableTask.Core;
    using ImpromptuInterface;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.Common;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.ApplicationInsights.W3C;

    /// <summary>
    /// Telemetry Initializer that sets correlation ids for W3C.
    /// This source is based on W3COperationCorrelationTelemetryInitializer.cs
    /// 1. Modified with CorrelationTraceContext.Current
    /// 2. Avoid to be overriden when it is RequestTelemetry
    /// Original Source is here <see cref="https://github.com/microsoft/ApplicationInsights-dotnet-server/blob/2.8.0/Src/Common/W3C/W3COperationCorrelationTelemetryInitializer.cs"/>
    /// </summary>
    [Obsolete("Not ready for public consumption.")]
    [EditorBrowsable(EditorBrowsableState.Never)]
#if DEPENDENCY_COLLECTOR
    public
#else
    internal
#endif
        class DurableTaskCorrelationTelemetryInitializer : ITelemetryInitializer
    {
        private const string RddDiagnosticSourcePrefix = "rdddsc";
        private const string SqlRemoteDependencyType = "SQL";

        /// These internal property is copied from W3CConstants
        /// <summary>Trace-Id tag name.</summary>
        internal const string TraceIdTag = "w3c_traceId";

        /// <summary>Span-Id tag name.</summary>
        internal const string SpanIdTag = "w3c_spanId";

        /// <summary>Parent span-Id tag name.</summary>
        internal const string ParentSpanIdTag = "w3c_parentSpanId";

        /// <summary>Version tag name.</summary>
        internal const string VersionTag = "w3c_version";

        /// <summary>Sampled tag name.</summary>
        internal const string SampledTag = "w3c_sampled";

        /// <summary>Tracestate tag name.</summary>
        internal const string TracestateTag = "w3c_tracestate";

        /// <summary>Default version value.</summary>
        internal const string DefaultVersion = "00";

        /// <summary>
        /// Default sampled flag value: may be recorded, not requested
        /// </summary>
        internal const string TraceFlagRecordedAndNotRequested = "02";

        /// <summary>Recorded and requested sampled flag value</summary>
        internal const string TraceFlagRecordedAndRequested = "03";

        /// <summary>Requested trace flag</summary>
        internal const byte RequestedTraceFlag = 1;

        /// <summary>Legacy root Id tag name.</summary>
        internal const string LegacyRootIdProperty = "ai_legacyRootId";

        /// <summary>Legacy root Id tag name.</summary>
        internal const string LegacyRequestIdProperty = "ai_legacyRequestId";

        /// <summary>
        /// Set of suppress telemetry tracking if you add Host name on this.
        /// </summary>
        public HashSet<string> ExcludeComponentCorrelationHttpHeadersOnDomains { get; set; }

        /// <summary>
        /// Constructor 
        /// </summary>
        public DurableTaskCorrelationTelemetryInitializer()
        {
            ExcludeComponentCorrelationHttpHeadersOnDomains = new HashSet<string>();
        }

        /// <summary>
        /// Initializes telemety item.
        /// </summary>
        /// <param name="telemetry">Telemetry item.</param>
        public void Initialize(ITelemetry telemetry)
        {
            if (IsSuppressedTelemetry(telemetry))
            {
                SuppressTelemetry(telemetry);
                return;
            }

            if (!(telemetry is RequestTelemetry))
            {
                Activity currentActivity = Activity.Current;
                if (currentActivity == null)
                {
                    if (CorrelationTraceContext.Current != null)
                    {
                        UpdateTelemetry(telemetry, CorrelationTraceContext.Current);
                    }
                }
                else
                {
                    if (CorrelationTraceContext.Current != null)
                    {
                        UpdateTelemetry(telemetry, CorrelationTraceContext.Current);
                    }
                    else 
                    {
                        UpdateTelemetry(telemetry, currentActivity, false);
                    }
                }
            }
        }

        internal static void UpdateTelemetry(ITelemetry telemetry, TraceContextBase contextBase)
        {
            switch (contextBase)
            {
                case NullObjectTraceContext nullObjectContext:
                    return;
                case W3CTraceContext w3cContext:
                    UpdateTelemetryW3C(telemetry, w3cContext);
                    break;
                case HttpCorrelationProtocolTraceContext httpCorrelationProtocolTraceContext:
                    UpdateTelemetryHttpCorrelationProtocol(telemetry, httpCorrelationProtocolTraceContext);
                    break;
                default:
                    return;
            }
        }

        internal static void UpdateTelemetryHttpCorrelationProtocol(ITelemetry telemetry, HttpCorrelationProtocolTraceContext context)
        {
            OperationTelemetry opTelemetry = telemetry as OperationTelemetry;

            bool initializeFromCurrent = opTelemetry != null;

            if (initializeFromCurrent)
            {
                initializeFromCurrent &= !(opTelemetry is DependencyTelemetry dependency &&
                    dependency.Type == SqlRemoteDependencyType &&
                    dependency.Context.GetInternalContext().SdkVersion
                        .StartsWith(RddDiagnosticSourcePrefix, StringComparison.Ordinal));
            }

            if (initializeFromCurrent)
            {
                opTelemetry.Id = !string.IsNullOrEmpty(opTelemetry.Id) ? opTelemetry.Id : context.TelemetryId;
                telemetry.Context.Operation.ParentId = !string.IsNullOrEmpty(telemetry.Context.Operation.ParentId) ? telemetry.Context.Operation.ParentId : context.TelemetryContextOperationParentId;
            }
            else
            {
                telemetry.Context.Operation.Id = !string.IsNullOrEmpty(telemetry.Context.Operation.Id) ? telemetry.Context.Operation.Id : context.TelemetryContextOperationId;
                telemetry.Context.Operation.ParentId = !string.IsNullOrEmpty(telemetry.Context.Operation.ParentId) ? telemetry.Context.Operation.ParentId : context.TelemetryContextOperationParentId;
            }
        }

        internal static void UpdateTelemetryW3C(ITelemetry telemetry, W3CTraceContext context)
        {
            OperationTelemetry opTelemetry = telemetry as OperationTelemetry;

            bool initializeFromCurrent = opTelemetry != null;

            if (initializeFromCurrent)
            {
                initializeFromCurrent &= !(opTelemetry is DependencyTelemetry dependency &&
                    dependency.Type == SqlRemoteDependencyType &&
                    dependency.Context.GetInternalContext().SdkVersion
                        .StartsWith(RddDiagnosticSourcePrefix, StringComparison.Ordinal));
            }

            if (!string.IsNullOrEmpty(context.TraceState))
            {
                opTelemetry.Properties["w3c_tracestate"] = context.TraceState;
            }

            TraceParent traceParent = context.TraceParent.ToTraceParent();

            if (initializeFromCurrent)
            {
                if (string.IsNullOrEmpty(opTelemetry.Id))
                    opTelemetry.Id = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, traceParent.SpanId);

                if (string.IsNullOrEmpty(context.ParentSpanId))
                {
                    telemetry.Context.Operation.ParentId = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, context.ParentSpanId);
                }
            }
            else
            {
                if (telemetry.Context.Operation.Id == null)
                {
                    telemetry.Context.Operation.Id = traceParent.TraceId;
                }

                if (telemetry.Context.Operation.ParentId == null) // TODO check if it works. 
                {
                    telemetry.Context.Operation.ParentId = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, traceParent.SpanId);
                }
            }
        }

        internal void SuppressTelemetry(ITelemetry telemetry)
        {
            // TODO change the strategy.
            telemetry.Context.Operation.Id = "suppressed";
            telemetry.Context.Operation.ParentId = "suppressed";
            // Context. Properties.  ai_legacyRequestId , ai_legacyRequestId
            foreach (var key in telemetry.Context.Properties.Keys)
            {
                if (key == "ai_legacyRootId" ||
                    key == "ai_legacyRequestId")
                {
                    telemetry.Context.Properties[key] = "suppressed";
                }
            }

            ((OperationTelemetry)telemetry).Id = "suppressed";
        }

        internal bool IsSuppressedTelemetry(ITelemetry telemetry)
        {
            OperationTelemetry opTelemetry = telemetry as OperationTelemetry;
            if (telemetry is DependencyTelemetry)
            {
                DependencyTelemetry dTelemetry = telemetry as DependencyTelemetry;

                if (!string.IsNullOrEmpty(dTelemetry.CommandName))
                {
                    var host = new Uri(dTelemetry.CommandName).Host;
                    if (ExcludeComponentCorrelationHttpHeadersOnDomains.Contains(host)) return true;
                }                  
            }

            return false;
        }

        [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "This method has different code for Net45/NetCore")]
        internal static void UpdateTelemetry(ITelemetry telemetry, Activity activity, bool forceUpdate)
        {
            if (activity == null)
            {
                return;
            }

            activity.UpdateContextOnActivity();

            // Requests and dependnecies are initialized from the current Activity 
            // (i.e. telemetry.Id = current.Id). Activity is created for such requests specifically
            // Traces, exceptions, events on the other side are children of current activity
            // There is one exception - SQL DiagnosticSource where current Activity is a parent
            // for dependency calls.

            OperationTelemetry opTelemetry = telemetry as OperationTelemetry;
            bool initializeFromCurrent = opTelemetry != null;

            if (initializeFromCurrent)
            {
                initializeFromCurrent &= !(opTelemetry is DependencyTelemetry dependency &&
                                           dependency.Type == SqlRemoteDependencyType &&
                                           dependency.Context.GetInternalContext().SdkVersion
                                               .StartsWith(RddDiagnosticSourcePrefix, StringComparison.Ordinal));
            }

            string spanId = null, parentSpanId = null;
            foreach (var tag in activity.Tags)
            {
                switch (tag.Key)
                {
                    case TraceIdTag:
#if NET45
                        // on .NET Fx Activities are not always reliable, this code prevents update
                        // of the telemetry that was forcibly updated during Activity lifetime
                        // ON .NET Core there is no such problem 
                        if (telemetry.Context.Operation.Id == tag.Value && !forceUpdate)
                        {
                            return;
                        }
#endif
                        telemetry.Context.Operation.Id = tag.Value;
                        break;
                    case SpanIdTag:
                        spanId = tag.Value;
                        break;
                    case ParentSpanIdTag:
                        parentSpanId = tag.Value;
                        break;
                    case TracestateTag:
                        if (telemetry is OperationTelemetry operation)
                        {
                            operation.Properties[TracestateTag] = tag.Value;
                        }

                        break;
                }
            }

            if (initializeFromCurrent)
            {
                opTelemetry.Id = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, spanId);
                if (parentSpanId != null)
                {
                    telemetry.Context.Operation.ParentId = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, parentSpanId);
                }
            }
            else
            {
                telemetry.Context.Operation.ParentId = StringUtilities.FormatRequestId(telemetry.Context.Operation.Id, spanId);
            }

            if (opTelemetry != null)
            {
                if (opTelemetry.Context.Operation.Id != activity.RootId)
                {
                    opTelemetry.Properties[LegacyRootIdProperty] = activity.RootId;
                }

                if (opTelemetry.Id != activity.Id)
                {
                    opTelemetry.Properties[LegacyRequestIdProperty] = activity.Id;
                }
            }
        }
    }
}
