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
    using DurableTask.Core.Settings;

    /// <summary>
    /// Delegate sending telemetry to the other side.
    /// Mainly send telemetry to the Durable Functions TelemetryClient
    /// </summary>
    public class CorrelationTraceClient
    {
        const string DiagnosticSourceName = "DurableTask.AzureStorage";
        const string RequestTrackEvent = "RequestEvent";
        const string DependencyTrackEvent = "DependencyEvent";
        const string ExceptionEvent = "ExceptionEvent";
        static DiagnosticSource logger = new DiagnosticListener(DiagnosticSourceName);
        static IDisposable applicationInsightsSubscription = null;
        static IDisposable listenerSubscription = null;

        /// <summary>
        /// Setup this class uses callbacks to enable send telemetry to the Application Insights.
        /// You need to call this method if you want to use this class. 
        /// </summary>
        /// <param name="trackRequestTelemetryAction">Action to send request telemetry using <see cref="Activity"></see></param>
        /// <param name="trackDependencyTelemetryAction">Action to send telemetry for <see cref="Activity"/></param>
        /// <param name="trackExceptionAction">Action to send telemetry for exception </param>
        public static void SetUp(
            Action<TraceContextBase> trackRequestTelemetryAction, 
            Action<TraceContextBase> trackDependencyTelemetryAction, 
            Action<Exception> trackExceptionAction)
        {
            if (listenerSubscription == null)
            {
                listenerSubscription = DiagnosticListener.AllListeners.Subscribe(
                    delegate (DiagnosticListener listener)
                    {
                        if (listener.Name == DiagnosticSourceName)
                        {
                            applicationInsightsSubscription?.Dispose();

                            applicationInsightsSubscription = listener.Subscribe((KeyValuePair<string, object> evt) =>
                            {
                                if (evt.Key == RequestTrackEvent)
                                {
                                    var context = (TraceContextBase)evt.Value;
                                    trackRequestTelemetryAction(context);
                                }

                                if (evt.Key == DependencyTrackEvent)
                                {
                                    // the parameter is DependencyTelemetry which is already stopped. 
                                    var context = (TraceContextBase)evt.Value;
                                    trackDependencyTelemetryAction(context);
                                }

                                if (evt.Key == ExceptionEvent)
                                {
                                    var e = (Exception)evt.Value;
                                    trackExceptionAction(e);
                                }
                            });
                        }
                    });
            }
        }

        /// <summary>
        /// Track the RequestTelemetry
        /// </summary>
        /// <param name="context"></param>
        public static void TrackRequestTelemetry(TraceContextBase context)
        {
            Tracking(() => logger.Write(RequestTrackEvent, context));
        }

        /// <summary>
        /// Track the DependencyTelemetry
        /// </summary>
        /// <param name="context"></param>
        public static void TrackDepencencyTelemetry(TraceContextBase context)
        {
            Tracking(() => logger.Write(DependencyTrackEvent, context));
        }

        /// <summary>
        /// Track the Exception
        /// </summary>
        /// <param name="e"></param>
        public static void TrackException(Exception e)
        {
            Tracking(() => logger.Write(ExceptionEvent, e));
        }

        /// <summary>
        /// Execute Action for Propagate correlation information.
        /// It suppresses the execution when <see cref="CorrelationSettings"/>.DisablePropagation is true.
        /// </summary>
        /// <param name="action"></param>
        public static void Propagate(Action action)
        {
            Execute(action);
        }

        static void Tracking(Action tracking)
        {
            Execute(tracking);
        }

        static void Execute(Action action)
        {
            if (CorrelationSettings.Current.EnableDistributedTracing)
            {
                action();
            }
        }
    }
}
