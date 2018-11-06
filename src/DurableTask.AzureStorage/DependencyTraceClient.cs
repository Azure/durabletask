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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Net;
    using System.Text;
    using Microsoft.ApplicationInsights;
    using System.Reactive;
    using Microsoft.ApplicationInsights.DataContracts;

    /// <summary>
    /// Delegate sending telemetry to the other side.
    /// Mainly send telemetry to the Durable Functions TelemetryClient
    /// </summary>
    public class DependencyTraceClient
    {
        private const string DiagnosticSourceName = "DurableTask.AzureStorage";
        private const string DependencyTrackEvent = "DependencyEvent";
        private const string ExceptionEvent = "ExceptionEvent";
        private static DiagnosticSource logger = new DiagnosticListener(DiagnosticSourceName);
        private static IDisposable applicationInsightsSubscription = null;
        private static IDisposable listenerSubscription = null;

        /// <summary>
        /// Setup this class uses the <see cref="TelemetryClient"/> instance which enable send telemetry to the Application Insights.
        /// You need to call this method if you want to use this class. 
        /// </summary>
        /// <param name="trackTelemetryAction">Action to send telemetry for <see cref="DependencyTelemetry"/></param>
        /// <param name="trackExceptionAction">Action to send telemetry for exception </param>
        public static void SetUp(Action<DependencyTelemetry> trackTelemetryAction, Action<Exception> trackExceptionAction)
        {
            listenerSubscription = DiagnosticListener.AllListeners.Subscribe(
                delegate(DiagnosticListener listener)
                {
                    if (listener.Name == DiagnosticSourceName)
                    {
                        applicationInsightsSubscription?.Dispose();

                        applicationInsightsSubscription = listener.Subscribe((KeyValuePair<string, object> evt) =>
                        {
                            if (evt.Key == DependencyTrackEvent)
                            {
                                // the parameter is DependencyTelemetry which is already stopped. 
                                var telemetry = (DependencyTelemetry) evt.Value;
                                trackTelemetryAction(telemetry);
                            }

                            if (evt.Key == ExceptionEvent)
                            {
                                var e = (Exception) evt.Value;
                                trackExceptionAction(e);
                            }
                        });
                    }
                });
        }

        /// <summary>
        /// Track the DependencyTelemetry
        /// </summary>
        /// <param name="telemetry"></param>
        public static void Track(DependencyTelemetry telemetry)
        {
            logger.Write(DependencyTrackEvent, telemetry);
        }

        /// <summary>
        /// Track the Exception
        /// </summary>
        /// <param name="e"></param>
        public static void TrackException(Exception e)
        {
            logger.Write(ExceptionEvent, e);
        }
    }
}
