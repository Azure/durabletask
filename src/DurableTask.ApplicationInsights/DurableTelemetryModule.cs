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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core.Tracing;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;

namespace DurableTask.ApplicationInsights
{
    /// Telemetry Module to convert Activity events to App Insights API
    public sealed class DurableTelemetryModule : ITelemetryModule, IAsyncDisposable
    {
        private const string DependencyTelemetryKey = "_tel";
        private const string DependencyTypeInProc = "InProc";

        private TelemetryClient _telemetryClient;
        private ActivityListener _listener;

        /// <inheritdoc/>
        public void Initialize(TelemetryConfiguration configuration)
        {
            _telemetryClient = new TelemetryClient(configuration);

            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name.StartsWith("DurableTask"),
                ActivityStarted = activity =>
                {
                    var dependency = _telemetryClient.StartOperation<DependencyTelemetry>(activity);
                    dependency.Telemetry.Type = DependencyTypeInProc; // Required for proper rendering in App Insights.
                    activity.SetCustomProperty(DependencyTelemetryKey, dependency);
                },
                ActivityStopped = activity =>
                {
                    // Check for Exceptions events
                    foreach (ActivityEvent activityEvent in activity.Events)
                    {
                        // TrackExceptionTelemetryFromActivityEvent(activityEvent, _telemetryClient);
                    }

                    if (activity.GetCustomProperty(DependencyTelemetryKey) is IOperationHolder<DependencyTelemetry> dependencyHolder)
                    {
                        var dependency = dependencyHolder.Telemetry;

                        foreach (var item in activity.Tags)
                        {
                            if (!dependency.Properties.ContainsKey(item.Key))
                            {
                                dependency.Properties[item.Key] = item.Value;
                            }
                        }

                        dependency.Success = activity.GetStatus() != ActivityStatusCode.Error;

                        _telemetryClient.StopOperation(dependencyHolder);
                        dependencyHolder.Dispose();
                    }
                },
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData
            };

            ActivitySource.AddActivityListener(_listener);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _listener?.Dispose();

            if (_telemetryClient != null)
            {
                CancellationTokenSource cts = new CancellationTokenSource(millisecondsDelay: 5000);
                try
                {
                    await _telemetryClient.FlushAsync(cts.Token);
                }
                catch
                {
                    // Ignore for now; potentially log this in the future.
                }
                finally
                {
                    cts.Dispose();
                }
            }

            GC.SuppressFinalize(this);
        }
    }
}