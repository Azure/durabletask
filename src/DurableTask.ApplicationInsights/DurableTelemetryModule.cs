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

namespace DurableTask.ApplicationInsights
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;

    /// <summary>
    /// Telemetry Module to convert Activity events to App Insights API
    /// </summary>
    public sealed class DurableTelemetryModule : ITelemetryModule, IAsyncDisposable
    {
        private TelemetryClient telemetryClient;
        private ActivityListener listener;

        /// <inheritdoc/>
        public void Initialize(TelemetryConfiguration configuration)
        {
            this.telemetryClient = new TelemetryClient(configuration);

            // ActivitySamplingResult.AllData means that the ActivityListener is going to collect all of the data
            // for any Activity that's sent to the "DurableTask" source. It isn't going to exclude any data.
            this.listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name.StartsWith("DurableTask"),
                ActivityStopped = OnEndActivity,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        void OnEndActivity(Activity activity)
        {
            if (!activity.IsAllDataRequested)
            {
                return;
            }

            OperationTelemetry telemetry = CreateTelemetry(activity);
            this.telemetryClient.Track(telemetry);
        }

        static OperationTelemetry CreateTelemetry(Activity activity)
        {
            OperationTelemetry telemetry;
            ActivityStatusCode status = activity.GetStatus(out string description);
            switch (activity.Kind)
            {
                case ActivityKind.Consumer or ActivityKind.Server:
                    RequestTelemetry request = CreateTelemetryCore<RequestTelemetry>(activity);
                    request.Success = status != ActivityStatusCode.Error;

                    if (string.IsNullOrEmpty(request.ResponseCode))
                    {
                        request.ResponseCode = (bool)request.Success ? "200" : "500";
                    }

                    telemetry = request;
                    break;
                default:
                    DependencyTelemetry dependency = CreateTelemetryCore<DependencyTelemetry>(activity);
                    dependency.Success = status != ActivityStatusCode.Error;
                    dependency.Type = activity.Kind is ActivityKind.Internal ? "InProc" : "DurableTask";
                    telemetry = dependency;
                    break;
            }

            telemetry.Properties["otel.status_description"] = description;
            return telemetry;
        }

        static T CreateTelemetryCore<T>(Activity activity)
            where T : OperationTelemetry, new()
        {
            T telemetry = new()
            {
                Name = activity.DisplayName,
                Id = activity.SpanId.ToString(),
                Timestamp = activity.StartTimeUtc,
                Duration = activity.Duration,
            };

            telemetry.Context.Operation.Id = activity.RootId;
            ActivitySpanId parentId = activity.ParentSpanId;
            if (parentId != default)
            {
                telemetry.Context.Operation.ParentId = parentId.ToString();
            }

            foreach (KeyValuePair<string, string> item in activity.Baggage)
            {
                telemetry.Properties[item.Key] = item.Value;
            }

            foreach (KeyValuePair<string, object> item in activity.TagObjects)
            {
                telemetry.Properties[item.Key] = item.Value.ToString();
            }

            return telemetry;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            this.listener?.Dispose();
            if (this.telemetryClient != null)
            {
                using CancellationTokenSource cts = new(millisecondsDelay: 5000);
                try
                {
                    await this.telemetryClient.FlushAsync(cts.Token);
                }
                catch
                {
                    // Ignore for now; potentially log this in the future.
                }
            }
        }
    }
}