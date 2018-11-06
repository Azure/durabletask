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

namespace DurableTask.AzureStorage.Tests.Correlation
{
    using System;
    using System.Diagnostics;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.DependencyCollector;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.ApplicationInsights.W3C;

    public class TelemetryActivator
    {        
        static TelemetryClient telemetryClient;

        public void Initialize()
        {
            SetUpTelemetryClient(null, null);
            SetUpTelemetryCallbacks();
        }

        public void Initialize(Action<ITelemetry> onSend, string instrumentationKey)
        {
            SetUpTelemetryClient(onSend, instrumentationKey);
            SetUpTelemetryCallbacks();
        }

        void SetUpTelemetryCallbacks()
        {
            CorrelationTraceClient.SetUp(
                (TraceContextBase requestTraceContext) =>
                {
                    requestTraceContext.Stop();

                    var requestTelemetry = requestTraceContext.CreateRequestTelemetry();
                    telemetryClient.TrackRequest(requestTelemetry);
                },
                (TraceContextBase dependencyTraceContext) =>
                {
                    dependencyTraceContext.Stop();
                    var dependencyTelemetry = dependencyTraceContext.CreateDependencyTelemetry();
                    telemetryClient.TrackDependency(dependencyTelemetry);
                },
                (Exception e) =>
                {
                    telemetryClient.TrackException(e);
                }
            );
        }

        void SetUpTelemetryClient(Action<ITelemetry> onSend, string instrumentationKey)
        {
            var module = new DependencyTrackingTelemetryModule();
            // Currently it seems have a problem https://github.com/microsoft/ApplicationInsights-dotnet-server/issues/536
            module.ExcludeComponentCorrelationHttpHeadersOnDomains.Add("core.windows.net");
            module.ExcludeComponentCorrelationHttpHeadersOnDomains.Add("127.0.0.1");

            TelemetryConfiguration config = TelemetryConfiguration.CreateDefault();
            if (onSend != null)
            {
                config.TelemetryChannel = new StubTelemetryChannel { OnSend = onSend };
            }

#pragma warning disable 618
            var telemetryInitializer = new DurableTaskCorrelationTelemetryInitializer();
            // TODO It should be suppressed by DependencyTrackingTelemetryModule, however, it doesn't work currently.
            // Once the bug is fixed, remove this settings. 
            telemetryInitializer.ExcludeComponentCorrelationHttpHeadersOnDomains.Add("127.0.0.1");
            config.TelemetryInitializers.Add(telemetryInitializer);
#pragma warning restore 618
            module.Initialize(config);
            instrumentationKey = instrumentationKey ?? Environment.GetEnvironmentVariable("APPINSIGHTS_INSTRUMENTATIONKEY");
            
            telemetryClient = new TelemetryClient(config);
        }
    }
}
