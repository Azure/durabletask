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
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Settings;
    using Microsoft.ApplicationInsights.W3C;

    public class ScenarioInvoker
    {
        public async Task ExecuteAsync(Type orchestratorType, object orchestratorInput, int timeoutSec)
        {
            new TelemetryActivator().Initialize();

            using (
                TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();
                var activity = new Activity("Start Orchestration");
                SetupActivity(activity);
                activity.Start();
                var client = await host.StartOrchestrationAsync(orchestratorType, orchestratorInput); // TODO The parameter null will throw exception. (for the experiment)
                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(timeoutSec));

                await host.StopAsync();
            }
        }

        void SetupActivity(Activity activity)
        {
            var protocol = Environment.GetEnvironmentVariable("CorrelationProtocol");
            switch (protocol)
            {
                case "HTTP":
                    CorrelationSettings.Current.Protocol = Protocol.HttpCorrelationProtocol;
                    return;
                default:
#pragma warning disable 618
                    activity.GenerateW3CContext();
#pragma warning restore 618
                    return;
            }
        }
    }
}
