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
    using DurableTask.Core.Settings;

    public class Program
    {
        static void Main(string[] args)
        {
            CorrelationSettings.Current.EnableDistributedTracing = true;
            InvokeScenario(typeof(HelloOrchestrator), "50", 50); // HelloWorldScenario.cs;
            // InvokeScenario(typeof(SubOrchestratorOrchestration), "SubOrchestrationWorld", 50);  // SubOrchestratorScenario.cs;
            // InvokeScenario(typeof(RetryOrchestration), "Retry Scenario", 50); // RetryScenario.cs;
            // InvokeScenario(typeof(MultiLayeredOrchestrationWithRetryOrchestrator), "world", 50); // MultiLayerOrchestrationWithRetryScenario.cs;
            // InvokeScenario(typeof(FanOutFanInOrchestrator), "50", 50); // FanOutFanInScenario.cs;
            // InvokeScenario(typeof(ContinueAsNewOrchestration), "50", 50); // ContinueAsNewScenario.cs;
            // InvokeScenario(typeof(TerminatedOrchestration), "50", 50); // TerminationScenario.cs;

            Console.WriteLine("Orchestration is successfully finished.");
            Console.ReadLine();
        }

        static void InvokeScenario(Type orchestratorType, object orchestratorInput, int timeoutSec)
        {
            new ScenarioInvoker().ExecuteAsync(orchestratorType, orchestratorInput, timeoutSec).GetAwaiter().GetResult();
        }
    }
}
