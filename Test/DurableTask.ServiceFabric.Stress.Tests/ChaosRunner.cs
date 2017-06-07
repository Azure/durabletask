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

namespace DurableTask.ServiceFabric.Failover.Tests
{
    using System;
    using System.ComponentModel;
    using System.Fabric;
    using System.Fabric.Testability.Scenario;
    using System.Threading;
    using System.Threading.Tasks;

    class ChaosRunner
    {
        public async Task RunFailoverScenario(TimeSpan timeToRun, CancellationToken token)
        {
            var partitionSelector = PartitionSelector.PartitionKeyOf(new Uri("fabric:/TestFabricApplication/TestStatefulService"), 1);
            TimeSpan maxServiceStabilizationTimeout = TimeSpan.FromSeconds(180);
            var client = new FabricClient("localhost:19000");

            var parameters = new FailoverTestScenarioParameters(
                partitionSelector,
                timeToRun,
                maxServiceStabilizationTimeout);
            parameters.WaitTimeBetweenFaults = TimeSpan.FromMinutes(1);

            Console.WriteLine($"FailoverTest Info : TimeToRun = {parameters.TimeToRun}");
            Console.WriteLine($"FailoverTest Info : WaitTimeBetweenFaults = {parameters.WaitTimeBetweenFaults}");
            Console.WriteLine($"FailoverTest Info : MaxServiceStabilizationTimeout = {parameters.MaxServiceStabilizationTimeout}");

            var testScenario = new FailoverTestScenario(client, parameters);
            testScenario.ProgressChanged += OnTestProgressChanged;

            try
            {
                await testScenario.ExecuteAsync(token);
            }
            catch (AggregateException ae)
            {
                Console.Error.WriteLine($"{DateTime.UtcNow} : Exception inside the failover test scenario : {ae.InnerException}");
            }
        }

        private void OnTestProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            var utcNow = DateTime.UtcNow;
            Console.WriteLine($"{utcNow} : FailoverTest Progress Percentage {e.ProgressPercentage}");
            Console.WriteLine($"{utcNow} : FailoverTest UserState {e.UserState}");
        }
    }
}
