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
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using DurableTask.Test.Orchestrations.Perf;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestApplication.Common;

namespace DurableTask.ServiceFabric.Test
{
    [TestClass]
    public class StressTests
    {
        [TestMethod]
        public async Task ExecuteStressTest()
        {
            var driverConfig = new DriverOrchestrationData()
            {
                NumberOfParallelOrchestrations = 40,
                SubOrchestrationData = new TestOrchestrationData()
                {
                    NumberOfParallelTasks = 10,
                    NumberOfSerialTasks = 5,
                    MaxDelay = 5,
                    MinDelay = 0,
                    DelayUnit = TimeSpan.FromSeconds(1)
                }
            };

            await RunDriverOrchestrationHelper(driverConfig);
            Console.WriteLine();

            driverConfig.SubOrchestrationData.UseTimeoutTask = true;
            driverConfig.SubOrchestrationData.ExecutionTimeout = TimeSpan.FromMinutes(2);

            await RunDriverOrchestrationHelper(driverConfig);
        }

        [TestMethod]
        public async Task SimplePerformanceExperiment()
        {
            var testOrchestratorInput = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 3,
                MaxDelay = 3,
                MinDelay = 3,
                DelayUnit = TimeSpan.FromSeconds(1)
            };

            await RunTestOrchestrationsHelper(100, testOrchestratorInput);
            Console.WriteLine();

            testOrchestratorInput.UseTimeoutTask = true;
            testOrchestratorInput.ExecutionTimeout = TimeSpan.FromMinutes(2);

            await RunTestOrchestrationsHelper(100, testOrchestratorInput);
        }

        [TestMethod]
        public async Task RunThousandOrchsWithHundredMilliSecondDelayInBetweenStartingEach()
        {
            var testOrchestratorInput = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 16,
                NumberOfSerialTasks = 4,
                MaxDelay = 500,
                MinDelay = 50,
                DelayUnit = TimeSpan.FromMilliseconds(1)
            };

            await RunTestOrchestrationsHelper(1000, testOrchestratorInput, delayGeneratorFunction: (totalRequestsSoFar) => TimeSpan.FromMilliseconds(100));
            Console.WriteLine();

            testOrchestratorInput.UseTimeoutTask = true;
            testOrchestratorInput.ExecutionTimeout = TimeSpan.FromMinutes(2);

            await RunTestOrchestrationsHelper(1000, testOrchestratorInput, delayGeneratorFunction: (totalRequestsSoFar) => TimeSpan.FromMilliseconds(100));
        }

        [TestMethod]
        public async Task InfrequentLoadScenario()
        {
            var testOrchestratorInput = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 2,
                MaxDelay = 0,
                MinDelay = 0,
            };

            await RunTestOrchestrationsHelper(200, testOrchestratorInput, delayGeneratorFunction: (totalRequestsSoFar) => TimeSpan.FromSeconds(1));
            Console.WriteLine();

            testOrchestratorInput.UseTimeoutTask = true;
            testOrchestratorInput.ExecutionTimeout = TimeSpan.FromMinutes(2);

            await RunTestOrchestrationsHelper(200, testOrchestratorInput, delayGeneratorFunction: (totalRequestsSoFar) => TimeSpan.FromSeconds(1));
        }

        async Task RunDriverOrchestrationHelper(DriverOrchestrationData driverConfig)
        {
            var serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplicationType/TestStatefulService"), new ServicePartitionKey(1));
            Console.WriteLine($"Orchestration getting scheduled: {DateTime.Now}");

            Stopwatch stopWatch = Stopwatch.StartNew();
            var state = await serviceClient.RunDriverOrchestrationAsync(driverConfig, TimeSpan.FromMinutes(5));
            stopWatch.Stop();

            Console.WriteLine($"Orchestration Status: {state.OrchestrationStatus}");
            Console.WriteLine($"Orchestration Result: {state.Output}");

            TimeSpan totalTime = stopWatch.Elapsed;
            TimeSpan orchestrationTime = state.CompletedTime - state.CreatedTime;

            Func<TimeSpan, string> elapsedTimeFormatter = timeSpan => $"{timeSpan.Hours:00}:{timeSpan.Minutes:00}:{timeSpan.Seconds:00}.{timeSpan.Milliseconds / 10:00}";
            Console.WriteLine($"Total Meastured Time: {elapsedTimeFormatter(totalTime)}");
            Console.WriteLine($"Top level Orchestration Time: {elapsedTimeFormatter(orchestrationTime)}");

            var expectedResult = driverConfig.NumberOfParallelOrchestrations * (driverConfig.SubOrchestrationData.NumberOfParallelTasks + driverConfig.SubOrchestrationData.NumberOfSerialTasks);

            Assert.AreEqual(expectedResult.ToString(), state.Output);
        }

        async Task RunTestOrchestrationsHelper(int numberOfInstances, TestOrchestrationData orchestrationInput, Func<int, TimeSpan> delayGeneratorFunction = null)
        {
            var serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplicationType/TestStatefulService"), new ServicePartitionKey(1));

            List<Task<OrchestrationState>> waitTasks = new List<Task<OrchestrationState>>();
            Stopwatch stopWatch = Stopwatch.StartNew();
            for (int i = 0; i < numberOfInstances; i++)
            {
                var instance = await serviceClient.StartTestOrchestrationAsync(orchestrationInput);
                var waitTask = serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));
                waitTasks.Add(waitTask);
                if (delayGeneratorFunction != null)
                {
                    await Task.Delay(delayGeneratorFunction(i));
                }
            }

            var outcomes = await Task.WhenAll(waitTasks);
            stopWatch.Stop();

            Func<TimeSpan, string> elapsedTimeFormatter = timeSpan => $"{timeSpan.Hours:00}:{timeSpan.Minutes:00}:{timeSpan.Seconds:00}.{timeSpan.Milliseconds / 10:00}";
            Console.WriteLine($"Total Meastured Time For All Orchestrations: {elapsedTimeFormatter(stopWatch.Elapsed)}");

            var expectedResult = (orchestrationInput.NumberOfParallelTasks + orchestrationInput.NumberOfSerialTasks).ToString();
            TimeSpan minTime = TimeSpan.MaxValue;
            TimeSpan maxTime = TimeSpan.FromMilliseconds(0);

            foreach (var outcome in outcomes)
            {
                Assert.IsNotNull(outcome);
                Assert.AreEqual(expectedResult, outcome.Output);
                TimeSpan orchestrationTime = outcome.CompletedTime - outcome.CreatedTime;
                if (minTime > orchestrationTime)
                {
                    minTime = orchestrationTime;
                }
                if (maxTime < orchestrationTime)
                {
                    maxTime = orchestrationTime;
                }
            }

            Console.WriteLine($"Minimum time taken for any orchestration instance : {elapsedTimeFormatter(minTime)}");
            Console.WriteLine($"Maximum time taken for any orchestration instance : {elapsedTimeFormatter(maxTime)}");
        }
    }
}
