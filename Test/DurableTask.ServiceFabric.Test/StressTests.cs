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
        public async Task Test_Lot_Of_Activities_Per_Orchestration()
        {
            var tests = new[] { 256, 1024, 2048, 3500, 4000, 8000, 10000, 15000, 20000 };
            foreach (var numberOfActivities in tests)
            {
                Console.WriteLine($"Begin testing orchestration with {numberOfActivities} parallel activities");
                var serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplicationType/TestStatefulService"), new ServicePartitionKey(1));
                var state = await serviceClient.RunOrchestrationAsync(typeof(ExecutionCountingOrchestration).Name, numberOfActivities, TimeSpan.FromMinutes(2));
                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Console.WriteLine($"Time for orchestration {state.OrchestrationInstance.InstanceId} with {numberOfActivities} parallel activities : {state.CompletedTime - state.CreatedTime}");
                Assert.AreEqual(numberOfActivities.ToString(), state.Output);
            }
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

        [Ignore] //Long running test and not too interesting anyway. (The test is passing - enable as needed to test and ensure).
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

            Dictionary<OrchestrationInstance, OrchestrationState> results = new Dictionary<OrchestrationInstance, OrchestrationState>();
            List<Task> waitTasks = new List<Task>();
            Stopwatch stopWatch = Stopwatch.StartNew();
            for (int i = 0; i < numberOfInstances; i++)
            {
                var instance = await serviceClient.StartTestOrchestrationAsync(orchestrationInput);
                waitTasks.Add(Task.Run(async () =>
                {
                    var state = await serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));
                    results.Add(instance, state);
                }));
                if (delayGeneratorFunction != null)
                {
                    await Task.Delay(delayGeneratorFunction(i));
                }
            }

            await Task.WhenAll(waitTasks);
            stopWatch.Stop();

            Func<TimeSpan, string> elapsedTimeFormatter = timeSpan => $"{timeSpan.Hours:00}:{timeSpan.Minutes:00}:{timeSpan.Seconds:00}.{timeSpan.Milliseconds / 10:00}";
            Console.WriteLine($"Total Meastured Time For All Orchestrations: {elapsedTimeFormatter(stopWatch.Elapsed)}");

            var expectedResult = (orchestrationInput.NumberOfParallelTasks + orchestrationInput.NumberOfSerialTasks).ToString();
            TimeSpan minTime = TimeSpan.MaxValue;
            TimeSpan maxTime = TimeSpan.FromMilliseconds(0);

            int failedOrchestrations = 0;
            foreach (var kvp in results)
            {
                if (kvp.Value == null)
                {
                    failedOrchestrations++;
                    var state = await serviceClient.GetOrchestrationState(kvp.Key);
                    Console.WriteLine($"Unfinished orchestration {kvp.Key}, state : {kvp.Value?.OrchestrationStatus}");
                    continue;
                }

                Assert.AreEqual(expectedResult, kvp.Value.Output, $"Unexpected output for Orchestration : {kvp.Key.InstanceId}");
                TimeSpan orchestrationTime = kvp.Value.CompletedTime - kvp.Value.CreatedTime;
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

            Assert.AreEqual(0, failedOrchestrations, $"{failedOrchestrations} orchestrations failed out of {numberOfInstances}.");
        }
    }
}
