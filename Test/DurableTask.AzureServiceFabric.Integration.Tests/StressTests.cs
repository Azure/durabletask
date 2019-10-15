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

namespace DurableTask.AzureServiceFabric.Integration.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Test.Orchestrations.Performance;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

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

        // This test uses an implementation detail of client - the polling interval in the client for WaitForOrchestration API.
        // The sleep time is chosen carefully to be close to polling interval to simulate WaitForOrchestration succeeding while
        // the session could still be there in the store (with a previous implementation which was fixed when the test was introduced).
        // The test should be modified if the polling interval changes or should be re thought if the polling logic itself changes.
        // Todo : The above logic did change since the test was initially developed. Need to re-think this test.
        [TestMethod]
        public async Task ExecuteSameOrchestrationBackToBack()
        {
            int numberOfAttempts = 10; //Try a few times to make sure there is no failure.
            var instanceId = nameof(ExecuteSameOrchestrationBackToBack);
            var taskHubClient = Utilities.CreateTaskHubClient();

            Dictionary<int, Tuple<OrchestrationInstance, OrchestrationState>> results = new Dictionary<int, Tuple<OrchestrationInstance, OrchestrationState>>();

            for (int attemptNumber = 0; attemptNumber < numberOfAttempts; attemptNumber++)
            {
                var instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, new TestOrchestrationData()
                {
                    NumberOfParallelTasks = 0,
                    NumberOfSerialTasks = 1,
                    MaxDelay = 1980,
                    MinDelay = 1900,
                    DelayUnit = TimeSpan.FromMilliseconds(1),
                });

                var state = await taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));
                results.Add(attemptNumber, new Tuple<OrchestrationInstance, OrchestrationState>(instance, state));
            }

            for(int attemptNumber = 0; attemptNumber < numberOfAttempts; attemptNumber++)
            {
                var instance = results[attemptNumber].Item1;
                var state = results[attemptNumber].Item2;
                Assert.IsNotNull(state, $"Attempt number {attemptNumber} with execution id {instance.ExecutionId} Failed with null value for WaitForOrchestration");
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus, $"Attempt number {attemptNumber} with execution id {instance.ExecutionId} did not reach 'Completed' state.");
                Console.WriteLine($"Time for orchestration in attempt number {attemptNumber} : {state.CompletedTime - state.CreatedTime}");
            }
        }

        [TestMethod]
        public async Task Test_Lot_Of_Activities_Per_Orchestration()
        {
            var tests = new[] { 256, 1024, 2048, 3500, 4000, 8000, 10000, 15000, 20000 };
            foreach (var numberOfActivities in tests)
            {
                Console.WriteLine($"Begin testing orchestration with {numberOfActivities} parallel activities");
                var taskHubClient = Utilities.CreateTaskHubClient();
                var instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(ExecutionCountingOrchestration), numberOfActivities);
                var state = await taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(3));
                await taskHubClient.PurgeOrchestrationInstanceHistoryAsync(DateTime.Now, OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);
                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Console.WriteLine($"Time for orchestration {state.OrchestrationInstance.InstanceId} with {numberOfActivities} parallel activities : {state.CompletedTime - state.CreatedTime}");
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
            var taskHubClient = Utilities.CreateTaskHubClient();
            Console.WriteLine($"Orchestration getting scheduled: {DateTime.Now}");

            Stopwatch stopWatch = Stopwatch.StartNew();
            var instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(DriverOrchestration), driverConfig);
            var state = await taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(5));
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
            var taskHubClient = Utilities.CreateTaskHubClient();

            List<Tuple<OrchestrationInstance, OrchestrationState>> results = new List<Tuple<OrchestrationInstance, OrchestrationState>>();
            List<Task> waitTasks = new List<Task>();
            Stopwatch stopWatch = Stopwatch.StartNew();
            for (int i = 0; i < numberOfInstances; i++)
            {
                var instance = await taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), orchestrationInput);
                waitTasks.Add(Task.Run(async () =>
                {
                    var state = await taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));
                    results.Add(Tuple.Create(instance, state));
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
                if (kvp.Item2 == null)
                {
                    failedOrchestrations++;
                    var state = await taskHubClient.GetOrchestrationStateAsync(kvp.Item1);
                    Console.WriteLine($"Unfinished orchestration {kvp.Item1}, state : {kvp.Item2?.OrchestrationStatus}");
                    continue;
                }

                Assert.AreEqual(expectedResult, kvp.Item2.Output, $"Unexpected output for Orchestration : {kvp.Item1.InstanceId}");
                TimeSpan orchestrationTime = kvp.Item2.CompletedTime - kvp.Item2.CreatedTime;
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
