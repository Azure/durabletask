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
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Test.Orchestrations.Performance;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using TestApplication.Common.Orchestrations;

    [TestClass]
    public class FunctionalTests
    {
        TaskHubClient taskHubClient;

        [TestInitialize]
        public void TestInitialize()
        {
            this.taskHubClient = Utilities.CreateTaskHubClient();
        }

        [TestMethod]
        public async Task Orchestration_With_ScheduledTasks_Finishes()
        {
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), null);
            var result =  await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_Timer_Finishes_After_The_Wait_Time()
        {
            var waitTime = 37;
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), waitTime);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
            var orchestrationTime = result.CompletedTime - result.CreatedTime;
            Console.WriteLine($"Time for Orchestration : {orchestrationTime}, Timer Wait time : {waitTime}");
        }

        [TestMethod]
        public async Task Orchestration_With_SubOrchestration_Finishes()
        {
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithSubOrchestration), null);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual($"\"TaskResult = Hello World , SubOrchestration1Result = Hello Gabbar, SubOrchestration2Result = Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_TimeoutWrapper_Test()
        {
            // Task finishes within timeout
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromMilliseconds(1),
                UseTimeoutTask = true,
                ExecutionTimeout = TimeSpan.FromMinutes(1)
            };
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Console.WriteLine($"Time for Orchestration with no delay running task wrapped in 1 minute timeout : {result.CompletedTime - result.CreatedTime}");

            // Task does not finish within timeout
            input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = true,
                ExecutionTimeout = TimeSpan.FromSeconds(1)
            };

            instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);
            result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Failed, result.OrchestrationStatus);
            Console.WriteLine($"Time for Orchestration with 5 second running task wrapped in 1 second timeout : {result.CompletedTime - result.CreatedTime}");
        }

        [TestMethod]
        public async Task Orchestration_With_Same_Id_Cant_Be_Started_While_Running()
        {
            var instanceId = nameof(Orchestration_With_Same_Id_Cant_Be_Started_While_Running);
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            Func<Task<Tuple<OrchestrationInstance, Exception>>> startFunc = async () =>
            {
                OrchestrationInstance instance = null;
                Exception exception = null;

                try
                {
                    instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, testData);
                }
                catch (Exception e)
                {
                    while (e is AggregateException)
                    {
                        e = e.InnerException;
                    }

                    exception = e;
                }

                return Tuple.Create(instance, exception);
            };

            var allResults = await Task.WhenAll(startFunc(), startFunc(), startFunc(), startFunc());

            OrchestrationInstance createdInstance = null;

            for(int i = 0; i < allResults.Length; i++)
            {
                var result = allResults[i];
                if (result.Item1 != null)
                {
                    if (createdInstance != null)
                    {
                        Assert.Fail($"Multiple orchestrations were started with the instance id {instanceId} at the same time");
                    }
                    else
                    {
                        createdInstance = result.Item1;
                    }
                }
                else
                {
                    Assert.IsInstanceOfType(result.Item2, typeof(Exception),
                        $"Exception Type Check Failed : Task {i} returned an unexpected exception {result.Item2}");
                }
            }

            Assert.IsNotNull(createdInstance, $"No task was able to create an orchestration with the given instance id {instanceId}");
            var orchestrationResult = await this.taskHubClient.WaitForOrchestrationAsync(createdInstance, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, orchestrationResult.OrchestrationStatus);
        }

        [TestMethod]
        public async Task FabricProviderClient_ReturnsRunningOrchestrations()
        {
            var instanceIdPrefix = nameof(FabricProviderClient_ReturnsRunningOrchestrations);
            int numberOfInstances = 5;
            var startTasks = new List<Task<OrchestrationInstance>>();

            for (int i = 0; i < numberOfInstances; i++)
            {
                var testData = new TestOrchestrationData()
                {
                    NumberOfParallelTasks = 0,
                    NumberOfSerialTasks = 1,
                    MaxDelay = 5,
                    MinDelay = 5,
                    DelayUnit = TimeSpan.FromSeconds(1),
                };

                var task = this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceIdPrefix + i, testData);

                startTasks.Add(task);
            }

            var scheduledInstances = await Task.WhenAll(startTasks);
            await Task.Delay(TimeSpan.FromSeconds(2));

            foreach (var scheduledInstance in scheduledInstances)
            {
                var runtimeState = await this.taskHubClient.GetOrchestrationStateAsync(scheduledInstance.InstanceId);
                Console.WriteLine($"Runtime Orchestration State for {scheduledInstance} : {runtimeState}");
            }

            var waitTasks = new List<Task<OrchestrationState>>();
            foreach (var scheduledInstance in scheduledInstances)
            {
                var waitTask = this.taskHubClient.WaitForOrchestrationAsync(scheduledInstance, TimeSpan.FromMinutes(2));
                waitTasks.Add(waitTask);
            }

            var results = await Task.WhenAll(waitTasks);
            foreach(var result in results)
            {
                Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            }
        }


        [TestMethod]
        public async Task QueryState_For_Latest_Execution()
        {
            var instanceId = nameof(QueryState_For_Latest_Execution);
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 15,
                MinDelay = 15,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, input);

            var state = await this.taskHubClient.GetOrchestrationStateAsync(instanceId);

            Assert.IsNotNull(state);
            Assert.AreEqual(instance.ExecutionId, state.OrchestrationInstance.ExecutionId);

            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var newInstance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, input);

            // We want to make sure that once an orchestration is complete, we can create another instance with the same id.
            var newState = await this.taskHubClient.GetOrchestrationStateAsync(instanceId);

            Assert.IsNotNull(newState);
            Assert.AreEqual(newInstance.ExecutionId, newState.OrchestrationInstance.ExecutionId);
            Assert.AreNotEqual(state.OrchestrationInstance.ExecutionId, newState.OrchestrationInstance.ExecutionId);

            result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        }

        [TestMethod]
        public async Task Retry_OnException_Test()
        {
            int retryAttempts = 3;
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(OrchestrationRunningIntoRetry), retryAttempts);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(retryAttempts.ToString(), result.Output);
            var orchestrationTime = result.CompletedTime - result.CreatedTime;
            Console.WriteLine($"Time for Orchestration : {orchestrationTime}");
        }

        [TestMethod]
        public async Task ForceTerminate_Terminates_LatestExecution()
        {
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 5,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), testData);
            await Task.Delay(TimeSpan.FromMilliseconds(1));

            var reason = "Testing Terminate Functionality";
            await this.taskHubClient.TerminateInstanceAsync(instance, reason);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.AreEqual(reason, result.Output);
        }

        [TestMethod]
        public async Task ForceTerminate_Twice_Terminates_LatestExecution()
        {
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 2,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), testData);
            await Task.Delay(TimeSpan.FromSeconds(1));

            var reason = "Testing Terminate (Twice) Functionality";
            var tasks = new List<Task>();
            tasks.Add(this.taskHubClient.TerminateInstanceAsync(instance, reason));
            tasks.Add(this.taskHubClient.TerminateInstanceAsync(instance, reason));
            await Task.WhenAll(tasks);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.AreEqual(reason, result.Output);
        }

        [TestMethod]
        public async Task Purge_Removes_State()
        {
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), null);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            instance = result.OrchestrationInstance;
            await this.taskHubClient.PurgeOrchestrationInstanceHistoryAsync(DateTime.UtcNow, OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);
            var state = await this.taskHubClient.GetOrchestrationStateAsync(instance);

            Assert.IsNull(state);
        }
    }
}
