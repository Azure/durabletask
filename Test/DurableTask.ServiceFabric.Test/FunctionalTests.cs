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

namespace DurableTask.ServiceFabric.Test
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Test.Orchestrations.Perf;
    using Microsoft.ServiceFabric.Services.Client;
    using Microsoft.ServiceFabric.Services.Remoting.Client;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TestApplication.Common;
    using TestStatefulService.TestOrchestrations;

    [TestClass]
    public class FunctionalTests
    {
        IRemoteClient serviceClient;

        [TestInitialize]
        public void TestInitialize()
        {
            this.serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri(Constants.TestFabricApplicationAddress), new ServicePartitionKey(1));
        }

        [TestMethod]
        public async Task Orchestration_With_ScheduledTasks_Finishes()
        {
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithTasks).Name, null, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_Timer_Finishes_After_The_Wait_Time()
        {
            var waitTime = 37;
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithTimer).Name, waitTime, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
            var orchestrationTime = result.CompletedTime - result.CreatedTime;
            Assert.IsTrue(orchestrationTime > TimeSpan.FromSeconds(waitTime));
            Console.WriteLine($"Time for Orchestration : {orchestrationTime}, Timer Wait time : {waitTime}");
        }

        [TestMethod]
        [Ignore]
        public async Task GenerationBasicTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            var result = await this.serviceClient.RunOrchestrationAsync(typeof(GenerationBasicOrchestration).Name, 4, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task Orchestration_With_SubOrchestration_Finishes()
        {
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithSubOrchestration).Name, null, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual($"\"TaskResult = Hello World , SubOrchestration1Result = Hello Gabbar, SubOrchestration2Result = Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_TimeoutWrapper_Test()
        {
            // Task finishes within timeout
            var instance = await this.serviceClient.StartTestOrchestrationAsync(new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromMilliseconds(1),
                UseTimeoutTask = true,
                ExecutionTimeout = TimeSpan.FromMinutes(1)
            });
            var result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));

            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Console.WriteLine($"Time for Orchestration with no delay running task wrapped in 1 minute timeout : {result.CompletedTime - result.CreatedTime}");

            // Task does not finish within timeout
            instance = await this.serviceClient.StartTestOrchestrationAsync(new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = true,
                ExecutionTimeout = TimeSpan.FromSeconds(1)
            });
            result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));

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
                    instance = await this.serviceClient.StartTestOrchestrationWithInstanceIdAsync(instanceId, testData);
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
                    Assert.IsInstanceOfType(result.Item2, typeof(InvalidOperationException),
                        $"Exception Type Check Failed : Task {i} returned an unexpected exception {result.Item2}");
                    Assert.AreEqual($"An orchestration with id '{instanceId}' is already running.", result.Item2.Message,
                        $"Exception Message Check Failed : Task {i} returned an unexpected exception {result.Item2}");
                }
            }

            Assert.IsNotNull(createdInstance, $"No task was able to create an orchestration with the given instance id {instanceId}");
            var orchestrationResult = await this.serviceClient.WaitForOrchestration(createdInstance, TimeSpan.FromMinutes(2));
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

                startTasks.Add(this.serviceClient.StartTestOrchestrationWithInstanceIdAsync(instanceIdPrefix + i, testData));
            }

            var scheduledInstances = await Task.WhenAll(startTasks);
            await Task.Delay(TimeSpan.FromSeconds(2));

            var allInstances = (await this.serviceClient.GetRunningOrchestrations()).ToList();
            Assert.AreEqual(numberOfInstances, allInstances.Count);

            foreach (var scheduledInstance in scheduledInstances)
            {
                var returnedInstance = allInstances.FirstOrDefault(i => string.Equals(i.InstanceId, scheduledInstance.InstanceId));
                Assert.IsNotNull(returnedInstance);
                Assert.AreEqual(scheduledInstance.ExecutionId, returnedInstance.ExecutionId);
                var runtimeState = await this.serviceClient.GetOrchestrationRuntimeState(scheduledInstance.InstanceId);
                Console.WriteLine($"Runtime Orchestration State for {scheduledInstance} : {runtimeState}");
            }

            var waitTasks = new List<Task<OrchestrationState>>();
            foreach (var scheduledInstance in scheduledInstances)
            {
                waitTasks.Add(this.serviceClient.WaitForOrchestration(scheduledInstance, TimeSpan.FromMinutes(2)));
            }

            var results = await Task.WhenAll(waitTasks);
            foreach(var result in results)
            {
                Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            }
        }

        [TestMethod]
        public async Task Orchestration_With_Long_InstanceId_Name_Test()
        {
            var instanceId = $"MySomewhatLongNamespaceName|MyRelativelyLongRootEntityName|{Guid.NewGuid().ToString("N")}|MyRatherLongRuleName";
            var instance = await this.serviceClient.StartTestOrchestrationWithInstanceIdAsync(instanceId, new TestOrchestrationData()
            {
                NumberOfParallelTasks = 16,
                NumberOfSerialTasks = 5,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromMilliseconds(1)
            });

            var result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(1));
            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        }

        [TestMethod]
        public async Task QueryState_For_Latest_Execution()
        {
            var instanceId = nameof(QueryState_For_Latest_Execution);
            var instance = await this.serviceClient.StartTestOrchestrationWithInstanceIdAsync(instanceId, new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 15,
                MinDelay = 15,
                DelayUnit = TimeSpan.FromSeconds(1),
            });

            var state = await this.serviceClient.GetOrchestrationStateWithInstanceId(instanceId);

            Assert.IsNotNull(state);
            Assert.AreEqual(instance.ExecutionId, state.OrchestrationInstance.ExecutionId);

            var result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            var newInstance = await this.serviceClient.StartTestOrchestrationWithInstanceIdAsync(instanceId, new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromSeconds(1),
            });

            // We want to make sure that once an orchestration is complete, we can create another instance with the same id.
            var newState = await this.serviceClient.GetOrchestrationStateWithInstanceId(instanceId);

            Assert.IsNotNull(newState);
            Assert.AreEqual(newInstance.ExecutionId, newState.OrchestrationInstance.ExecutionId);
            Assert.AreNotEqual(state.OrchestrationInstance.ExecutionId, newState.OrchestrationInstance.ExecutionId);

            result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        }

        [TestMethod]
        public async Task Retry_OnException_Test()
        {
            int retryAttempts = 3;
            var minumumExpectedTime = TimeSpan.FromSeconds(7); //This should be calculated based on RetryOptions in the Orchestration code.

            var result = await this.serviceClient.RunOrchestrationAsync(typeof(OrchestrationRunningIntoRetry).Name, retryAttempts, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(retryAttempts.ToString(), result.Output);
            var orchestrationTime = result.CompletedTime - result.CreatedTime;
            Console.WriteLine($"Time for Orchestration : {orchestrationTime}, Minumum expected time : {minumumExpectedTime}");
            Assert.IsTrue(orchestrationTime > minumumExpectedTime);
        }

        [TestMethod]
        public async Task ForceTerminate_Terminates_LatestExecution()
        {
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 2,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.serviceClient.StartTestOrchestrationAsync(testData);
            await Task.Delay(TimeSpan.FromSeconds(1));

            var reason = "Testing Terminate Functionality";
            await this.serviceClient.TerminateOrchestration(instance.InstanceId, reason);
            var result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.AreEqual(reason, result.Output);
        }

        [TestMethod]
        [Ignore] // Calling TermiateOrchestration twice while orchestration is running does not work. This test should be enabled after that is fixed.
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

            var instance = await this.serviceClient.StartTestOrchestrationAsync(testData);
            await Task.Delay(TimeSpan.FromSeconds(1));

            var reason = "Testing Terminate (Twice) Functionality";
            var tasks = new List<Task>();
            tasks.Add(this.serviceClient.TerminateOrchestration(instance.InstanceId, reason));
            tasks.Add(this.serviceClient.TerminateOrchestration(instance.InstanceId, reason));
            await Task.WhenAll(tasks);
            var result = await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.AreEqual(reason, result.Output);
        }

        [TestMethod]
        public async Task Purge_Removes_State()
        {
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithTasks).Name, null, TimeSpan.FromMinutes(2));
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            var instance = result.OrchestrationInstance;

            await this.serviceClient.PurgeOrchestrationHistoryEventsAsync();

            var state = await this.serviceClient.GetOrchestrationState(instance);
            Assert.IsNull(state);
        }
    }
}
