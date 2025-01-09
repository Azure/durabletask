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
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureServiceFabric.Exceptions;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Test.Orchestrations.Performance;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
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
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_InstanceId_SpecialChars()
        {
            var instanceId = typeof(SimpleOrchestrationWithTasks) + "$abc";
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), instanceId, null);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

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
        public async Task Orchestration_With_NestedSubOrchestrations_Finish()
        {
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithSubOrchestration2), null);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            string subSubOrchResult = $"TaskResult = Hello World , SubOrchestration1Result = Hello Gabbar, SubOrchestration2Result = Hello Gabbar";
            Assert.AreEqual($"\"TaskResult = Hello World , SubSubOrchestration1Result = {subSubOrchResult}, SubOrchestration2Result = Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_RecurringSubOrchestration_Finishes()
        {
            int recurrenceCount = 100; 
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(SimpleRecurringSubOrchestration), recurrenceCount);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual($"\"{SimpleRecurringSubOrchestration.GetExpectedResult(recurrenceCount)}\"", result.Output);
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
        public async Task GenerationBasicTest()
        {
            var instanceId = nameof(GenerationBasicOrchestration);
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), instanceId, 2);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, result.OrchestrationStatus);

            var state = await this.taskHubClient.GetOrchestrationStateAsync(instanceId);
            result = await this.taskHubClient.WaitForOrchestrationAsync(state.OrchestrationInstance, TimeSpan.FromMinutes(2));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("2", result.Output, "Orchestration Result is wrong!!!");
            Assert.AreEqual(result.OrchestrationInstance.InstanceId, instance.InstanceId);
            Assert.AreNotEqual(result.OrchestrationInstance.ExecutionId, instance.ExecutionId);
        }

        [TestMethod]
        public async Task RecurringOrchestrationTest()
        {
            var instanceId = nameof(RecurringOrchestration);
            var input = new RecurringOrchestrationInput
            {
                TargetOrchestrationInput = 0,
                TargetOrchestrationType = typeof(RecurringTargetOrchestration).ToString(),
                TargetOrchestrationInstanceId = nameof(RecurringTargetOrchestration)
            };
            var firstInstance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(RecurringOrchestration), instanceId, input);

            // Parent orchestration runs for 4 times with ContinueAsNew and executes the
            // RecurringTargetOrchestration each time by incrementing the counter

            // 1. Check if the first iteration of parent orchestration ends with ContinuedAsNew
            // firstInstance : Includes instanceId and first iteration executionId
            OrchestrationState state = await this.taskHubClient.WaitForOrchestrationAsync(firstInstance, TimeSpan.FromSeconds(30));
            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, state.OrchestrationStatus);

            // 2. Check if the last instance of the parent orchestration completes with Completed
            var lastInstance = new OrchestrationInstance()
            {
                InstanceId = instanceId,
                ExecutionId = null
            };

            state = await this.taskHubClient.WaitForOrchestrationAsync(lastInstance, TimeSpan.FromMinutes(1));

            if (state.OrchestrationStatus == OrchestrationStatus.Running)
            {
                await this.taskHubClient.TerminateInstanceAsync(lastInstance);
            }

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("4", state.Output, "Parent Orchestration Result is wrong!!!");
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

            for (int i = 0; i < allResults.Length; i++)
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
        public async Task Orchestration_With_Same_Id_Can_Be_Started_After_First_One_Finishes()
        {
            var instanceId = nameof(Orchestration_With_Same_Id_Can_Be_Started_After_First_One_Finishes);

            OrchestrationInstance instance1 = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(ExecutionCountingOrchestration), instanceId, 10);
            await this.taskHubClient.WaitForOrchestrationAsync(instance1, TimeSpan.FromSeconds(10));
            var state1 = await this.taskHubClient.GetOrchestrationStateAsync(instance1);

            OrchestrationInstance instance2 = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(ExecutionCountingOrchestration), instanceId, 20);
            await this.taskHubClient.WaitForOrchestrationAsync(instance2, TimeSpan.FromSeconds(10));
            var state2 = await this.taskHubClient.GetOrchestrationStateAsync(instance2);

            Assert.AreNotEqual(instance1.ExecutionId, instance2.ExecutionId);
            Assert.AreNotEqual(state1.Output, state2.Output);
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
            foreach (var result in results)
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
        [ExpectedException(typeof(OrchestrationAlreadyExistsException))]
        public async Task Duplicate_Orchestration_Instance_Fails_With_OrchestrationAlreadyExistsException()
        {
            var instanceId = nameof(Duplicate_Orchestration_Instance_Fails_With_OrchestrationAlreadyExistsException);
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 15,
                MinDelay = 15,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, input);
            var instance2 = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, input);
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
                NumberOfSerialTasks = 100,
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
        public async Task ForceTerminate_Already_Finished_Orchestration()
        {
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 2,
                MaxDelay = 1,
                MinDelay = 1,
                DelayUnit = TimeSpan.FromMilliseconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), testData);
            await Task.Delay(TimeSpan.FromSeconds(1));

            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            var reason = "Testing terminatiom of already finished orchestration";

            await Assert.ThrowsExceptionAsync<RemoteServiceException>(() => this.taskHubClient.TerminateInstanceAsync(instance, reason));
        }

        [TestMethod]
        public async Task ForceTerminate_With_CleanupStore()
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

            var reason = "CleanupStore";
            await this.taskHubClient.TerminateInstanceAsync(instance, reason);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.IsTrue(result.Output.Contains(reason));
        }

        [TestMethod]
        public async Task ForceTerminate_With_CleanupStore_Start_New_Orchestration_With_Same_InstanceId()
        {
            string instanceId = "ForceTerminate_With_CleanupStore_Start_New_Orchestration_With_Same_InstanceId";
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 5,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, testData);
            await Task.Delay(TimeSpan.FromMilliseconds(1));

            var reason = "CleanupStore";
            await this.taskHubClient.TerminateInstanceAsync(instance, reason);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.IsTrue(result.Output.Contains(reason));

            instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, testData);
            await Task.Delay(TimeSpan.FromMilliseconds(1));

            result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        }

        [TestMethod]
        public async Task ForceTerminate_And_Start_New_Orchestration_With_Same_InstanceId()
        {
            string instanceId = "ForceTerminate_And_Start_New_Orchestration_With_Same_InstanceId";
            var testData = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 5,
                MaxDelay = 5,
                MinDelay = 5,
                DelayUnit = TimeSpan.FromSeconds(1),
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, testData);
            await Task.Delay(TimeSpan.FromMilliseconds(1));

            var reason = "ForceTerminate_And_Start_New_Orchestration_With_Same_InstanceId";
            await this.taskHubClient.TerminateInstanceAsync(instance, reason);
            var result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Terminated, result.OrchestrationStatus);
            Assert.IsTrue(result.Output.Contains(reason));

            instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), instanceId, testData);
            await Task.Delay(TimeSpan.FromMilliseconds(1));

            result = await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
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

        /// <summary>
        /// Validates scheduled starts, ensuring that invalid operation exception is raised since the feature is not supported
        /// </summary>
        [TestMethod]
        public async Task ScheduledStartTest_NotSupported()
        {
            var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
            await Assert.ThrowsExceptionAsync<RemoteServiceException>(() => this.taskHubClient.CreateScheduledOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), null, expectedStartTime));
        }

        [TestMethod]
        public async Task CreateTaskOrchestration_HandlesConflictResponse_When_HttpClientReturnsNullContent()
        {
            var httpClientMock = new Mock<HttpClient>();
            var response = new HttpResponseMessage(System.Net.HttpStatusCode.Conflict) { Content = null };
            SetupHttpClientMockForPut(httpClientMock, response);

            var taskHubClient = Utilities.CreateTaskHubClient((serviceClient) =>
            {
                serviceClient.HttpClient = httpClientMock.Object;
            });

            await Assert.ThrowsExceptionAsync<OrchestrationAlreadyExistsException>(async () =>
            {
                await taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), new TestOrchestrationData());
            });
        }

        private static void SetupHttpClientMockForPut(Mock<HttpClient> httpClientMock, HttpResponseMessage response)
        {
            httpClientMock
                .Setup(x => x.PutAsync(It.IsAny<string>(), It.IsAny<HttpContent>()))
                .ReturnsAsync(response);

            httpClientMock
                .Setup(x => x.PutAsync(It.IsAny<Uri>(), It.IsAny<HttpContent>()))
                .ReturnsAsync(response);

            httpClientMock
                .Setup(x => x.PutAsync(It.IsAny<string>(), It.IsAny<HttpContent>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            httpClientMock
                .Setup(x => x.PutAsync(It.IsAny<Uri>(), It.IsAny<HttpContent>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);
        }
    }
}
