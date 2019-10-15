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
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Test.Orchestrations.Performance;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class WaitForOrchestrationTests
    {
        TaskHubClient taskHubClient;

        [TestInitialize]
        public void TestInitialize()
        {
            this.taskHubClient = Utilities.CreateTaskHubClient();
        }

        [TestMethod]
        public async Task WaitForOrchestration_Returns_Null_When_Orchestration_Not_Started()
        {
            var instance = new OrchestrationInstance() { InstanceId = "NonExisting", ExecutionId = "NonExisting" };
            OrchestrationState result = null;
            var waitTime = TimeSpan.FromMinutes(1);
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.taskHubClient.WaitForOrchestrationAsync(instance, waitTime);
            });

            Assert.IsNull(result);
            Assert.IsTrue(time < waitTime + waitTime);
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait {time}");
        }

        [TestMethod]
        public async Task WaitForOrchestration_Returns_State_When_Orchestration_Already_Finished()
        {
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromMilliseconds(1),
                UseTimeoutTask = false,
            };
            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);
            await this.taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1)); //Wait till orchestration is finished

            var waitTime = TimeSpan.FromMilliseconds(100); //Let this be very low for the test to be effective.
            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.taskHubClient.WaitForOrchestrationAsync(instance, waitTime);
            });

            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.IsTrue(time < waitTime);
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait {time}, Actual Time taken for Orchestration : {result.CompletedTime - result.CreatedTime}");
        }

        [TestMethod]
        public async Task WaitForOrchestration_Waits_And_Returns_State_When_Orchestration_IsRunning()
        {
            var waitTime = TimeSpan.FromMinutes(1);
            var orchestrationTimeInSeconds = 5;
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = orchestrationTimeInSeconds,
                MinDelay = orchestrationTimeInSeconds,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = false,
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);

            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.taskHubClient.WaitForOrchestrationAsync(instance, waitTime);
            });

            Assert.IsNotNull(result);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.IsTrue(time < waitTime);
            Assert.IsTrue(time > TimeSpan.FromSeconds(orchestrationTimeInSeconds));
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait : {time}, Expected Minimum time for orchestration : {orchestrationTimeInSeconds} seconds, Actual Time taken for Orchestration : {result.CompletedTime - result.CreatedTime}");
        }

        [TestMethod]
        public async Task WaitForOrchestration_TimesOut_And_Returns_Null_When_Orchestration_DidNotFinishYet()
        {
            var initialwaitTimeInSeconds = 2;
            var orchestrationTimeInSeconds = 5;
            var waitTime = TimeSpan.FromSeconds(initialwaitTimeInSeconds);
            var input = new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = orchestrationTimeInSeconds,
                MinDelay = orchestrationTimeInSeconds,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = false,
            };

            var instance = await this.taskHubClient.CreateOrchestrationInstanceAsync(typeof(TestOrchestration), input);

            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.taskHubClient.WaitForOrchestrationAsync(instance, waitTime);
            });

            Assert.IsNotNull(result);
            Assert.AreEqual(result.OrchestrationStatus, OrchestrationStatus.Running);
            Assert.IsTrue(time > waitTime);
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait : {time}, Expected Orchestration Running Time : {orchestrationTimeInSeconds} seconds");

            var newWaitTime = TimeSpan.FromMinutes(1);
            time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.taskHubClient.WaitForOrchestrationAsync(instance, newWaitTime);
            });

            Assert.IsNotNull(result);
            Assert.IsTrue(time < newWaitTime);
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Console.WriteLine($"Full WaitTime : {newWaitTime}, Actual time taken for Wait : {time}, Time taken for Orchestration : {result.CompletedTime - result.CreatedTime}");
        }
    }
}
