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
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Test.Orchestrations.Perf;
    using Microsoft.ServiceFabric.Services.Client;
    using Microsoft.ServiceFabric.Services.Remoting.Client;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TestApplication.Common;

    [TestClass]
    public class WaitForOrchestrationTests
    {
        IRemoteClient serviceClient;

        [TestInitialize]
        public void TestInitialize()
        {
            this.serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri(Constants.TestFabricApplicationAddress), new ServicePartitionKey(1));
        }

        [TestMethod]
        public async Task WaitForOrchestration_Returns_Null_When_Orchestration_Not_Started()
        {
            var instance = new OrchestrationInstance() { InstanceId = "NonExisting", ExecutionId = "NonExisting" };
            OrchestrationState result = null;
            var waitTime = TimeSpan.FromMinutes(1);
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.serviceClient.WaitForOrchestration(instance, waitTime);
            });

            Assert.IsNull(result);
            Assert.IsTrue(time < waitTime);
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait {time}");
        }

        [TestMethod]
        public async Task WaitForOrchestration_Returns_State_When_Orchestration_Already_Finished()
        {
            var instance = await this.serviceClient.StartTestOrchestrationAsync(new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = 0,
                MinDelay = 0,
                DelayUnit = TimeSpan.FromMilliseconds(1),
                UseTimeoutTask = false,
            });
            await this.serviceClient.WaitForOrchestration(instance, TimeSpan.FromMinutes(1)); //Wait till orchestration is finished

            var waitTime = TimeSpan.FromMilliseconds(100); //Let this be very low for the test to be effective.
            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.serviceClient.WaitForOrchestration(instance, waitTime);
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
            var instance = await this.serviceClient.StartTestOrchestrationAsync(new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = orchestrationTimeInSeconds,
                MinDelay = orchestrationTimeInSeconds,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = false,
            });

            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.serviceClient.WaitForOrchestration(instance, waitTime);
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
            var instance = await this.serviceClient.StartTestOrchestrationAsync(new TestOrchestrationData()
            {
                NumberOfParallelTasks = 0,
                NumberOfSerialTasks = 1,
                MaxDelay = orchestrationTimeInSeconds,
                MinDelay = orchestrationTimeInSeconds,
                DelayUnit = TimeSpan.FromSeconds(1),
                UseTimeoutTask = false,
            });

            OrchestrationState result = null;
            var time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.serviceClient.WaitForOrchestration(instance, waitTime);
            });

            Assert.IsNull(result);
            Assert.IsTrue(time > waitTime);
            Assert.IsTrue(time < TimeSpan.FromSeconds(orchestrationTimeInSeconds));
            Console.WriteLine($"Full WaitTime : {waitTime}, Actual time taken for Wait : {time}, Expected Orchestration Running Time : {orchestrationTimeInSeconds} seconds");

            var newWaitTime = TimeSpan.FromMinutes(1);
            time = await Utilities.MeasureAsync(async () =>
            {
                result = await this.serviceClient.WaitForOrchestration(instance, newWaitTime);
            });

            Assert.IsNotNull(result);
            Assert.IsTrue(time < newWaitTime);
            Assert.IsTrue(time > TimeSpan.FromSeconds(orchestrationTimeInSeconds - initialwaitTimeInSeconds)); //Theoritically, this could be flakey. The right way is to check for the difference being within a small internval, adjust this if needed based on the output below.
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Console.WriteLine($"Full WaitTime : {newWaitTime}, Actual time taken for Wait : {time}, Time taken for Orchestration : {result.CompletedTime - result.CreatedTime}");
        }
    }
}
