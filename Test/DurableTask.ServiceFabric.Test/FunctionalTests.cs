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
using System.Threading.Tasks;
using DurableTask.Test.Orchestrations.Perf;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestApplication.Common;
using TestStatefulService.TestOrchestrations;

namespace DurableTask.ServiceFabric.Test
{
    [TestClass]
    public class FunctionalTests
    {
        IRemoteClient serviceClient;

        [TestInitialize]
        public void TestInitialize()
        {
            this.serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplicationType/TestStatefulService"), new ServicePartitionKey(1));
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
