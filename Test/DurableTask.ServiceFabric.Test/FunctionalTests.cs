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
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestStatefulService;
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
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithTasks).Name, null, TimeSpan.FromMinutes(5));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
        }

        [TestMethod]
        public async Task Orchestration_With_Timer_Finishes_After_The_Wait_Time()
        {
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithTimer).Name, 37, TimeSpan.FromMinutes(5));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("\"Hello Gabbar\"", result.Output);
            Assert.IsTrue((result.CompletedTime - result.CreatedTime) > TimeSpan.FromSeconds(37));
        }

        [TestMethod]
        [Ignore]
        public async Task GenerationBasicTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            var result = await this.serviceClient.RunOrchestrationAsync(typeof(GenerationBasicOrchestration).Name, 4, TimeSpan.FromMinutes(5));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task Orchestration_With_SubOrchestration_Finishes()
        {
            var result = await this.serviceClient.RunOrchestrationAsync(typeof(SimpleOrchestrationWithSubOrchestration).Name, null, TimeSpan.FromMinutes(5));

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual($"\"TaskResult = Hello World , SubOrchestration1Result = Hello Gabbar, SubOrchestration2Result = Hello Gabbar\"", result.Output);
        }
    }
}
