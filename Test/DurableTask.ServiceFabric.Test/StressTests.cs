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
using System.Diagnostics;
using System.Threading.Tasks;
using DurableTask.Test.Orchestrations.Stress;
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
            var serviceClient = ServiceProxy.Create<IRemoteClient>(new Uri("fabric:/TestFabricApplicationType/TestStatefulService"), new ServicePartitionKey(1));

            var driverConfig = new DriverOrchestrationData()
            {
                NumberOfIteration = 1,
                NumberOfParallelTasks = 40,
                SubOrchestrationData = new TestOrchestrationData()
                {
                    NumberOfParallelTasks = 10,
                    NumberOfSerialTasks = 5,
                    MaxDelayTime = 5,
                    DelayUnit = TimeSpan.FromSeconds(1)
                }
            };

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

            var expectedResult = driverConfig.NumberOfIteration * driverConfig.NumberOfParallelTasks *
                (driverConfig.SubOrchestrationData.NumberOfParallelTasks + driverConfig.SubOrchestrationData.NumberOfSerialTasks);

            Assert.AreEqual(expectedResult.ToString(), state.Output);
        }
    }
}
