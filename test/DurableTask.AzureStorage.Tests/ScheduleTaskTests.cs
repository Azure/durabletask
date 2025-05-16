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

namespace DurableTask.AzureStorage.Tests
{
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestClass]
    public class ScheduleTaskTests
    {
        /// <summary>
        /// This test checks that an orchestration fails with an InvalidOperationException
        /// when a typed CallActivityAsync() call that expects a Task actually returns void.
        /// </summary>
        [TestMethod]
        public async Task TaskReturnsVoid_OrchestratorFails()
        {
            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
            {
                await host.StartAsync();

                TaskActivity activity = TestOrchestrationHost.MakeActivity<int, Task>(
                    delegate (TaskContext ctx, int input)
                    {
                        return null;
                    });

                TestInstance<int> instance = await host.StartInlineOrchestration(
                    input: 123,
                    orchestrationName: "TestOrchestration",
                    implementation: (ctx, input) => ctx.ScheduleTask<int>("Activity", "", input),
                    activities: ("Activity", activity));

                // The expectedOutput value is the string that's passed into the InvalidOperationException
                await Task.WhenAll(instance.WaitForCompletion(
                    expectedStatus: OrchestrationStatus.Completed,
                    expectedOutput: 0));

                await host.StopAsync();
            }
        }
    }
}
