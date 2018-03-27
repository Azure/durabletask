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

namespace DurableTask.Samples.Tests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Samples.Cron;
    using DurableTask.Core.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CronTest
    {
        class MockCronTask : TaskActivity<string, string>
        {
            public static int Count = 0;

            public MockCronTask()
            {
            }

            protected override string Execute(DurableTask.Core.TaskContext context, string input)
            {
                Console.WriteLine("Executing Cron Job.  Started At: '" + DateTime.Now.ToString() + "' Number: " + input);

                string completed = "Cron Job '" + input + "' Completed...";
                Console.WriteLine(completed);

                Count++;
                return completed;
            }
        }

        [TestMethod]
        public async Task CronBasic()
        {
            OrchestrationTestHost host = new OrchestrationTestHost();
            host.ClockSpeedUpFactor = 5;
            MockCronTask cronTask = new MockCronTask();
            host.AddTaskOrchestrations(typeof(CronOrchestration))
                .AddTaskActivities(new MockObjectCreator<TaskActivity>(typeof(CronTask).ToString(), string.Empty, () =>
                    {
                        return cronTask;
                    }));

            var result = await host.RunOrchestration<string>(typeof(CronOrchestration), null);

            Assert.AreEqual<string>("Done", result);
            Assert.AreEqual<double>(4, MockCronTask.Count);
        }
    }
}
