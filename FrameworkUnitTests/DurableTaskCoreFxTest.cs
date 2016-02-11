
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

namespace FrameworkUnitTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Mocks;
    using System.Diagnostics;
    /// <summary>
    /// Test the core dtfx via a mock orchestration service and client provider
    /// </summary>
    [TestClass]
    public class DurableTaskCoreFxTest
    {

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }

        [TestMethod]
        public async Task MockOrchestrationTest()
        {
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker2 worker = new TaskHubWorker2(orchService, "test", new TaskHubWorkerSettings());

            worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .Start();

            TaskHubClient2 client = new TaskHubClient2(orchService, "test", new TaskHubClientSettings());

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");

            worker.Stop();
        }

        [TestMethod]
        public async Task MockTimerTest()
        {
            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker2 worker = new TaskHubWorker2(orchService, "test", new TaskHubWorkerSettings());

            worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .Start();

            TaskHubClient2 client = new TaskHubClient2(orchService, "test", new TaskHubClientSettings());

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "30");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.IsTrue(sw.Elapsed.Seconds > 30);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");

            worker.Stop();
        }

        [TestMethod]
        public async Task MockGenerationTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            LocalOrchestrationService orchService = new LocalOrchestrationService();

            TaskHubWorker2 worker = new TaskHubWorker2(orchService, "test", new TaskHubWorkerSettings());
            TaskHubClient2 client = new TaskHubClient2(orchService, "test", new TaskHubClientSettings());

            worker.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .Start();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            // strip out the eid so we wait for the latest one always
            OrchestrationInstance masterid = new OrchestrationInstance { InstanceId = id.InstanceId };

            OrchestrationState result1 = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());

            OrchestrationState result2 = await client.WaitForOrchestrationAsync(masterid, TimeSpan.FromSeconds(500), new CancellationToken());

            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, result1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, result2.OrchestrationStatus);

            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class GenerationBasicOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await context.ScheduleTask<int>(typeof(GenerationBasicTask));
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count;
                return count;
            }
        }

        public sealed class GenerationBasicTask : TaskActivity<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int GenerationCount = 0;

            protected override int Execute(TaskContext context, string input)
            {
                GenerationCount++;
                return GenerationCount;
            }
        }


        //[TestMethod]
        //public async Task TerminateOrchestrationTest()
        //{
        //    LocalOrchestrationService orchService = new LocalOrchestrationService();

        //    await orchService.StartAsync();

        //    TaskHubWorker2 worker = new TaskHubWorker2(orchService, "test", new TaskHubWorkerSettings());

        //    worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
        //        .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
        //        .Start();

        //    TaskHubClient2 client = new TaskHubClient2(orchService, "test", new TaskHubClientSettings());

        //    OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "60");

        //    OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), new CancellationToken());
        //    Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
        //        "Orchestration Result is wrong!!!");

        //    await orchService.StopAsync();
        //}


        public sealed class SimplestGetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public class SimpleGeneratioOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                
                string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
                int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

                if (delayInSeconds > 0)
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
                }

                string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        public class SimplestGreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
                int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

                if (delayInSeconds > 0)
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
                }

                string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        public sealed class SimplestSendGreetingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string user)
            {
                return "Greeting send to " + user;
            }
        }

    }
}