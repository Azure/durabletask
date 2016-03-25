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
    using DurableTask.Exceptions;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class FunctionalTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;
        TaskHubWorker taskHubNoCompression;

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                client = TestHelpers.CreateTaskHubClient();
                taskHub = TestHelpers.CreateTaskHub();
                taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
                taskHub.orchestrationService.CreateAsync(true).Wait();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                taskHub.StopAsync(true).Wait();
                taskHubNoCompression.StopAsync(true).Wait();
                taskHub.orchestrationService.DeleteAsync(true).Wait();
            }
        }

        #region Generation Basic Test

        [TestMethod]
        public async Task GenerationBasicTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task SessionStateDeleteTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");

            Assert.AreEqual(0, TestHelpers.GetOrchestratorQueueSizeInBytes());
        }

        [TestMethod]
        public async Task GenerationBasicNoCompressionTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class GenerationBasicOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await context.ScheduleTask<int>(typeof (GenerationBasicTask));
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

        #endregion

        #region Generation with sub orchestration test 

        [TestMethod]
        public async Task GenerationSubTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            await taskHub.AddTaskOrchestrations(typeof (GenerationParentOrchestration), typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(4, GenerationSubTask.GenerationCount, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task GenerationSubNoCompressionTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            await taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationParentOrchestration),
                typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(4, GenerationSubTask.GenerationCount, "Orchestration Result is wrong!!!");
        }

        public class GenerationChildOrchestration : TaskOrchestration<int, int>
        {
            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await context.ScheduleTask<int>(typeof (GenerationSubTask));
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                return count;
            }
        }

        public class GenerationParentOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count =
                    await
                        context.CreateSubOrchestrationInstance<int>(typeof (GenerationChildOrchestration),
                            numberOfGenerations);

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count;
                return count;
            }
        }

        public sealed class GenerationSubTask : TaskActivity<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int GenerationCount = 0;

            protected override int Execute(TaskContext context, string input)
            {
                GenerationCount++;
                return GenerationCount;
            }
        }

        #endregion

        #region Generation with SubOrchestrationInstance Failure Test

        [TestMethod]
        public async Task GenerationSubFailedTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (GenerationSubFailedParentOrchestration),
                typeof (GenerationSubFailedChildOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();
            taskHub.TaskOrchestrationDispatcher.IncludeDetails = true;

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            OrchestrationInstance id =
                await client.CreateOrchestrationInstanceAsync(typeof (GenerationSubFailedParentOrchestration), true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationSubFailedParentOrchestration), false);

            isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");
        }

        public class GenerationSubFailedChildOrchestration : TaskOrchestration<string, int>
        {
            public static int Count;

            public override Task<string> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                if (numberOfGenerations == 1)
                {
                    throw new InvalidOperationException("Test");
                }

                Interlocked.Increment(ref Count);

                return Task.FromResult("done");
            }
        }

        public class GenerationSubFailedParentOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                int numberOfChildGenerations = 3;
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Task<string> r =
                            context.CreateSubOrchestrationInstance<string>(
                                typeof (GenerationSubFailedChildOrchestration), numberOfChildGenerations);
                        if (waitForCompletion)
                        {
                            await r;
                        }

                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                }
                catch (SubOrchestrationFailedException e)
                {
                    Assert.IsInstanceOfType(e.InnerException, typeof (InvalidOperationException),
                        "Actual exception is not Invalid Operation Exception.");
                    Result = e.Message;
                }

                return Result;
            }
        }

        #endregion

        #region Generation Signal Test

        [TestMethod]
        public async Task GenerationSignalOrchestrationTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance {InstanceId = id.InstanceId};

            await Task.Delay(2*1000);
            await client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.signal.Set();

            await Task.Delay(2*1000);
            GenerationSignalOrchestration.signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2*1000);
            await client.RaiseEventAsync(signalId, "Count", "3"); // will be recieved by next generation
            GenerationSignalOrchestration.signal.Set();

            await Task.Delay(2*1000);
            GenerationSignalOrchestration.signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2*1000);
            await client.RaiseEventAsync(signalId, "Count", "5"); // will be recieved by next generation
            await client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.signal.Set();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, signalId, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("5", GenerationSignalOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class GenerationSignalOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            public static ManualResetEvent signal = new ManualResetEvent(false);

            TaskCompletionSource<string> resumeHandle;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await WaitForSignal();
                signal.WaitOne();
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count.ToString();
                return count;
            }

            async Task<int> WaitForSignal()
            {
                resumeHandle = new TaskCompletionSource<string>();
                string data = await resumeHandle.Task;
                resumeHandle = null;
                return int.Parse(data);
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                Assert.AreEqual("Count", name, "Unknown signal recieved...");
                if (resumeHandle != null)
                {
                    resumeHandle.SetResult(input);
                }
            }
        }

        #endregion

        #region Generation New Version Test

        [TestMethod]
        public async Task GenerationVersionTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V1", typeof (GenerationV1Orchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V2", typeof (GenerationV2Orchestration));

            var c3 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V3", typeof (GenerationV3Orchestration));

            await taskHub.AddTaskOrchestrations(c1, c2, c3)
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync("GenerationOrchestration", "V1", null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(GenerationV1Orchestration.WasRun);
            Assert.IsTrue(GenerationV2Orchestration.WasRun);
            Assert.IsTrue(GenerationV3Orchestration.WasRun);
        }

        [TestMethod]
        public async Task TestHostGenerationVersionTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V1", typeof (GenerationV1Orchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V2", typeof (GenerationV2Orchestration));

            var c3 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V3", typeof (GenerationV3Orchestration));

            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(c1, c2, c3);

            object res = await testHost.RunOrchestration<object>("GenerationOrchestration", "V1", null);

            Assert.IsTrue(GenerationV1Orchestration.WasRun);
            Assert.IsTrue(GenerationV2Orchestration.WasRun);
            Assert.IsTrue(GenerationV3Orchestration.WasRun);
        }

        public class GenerationV1Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun = false;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V2", null);
                return Task.FromResult<object>(null);
            }
        }

        public class GenerationV2Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun = false;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V3", null);
                return Task.FromResult<object>(null);
            }
        }

        public class GenerationV3Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun = false;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                return Task.FromResult<object>(null);
            }
        }

        #endregion
    }
}