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
    using DurableTask.History;
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
                taskHub.DeleteHub();
                taskHub.CreateHubIfNotExists();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                taskHub.Stop(true);
                taskHubNoCompression.Stop(true);
                taskHub.DeleteHub();
            }
        }

        #region Generation Basic Test

        [TestMethod]
        public void GenerationBasicTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void SessionStateDeleteTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");

            Assert.AreEqual(0, TestHelpers.GetOrchestratorQueueSizeInBytes());
        }

        [TestMethod]
        public void GenerationBasicNoCompressionTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
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
        public void GenerationSubTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            taskHub.AddTaskOrchestrations(typeof (GenerationParentOrchestration), typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(4, GenerationParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(4, GenerationSubTask.GenerationCount, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void GenerationSubNoCompressionTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationParentOrchestration),
                typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
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
        public void GenerationSubFailedTest()
        {
            taskHub.AddTaskOrchestrations(typeof (GenerationSubFailedParentOrchestration),
                typeof (GenerationSubFailedChildOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .Start();
            taskHub.TaskOrchestrationDispatcher.IncludeDetails = true;

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            OrchestrationInstance id =
                client.CreateOrchestrationInstance(typeof (GenerationSubFailedParentOrchestration), true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            id = client.CreateOrchestrationInstance(typeof (GenerationSubFailedParentOrchestration), false);

            isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(10, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");
        }

        public class GenerationSubFailedChildOrchestration : TaskOrchestration<string, int>
        {
            public static int Count;

            public override async Task<string> RunTask(OrchestrationContext context, int numberOfGenerations)
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

                return "done";
            }
        }

        public class GenerationSubFailedParentOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                int numberOfChildGenerations = 4;
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
        public void GenerationSignalOrchestrationTest()
        {
            taskHub.AddTaskOrchestrations(typeof (GenerationSignalOrchestration))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance {InstanceId = id.InstanceId};

            Thread.Sleep(2*1000);
            client.RaiseEvent(signalId, "Count", "1");
            GenerationSignalOrchestration.signal.Set();

            Thread.Sleep(2*1000);
            GenerationSignalOrchestration.signal.Reset();
            client.RaiseEvent(signalId, "Count", "2");
            Thread.Sleep(2*1000);
            client.RaiseEvent(signalId, "Count", "3"); // will be recieved by next generation
            GenerationSignalOrchestration.signal.Set();

            Thread.Sleep(2*1000);
            GenerationSignalOrchestration.signal.Reset();
            client.RaiseEvent(signalId, "Count", "4");
            Thread.Sleep(2*1000);
            client.RaiseEvent(signalId, "Count", "5"); // will be recieved by next generation
            client.RaiseEvent(signalId, "Count", "6"); // lost
            client.RaiseEvent(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.signal.Set();

            bool isCompleted = TestHelpers.WaitForInstance(client, signalId, 60);
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
        public void GenerationVersionTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V1", typeof (GenerationV1Orchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V2", typeof (GenerationV2Orchestration));

            var c3 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V3", typeof (GenerationV3Orchestration));

            taskHub.AddTaskOrchestrations(c1, c2, c3)
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance("GenerationOrchestration", "V1", null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
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

            public override async Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V2", null);
                return null;
            }
        }

        public class GenerationV2Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun = false;

            public override async Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V3", null);
                return null;
            }
        }

        public class GenerationV3Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun = false;

            public override async Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                return null;
            }
        }

        #endregion

        [TestMethod]
        public async Task DuplicateSubOrchestrationInstanceTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof(ParentOrchestration),
                typeof(ChildOrchestration))
                .Start();

            // First create 2 child orchestrations with different ids and validate both completes successfully
            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof(ParentOrchestration), "child1");
            await Task.Delay(1000);
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof(ParentOrchestration), "child2");

            bool isCompleted = TestHelpers.WaitForInstance(client, id1, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id1, 60));
            OrchestrationState state = client.GetOrchestrationState(id1);
            Assert.AreEqual("\"Success\"", state.Output, TestHelpers.PrintHistory(client, id1));

            isCompleted = TestHelpers.WaitForInstance(client, id2, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id2, 60));
            state = client.GetOrchestrationState(id2);
            Assert.AreEqual("\"Success\"", state.Output, TestHelpers.PrintHistory(client, id2));

            // Now create 2 child orchestrations with different ids and validate one fails
            id1 = client.CreateOrchestrationInstance(typeof(ParentOrchestration), "child1");
            await Task.Delay(1000);
            id2 = client.CreateOrchestrationInstance(typeof(ParentOrchestration), "child1");

            isCompleted = TestHelpers.WaitForInstance(client, id1, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id1, 60));
            state = client.GetOrchestrationState(id1);
            Assert.AreEqual("\"Success\"", state.Output, TestHelpers.PrintHistory(client, id1));

            isCompleted = TestHelpers.WaitForInstance(client, id2, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id2, 60));
            state = client.GetOrchestrationState(id2);
            Assert.AreEqual("\"Failed\"", state.Output, TestHelpers.PrintHistory(client, id2));
        }

        public class ParentOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string childOrchestrationId)
            {
                string childResult = string.Empty;
                try
                {
                    childResult = await
                        context.CreateSubOrchestrationInstance<string>(typeof(ChildOrchestration),
                        childOrchestrationId,
                        context.OrchestrationInstance.InstanceId);

                    
                }
                catch (SubOrchestrationFailedException ex)
                {
                    SubOrchestrationInstanceStartFailedException startFailedEx = ex.InnerException as SubOrchestrationInstanceStartFailedException;
                    Assert.IsNotNull(startFailedEx, "unexpected inner exception.");
                    Assert.AreEqual<OrchestrationInstanceStartFailureCause>(OrchestrationInstanceStartFailureCause.OrchestrationAlreadyRunning, startFailedEx.Cause, "Incorrect cause.");
                    childResult = "Failed";
                }

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                return childResult;
            }
        }

        public class ChildOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string parentInstanceId)
            {
                return await context.CreateTimer<string>(context.CurrentUtcDateTime.AddSeconds(10), "Success");
            }
        }


    }
}