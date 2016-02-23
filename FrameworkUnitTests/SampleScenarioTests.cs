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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;

    using DurableTask;
    using DurableTask.Exceptions;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SampleScenarioTests
    {
        TaskHubClient client;
        TaskHubWorker fakeTaskHub;
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
                fakeTaskHub = TestHelpers.CreateTaskHub();

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
                taskHubNoCompression.Stop();
                fakeTaskHub.Stop(true);
                taskHub.DeleteHub();
            }
        }

        #region Common TaskActivities

        public sealed class SendGreetingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string user)
            {
                return "Greeting send to " + user;
            }
        }

        #endregion

        #region Simplest Greetings Test

        [TestMethod]
        public void SimplestGreetingsTest()
        {
            taskHub.AddTaskOrchestrations(typeof (SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof (SimplestGetUserTask), typeof (SimplestSendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (SimplestGreetingsOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void SimplestGreetingsNoCompressionTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof (SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof (SimplestGetUserTask), typeof (SimplestSendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (SimplestGreetingsOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        public sealed class SimplestGetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public class SimplestGreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof (SimplestGetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof (SimplestSendGreetingTask), user);
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

        #endregion

        #region Greetings Test

        [TestMethod]
        public void GreetingsTest()
        {
            taskHub.AddTaskOrchestrations(typeof (GreetingsOrchestration))
                .AddTaskActivities(typeof (GetUserTask), typeof (SendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GreetingsOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", GreetingsOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public sealed class GetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public class GreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof (GetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof (SendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        #endregion

        #region Greetings2 Test

        [TestMethod]
        public void Greetings2Test()
        {
            taskHub.AddTaskOrchestrations(typeof (GreetingsOrchestration2))
                .AddTaskActivities(typeof (GetUserTask2), typeof (SendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (GreetingsOrchestration2), 20);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", GreetingsOrchestration2.Result,
                "Orchestration Result is wrong!!!");

            id = client.CreateOrchestrationInstance(typeof (GreetingsOrchestration2), 2);

            isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to TimedOut", GreetingsOrchestration2.Result,
                "Orchestration Result is wrong!!!");
        }

        public sealed class GetUserTask2 : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                Thread.Sleep(15*1000);
                return "Gabbar";
            }
        }

        public class GreetingsOrchestration2 : TaskOrchestration<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, int secondsToWait)
            {
                Task<string> user = context.ScheduleTask<string>(typeof (GetUserTask2));
                Task<string> timer = context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(secondsToWait),
                    "TimedOut");

                Task<string> u = await Task.WhenAny(user, timer);
                string greeting = await context.ScheduleTask<string>(typeof (SendGreetingTask), u.Result);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        #endregion

        #region AverageCalculator Test

        [TestMethod]
        public void AverageCalculatorTest()
        {
            taskHub.AddTaskOrchestrations(typeof (AverageCalculatorOrchestration))
                .AddTaskActivities(typeof (ComputeSumTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (AverageCalculatorOrchestration),
                new[] {1, 50, 10});

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(25, AverageCalculatorOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        class AverageCalculatorOrchestration : TaskOrchestration<double, int[]>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static double Result;

            public override async Task<double> RunTask(OrchestrationContext context, int[] input)
            {
                if (input == null || input.Length != 3)
                {
                    throw new ArgumentException("input");
                }

                int start = input[0];
                int end = input[1];
                int step = input[2];
                int total = end - start + 1;

                var chunks = new List<Task<int>>();
                int current;
                while (start < end)
                {
                    current = start + step - 1;
                    if (current > end)
                    {
                        current = end;
                    }

                    Task<int> chunk = context.ScheduleTask<int>(typeof (ComputeSumTask), new[] {start, current});
                    chunks.Add(chunk);

                    start = current + 1;
                }

                int sum = 0;
                int[] allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    sum += result;
                }

                double r = sum/total;
                Result = r;
                return r;
            }
        }

        public sealed class ComputeSumTask : TaskActivity<int[], int>
        {
            protected override int Execute(TaskContext context, int[] chunk)
            {
                if (chunk == null || chunk.Length != 2)
                {
                    throw new ArgumentException("chunk");
                }

                Console.WriteLine("Compute Sum for " + chunk[0] + "," + chunk[1]);
                int sum = 0;
                int start = chunk[0];
                int end = chunk[1];
                for (int i = start; i <= end; i++)
                {
                    sum += i;
                }

                Console.WriteLine("Total Sum for Chunk '" + chunk[0] + "," + chunk[1] + "' is " + sum);

                return sum;
            }
        }

        #endregion

        #region Signal Test

        [TestMethod]
        public void SignalTest()
        {
            taskHub.AddTaskOrchestrations(typeof (SignalOrchestration))
                .AddTaskActivities(typeof (SendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (SignalOrchestration), null);

            Thread.Sleep(2*1000);
            client.RaiseEvent(id, "GetUser", "Gabbar");

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", SignalOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        public class SignalOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            TaskCompletionSource<string> resumeHandle;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await WaitForSignal();
                string greeting = await context.ScheduleTask<string>(typeof (SendGreetingTask), user);
                Result = greeting;
                return greeting;
            }

            async Task<string> WaitForSignal()
            {
                resumeHandle = new TaskCompletionSource<string>();
                string data = await resumeHandle.Task;
                resumeHandle = null;
                return data;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                Assert.AreEqual("GetUser", name, "Unknown signal recieved...");
                if (resumeHandle != null)
                {
                    resumeHandle.SetResult(input);
                }
            }
        }

        #endregion

        #region ErrorHandling Test

        [TestMethod]
        public void ErrorHandlingTest()
        {
            taskHub.AddTaskOrchestrations(typeof (ErrorHandlingOrchestration))
                .AddTaskActivities(typeof (GoodTask), typeof (BadTask), typeof (CleanupTask))
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (ErrorHandlingOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("CleanupResult", ErrorHandlingOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        public sealed class BadTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new InvalidOperationException("BadTask failed.");
            }
        }

        public sealed class CleanupTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "CleanupResult";
            }
        }

        public class ErrorHandlingOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string goodResult = null;
                string badResult = null;
                string result = null;
                bool hasError = false;
                try
                {
                    goodResult = await context.ScheduleTask<string>(typeof (GoodTask));
                    badResult = await context.ScheduleTask<string>(typeof (BadTask));
                    result = goodResult + badResult;
                }
                catch (TaskFailedException e)
                {
                    Assert.IsInstanceOfType(e.InnerException, typeof (InvalidOperationException));
                    Assert.AreEqual("BadTask failed.", e.Message);
                    hasError = true;
                }

                if (hasError && !string.IsNullOrEmpty(goodResult))
                {
                    result = await context.ScheduleTask<string>(typeof (CleanupTask));
                }

                Result = result;
                return result;
            }
        }

        public sealed class GoodTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "GoodResult";
            }
        }

        #endregion

        #region Cron Test

        public enum RecurrenceFrequency
        {
            Second,
            Minute,
            Hour,
            Day,
            Week,
            Month,
            Year
        }

        [TestMethod]
        public void CronTest()
        {
            taskHub.AddTaskOrchestrations(typeof (CronOrchestration))
                .AddTaskActivities(typeof (CronTask))
                .Start();

            CronOrchestration.Tasks.Clear();
            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (CronOrchestration), new CronJob
            {
                Frequency = RecurrenceFrequency.Second,
                Count = 5,
                Interval = 3,
            });

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 120));
            Assert.AreEqual(5, CronTask.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, CronOrchestration.Result, "Orchestration Result is wrong!!!");
            int taskExceptions = CronOrchestration.Tasks.Count(task => task.Exception != null);
            Assert.AreEqual(0, taskExceptions, $"Orchestration Result contains {taskExceptions} exceptions!!!");
        }

        public class CronJob
        {
            public RecurrenceFrequency Frequency { get; set; }
            public int Interval { get; set; }
            public int Count { get; set; }
        }

        public class CronOrchestration : TaskOrchestration<string, CronJob>
        {
            public static int Result;
            public static List<Task<string>> Tasks = new List<Task<string>>();

            public override async Task<string> RunTask(OrchestrationContext context, CronJob job)
            {
                int runAfterEverySeconds = 0;
                if (job.Frequency == RecurrenceFrequency.Second)
                {
                    runAfterEverySeconds = job.Interval;
                }
                else
                {
                    throw new NotSupportedException("Job Frequency '" + job.Frequency + "' not supported...");
                }

                int i;
                for (i = 1; i <= job.Count; i++)
                {
                    DateTime currentTime = context.CurrentUtcDateTime;
                    DateTime fireAt;
                    fireAt = currentTime.AddSeconds(runAfterEverySeconds);

                    string attempt = await context.CreateTimer(fireAt, i.ToString());

                    Tasks.Add(context.ScheduleTask<string>(typeof(CronTask), attempt));
                }

                Result = i - 1;
                return "Done";
            }
        }

        sealed class CronTask : TaskActivity<string, string>
        {
            public static int Result;

            protected override string Execute(TaskContext context, string input)
            {
                Result++;
                Thread.Sleep(2*1000);
                string completed = "Cron Job '" + input + "' Completed...";
                return completed;
            }
        }

        #endregion

        #region SubOrchestrationInstance Test

        [TestMethod]
        public void SubOrchestrationTest()
        {
            taskHub.AddTaskOrchestrations(typeof (ParentWorkflow), typeof (ChildWorkflow))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (ParentWorkflow), true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            id = client.CreateOrchestrationInstance(typeof (ParentWorkflow), false);

            isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");
        }

        public class ChildWorkflow : TaskOrchestration<string, int>
        {
            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                return Task.FromResult($"Child '{input}' completed.");
            }
        }

        public class ParentWorkflow : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                for (int i = 0; i < 5; i++)
                {
                    Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof (ChildWorkflow), i);
                    if (waitForCompletion)
                    {
                        await r;
                    }

                    results[i] = r;
                }

                string[] data = await Task.WhenAll(results);
                Result = string.Concat(data);
                return Result;
            }
        }

        #endregion

        #region SubOrchestrationInstance Failure Test

        [TestMethod]
        public void SubOrchestrationFailedTest()
        {
            taskHub.AddTaskOrchestrations(typeof (ParentWorkflow2), typeof (ChildWorkflow2))
                .Start();
            taskHub.TaskOrchestrationDispatcher.IncludeDetails = true;

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (ParentWorkflow2), true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            Assert.AreEqual("Test", ParentWorkflow2.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, ChildWorkflow2.Count, "Child Workflow Count invalid.");

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            id = client.CreateOrchestrationInstance(typeof (ParentWorkflow2), false);

            isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Test", ParentWorkflow2.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, ChildWorkflow2.Count, "Child Workflow Count invalid.");
        }

        public class ChildWorkflow2 : TaskOrchestration<string, int>
        {
            public static int Count;

            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                Count++;
                throw new InvalidOperationException("Test");
            }
        }

        public class ParentWorkflow2 : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof (ChildWorkflow2), i);
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
                        "Actual exception is not InvalidOperationException.");
                    Result = e.Message;
                }

                return Result;
            }
        }

        #endregion

        #region BadOrchestration Test

        [TestMethod]
        public void BadOrchestrationTest()
        {
            taskHub.AddTaskOrchestrations(typeof (BadOrchestration))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (BadOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
        }

        public class BadOrchestration : TaskOrchestration<string, string>
        {
#pragma warning disable 1998
            public override async Task<string> RunTask(OrchestrationContext context, string input)
#pragma warning restore 1998
            {
                throw new Exception("something very bad happened");
            }
        }

        #endregion

        #region SubOrchestrationInstance Explicit InstanceId Test

        [TestMethod]
        public void SubOrchestrationExplicitIdTest()
        {
            SimpleChildWorkflow.ChildInstanceId = null;
            taskHub.AddTaskOrchestrations(typeof (SimpleParentWorkflow), typeof (SimpleChildWorkflow))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof (SimpleParentWorkflow), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("foo_instance", SimpleChildWorkflow.ChildInstanceId);
        }

        [TestMethod]
        public async Task TestHostSubOrchestrationExplicitIdTest()
        {
            SimpleChildWorkflow.ChildInstanceId = null;
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (SimpleParentWorkflow), typeof (SimpleChildWorkflow));

            object res = await testHost.RunOrchestration<object>(typeof (SimpleParentWorkflow), null);

            Assert.AreEqual("foo_instance", SimpleChildWorkflow.ChildInstanceId);
        }

        public class SimpleChildWorkflow : TaskOrchestration<string, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string ChildInstanceId = null;

            public override Task<string> RunTask(OrchestrationContext context, object input)
            {
                ChildInstanceId = context.OrchestrationInstance.InstanceId;
                return Task.FromResult<string>(null);
            }
        }

        public class SimpleParentWorkflow : TaskOrchestration<string, object>
        {
            public override async Task<string> RunTask(OrchestrationContext context, object input)
            {
                await
                    context.CreateSubOrchestrationInstanceWithRetry<string>(typeof (SimpleChildWorkflow), "foo_instance",
                        new RetryOptions(TimeSpan.FromSeconds(5), 3), null);
                return null;
            }
        }

        #endregion
    }
}