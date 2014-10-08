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
    using DurableTask;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationTestHostTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
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

        #region Retry Interceptor Tests

        static readonly string RETRY_NAME = "RetryOrchestration";
        static readonly string RETRY_VERSION = string.Empty;
        static readonly string DO_WORK_NAME = "DoWork";
        static readonly string DO_WORK_VERSION = string.Empty;

        [TestMethod]
        public async Task TestHostBasicRetryTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 4);
            var retryTask = new RetryTask(3);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, false);
            Assert.AreEqual("DoWork Succeeded. Attempts: 3", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task TestHostBasicRetryFailTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, false);
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task TestHostRetryCustomHandlerFailThroughProxyTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryCustomHandlerFailTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryCustomHandlerPassThroughProxyTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryCustomHandlerPassTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryOnReasonCustomHandlerThroughProxyTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;
                Assert.IsInstanceOfType(taskFailed.InnerException, typeof (InvalidOperationException),
                    "InnerException is not InvalidOperationException.");
                return e.Message.StartsWith("DoWork Failed. RetryCount is:");
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryOnReasonCustomHandlerTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;
                Assert.IsInstanceOfType(taskFailed.InnerException, typeof (InvalidOperationException),
                    "InnerException is not InvalidOperationException.");
                return e.Message.StartsWith("DoWork Failed. RetryCount is:");
            };

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, false);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryTimeoutThroughProxyTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.IsTrue(result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + result);
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public async Task TestHostRetryTimeoutTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, false);
            Assert.IsTrue(result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + result);
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public async Task TestHostRetryMaxIntervalThroughProxyTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, true);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task TestHostRetryMaxIntervalTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            testHost.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask);

            RetryOrchestration.Result = null;
            string result = await testHost.RunOrchestration<string>(RETRY_NAME, RETRY_VERSION, false);
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        public interface IRetryTask
        {
            string DoWork();
        }

        public interface IRetryTaskClient
        {
            Task<string> DoWork();
        }

        sealed class RetryOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            readonly RetryOptions retryPolicy;

            public RetryOrchestration(RetryOptions retryOptions)
            {
                retryPolicy = retryOptions;
            }

            public override async Task<string> RunTask(OrchestrationContext context, bool useTypedClient)
            {
                string result;
                try
                {
                    if (useTypedClient)
                    {
                        var client = context.CreateRetryableClient<IRetryTaskClient>(retryPolicy);
                        result = await client.DoWork();
                    }
                    else
                    {
                        result = await context.ScheduleWithRetry<string>(DO_WORK_NAME, DO_WORK_VERSION, retryPolicy);
                    }
                }
                catch (TaskFailedException e)
                {
                    result = e.Message;
                }

                Result = result;
                return result;
            }
        }

        sealed class RetryTask : IRetryTask
        {
            public RetryTask(int failAttempts)
            {
                RetryCount = 0;
                FailAttempts = failAttempts;
            }

            public int RetryCount { get; set; }
            public int FailAttempts { get; set; }

            public string DoWork()
            {
                if (RetryCount < FailAttempts)
                {
                    RetryCount++;
                    throw new InvalidOperationException("DoWork Failed. RetryCount is: " + RetryCount);
                }

                return "DoWork Succeeded. Attempts: " + RetryCount;
            }
        }

        #endregion

        #region Generation Basic Test

        [TestMethod]
        public async Task TestHostGenerationBasicTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask());

            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;
            int result = await testHost.RunOrchestration<int>(typeof (GenerationBasicOrchestration), 4);
            Assert.AreEqual(4, result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostGenerationSubTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (GenerationParentOrchestration), typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask());

            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;
            int result = await testHost.RunOrchestration<int>(typeof (GenerationParentOrchestration), 4);
            Assert.AreEqual(4, result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostGenerationSubFailedTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (GenerationSubFailedParentOrchestration),
                typeof (GenerationSubFailedChildOrchestration));

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            string result =
                await testHost.RunOrchestration<string>(typeof (GenerationSubFailedParentOrchestration), true);
            Assert.AreEqual("Test", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            result = await testHost.RunOrchestration<string>(typeof (GenerationSubFailedParentOrchestration), false);
            Assert.AreEqual("Test", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");
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

        #region Greetings Test

        [TestMethod]
        public async Task TestHostGreetingsTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (GreetingsOrchestration))
                .AddTaskActivities(new GetUserTask(), new SendGreetingTask());

            string result = await testHost.RunOrchestration<string>(typeof (GreetingsOrchestration), null);
            Assert.AreEqual("Greeting send to Gabbar", result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostGreetings2Test()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (GreetingsOrchestration2))
                .AddTaskActivities(new GetUserTask2(() => { testHost.Delay(2*1000).Wait(); }), new SendGreetingTask());

            string result = await testHost.RunOrchestration<string>(typeof (GreetingsOrchestration2), 3);
            Assert.AreEqual("Greeting send to Gabbar", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting send to Gabbar", GreetingsOrchestration2.Result, "Orchestration Result is wrong!!!");

            result = await testHost.RunOrchestration<string>(typeof (GreetingsOrchestration2), 1);
            Assert.AreEqual("Greeting send to TimedOut", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting send to TimedOut", GreetingsOrchestration2.Result,
                "Orchestration Result is wrong!!!");
        }

        public sealed class GetUserTask2 : TaskActivity<string, string>
        {
            readonly Action sleep;

            public GetUserTask2(Action sleep)
            {
                this.sleep = sleep;
                if (this.sleep == null)
                {
                    this.sleep = () => { Thread.Sleep(15*1000); };
                }
            }

            protected override string Execute(TaskContext context, string input)
            {
                sleep();
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
        public async Task TestHostAverageCalculatorTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (AverageCalculatorOrchestration))
                .AddTaskActivities(new ComputeSumTask());

            double result =
                await testHost.RunOrchestration<double>(typeof (AverageCalculatorOrchestration), new[] {1, 50, 10});
            Assert.AreEqual(25, result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostSignalTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (SignalOrchestration))
                .AddTaskActivities(new SendGreetingTask());

            var instance = new OrchestrationInstance {InstanceId = "Test"};
            Task.Factory.StartNew(() =>
            {
                testHost.Delay(2*1000).Wait();
                testHost.RaiseEvent(instance, "GetUser", "Gabbar");
            }
                );
            string result = await testHost.RunOrchestration<string>(instance, typeof (SignalOrchestration), null);
            Assert.AreEqual("Greeting send to Gabbar", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting send to Gabbar", SignalOrchestration.Result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostErrorHandlingTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (ErrorHandlingOrchestration))
                .AddTaskActivities(new GoodTask(), new BadTask(), new CleanupTask());

            string result = await testHost.RunOrchestration<string>(typeof (ErrorHandlingOrchestration), null);
            Assert.AreEqual("CleanupResult", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("CleanupResult", ErrorHandlingOrchestration.Result, "Orchestration Result is wrong!!!");
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
        public async Task TestHostCronTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.ClockSpeedUpFactor = 10;
            testHost.AddTaskOrchestrations(typeof (CronOrchestration))
                .AddTaskActivities(new CronTask(() => { testHost.Delay(2*1000).Wait(); }));

            CronTask.Result = 0;
            string result = await testHost.RunOrchestration<string>(typeof (CronOrchestration), new CronJob
            {
                Frequency = RecurrenceFrequency.Second,
                Count = 5,
                Interval = 3,
            });
            Assert.AreEqual(5, CronTask.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Done", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, CronOrchestration.Result, "Orchestration Result is wrong!!!");
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

                    context.ScheduleTask<string>(typeof (CronTask), attempt);
                }

                Result = i - 1;
                return "Done";
            }
        }

        sealed class CronTask : TaskActivity<string, string>
        {
            public static int Result;
            readonly Action sleep;

            public CronTask(Action sleep)
            {
                this.sleep = sleep;
                if (this.sleep == null)
                {
                    this.sleep = () => { Thread.Sleep(2*1000); };
                }
            }

            protected override string Execute(TaskContext context, string input)
            {
                Result++;
                sleep();

                string completed = "Cron Job '" + input + "' Completed...";
                return completed;
            }
        }

        #endregion

        #region SubOrchestrationInstance Test

        [TestMethod]
        public async Task TestHostSubOrchestrationTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (ParentWorkflow), typeof (ChildWorkflow));

            string result = await testHost.RunOrchestration<string>(typeof (ParentWorkflow), true);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            result = await testHost.RunOrchestration<string>(typeof (ParentWorkflow), false);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");
        }

        public class ChildWorkflow : TaskOrchestration<string, int>
        {
            public override async Task<string> RunTask(OrchestrationContext context, int input)
            {
                return "Child '" + input + "' completed.";
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
        public async Task TestHostSubOrchestrationFailedTest()
        {
            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof (ParentWorkflow2), typeof (ChildWorkflow2));

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            string result = await testHost.RunOrchestration<string>(typeof (ParentWorkflow2), true);
            Assert.AreEqual("Test", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Test", ParentWorkflow2.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, ChildWorkflow2.Count, "Child Workflow Count invalid.");

            ChildWorkflow2.Count = 0;
            ParentWorkflow2.Result = null;
            result = await testHost.RunOrchestration<string>(typeof (ParentWorkflow2), false);
            Assert.AreEqual("Test", result, "Orchestration Result is wrong!!!");
            Assert.AreEqual("Test", ParentWorkflow2.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, ChildWorkflow2.Count, "Child Workflow Count invalid.");
        }

        public class ChildWorkflow2 : TaskOrchestration<string, int>
        {
            public static int Count;

            public override async Task<string> RunTask(OrchestrationContext context, int input)
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
    }
}