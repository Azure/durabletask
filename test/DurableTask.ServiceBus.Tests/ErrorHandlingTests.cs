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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Tests;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ErrorHandlingTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;
        TaskHubWorker taskHubNoCompression;

        [TestInitialize]
        public void TestInitialize()
        {
            client = TestHelpers.CreateTaskHubClient();

            taskHub = TestHelpers.CreateTaskHub();

            taskHub.orchestrationService.CreateAsync(true).Wait();

            taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            taskHub.StopAsync(true).Wait();
            taskHubNoCompression.StopAsync(true).Wait();
            taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        #region Retry Interceptor Tests

        static readonly string RETRY_PARENT_NAME = "ParentOrchestration";
        static readonly string RETRY_PARENT_VERSION = string.Empty;
        static readonly string RETRY_NAME = "RetryOrchestration";
        static readonly string RETRY_VERSION = string.Empty;
        static readonly string DO_WORK_NAME = "DoWork";
        static readonly string DO_WORK_VERSION = string.Empty;

        [TestMethod]
        public async Task BasicRetryTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 4);
            var retryTask = new RetryTask(3);

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 120));
            Assert.AreEqual("DoWork Succeeded. Attempts: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task BasicRetryFailTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task BasicRetryFailNoCompressionTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);

            await taskHubNoCompression.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME,
                RETRY_VERSION, () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task RetryCustomHandlerFailThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryCustomHandlerFailTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryCustomHandlerPassThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryCustomHandlerPassTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryOnReasonCustomHandlerThroughProxyTest()
        {
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
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryOnReasonCustomHandlerTest()
        {
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
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryTimeoutThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public async Task RetryTimeoutTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public async Task RetryMaxIntervalThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task RetryMaxIntervalTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public async Task BasicSubOrchestrationRetryTest()
        {
            var parentRetryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 2)
            {
                BackoffCoefficient = 2.0,
                MaxRetryInterval = TimeSpan.FromSeconds(4),
            };
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(4);
            RetryOrchestration.rethrowException = true;

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_PARENT_NAME, RETRY_PARENT_VERSION,
                    () => new ParentOrchestration(parentRetryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            ParentOrchestration.Result = null;
            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_PARENT_NAME, RETRY_PARENT_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 4", ParentOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task SubOrchestrationRetryExhaustedTest()
        {
            ArgumentException argumentException = null;
            try
            {
                new RetryOptions(TimeSpan.Zero, 10);
            }
            catch (ArgumentException ex)
            {
                argumentException = ex;
            }

            Assert.IsNotNull(argumentException);
            Assert.AreEqual(
                "Invalid interval.  Specify a TimeSpan value greater then TimeSpan.Zero.\r\nParameter name: firstRetryInterval",
                argumentException.Message);

            var parentRetryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 2)
            {
                BackoffCoefficient = 2.0,
                MaxRetryInterval = TimeSpan.FromSeconds(4),
            };
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 2);
            var retryTask = new RetryTask(4);
            RetryOrchestration.rethrowException = true;

            await taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_PARENT_NAME, RETRY_PARENT_VERSION,
                    () => new ParentOrchestration(parentRetryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            ParentOrchestration.Result = null;
            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(RETRY_PARENT_NAME, RETRY_PARENT_VERSION, false);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 4", ParentOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [Ignore]
        [TestMethod]
        // Disabled until bug https://github.com/Azure/durabletask/issues/47 is fixed
        // Also the test does not work as expected due to debug mode supressing UnobservedTaskException's
        public async Task ParallelInterfaceExceptionsTest()
        {
            var failureClient = new FailureClient();
            bool unobservedTaskExceptionThrown = false;

            TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
            {
                Task t = (Task)sender;
                string message = $"id:{t.Id}; {sender.GetType()}; {t.AsyncState}; {t.Status}";
                Trace.TraceError($"UnobservedTaskException caught: {message}");

                eventArgs.SetObserved();
                unobservedTaskExceptionThrown = true;
            };

            await taskHub
                .AddTaskOrchestrations(typeof(FailureClientOrchestration))
                .AddTaskActivitiesFromInterface<IFailureClient>(failureClient)
                .StartAsync();

            ParentOrchestration.Result = null;
            RetryOrchestration.Result = null;
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(FailureClientOrchestration), "test");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.IsFalse(unobservedTaskExceptionThrown, "UnobservedTaskException should not be thrown");
        }

        public interface IFailureClient
        {
            Task<IEnumerable<string>> GetValues(bool fail);
        }

        public class FailureClient : IFailureClient
        {
            public async Task<IEnumerable<string>> GetValues(bool fail)
            {
                // If we going to fail, let's do so faster than the 'success' path
                await Task.Delay(fail ? 1000 : 5000);
                if (fail)
                {
                    throw new Exception("getvalues failed");
                }

                // We are in the same process so let's force GC collection to check for an unobserved task
                GC.Collect();
                GC.WaitForPendingFinalizers();

                return new List<string>() { "test" };
            }
        }

        class FailureClientOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string value)
            {
                IFailureClient client = context.CreateClient<IFailureClient>(false);
                string result = value;
                IEnumerable<string>[] completedTasks;
                try
                {
                    Task<IEnumerable<string>> t1 = client.GetValues(false);
                    // This is the task that gets thrown as a unhandled task exception when it gets created during replay (aka context.isReplaying = true)
                    Task<IEnumerable<string>> t2 = client.GetValues(true);
                    completedTasks = await Task.WhenAll(t1, t2);
                }
                catch (Exception ex)
                {
                    return ex.Message;
                }

                IEnumerable<string> allResults = completedTasks.SelectMany(t => t);

                return string.Join(",", allResults);
            }
        }

        public interface IRetryTask
        {
            string DoWork();
        }

        public interface IRetryTaskClient
        {
            Task<string> DoWork();
        }

        sealed class ParentOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            readonly RetryOptions retryPolicy;

            public ParentOrchestration(RetryOptions retryOptions)
            {
                retryPolicy = retryOptions;
            }

            public override async Task<string> RunTask(OrchestrationContext context, bool useTypedClient)
            {
                string result;
                try
                {
                    result =
                        await
                            context.CreateSubOrchestrationInstanceWithRetry<string>(RETRY_NAME, RETRY_VERSION,
                                retryPolicy, useTypedClient);
                }
                catch (SubOrchestrationFailedException ex)
                {
                    result = ex.Message;
                }

                Result = result;
                return result;
            }
        }

        sealed class RetryOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            public static bool rethrowException;
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
                    if (rethrowException)
                    {
                        throw e;
                    }
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
    }
}