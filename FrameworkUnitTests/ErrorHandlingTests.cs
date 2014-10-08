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
    using System.Threading.Tasks;
    using DurableTask;
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
            taskHub.CreateHub();

            taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            taskHub.Stop(true);
            taskHubNoCompression.Stop(true);
            taskHub.DeleteHub();
        }

        #region Retry Interceptor Tests

        static readonly string RETRY_PARENT_NAME = "ParentOrchestration";
        static readonly string RETRY_PARENT_VERSION = string.Empty;
        static readonly string RETRY_NAME = "RetryOrchestration";
        static readonly string RETRY_VERSION = string.Empty;
        static readonly string DO_WORK_NAME = "DoWork";
        static readonly string DO_WORK_VERSION = string.Empty;

        [TestMethod]
        public void BasicRetryTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 4);
            var retryTask = new RetryTask(3);

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 120));
            Assert.AreEqual("DoWork Succeeded. Attempts: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void BasicRetryFailTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void BasicRetryFailNoCompressionTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);

            taskHubNoCompression.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME,
                RETRY_VERSION, () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 3", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void RetryCustomHandlerFailThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryCustomHandlerFailTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is ArgumentNullException;
            };

            var retryTask = new RetryTask(2);
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 1", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryCustomHandlerPassThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryCustomHandlerPassTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.Handle = e =>
            {
                Assert.IsInstanceOfType(e, typeof (TaskFailedException), "Exception is not TaskFailedException.");
                var taskFailed = (TaskFailedException) e;

                return taskFailed.InnerException is InvalidOperationException;
            };

            var retryTask = new RetryTask(2);
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryOnReasonCustomHandlerThroughProxyTest()
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
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryOnReasonCustomHandlerTest()
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
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryTimeoutThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public void RetryTimeoutTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(5), 10);
            retryOptions.BackoffCoefficient = 2;
            retryOptions.RetryTimeout = TimeSpan.FromSeconds(10);

            var retryTask = new RetryTask(3);
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.IsTrue(RetryOrchestration.Result.StartsWith("DoWork Failed. RetryCount is:"),
                "Orchestration Result is wrong!!!. Result: " + RetryOrchestration.Result);
            Assert.IsTrue(retryTask.RetryCount < 4, "Retry Count is wrong. Count: " + retryTask.RetryCount);
        }

        [TestMethod]
        public void RetryMaxIntervalThroughProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, true);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void RetryMaxIntervalTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            retryOptions.BackoffCoefficient = 10;
            retryOptions.MaxRetryInterval = TimeSpan.FromSeconds(5);

            var retryTask = new RetryTask(2);
            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_NAME, RETRY_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 2", RetryOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual(2, retryTask.RetryCount, "Retry Count is wrong");
        }

        [TestMethod]
        public void BasicSubOrchestrationRetryTest()
        {
            var parentRetryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 2)
            {
                BackoffCoefficient = 2.0,
                MaxRetryInterval = TimeSpan.FromSeconds(4),
            };
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(4);
            RetryOrchestration.rethrowException = true;

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_PARENT_NAME, RETRY_PARENT_VERSION,
                    () => new ParentOrchestration(parentRetryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            ParentOrchestration.Result = null;
            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_PARENT_NAME, RETRY_PARENT_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Succeeded. Attempts: 4", ParentOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public void SubOrchestrationRetryExhaustedTest()
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

            taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_PARENT_NAME, RETRY_PARENT_VERSION,
                    () => new ParentOrchestration(parentRetryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .Start();

            ParentOrchestration.Result = null;
            RetryOrchestration.Result = null;
            OrchestrationInstance id = client.CreateOrchestrationInstance(RETRY_PARENT_NAME, RETRY_PARENT_VERSION, false);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 90));
            Assert.AreEqual("DoWork Failed. RetryCount is: 4", ParentOrchestration.Result,
                "Orchestration Result is wrong!!!");
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