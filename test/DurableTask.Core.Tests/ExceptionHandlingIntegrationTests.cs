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
#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core.Exceptions;
    using DurableTask.Emulator;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class ExceptionHandlingIntegrationTests
    {
        static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 300 : 10);

        readonly TaskHubWorker worker;
        readonly TaskHubClient client;

        public ExceptionHandlingIntegrationTests()
        {
            // configure logging so traces are emitted during tests.
            // This facilitates debugging when tests fail.

            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole().SetMinimumLevel(LogLevel.Trace);
            });

            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service, loggerFactory);
            this.client = new TaskHubClient(service, loggerFactory: loggerFactory);
        }

        [DataTestMethod]
        [DataRow(ErrorPropagationMode.SerializeExceptions)]
        [DataRow(ErrorPropagationMode.UseFailureDetails)]
        public async Task CatchInvalidOperationException(ErrorPropagationMode mode)
        {
            // The error propagation mode must be set before the worker is started
            this.worker.ErrorPropagationMode = mode;

            await this.worker
                .AddTaskOrchestrations(typeof(ExceptionHandlingOrchestration))
                .AddTaskActivities(typeof(ThrowInvalidOperationException))
                .StartAsync();

            // This is required for exceptions to be serialized
            this.worker.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(ExceptionHandlingOrchestration), null);
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, DefaultTimeout);
            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.IsNotNull(state.Output, "The expected error information wasn't found!");

            if (mode == ErrorPropagationMode.SerializeExceptions)
            {
                // The exception should be deserializable
                InvalidOperationException? e = JsonConvert.DeserializeObject<InvalidOperationException>(state.Output);
                Assert.IsNotNull(e);
                Assert.AreEqual("This is a test exception", e!.Message);
            }
            else if (mode == ErrorPropagationMode.UseFailureDetails)
            {
                // The failure details should contain the relevant exception metadata
                FailureDetails? details = JsonConvert.DeserializeObject<FailureDetails>(state.Output);
                Assert.IsNotNull(details);
                Assert.AreEqual(typeof(InvalidOperationException).FullName, details!.ErrorType);
                Assert.IsTrue(details.IsCausedBy<InvalidOperationException>());
                Assert.IsTrue(details.IsCausedBy<Exception>()); // check that base types work too
                Assert.AreEqual("This is a test exception", details.ErrorMessage);
                Assert.IsNotNull(details.StackTrace);

                // The callstack should be in the error details
                string expectedCallstackSubstring = typeof(ThrowInvalidOperationException).FullName!.Replace('+', '.');
                Assert.IsTrue(
                    details.StackTrace!.IndexOf(expectedCallstackSubstring) > 0,
                    $"Expected to find {expectedCallstackSubstring} in the exception details. Actual: {details.StackTrace}");
            }
            else
            {
                Assert.Fail($"Unexpected {nameof(ErrorPropagationMode)} value: {mode}");
            }
        }

        [TestMethod]
        public async Task FailureDetailsOnHandled()
        {
            // The error propagation mode must be set before the worker is started
            this.worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;

            await this.worker
                .AddTaskOrchestrations(typeof(ExceptionHandlingWithRetryOrchestration))
                .AddTaskActivities(typeof(ThrowInvalidOperationException))
                .StartAsync();

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(ExceptionHandlingWithRetryOrchestration), null);
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, DefaultTimeout);
            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.IsNotNull(state.Output, "No output was returned!");

            // The orchestration is written in such a way that there should be only one call into the retry policy
            int retryPolicyInvokedCount = JsonConvert.DeserializeObject<int>(state.Output);
            Assert.AreEqual(1, retryPolicyInvokedCount);
        }

        [DataTestMethod]
        [DataRow(ErrorPropagationMode.SerializeExceptions)]
        [DataRow(ErrorPropagationMode.UseFailureDetails)]
        public async Task FailureDetailsOnUnhandled(ErrorPropagationMode mode)
        {
            // The error propagation mode must be set before the worker is started
            this.worker.ErrorPropagationMode = mode;

            await this.worker
                .AddTaskOrchestrations(typeof(NoExceptionHandlingOrchestration))
                .AddTaskActivities(typeof(ThrowInvalidOperationException))
                .StartAsync();

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                typeof(NoExceptionHandlingOrchestration),
                input: null);
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, DefaultTimeout);
            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);

            string expectedErrorMessage = "This is a test exception";
            if (mode == ErrorPropagationMode.SerializeExceptions)
            {
                // Legacy behavior is to set the output of the orchestration to be the exception message
                Assert.AreEqual(expectedErrorMessage, state.Output);
            }
            else if (mode == ErrorPropagationMode.UseFailureDetails)
            {
                string activityName = typeof(ThrowInvalidOperationException).FullName!;
                string expectedOutput = $"{typeof(TaskFailedException).FullName}: Task '{activityName}' (#0) failed with an unhandled exception: {expectedErrorMessage}";
                Assert.AreEqual(expectedOutput, state.Output);
           }
            else
            {
                Assert.Fail($"Unexpected {nameof(ErrorPropagationMode)} value: {mode}");
            }
        }

        [TestMethod]
        public void TaskFailureOnNullContextTaskActivity()
        {
            TaskActivity activity = new ThrowInvalidOperationExceptionAsync();
            string input = JsonConvert.SerializeObject(new string[] { "test" });

            // Pass a null context to check that it doesn't affect error handling.
            Task<string> task = activity.RunAsync(null, input);

            Assert.IsTrue(task.IsFaulted);
            Assert.IsNotNull(task.Exception);
            Assert.IsNotNull(task.Exception?.InnerException);
            Assert.IsInstanceOfType(task.Exception?.InnerException, typeof(TaskFailureException));
            Assert.AreEqual("This is a test exception", task.Exception?.InnerException?.Message);
        }

        class ExceptionHandlingOrchestration : TaskOrchestration<object, string>
        {
            public override async Task<object> RunTask(OrchestrationContext context, string input)
            {
                try
                {
                    return await context.ScheduleTask<object>(typeof(ThrowInvalidOperationException));
                }
                catch (TaskFailedException e)
                {
                    // Exactly one of these properties should be null
                    return (object)e.FailureDetails! ?? e.InnerException!;
                }
            }
        }

        class ExceptionHandlingWithRetryOrchestration : TaskOrchestration<int, string>
        {
            public override async Task<int> RunTask(OrchestrationContext context, string input)
            {
                int handleCount = 0;
                try
                {
                    await context.ScheduleWithRetry<object>(
                        typeof(ThrowInvalidOperationException),
                        new RetryOptions(TimeSpan.FromMilliseconds(1), maxNumberOfAttempts: 3)
                        {
                            Handle = e =>
                            {
                                handleCount++;

                                // Users should be able to examine the structured exception details when
                                // ErrorPropagationMode is set to UseFailureDetails
                                if (e is TaskFailedException tfe &&
                                    tfe.FailureDetails != null &&
                                    tfe.FailureDetails.ErrorType == typeof(InvalidOperationException).FullName &&
                                    tfe.FailureDetails.ErrorMessage == "This is a test exception" &&
                                    tfe.FailureDetails.StackTrace!.Contains(typeof(ThrowInvalidOperationException).Name) &&
                                    tfe.FailureDetails.IsCausedBy<InvalidOperationException>() &&
                                    tfe.FailureDetails.IsCausedBy<Exception>() &&
                                    tfe.FailureDetails.InnerFailure != null &&
                                    tfe.FailureDetails.InnerFailure.IsCausedBy<CustomException>() &&
                                    tfe.FailureDetails.InnerFailure.ErrorMessage == "And this is its custom inner exception") 
                                {
                                    // Stop retrying
                                    return false;
                                }

                                // Keep retrying
                                return true;
                            }
                        });
                }
                catch (TaskFailedException)
                {
                }

                return handleCount;
            }
        }

        class NoExceptionHandlingOrchestration : TaskOrchestration<object, string>
        {
            public override Task<object> RunTask(OrchestrationContext context, string input)
            {
                // let the exception go unhandled and fail the orchestration
                return context.ScheduleTask<object>(typeof(ThrowInvalidOperationException));
            }
        }

        class ThrowInvalidOperationException : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new InvalidOperationException("This is a test exception",
                    new CustomException("And this is its custom inner exception"));
            }
        }

        class ThrowInvalidOperationExceptionAsync : AsyncTaskActivity<string, string>
        {
            protected override Task<string> ExecuteAsync(TaskContext context, string input)
            {
                throw new InvalidOperationException("This is a test exception",
                    new CustomException("And this is its custom inner exception"));
            }
        }

        [Serializable]
        class CustomException : Exception
        {
            public CustomException(string message)
                : base(message)
            {
            }

            protected CustomException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }
        }
    }
}
