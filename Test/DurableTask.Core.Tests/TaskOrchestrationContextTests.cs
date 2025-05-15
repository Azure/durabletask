// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    [TestClass]
    public class TaskOrchestrationContextTests
    {
        private MockTaskOrchestrationContext context;
        private OrchestrationInstance instance;

        [TestInitialize]
        public void Initialize()
        {
            instance = new OrchestrationInstance { InstanceId = "TestInstance", ExecutionId = Guid.NewGuid().ToString() };
            context = new MockTaskOrchestrationContext(instance, TaskScheduler.Default);
        }

        [TestMethod]
        public async Task ScheduleTask_Basic_ShouldScheduleTask()
        {
            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", 10, 20);
            Assert.IsFalse(resultTask.IsCompleted);

            // Verify task was scheduled
            Assert.AreEqual(1, context.ScheduledTasks.Count);
            ScheduledTaskInfo scheduledTask = context.ScheduledTasks[0];
            Assert.AreEqual("TestActivity", scheduledTask.Name);
            Assert.AreEqual("1.0", scheduledTask.Version);
            CollectionAssert.AreEqual(new object[] { 10, 20 }, scheduledTask.Parameters);

            // Complete the task and verify result
            context.CompleteTask<int>(0, 30);
            int result = await resultTask;
            Assert.AreEqual(30, result);
        }

        [TestMethod]
        public async Task ScheduleTask_WithNullOptions_ShouldScheduleTask()
        {
            // Act
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder().Build();
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);
            Assert.IsFalse(resultTask.IsCompleted);

            // Verify task was scheduled
            Assert.AreEqual(1, context.ScheduledTasks.Count);
            ScheduledTaskInfo scheduledTask = context.ScheduledTasks[0];
            Assert.AreEqual("TestActivity", scheduledTask.Name);
            Assert.AreEqual("1.0", scheduledTask.Version);
            CollectionAssert.AreEqual(new object[] { 10, 20 }, scheduledTask.Parameters);
            Assert.IsNull(scheduledTask.Options.Tags);
            Assert.IsNull(scheduledTask.Options.RetryOptions);

            // Complete the task and verify result
            context.CompleteTask<int>(0, 30);
            int result = await resultTask;
            Assert.AreEqual(30, result);
        }

        [TestMethod]
        public async Task ScheduleTask_WithTags_ShouldPassTags()
        {
            // Arrange
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .AddTag("key1", "value1")
                .AddTag("key2", "value2")
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Verify task was scheduled with tags
            Assert.AreEqual(1, context.ScheduledTasks.Count);
            ScheduledTaskInfo scheduledTask = context.ScheduledTasks[0];

            Assert.IsNotNull(scheduledTask.Options.Tags);
            Assert.AreEqual(2, scheduledTask.Options.Tags.Count);
            Assert.AreEqual("value1", scheduledTask.Options.Tags["key1"]);
            Assert.AreEqual("value2", scheduledTask.Options.Tags["key2"]);

            // Complete the task
            context.CompleteTask<int>(0, 30);
            await resultTask;
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryOptions_ShouldRetryOnFailure()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1000), 3);
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Fail the first attempt
            context.FailTask(0, new InvalidOperationException("First failure"));

            // Verify that a retry was scheduled
            Assert.AreEqual(3, context.ScheduledTasks.Count);

            // Fail the second attempt
            context.FailTask(1, new InvalidOperationException("Second failure"));

            // Verify that another retry was scheduled
            Assert.AreEqual(4, context.ScheduledTasks.Count);

            // Complete the third attempt successfully
            context.CompleteTask<int>(2, 30);

            // Verify result
            int result = await resultTask;
            Assert.AreEqual(30, result);

            // Verify all tasks had the same name, version, and parameters
            foreach (ScheduledTaskInfo task in context.ScheduledTasks)
            {
                Assert.AreEqual("TestActivity", task.Name);
                Assert.AreEqual("1.0", task.Version);
                CollectionAssert.AreEqual(new object[] { 10, 20 }, task.Parameters);
            }
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryOptions_ShouldRespectMaxRetries()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1), 2);
            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Fail all attempts
            context.FailTask(0, new InvalidOperationException("First failure"));
            context.FailTask(1, new InvalidOperationException("Second failure"));

            // Verify that no more retries are scheduled after max retries
            Assert.AreEqual(3, context.ScheduledTasks.Count);

            // Verify the task failed
            try
            {
                await resultTask;
                Assert.Fail("Task should have failed");
            }
            catch (TaskFailedException ex)
            {
                Assert.IsInstanceOfType(ex.InnerException, typeof(InvalidOperationException));
                Assert.AreEqual("Second failure", ex.InnerException.Message);
            }
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryOptions_ShouldRespectRetryHandler()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(1), 3)
            {
                Handle = ex => ex is ArgumentException // Only retry ArgumentException
            };

            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Fail with exception that should be retried
            context.FailTask(0, new ArgumentException("Should retry"));

            // Verify that a retry was scheduled
            Assert.AreEqual(2, context.ScheduledTasks.Count);

            // Fail with exception that should NOT be retried
            context.FailTask(1, new InvalidOperationException("Should not retry"));

            // Verify that no more retries are scheduled - but expect dummy timer for backwards compatibility
            Assert.AreEqual(2, context.ScheduledTasks.Count);

            // Verify the task failed with the expected exception
            try
            {
                await resultTask;
                Assert.Fail("Task should have failed");
            }
            catch (TaskFailedException ex)
            {
                // Since we're using RetryInterceptor, the last exception is what gets propagated
                // which would be the ArgumentException from the first failure
                Assert.IsInstanceOfType(ex.InnerException, typeof(ArgumentException));
                Assert.AreEqual("Should retry", ex.InnerException.Message);
            }
        }

        [TestMethod]
        public async Task ScheduleTask_WithDefaultReturnValue_ShouldHandleNullResult()
        {
            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", 10, 20);

            // Complete the task with null result
            context.CompleteTaskWithNullResult(0);

            // Verify default value is returned
            int result = await resultTask;
            Assert.AreEqual(0, result); // default for int is 0
        }

        [TestMethod]
        public async Task ScheduleTask_WithReferenceType_ShouldHandleNullResult()
        {
            // Act
            Task<string> resultTask = context.ScheduleTask<string>("TestActivity", "1.0", 10, 20);

            // Complete the task with null result
            context.CompleteTaskWithNullResult(0);

            // Verify null is returned
            string result = await resultTask;
            Assert.IsNull(result); // default for reference type is null
        }

        [TestMethod]
        public async Task ScheduleTask_WithFailure_ShouldPropagateException()
        {
            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", 10, 20);

            // Fail the task
            InvalidOperationException expectedException = new InvalidOperationException("Expected failure");
            context.FailTask(0, expectedException);

            // Verify exception is propagated
            try
            {
                await resultTask;
                Assert.Fail("Task should have failed");
            }
            catch (TaskFailedException ex)
            {
                Assert.AreEqual("TestActivity", ex.Name);
                Assert.AreEqual("1.0", ex.Version);
                Assert.IsInstanceOfType(ex.InnerException, typeof(InvalidOperationException));
                Assert.AreEqual("Expected failure", ex.InnerException.Message);
            }
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryBackoff_ShouldApplyBackoffPolicy()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(10), 3)
            {
                BackoffCoefficient = 2.0, // Double the interval each time
                MaxRetryInterval = TimeSpan.FromSeconds(1)
            };

            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Fail the first two attempts
            context.FailTask(0, new InvalidOperationException("First failure"));
            context.FailTask(1, new InvalidOperationException("Second failure"));

            // Complete the third attempt
            context.CompleteTask<int>(2, 30);

            // Verify the timer delays
            Assert.AreEqual(2, context.Delays.Count);
            // First retry should be at the initial interval
            Assert.AreEqual(TimeSpan.FromMilliseconds(10), context.Delays[0]);
            // Second retry should be at double the interval
            Assert.AreEqual(TimeSpan.FromMilliseconds(20), context.Delays[1]);

            // Verify result
            int result = await resultTask;
            Assert.AreEqual(30, result);
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryAndMaxRetryInterval_ShouldCapRetryInterval()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(10), 4)
            {
                BackoffCoefficient = 10.0, // 10x the interval each time
                MaxRetryInterval = TimeSpan.FromMilliseconds(50) // Cap at 50ms
            };

            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Fail the first three attempts
            context.FailTask(0, new Exception("Failure 1"));
            context.FailTask(1, new Exception("Failure 2"));
            context.FailTask(2, new Exception("Failure 3"));

            // Complete the fourth attempt
            context.CompleteTask<int>(3, 30);

            // Verify the timer delays
            Assert.AreEqual(3, context.Delays.Count);
            Assert.AreEqual(TimeSpan.FromMilliseconds(10), context.Delays[0]); // First retry: 10ms
            Assert.AreEqual(TimeSpan.FromMilliseconds(50), context.Delays[1]); // Second retry: capped at 50ms (would be 100ms)
            Assert.AreEqual(TimeSpan.FromMilliseconds(50), context.Delays[2]); // Third retry: capped at 50ms (would be 1000ms)

            int result = await resultTask;
            Assert.AreEqual(30, result);
        }

        [TestMethod]
        public async Task ScheduleTask_WithRetryAndRetryTimeout_ShouldFailAfterTimeout()
        {
            // Arrange
            RetryOptions retryOptions = new RetryOptions(TimeSpan.FromMilliseconds(200), 10) // Allow up to 10 retries
            {
                RetryTimeout = TimeSpan.FromMilliseconds(250) // But timeout after 250ms total
            };

            ScheduleTaskOptions options = ScheduleTaskOptions.CreateBuilder()
                .WithRetryOptions(retryOptions)
                .Build();

            // Act - start a task that we'll retry
            Task<int> resultTask = context.ScheduleTask<int>("TestActivity", "1.0", options, 10, 20);

            // Simulate the first failure immediately
            context.FailTask(0, new Exception("First failure"));

            // Verify we attempted a retry
            Assert.AreEqual(3, context.ScheduledTasks.Count);

            // Now advance the time beyond the timeout
            context.CurrentUtcDateTime = context.CurrentUtcDateTime.AddMilliseconds(300);

            // Fail the second attempt (this should now exceed the retry timeout)
            context.FailTask(1, new Exception("Second failure, after timeout"));

            // Verify the number of scheduled tasks
            // We expect 3 tasks because:
            // 1. Original task
            // 2. First retry
            // 3. RetryInterceptor schedules a dummy immediate timer for backward compatibility
            Assert.AreEqual(3, context.ScheduledTasks.Count);

            // Verify the task failed
            try
            {
                await resultTask;
                Assert.Fail("Task should have failed");
            }
            catch (TaskFailedException ex)
            {
                Assert.IsInstanceOfType(ex.InnerException, typeof(Exception));
                Assert.AreEqual("Second failure, after timeout", ex.InnerException.Message);
            }
        }

        private class MockTaskOrchestrationContext : TaskOrchestrationContext
        {
            public List<ScheduledTaskInfo> ScheduledTasks { get; } = new List<ScheduledTaskInfo>();
            public List<TimeSpan> Delays { get; } = new List<TimeSpan>();

            public MockTaskOrchestrationContext(OrchestrationInstance orchestrationInstance, TaskScheduler taskScheduler)
                : base(orchestrationInstance, taskScheduler)
            {
                CurrentUtcDateTime = DateTime.UtcNow;
            }

            public void CompleteTask<T>(int taskIndex, T result)
            {
                string serializedResult = MessageDataConverter.SerializeInternal(result);
                TaskCompletedEvent taskCompletedEvent = new TaskCompletedEvent(0, taskIndex, serializedResult);
                HandleTaskCompletedEvent(taskCompletedEvent);
            }

            public void CompleteTaskWithNullResult(int taskIndex)
            {
                TaskCompletedEvent taskCompletedEvent = new TaskCompletedEvent(0, taskIndex, null);
                HandleTaskCompletedEvent(taskCompletedEvent);
            }

            public void FailTask(int taskIndex, Exception exception)
            {
                string details = ErrorDataConverter.SerializeInternal(exception);
                TaskFailedEvent taskFailedEvent = new TaskFailedEvent(0, taskIndex, exception.Message, details);
                HandleTaskFailedEvent(taskFailedEvent);
            }

            public override async Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
            {
                TimeSpan delay = fireAt - CurrentUtcDateTime;
                Delays.Add(delay);

                CurrentUtcDateTime = fireAt; // Advance the time

                return await Task.FromResult(state);
            }

            public override async Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters)
            {
                ScheduledTasks.Add(new ScheduledTaskInfo
                {
                    Name = name,
                    Version = version,
                    Parameters = parameters,
                    Options = null
                });

                // This just sets up the infrastructure needed for completing the task
                return await base.ScheduleTask<TResult>(name, version, parameters);
            }

            public override async Task<TResult> ScheduleTask<TResult>(string name, string version, ScheduleTaskOptions options, params object[] parameters)
            {
                ScheduledTasks.Add(new ScheduledTaskInfo
                {
                    Name = name,
                    Version = version,
                    Parameters = parameters,
                    Options = options
                });

                // This will go through TaskOrchestrationContext's implementation, which handles retries
                return await base.ScheduleTask<TResult>(name, version, options, parameters);
            }
        }

        private class ScheduledTaskInfo
        {
            public string Name { get; set; }
            public string Version { get; set; }
            public object[] Parameters { get; set; }
            public ScheduleTaskOptions Options { get; set; }
        }
    }
}