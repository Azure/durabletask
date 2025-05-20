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