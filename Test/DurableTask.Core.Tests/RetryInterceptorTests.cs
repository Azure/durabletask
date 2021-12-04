// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RetryInterceptorTests
    {
        MockOrchestrationContext context;

        [TestInitialize]
        public void Initialize()
        {
            this.context = new MockOrchestrationContext(new OrchestrationInstance(), TaskScheduler.Default);
        }

        [TestMethod]
        public async Task Invoke_WithFailingRetryCall_ShouldThrowCorrectException()
        {
            var interceptor = new RetryInterceptor<object>(this.context, new RetryOptions(TimeSpan.FromMilliseconds(100), 1), () => throw new IOException());
            async Task Invoke() => await interceptor.Invoke();

            await Assert.ThrowsExceptionAsync<IOException>(Invoke, "Interceptor should throw the original exception after exceeding max retry attempts.");
        }

        [TestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(10)]
        public async Task Invoke_WithFailingRetryCall_ShouldHaveCorrectNumberOfCalls(int maxAttempts)
        {
            var callCount = 0;
            var interceptor = new RetryInterceptor<object>(
                this.context,
                new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts),
                () =>
                {
                    callCount++;
                    throw new Exception();
                });
            try
            {
                await interceptor.Invoke();
            }
            catch
            {
                // ignored
            }

            Assert.AreEqual(maxAttempts, callCount, 0, $"There should be {maxAttempts} function calls for {maxAttempts} max attempts.");
        }

        [TestMethod]
        [DataRow(1)]
        [DataRow(2)]
        [DataRow(3)]
        [DataRow(10)]
        public async Task Invoke_WithFailingRetryCall_ShouldHaveCorrectNumberOfSleeps(int maxAttempts)
        {
            var interceptor = new RetryInterceptor<object>(this.context, new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts), () => throw new Exception());
            try
            {
                await interceptor.Invoke();
            }
            catch
            {
                // ignored
            }

            Assert.AreEqual(maxAttempts - 1, this.context.TimerCalls, 0, $"There should be {maxAttempts - 1} sleeps for {maxAttempts} function calls");
        }

        sealed class MockOrchestrationContext : TaskOrchestrationContext
        {
            public MockOrchestrationContext(OrchestrationInstance orchestrationInstance, TaskScheduler taskScheduler)
                : base(orchestrationInstance, taskScheduler)
            {
                CurrentUtcDateTime = DateTime.UtcNow;
            }

            public int TimerCalls { get; private set; }

            public override async Task<T> CreateTimer<T>(DateTime fireAt, T state)
            {
                TimerCalls++;

                await Task.Delay(fireAt - CurrentUtcDateTime);

                return state;
            }
        }
    }
}