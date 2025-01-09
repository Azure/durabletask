// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
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

        [DataTestMethod]
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

        [DataTestMethod]
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

            // Ideally there would be maxAttempts - 1 sleeps. However, a bug in an earlier version of the retryInterceptor
            // resulted in an extra sleep. Unfortunately, "fixing" this bug by removing the extra sleep is a breaking change
            // for existing orchestrations, so we need to ensure the extra sleep continues to be scheduled.
            Assert.AreEqual(maxAttempts, this.context.TimerCalls, 0, $"There should be {maxAttempts} sleeps for {maxAttempts} function calls");
            Assert.AreEqual(TimeSpan.Zero, this.context.Delays.Last());
            Assert.AreEqual(maxAttempts - 1, this.context.Delays.Sum(time => time.Milliseconds), $"The total sleep time should be {maxAttempts} millisecond(s).");
        }

        sealed class MockOrchestrationContext : TaskOrchestrationContext
        {
            readonly List<TimeSpan> delays = new List<TimeSpan>();

            public MockOrchestrationContext(OrchestrationInstance orchestrationInstance, TaskScheduler taskScheduler)
                : base(orchestrationInstance, taskScheduler)
            {
                CurrentUtcDateTime = DateTime.UtcNow;
            }

            public int TimerCalls => this.delays.Count;

            public IReadOnlyList<TimeSpan> Delays => this.delays;

            public override async Task<T> CreateTimer<T>(DateTime fireAt, T state)
            {
                TimeSpan delay = fireAt - CurrentUtcDateTime;
                this.delays.Add(delay);
                await Task.Delay(delay);

                return state;
            }
        }
    }
}