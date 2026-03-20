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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Logging;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class WorkItemDispatcherTests
    {
        // A generous-but-bounded timeout for CI. Tests normally complete in <1s;
        // this guards against hangs without wasting time on every run.
        static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);
        [TestMethod]
        public async Task DispatchLoop_SurvivesOuterException_ViaFaultyDelayCallback()
        {
            // Arrange: GetDelayInSecondsAfterOnFetchException is called from inside the
            // inner catch block, but if it throws, the exception escapes the inner
            // catch and is caught by the outer catch — exercising the new safety net.
            // We verify that DispatcherLoopFailed is logged and the loop continues.
            int fetchCallCount = 0;
            var secondFetchStarted = new TaskCompletionSource<bool>();

            var logMessages = new ConcurrentBag<LogEntry>();
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new InMemoryLoggerProvider(logMessages));
                builder.SetMinimumLevel(LogLevel.Trace);
            });

            var dispatcher = new WorkItemDispatcher<string>(
                "TestDispatcher",
                workItemIdentifier: item => item ?? "null",
                fetchWorkItem: (timeout, ct) =>
                {
                    int count = Interlocked.Increment(ref fetchCallCount);
                    if (count == 1)
                    {
                        // This exception will be caught by the inner catch, which then
                        // calls GetDelayInSecondsAfterOnFetchException (which also throws).
                        throw new InvalidOperationException("Trigger inner catch");
                    }

                    // If we reach here, the outer catch handled the callback failure
                    // and the loop continued.
                    secondFetchStarted.TrySetResult(true);
                    return Task.FromResult<string>(null!);
                },
                processWorkItem: item => Task.CompletedTask);

            dispatcher.MaxConcurrentWorkItems = 1;
            dispatcher.DispatcherCount = 1;
            dispatcher.LogHelper = new LogHelper(loggerFactory.CreateLogger("DurableTask.Core"));

            // This callback is invoked from the inner catch. Throwing here causes
            // the exception to escape to the outer catch.
            int delayCallCount = 0;
            dispatcher.GetDelayInSecondsAfterOnFetchException = (ex) =>
            {
                if (Interlocked.Increment(ref delayCallCount) == 1)
                {
                    throw new InvalidOperationException("Failure in delay callback");
                }

                return 0;
            };

            // Act
            await dispatcher.StartAsync();

            // Wait for the loop to recover from the outer catch and start a second fetch.
            // The outer catch backoff is 10s, but may be cancelled on shutdown. We give it
            // enough time for the backoff to elapse. If the loop died, this will time out.
            bool recovered = await Task.WhenAny(
                secondFetchStarted.Task,
                Task.Delay(TimeSpan.FromSeconds(15))) == secondFetchStarted.Task;

            await dispatcher.StopAsync(forced: true);
            dispatcher.Dispose();

            // Assert: The dispatch loop survived the outer exception and continued
            Assert.IsTrue(recovered, "Dispatch loop should have recovered after outer catch exception.");

            // The outer catch should have logged DispatcherLoopFailed
            bool hasLoopFailed = logMessages.Any(m =>
                m.EventId.Name == nameof(Logging.EventIds.DispatcherLoopFailed));
            Assert.IsTrue(hasLoopFailed, "Expected DispatcherLoopFailed log event from the outer catch.");
        }

        [TestMethod]
        public async Task DispatchLoop_SurvivesMultipleExceptionTypes_AndContinuesProcessing()
        {
            // Arrange: Throw a variety of exception types from fetch across consecutive calls.
            // All are handled by the inner catch, but this verifies the loop keeps going.
            int fetchCallCount = 0;
            var recoverySignal = new TaskCompletionSource<bool>();

            var dispatcher = new WorkItemDispatcher<string>(
                "TestDispatcher",
                workItemIdentifier: item => item ?? "null",
                fetchWorkItem: (timeout, ct) =>
                {
                    int count = Interlocked.Increment(ref fetchCallCount);
                    switch (count)
                    {
                        case 1: throw new InvalidOperationException("Test exception");
                        case 2: throw new TimeoutException("Test timeout");
                        case 3: throw new TaskCanceledException("Test cancel");
                        default:
                            if (count >= 5)
                            {
                                recoverySignal.TrySetResult(true);
                            }

                            return Task.FromResult<string>(null!);
                    }
                },
                processWorkItem: item => Task.CompletedTask);

            dispatcher.MaxConcurrentWorkItems = 1;
            dispatcher.DispatcherCount = 1;

            // Act
            await dispatcher.StartAsync();

            bool recovered = await Task.WhenAny(
                recoverySignal.Task,
                Task.Delay(TestTimeout)) == recoverySignal.Task;

            await dispatcher.StopAsync(forced: true);
            dispatcher.Dispose();

            // Assert: The dispatcher survived all exception types and kept fetching
            Assert.IsTrue(recovered, "Dispatch loop should have recovered after multiple exception types.");
            Assert.IsTrue(fetchCallCount >= 5, $"Expected at least 5 fetch calls, got {fetchCallCount}.");
        }

        [TestMethod]
        public async Task DispatchLoop_LogsFetchWorkItemFailure_WhenFetchThrows()
        {
            // Arrange: Use a logging ILogger to capture log events.
            // The fetch exception is handled by the inner catch, which logs
            // FetchWorkItemFailure — verifying that path works correctly.
            var logMessages = new ConcurrentBag<LogEntry>();
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new InMemoryLoggerProvider(logMessages));
                builder.SetMinimumLevel(LogLevel.Trace);
            });

            int fetchCallCount = 0;
            var dispatcherRecovered = new TaskCompletionSource<bool>();

            var dispatcher = new WorkItemDispatcher<string>(
                "TestDispatcher",
                workItemIdentifier: item => item ?? "null",
                fetchWorkItem: (timeout, ct) =>
                {
                    int count = Interlocked.Increment(ref fetchCallCount);
                    if (count == 1)
                    {
                        // This exception is caught by the inner catch block,
                        // which logs FetchWorkItemFailure (not DispatcherLoopFailed).
                        throw new InvalidOperationException("Simulated fetch failure");
                    }

                    if (count >= 3)
                    {
                        dispatcherRecovered.TrySetResult(true);
                    }

                    return Task.FromResult<string>(null!);
                },
                processWorkItem: item => Task.CompletedTask);

            dispatcher.MaxConcurrentWorkItems = 1;
            dispatcher.DispatcherCount = 1;
            dispatcher.LogHelper = new LogHelper(loggerFactory.CreateLogger("DurableTask.Core"));

            // Act
            await dispatcher.StartAsync();

            bool recovered = await Task.WhenAny(
                dispatcherRecovered.Task,
                Task.Delay(TestTimeout)) == dispatcherRecovered.Task;

            await dispatcher.StopAsync(forced: true);
            dispatcher.Dispose();

            // Assert
            Assert.IsTrue(recovered, "Dispatch loop should have recovered after exception.");

            // The inner catch handles InvalidOperationException from fetch,
            // so we verify FetchWorkItemFailure was logged
            bool hasFetchFailure = logMessages.Any(m =>
                m.EventId.Name == nameof(Logging.EventIds.FetchWorkItemFailure));
            Assert.IsTrue(hasFetchFailure, "Expected FetchWorkItemFailure log event.");
        }

        [TestMethod]
        public async Task DispatchLoop_ProcessesWorkItemsSuccessfully()
        {
            // Arrange
            var processedItems = new ConcurrentBag<string>();
            int fetchCallCount = 0;
            var allItemsProcessed = new TaskCompletionSource<bool>();
            string[] workItems = { "item-1", "item-2", "item-3" };

            var dispatcher = new WorkItemDispatcher<string>(
                "TestDispatcher",
                workItemIdentifier: item => item,
                fetchWorkItem: (timeout, ct) =>
                {
                    int count = Interlocked.Increment(ref fetchCallCount);
                    if (count <= workItems.Length)
                    {
                        return Task.FromResult(workItems[count - 1]);
                    }

                    return Task.FromResult<string>(null!);
                },
                processWorkItem: item =>
                {
                    processedItems.Add(item);
                    if (processedItems.Count >= workItems.Length)
                    {
                        allItemsProcessed.TrySetResult(true);
                    }

                    return Task.CompletedTask;
                });

            dispatcher.MaxConcurrentWorkItems = 5;
            dispatcher.DispatcherCount = 1;

            // Act
            await dispatcher.StartAsync();

            bool completed = await Task.WhenAny(
                allItemsProcessed.Task,
                Task.Delay(TestTimeout)) == allItemsProcessed.Task;

            await dispatcher.StopAsync(forced: true);
            dispatcher.Dispose();

            // Assert
            Assert.IsTrue(completed, "All work items should have been processed.");
            CollectionAssert.AreEquivalent(workItems, processedItems.ToArray());
        }

        [TestMethod]
        public async Task DispatchLoop_StopsGracefully_WhenNoExceptions()
        {
            // Arrange
            var logMessages = new ConcurrentBag<LogEntry>();
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddProvider(new InMemoryLoggerProvider(logMessages));
                builder.SetMinimumLevel(LogLevel.Trace);
            });

            // Use a signal so we know the dispatch loop is actively running
            // before we ask it to stop, rather than relying on a fixed delay.
            var fetchStarted = new TaskCompletionSource<bool>();

            var dispatcher = new WorkItemDispatcher<string>(
                "TestDispatcher",
                workItemIdentifier: item => item ?? "null",
                fetchWorkItem: (timeout, ct) =>
                {
                    fetchStarted.TrySetResult(true);
                    return Task.FromResult<string>(null!);
                },
                processWorkItem: item => Task.CompletedTask);

            dispatcher.MaxConcurrentWorkItems = 1;
            dispatcher.DispatcherCount = 1;
            dispatcher.LogHelper = new LogHelper(loggerFactory.CreateLogger("DurableTask.Core"));

            // Act
            await dispatcher.StartAsync();

            // Wait until the dispatch loop has actually started fetching
            await Task.WhenAny(fetchStarted.Task, Task.Delay(TestTimeout));
            Assert.IsTrue(fetchStarted.Task.IsCompleted, "Dispatch loop should have started fetching.");

            await dispatcher.StopAsync(forced: false);

            // The DispatcherStopped event is logged asynchronously after the
            // dispatch loop exits. Poll for it instead of using a fixed delay.
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TestTimeout)
            {
                if (logMessages.Any(m => m.EventId.Name == nameof(Logging.EventIds.DispatcherStopped)))
                {
                    break;
                }

                await Task.Delay(50);
            }

            dispatcher.Dispose();

            // Assert: DispatcherStopped event should be logged, not DispatcherLoopFailed
            bool hasStopped = logMessages.Any(m =>
                m.EventId.Name == nameof(Logging.EventIds.DispatcherStopped));
            bool hasFailed = logMessages.Any(m =>
                m.EventId.Name == nameof(Logging.EventIds.DispatcherLoopFailed));

            Assert.IsTrue(hasStopped, "Expected DispatcherStopped log event on graceful shutdown.");
            Assert.IsFalse(hasFailed, "DispatcherLoopFailed should not be logged during graceful shutdown.");
        }

        [TestMethod]
        public void DispatcherLoopFailed_LogEvent_HasCorrectProperties()
        {
            // Arrange
            var context = new WorkItemDispatcherContext("TestDispatcher", "test-id", "0");
            var exception = new ObjectDisposedException("testSemaphore");

            // Act
            var logEvent = new LogEvents.DispatcherLoopFailed(context, exception);

            // Assert
            Assert.AreEqual(LogLevel.Error, logEvent.Level);
            Assert.AreEqual(Logging.EventIds.DispatcherLoopFailed, logEvent.EventId.Id);
            Assert.AreEqual(nameof(Logging.EventIds.DispatcherLoopFailed), logEvent.EventId.Name);

            string message = ((ILogEvent)logEvent).FormattedMessage;
            Assert.IsTrue(message.Contains("TestDispatcher"), "Message should contain the dispatcher name.");
            Assert.IsTrue(message.Contains("Unhandled exception"), "Message should mention unhandled exception.");
            Assert.IsTrue(message.Contains("testSemaphore"), "Message should contain exception details.");
        }

        [TestMethod]
        public void DispatcherLoopFailed_LogEvent_ExposesStructuredFields()
        {
            // Arrange
            var context = new WorkItemDispatcherContext("ActivityDispatcher", "abc123", "1");
            var exception = new InvalidOperationException("Something went wrong");

            // Act
            var logEvent = new LogEvents.DispatcherLoopFailed(context, exception);

            // Assert: Verify the structured log fields are accessible via the dictionary interface
            var dict = (IReadOnlyDictionary<string, object>)logEvent;
            Assert.IsTrue(dict.ContainsKey("Dispatcher"), "Should have 'Dispatcher' field.");
            Assert.IsTrue(dict.ContainsKey("Details"), "Should have 'Details' field.");

            string dispatcher = (string)dict["Dispatcher"];
            string details = (string)dict["Details"];

            Assert.IsTrue(dispatcher.Contains("ActivityDispatcher"), "Dispatcher field should contain dispatcher name.");
            Assert.IsTrue(details.Contains("Something went wrong"), "Details field should contain exception message.");
        }

        #region Test Helpers

        /// <summary>
        /// A simple log entry captured by the in-memory logger.
        /// </summary>
        class LogEntry
        {
            public LogLevel Level { get; set; }
            public EventId EventId { get; set; }
            public string? Message { get; set; }
            public Exception? Exception { get; set; }
        }

        class NoOpDisposable : IDisposable
        {
            public static readonly NoOpDisposable Instance = new NoOpDisposable();
            public void Dispose() { }
        }

        /// <summary>
        /// An ILogger that captures log entries to a concurrent bag for test assertions.
        /// </summary>
        class InMemoryLogger : ILogger
        {
            readonly ConcurrentBag<LogEntry> logs;

            public InMemoryLogger(ConcurrentBag<LogEntry> logs)
            {
                this.logs = logs;
            }

            public IDisposable BeginScope<TState>(TState state) => NoOpDisposable.Instance;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                this.logs.Add(new LogEntry
                {
                    Level = logLevel,
                    EventId = eventId,
                    Message = formatter(state, exception),
                    Exception = exception,
                });
            }
        }

        /// <summary>
        /// An ILoggerProvider that creates InMemoryLogger instances.
        /// </summary>
        class InMemoryLoggerProvider : ILoggerProvider
        {
            readonly ConcurrentBag<LogEntry> logs;

            public InMemoryLoggerProvider(ConcurrentBag<LogEntry> logs)
            {
                this.logs = logs;
            }

            public ILogger CreateLogger(string categoryName) => new InMemoryLogger(this.logs);

            public void Dispose() { }
        }

        #endregion
    }
}
