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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    /// <summary>
    /// Tests that validate the shutdown cancellation behavior for extended sessions.
    /// These tests verify fixes for the issue where host shutdown was blocked by the
    /// extended session idle timeout (default 30s) because no cancellation token was
    /// propagated to the session wait, and isForced was ignored during StopAsync.
    /// </summary>
    [TestClass]
    public class OrchestrationSessionTests
    {
        /// <summary>
        /// Verifies that <see cref="AsyncAutoResetEvent.WaitAsync(TimeSpan, CancellationToken)"/>
        /// exits immediately when the cancellation token is cancelled, rather than waiting for
        /// the full timeout. This is the core mechanism behind Bug 1 fix.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_CancellationToken_ExitsImmediately()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            // Start a wait with a long timeout (simulating ExtendedSessionIdleTimeoutInSeconds = 30s)
            TimeSpan longTimeout = TimeSpan.FromSeconds(30);
            Task<bool> waitTask = resetEvent.WaitAsync(longTimeout, cts.Token);

            // The wait should not have completed yet
            Assert.IsFalse(waitTask.IsCompleted, "Wait should not complete immediately");

            // Cancel the token (simulating shutdown)
            var stopwatch = Stopwatch.StartNew();
            cts.Cancel();

            // The wait should return false almost immediately (same as timeout)
            bool result = await waitTask;
            stopwatch.Stop();

            Assert.IsFalse(result, "Cancellation should return false (no signal received)");

            // Verify it completed quickly (well under the 30s timeout)
            Assert.IsTrue(
                stopwatch.ElapsedMilliseconds < 5000,
                $"Cancellation should complete in under 5s, but took {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that the wait still works normally (returns true) when signaled,
        /// even when a cancellation token is provided.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_WithCancellationToken_SignalStillWorks()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            Task<bool> waitTask = resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
            Assert.IsFalse(waitTask.IsCompleted);

            // Signal the event (simulating new messages arriving)
            resetEvent.Set();

            // Wait for the task with a reasonable timeout
            Task winner = await Task.WhenAny(waitTask, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.IsTrue(winner == waitTask, "Signal should wake the waiter");
            Assert.IsTrue(waitTask.Result, "Wait result should be true when signaled");
        }

        /// <summary>
        /// Verifies that the wait returns false on timeout even when a cancellation
        /// token is provided but not cancelled (normal idle timeout behavior).
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_WithCancellationToken_TimeoutStillWorks()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            // Use a short timeout to keep the test fast
            bool result = await resetEvent.WaitAsync(TimeSpan.FromMilliseconds(100), cts.Token);

            Assert.IsFalse(result, "Wait should return false on timeout");
        }

        /// <summary>
        /// Verifies that cancellation works when multiple waiters are queued.
        /// All waiters should return false when the token fires.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_CancellationToken_MultipleWaiters()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            // Queue multiple waiters (simulating multiple sessions waiting during shutdown)
            var waiters = new List<Task<bool>>();
            for (int i = 0; i < 5; i++)
            {
                waiters.Add(resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token));
            }

            // None should be completed yet
            foreach (var waiter in waiters)
            {
                Assert.IsFalse(waiter.IsCompleted);
            }

            // Cancel simulating shutdown
            var stopwatch = Stopwatch.StartNew();
            cts.Cancel();

            // All waiters should return false (cancelled = not signaled)
            await Task.WhenAll(
                waiters.Select(
                    async waiter =>
                    {
                        bool result = await waiter;
                        Assert.IsFalse(result, "Cancelled waiter should return false");
                    }));

            stopwatch.Stop();

            // All should complete quickly
            Assert.IsTrue(
                stopwatch.ElapsedMilliseconds < 5000,
                $"All waiters should complete in under 5s, but took {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that pre-cancelled tokens cause WaitAsync to return false immediately,
        /// matching the behavior during shutdown if the token is cancelled before
        /// FetchNewOrchestrationMessagesAsync is called.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_AlreadyCancelledToken_ReturnsFalseImmediately()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Pre-cancel

            var stopwatch = Stopwatch.StartNew();
            bool result = await resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
            stopwatch.Stop();

            Assert.IsFalse(result, "Pre-cancelled token should cause immediate false return");
            Assert.IsTrue(
                stopwatch.ElapsedMilliseconds < 5000,
                $"Should complete immediately, but took {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that a pre-cancelled token still returns true if the event
        /// is already signaled (messages were already queued before shutdown).
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_AlreadySignaledAndCancelled_ReturnsTrue()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: true);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            // Even with a cancelled token, if the event is already signaled,
            // WaitAsync should return true immediately without blocking
            bool result = await resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
            Assert.IsTrue(result, "Already signaled event should return true even with cancelled token");
        }

        /// <summary>
        /// Verifies that <see cref="OrchestrationSessionManager.AbortAllSessions"/> clears
        /// all active sessions, causing <see cref="OrchestrationSessionManager.IsControlQueueProcessingMessages"/>
        /// to return false. This is the core mechanism behind Bug 2 fix (isForced support).
        /// </summary>
        [TestMethod]
        public void AbortAllSessions_ClearsActiveSessions()
        {
            var settings = new AzureStorageOrchestrationServiceSettings();
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            // Use reflection to add fake sessions to the internal dictionary, since constructing
            // a real OrchestrationSession requires Azure infrastructure (ControlQueue)
            var sessionsField = typeof(OrchestrationSessionManager)
                .GetField("activeOrchestrationSessions", BindingFlags.NonPublic | BindingFlags.Instance);
            var sessions = (Dictionary<string, OrchestrationSession>)sessionsField.GetValue(manager);

            // Add null entries to simulate active sessions (we only need the dictionary to be non-empty
            // to test that AbortAllSessions clears it and IsControlQueueProcessingMessages returns false)
            // Use the internal dictionary directly with a partition check
            manager.GetStats(out _, out _, out int initialCount);
            Assert.AreEqual(0, initialCount, "Should start with no active sessions");

            // Manually insert entries to simulate active sessions
            sessions["instance1"] = null;
            sessions["instance2"] = null;
            sessions["instance3"] = null;

            manager.GetStats(out _, out _, out int activeCount);
            Assert.AreEqual(3, activeCount, "Should have 3 active sessions");

            // Act: simulate forced shutdown by aborting all sessions
            manager.AbortAllSessions();

            // Assert: all sessions should be cleared
            manager.GetStats(out _, out _, out int afterAbortCount);
            Assert.AreEqual(0, afterAbortCount, "AbortAllSessions should clear all active sessions");
        }

        /// <summary>
        /// Verifies that <see cref="OrchestrationSessionManager.AbortAllSessions"/> is safe to call
        /// when there are no active sessions (idempotent).
        /// </summary>
        [TestMethod]
        public void AbortAllSessions_NoSessions_DoesNotThrow()
        {
            var settings = new AzureStorageOrchestrationServiceSettings();
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            // Should not throw when called with no active sessions
            manager.AbortAllSessions();

            manager.GetStats(out _, out _, out int count);
            Assert.AreEqual(0, count, "Should still have no active sessions");
        }
    }
}
