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
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    /// <summary>
    /// Tests for shutdown cancellation behavior with extended sessions.
    /// </summary>
    [TestClass]
    public class OrchestrationSessionTests
    {
        /// <summary>
        /// Verifies that <see cref="AsyncAutoResetEvent.WaitAsync(TimeSpan, CancellationToken)"/>
        /// exits immediately when the cancellation token is cancelled.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_CancellationToken_ExitsImmediately()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            TimeSpan longTimeout = TimeSpan.FromSeconds(30);
            Task<bool> waitTask = resetEvent.WaitAsync(longTimeout, cts.Token);

            Assert.IsFalse(waitTask.IsCompleted, "Wait should not complete immediately");

            var stopwatch = Stopwatch.StartNew();
            cts.Cancel();

            bool result = await waitTask;
            stopwatch.Stop();

            Assert.IsFalse(result, "Cancellation should return false (no signal received)");
            Assert.IsTrue(
                stopwatch.ElapsedMilliseconds < 5000,
                $"Cancellation should complete in under 5s, but took {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that signaling still returns true when a cancellation token is provided.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_WithCancellationToken_SignalStillWorks()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            Task<bool> waitTask = resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
            Assert.IsFalse(waitTask.IsCompleted);

            resetEvent.Set();

            Task winner = await Task.WhenAny(waitTask, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.IsTrue(winner == waitTask, "Signal should wake the waiter");
            Assert.IsTrue(waitTask.Result, "Wait result should be true when signaled");
        }

        /// <summary>
        /// Verifies that the wait returns false on timeout when a cancellation token is provided but not cancelled.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_WithCancellationToken_TimeoutStillWorks()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            bool result = await resetEvent.WaitAsync(TimeSpan.FromMilliseconds(100), cts.Token);

            Assert.IsFalse(result, "Wait should return false on timeout");
        }

        /// <summary>
        /// Verifies that all queued waiters return false when the token is cancelled.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_CancellationToken_MultipleWaiters()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: false);
            using var cts = new CancellationTokenSource();

            var waiters = new List<Task<bool>>();
            for (int i = 0; i < 5; i++)
            {
                waiters.Add(resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token));
            }

            foreach (var waiter in waiters)
            {
                Assert.IsFalse(waiter.IsCompleted);
            }

            var stopwatch = Stopwatch.StartNew();
            cts.Cancel();

            foreach (var waiter in waiters)
            {
                bool result = await waiter;
                Assert.IsFalse(result, "Cancelled waiter should return false");
            }

            stopwatch.Stop();

            Assert.IsTrue(
                stopwatch.ElapsedMilliseconds < 5000,
                $"All waiters should complete in under 5s, but took {stopwatch.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that a pre-cancelled token causes WaitAsync to return false immediately.
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
        /// Verifies that a pre-cancelled token still returns true if the event is already signaled.
        /// </summary>
        [TestMethod]
        public async Task WaitAsync_AlreadySignaledAndCancelled_ReturnsTrue()
        {
            var resetEvent = new AsyncAutoResetEvent(signaled: true);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            bool result = await resetEvent.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
            Assert.IsTrue(result, "Already signaled event should return true even with cancelled token");
        }

        /// <summary>
        /// Verifies that <see cref="OrchestrationSessionManager.AbortAllSessions"/> clears all active sessions.
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

            // Use reflection to access the internal sessions dictionary.
            var sessionsField = typeof(OrchestrationSessionManager)
                .GetField("activeOrchestrationSessions", BindingFlags.NonPublic | BindingFlags.Instance);
            var sessions = (Dictionary<string, OrchestrationSession>)sessionsField.GetValue(manager);

            manager.GetStats(out _, out _, out int initialCount);
            Assert.AreEqual(0, initialCount, "Should start with no active sessions");

            sessions["instance1"] = null;
            sessions["instance2"] = null;
            sessions["instance3"] = null;

            manager.GetStats(out _, out _, out int activeCount);
            Assert.AreEqual(3, activeCount, "Should have 3 active sessions");

            manager.AbortAllSessions();

            manager.GetStats(out _, out _, out int afterAbortCount);
            Assert.AreEqual(0, afterAbortCount, "AbortAllSessions should clear all active sessions");
        }

        /// <summary>
        /// Verifies that <see cref="OrchestrationSessionManager.AbortAllSessions"/> is safe to call with no active sessions.
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

            manager.AbortAllSessions();

            manager.GetStats(out _, out _, out int count);
            Assert.AreEqual(0, count, "Should still have no active sessions");
        }
    }
}
