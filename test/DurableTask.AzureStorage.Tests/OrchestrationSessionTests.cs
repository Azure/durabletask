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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Queues;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
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

            // All waiters should return false (cancelled = not signaled)
            await Task.WhenAll(
                waiters.Select(
                    async waiter =>
                    {
                        bool result = await waiter;
                        Assert.IsFalse(result, "Cancelled waiter should return false");
                    }));

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

        [TestMethod]
        public async Task GetNextSessionAsync_DrainedReadyQueueNode_ReturnsNullWhenNoQueuesRemain()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);

            object pendingBatch = CreatePendingBatch(controlQueue);
            object node = AddPendingBatchNode(manager, pendingBatch);
            RemovePendingBatchNode(manager, node);
            EnqueueReadyForProcessingNode(manager, node);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            OrchestrationSession session = await manager.GetNextSessionAsync(entitiesOnly: false, cts.Token);

            Assert.IsNull(session, "Detached ready nodes should not block dispatch when no queues remain.");
        }

        [TestMethod]
        public async Task ScheduleOrchestrationStatePrefetch_DetachedNode_DoesNotFetchHistory()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var stats = new AzureStorageOrchestrationServiceStats();
            int fetchCount = 0;
            var trackingStore = new Mock<ITrackingStore>();
            trackingStore
                .Setup(t => t.GetHistoryEventsAsync("instance1", "execution1", It.IsAny<CancellationToken>()))
                .Callback(() => fetchCount++)
                .ThrowsAsync(new OperationCanceledException());

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);

            object pendingBatch = CreatePendingBatch(controlQueue);
            object node = AddPendingBatchNode(manager, pendingBatch);
            RemovePendingBatchNode(manager, node);

            await InvokeScheduleOrchestrationStatePrefetch(manager, node, CancellationToken.None);

            Assert.AreEqual(0, fetchCount, "Detached pending batches should not fetch orchestration history.");
        }

        [TestMethod]
        public void AddMessageToPendingOrchestration_ReleasedControlQueue_ReturnsMessagesToAbandon()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);
            controlQueue.Release(null, "test");

            MessageData message = CreateMessageData();
            MethodInfo addMessage = typeof(OrchestrationSessionManager).GetMethod(
                "AddMessageToPendingOrchestration",
                BindingFlags.Instance | BindingFlags.NonPublic);

            var messagesToAbandon = (IReadOnlyList<MessageData>)addMessage.Invoke(
                manager,
                new object[] { controlQueue, new[] { message }, Guid.NewGuid(), CancellationToken.None });

            Assert.IsNotNull(messagesToAbandon, "Released queue messages should be returned for immediate abandon.");
            Assert.AreEqual(1, messagesToAbandon.Count);
            Assert.AreSame(message, messagesToAbandon[0]);

            manager.GetStats(out int pendingOrchestratorInstances, out _, out _);
            Assert.AreEqual(0, pendingOrchestratorInstances, "Released queue messages should not be added to pending batches.");
        }

        [TestMethod]
        public async Task WaitForDequeueLoopToStopAsync_FaultedDequeueLoop_PropagatesUnexpectedException()
        {
            var settings = new AzureStorageOrchestrationServiceSettings();
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var dequeueLoopTasks = (ConcurrentDictionary<string, Task>)GetPrivateField(manager, "dequeueLoopTasks");
            var expected = new InvalidOperationException("unexpected dequeue loop failure");
            dequeueLoopTasks["partition-0"] = Task.FromException(expected);

            MethodInfo wait = typeof(OrchestrationSessionManager).GetMethod(
                "WaitForDequeueLoopToStopAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);

            Task waitTask = (Task)wait.Invoke(manager, new object[] { "partition-0", CancellationToken.None });

            InvalidOperationException actual = await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => waitTask);

            Assert.AreSame(expected, actual);
        }

        [TestMethod]
        public async Task AbandonMessageForDrainAsync_DurableTaskStorageException_DoesNotThrow()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);

            var queueClient = new Mock<QueueClient>();
            queueClient.SetupGet(q => q.Name).Returns("partition-0");
            queueClient
                .Setup(
                    q => q.UpdateMessageAsync(
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<TimeSpan>(),
                        It.IsAny<CancellationToken>()))
                .ThrowsAsync(new RequestFailedException(404, "queue update failed"));
            SetPrivateField(controlQueue.InnerQueue, "queueClient", queueClient.Object);

            await controlQueue.AbandonMessageForDrainAsync(CreateMessageData());
        }

        [TestMethod]
        public async Task GetNextSessionAsync_ReleasedDelayedRequeueNode_AbandonsMessagesAndReturnsNullWhenNoQueuesRemain()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);
            MessageData message = CreateMessageData();
            int abandonCount = 0;
            var queueClient = new Mock<QueueClient>();
            queueClient.SetupGet(q => q.Name).Returns("partition-0");
            queueClient
                .Setup(
                    q => q.UpdateMessageAsync(
                        message.OriginalQueueMessage.MessageId,
                        message.OriginalQueueMessage.PopReceipt,
                        It.IsAny<string>(),
                        TimeSpan.Zero,
                        It.IsAny<CancellationToken>()))
                .Callback(() => abandonCount++)
                .ReturnsAsync(Response.FromValue(
                    QueuesModelFactory.UpdateReceipt("newPopReceipt", DateTimeOffset.UtcNow),
                    Mock.Of<Response>()));
            SetPrivateField(controlQueue.InnerQueue, "queueClient", queueClient.Object);

            AddActiveSession(manager, settings, controlQueue, "instance1", "activeExecution");
            object pendingBatch = CreatePendingBatch(controlQueue);
            AddMessageToPendingBatch(pendingBatch, message);
            object node = AddPendingBatchNode(manager, pendingBatch);
            EnqueueReadyForProcessingNode(manager, node);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            Task<OrchestrationSession> getNextTask = manager.GetNextSessionAsync(entitiesOnly: false, cts.Token);

            await WaitUntilAsync(() => IsNodeDetached(node), TimeSpan.FromSeconds(2));
            controlQueue.Release(null, "test");
            OrchestrationSession session = await getNextTask;

            Assert.IsNull(session, "Released delayed requeue nodes should not block dispatch when no queues remain.");
            await WaitUntilAsync(() => abandonCount == 1, TimeSpan.FromSeconds(2));

            Assert.AreEqual(0, GetPendingBatchCount(manager), "Released queue nodes should not be requeued after a delay.");
            Assert.AreEqual(0, GetReadyQueueCount(manager), "Released queue nodes should not be made ready for dispatch.");
            Assert.AreEqual(1, abandonCount, "Messages from a released delayed requeue node should be immediately abandoned.");
        }

        [TestMethod]
        public async Task GetNextSessionAsync_ReleasedReadyQueueNode_AbandonsMessagesAndReturnsNullWhenNoQueuesRemain()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new Mock<ITrackingStore>();

            using var manager = new OrchestrationSessionManager(
                "testaccount",
                settings,
                stats,
                trackingStore.Object);

            var storageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(settings, storageClient, settings.TaskHubName);
            var controlQueue = new ControlQueue(storageClient, "partition-0", messageManager);
            MessageData message = CreateMessageData();
            int abandonCount = 0;
            var queueClient = new Mock<QueueClient>();
            queueClient.SetupGet(q => q.Name).Returns("partition-0");
            queueClient
                .Setup(
                    q => q.UpdateMessageAsync(
                        message.OriginalQueueMessage.MessageId,
                        message.OriginalQueueMessage.PopReceipt,
                        It.IsAny<string>(),
                        TimeSpan.Zero,
                        It.IsAny<CancellationToken>()))
                .Callback(() => abandonCount++)
                .ReturnsAsync(Response.FromValue(
                    QueuesModelFactory.UpdateReceipt("newPopReceipt", DateTimeOffset.UtcNow),
                    Mock.Of<Response>()));
            SetPrivateField(controlQueue.InnerQueue, "queueClient", queueClient.Object);

            object pendingBatch = CreatePendingBatch(controlQueue);
            AddMessageToPendingBatch(pendingBatch, message);
            object node = AddPendingBatchNode(manager, pendingBatch);
            EnqueueReadyForProcessingNode(manager, node);
            controlQueue.Release(null, "test");

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            OrchestrationSession session = await manager.GetNextSessionAsync(entitiesOnly: false, cts.Token);

            Assert.IsNull(session, "Released queue nodes should not block dispatch when no queues remain.");
            Assert.AreEqual(0, GetPendingBatchCount(manager), "Released ready nodes should be removed from pending batches.");
            Assert.AreEqual(1, abandonCount, "Messages from released ready nodes should be immediately abandoned.");
        }

        static object CreatePendingBatch(ControlQueue controlQueue)
        {
            Type pendingBatchType = typeof(OrchestrationSessionManager)
                .GetNestedType("PendingMessageBatch", BindingFlags.NonPublic);

            return Activator.CreateInstance(
                pendingBatchType,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                args: new object[] { controlQueue, "instance1", "execution1" },
                culture: null);
        }

        static object AddPendingBatchNode(OrchestrationSessionManager manager, object pendingBatch)
        {
            object pendingBatches = GetPrivateField(manager, "pendingOrchestrationMessageBatches");
            MethodInfo addLast = pendingBatches.GetType().GetMethod("AddLast", new[] { pendingBatch.GetType() });
            return addLast.Invoke(pendingBatches, new[] { pendingBatch });
        }

        static void RemovePendingBatchNode(OrchestrationSessionManager manager, object node)
        {
            object pendingBatches = GetPrivateField(manager, "pendingOrchestrationMessageBatches");
            MethodInfo remove = pendingBatches.GetType().GetMethod("Remove", new[] { node.GetType() });
            remove.Invoke(pendingBatches, new[] { node });
        }

        static void EnqueueReadyForProcessingNode(OrchestrationSessionManager manager, object node)
        {
            object readyQueue = GetPrivateField(manager, "orchestrationsReadyForProcessingQueue");
            MethodInfo enqueue = readyQueue.GetType().GetMethod("Enqueue");
            enqueue.Invoke(readyQueue, new[] { node });
        }

        static void AddMessageToPendingBatch(object pendingBatch, MessageData message)
        {
            var messages = (ICollection<MessageData>)pendingBatch.GetType().GetProperty("Messages").GetValue(pendingBatch);
            messages.Add(message);
        }

        static Task InvokeScheduleOrchestrationStatePrefetch(
            OrchestrationSessionManager manager,
            object node,
            CancellationToken cancellationToken)
        {
            MethodInfo schedule = typeof(OrchestrationSessionManager).GetMethod(
                "ScheduleOrchestrationStatePrefetch",
                BindingFlags.NonPublic | BindingFlags.Instance);

            return (Task)schedule.Invoke(manager, new[] { node, Guid.NewGuid(), cancellationToken });
        }

        static void AddActiveSession(
            OrchestrationSessionManager manager,
            AzureStorageOrchestrationServiceSettings settings,
            ControlQueue controlQueue,
            string instanceId,
            string executionId)
        {
            var sessions = (Dictionary<string, OrchestrationSession>)GetPrivateField(manager, "activeOrchestrationSessions");
            var instance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            };
            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(new ExecutionStartedEvent(-1, string.Empty)
            {
                OrchestrationInstance = instance,
            });

            sessions[instanceId] = new OrchestrationSession(
                settings,
                "testaccount",
                instance,
                controlQueue,
                new List<MessageData>(),
                runtimeState,
                eTags: null,
                DateTime.UtcNow,
                trackingStoreContext: null,
                TimeSpan.FromSeconds(30),
                CancellationToken.None,
                Guid.NewGuid());
        }

        static bool IsNodeDetached(object node)
        {
            object list = node.GetType().GetProperty("List").GetValue(node);
            return list == null;
        }

        static int GetPendingBatchCount(OrchestrationSessionManager manager)
        {
            object pendingBatches = GetPrivateField(manager, "pendingOrchestrationMessageBatches");
            return (int)pendingBatches.GetType().GetProperty("Count").GetValue(pendingBatches);
        }

        static int GetReadyQueueCount(OrchestrationSessionManager manager)
        {
            object readyQueue = GetPrivateField(manager, "orchestrationsReadyForProcessingQueue");
            return (int)readyQueue.GetType().GetProperty("Count").GetValue(readyQueue);
        }

        static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
        {
            var stopwatch = Stopwatch.StartNew();
            while (!condition())
            {
                if (stopwatch.Elapsed > timeout)
                {
                    Assert.Fail("Condition was not reached before timeout.");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        static MessageData CreateMessageData()
        {
            var instance = new OrchestrationInstance
            {
                InstanceId = "instance1",
                ExecutionId = "execution1",
            };

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = instance,
                Event = new TimerFiredEvent(0),
            };

            var message = new MessageData(
                taskMessage,
                Guid.NewGuid(),
                "partition-0",
                orchestrationEpisode: null,
                sender: instance);

            message.OriginalQueueMessage = QueuesModelFactory.QueueMessage(
                Guid.NewGuid().ToString("N"),
                Guid.NewGuid().ToString("N"),
                string.Empty,
                1,
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddHours(1),
                DateTimeOffset.UtcNow.AddMinutes(5));

            return message;
        }

        static object GetPrivateField(object target, string fieldName)
        {
            FieldInfo field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(field);
            return field.GetValue(target);
        }

        static void SetPrivateField(object target, string fieldName, object value)
        {
            FieldInfo field = target.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(field);
            field.SetValue(target, value);
        }
    }
}
