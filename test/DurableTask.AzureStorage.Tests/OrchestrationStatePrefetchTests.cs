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
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the retries of the orchestration state prefetch in <see cref="OrchestrationSessionManager"/>.
    /// The tests use an in-memory tracking store. They do not send requests to Azure Storage.
    /// </summary>
    [TestClass]
    public class OrchestrationStatePrefetchTests
    {
        const string InstanceId = "prefetch-instance";
        const string ExecutionId = "prefetch-execution";
        static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

        AzureStorageOrchestrationServiceSettings settings;
        ControlQueue controlQueue;

        [TestInitialize]
        public void Initialize()
        {
            this.settings = new AzureStorageOrchestrationServiceSettings
            {
                // The storage clients are necessary to create a control queue, but the tests do not use them.
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };

            var azureStorageClient = new AzureStorageClient(this.settings);
            var messageManager = new MessageManager(this.settings, azureStorageClient, "prefetch-largemessages");
            this.controlQueue = new ControlQueue(azureStorageClient, "prefetch-control-00", messageManager);
        }

        [TestCleanup]
        public void Cleanup()
        {
            this.controlQueue?.Dispose();
        }

        /// <summary>
        /// A history load that fails each time must not cause the failed attempts to stay in memory.
        /// Each retry must release the previous attempt and its exception, also when no attempt completes.
        /// </summary>
        [TestMethod]
        public async Task PrefetchRetry_PermanentFailure_DoesNotKeepFailedAttemptsInMemory()
        {
            const int FailedAttempts = 20;
            var exceptions = new WeakReference[FailedAttempts];
            var pendingLoad = new TaskCompletionSource<OrchestrationHistory>(TaskCreationOptions.RunContinuationsAsynchronously);

            var trackingStore = new FakeTrackingStore
            {
                OnGetHistory = attempt =>
                {
                    if (attempt > FailedAttempts)
                    {
                        // This attempt does not complete. Thus the retries are active, as they are in production.
                        return pendingLoad.Task;
                    }

                    var exception = new SimulatedStorageException(attempt);
                    exceptions[attempt - 1] = new WeakReference(exception);
                    throw exception;
                },
            };

            using var manager = this.CreateManager(trackingStore);
            using var shutdown = new CancellationTokenSource();
            manager.AddMessageToPendingOrchestration(this.controlQueue, new[] { this.CreateMessage() }, Guid.NewGuid(), shutdown.Token);

            await TestHelpers.WaitFor(() => trackingStore.HistoryCalls > FailedAttempts, WaitTimeout);

            bool[] live = GetLiveStatesAfterFullCollection(exceptions);

            shutdown.Cancel();
            pendingLoad.TrySetCanceled();

            // In a build without optimization, the compiler can keep the exception of the latest failed attempt
            // in the state machine until the next failure. The exceptions of the earlier attempts must not stay in memory.
            int liveEarlierExceptions = live.Take(FailedAttempts - 1).Count(isLive => isLive);
            Assert.AreEqual(
                0,
                liveEarlierExceptions,
                $"{liveEarlierExceptions} of {FailedAttempts - 1} exceptions from earlier failed history loads are still in memory. " +
                $"Total live exceptions: {live.Count(isLive => isLive)} of {FailedAttempts}.");
        }

        /// <summary>
        /// When the history load succeeds after some failures, the batch becomes ready one time, with the loaded state.
        /// </summary>
        [TestMethod]
        public async Task PrefetchRetry_SuccessAfterFailures_BatchIsReadyOneTimeWithState()
        {
            const int FailedAttempts = 3;
            OrchestrationHistory history = CreateHistory();
            var trackingStore = new FakeTrackingStore
            {
                OnGetHistory = attempt => attempt <= FailedAttempts
                    ? throw new SimulatedStorageException(attempt)
                    : Task.FromResult(history),
            };

            using var manager = this.CreateManager(trackingStore);
            manager.AddMessageToPendingOrchestration(this.controlQueue, new[] { this.CreateMessage() }, Guid.NewGuid(), CancellationToken.None);

            OrchestrationSession session;
            using (var timeout = new CancellationTokenSource(WaitTimeout))
            {
                session = await manager.GetNextSessionAsync(entitiesOnly: false, timeout.Token);
            }

            Assert.IsNotNull(session);
            Assert.AreEqual(FailedAttempts + 1, trackingStore.HistoryCalls);
            Assert.AreEqual(InstanceId, session.Instance.InstanceId);
            Assert.AreEqual(ExecutionId, session.Instance.ExecutionId);
            Assert.AreSame(history.Events[0], session.RuntimeState.ExecutionStartedEvent);
            Assert.AreEqual(history.ETag, session.ConcurrencyTags.HistoryETag);
            Assert.AreEqual(1, session.CurrentMessageBatch.Count);

            // The batch must not become ready a second time.
            using (var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500)))
            {
                try
                {
                    OrchestrationSession secondSession = await manager.GetNextSessionAsync(entitiesOnly: false, timeout.Token);
                    Assert.IsNull(secondSession, "The batch became ready for processing more than one time.");
                }
                catch (OperationCanceledException)
                {
                    // Expected: no other batch is ready.
                }
            }

            Assert.AreEqual(FailedAttempts + 1, trackingStore.HistoryCalls);
        }

        /// <summary>
        /// When the shutdown token is cancelled while the prefetch waits to try again, the prefetch stops without a new attempt.
        /// </summary>
        [TestMethod]
        public async Task PrefetchRetry_CancelledDuringRetryDelay_StopsRetrying()
        {
            TimeSpan retryDelay = TimeSpan.FromMilliseconds(200);
            var trackingStore = new FakeTrackingStore
            {
                OnGetHistory = attempt => throw new SimulatedStorageException(attempt),
            };

            using var manager = this.CreateManager(trackingStore, retryDelay);
            using var shutdown = new CancellationTokenSource();
            manager.AddMessageToPendingOrchestration(this.controlQueue, new[] { this.CreateMessage() }, Guid.NewGuid(), shutdown.Token);

            await TestHelpers.WaitFor(() => trackingStore.HistoryCalls == 1, WaitTimeout);
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            shutdown.Cancel();

            // Wait much longer than the retry delay. A retry that ignores the token would load the history again.
            await Task.Delay(retryDelay + TimeSpan.FromSeconds(1));

            Assert.AreEqual(1, trackingStore.HistoryCalls, "The prefetch tried to load the history again after cancellation.");
        }

        /// <summary>
        /// When the shutdown token is already cancelled, the prefetch stops after the first failure.
        /// This must also occur when the store does not monitor the token.
        /// </summary>
        [TestMethod]
        public async Task PrefetchRetry_TokenAlreadyCancelled_StopsAfterFirstFailure()
        {
            var trackingStore = new FakeTrackingStore
            {
                IgnoreCancellation = true,
                OnGetHistory = attempt => throw new SimulatedStorageException(attempt),
            };

            using var manager = this.CreateManager(trackingStore);
            using var shutdown = new CancellationTokenSource();
            shutdown.Cancel();

            try
            {
                manager.AddMessageToPendingOrchestration(this.controlQueue, new[] { this.CreateMessage() }, Guid.NewGuid(), shutdown.Token);
                await TestHelpers.WaitFor(() => trackingStore.HistoryCalls >= 1, WaitTimeout);

                // The retry delay is 1 ms. A retry that does not monitor the token would load the history many times in this time.
                await Task.Delay(TimeSpan.FromMilliseconds(500));

                Assert.AreEqual(1, trackingStore.HistoryCalls, "The prefetch tried to load the history again after cancellation.");
            }
            finally
            {
                // Make sure that a retry that continues stops at the next load.
                trackingStore.IgnoreCancellation = false;
            }
        }

        /// <summary>
        /// When the history is loaded but the instance ETag load fails, the retry loads the instance ETag again.
        /// The retry must not load the history again.
        /// </summary>
        [TestMethod]
        public async Task PrefetchRetry_HistoryAlreadyLoaded_DoesNotLoadHistoryAgain()
        {
            // The instance ETag is loaded after the history. Make the first instance ETag load fail.
            this.settings.UseInstanceTableEtag = true;
            OrchestrationHistory history = CreateHistory();
            var instanceETag = new ETag("instance-etag");
            var trackingStore = new FakeTrackingStore
            {
                OnGetHistory = attempt => Task.FromResult(history),
                OnFetchInstanceStatus = attempt => attempt == 1
                    ? throw new SimulatedStorageException(attempt)
                    : Task.FromResult(new InstanceStatus(new OrchestrationState(), instanceETag)),
            };

            using var manager = this.CreateManager(trackingStore);
            manager.AddMessageToPendingOrchestration(this.controlQueue, new[] { this.CreateMessage() }, Guid.NewGuid(), CancellationToken.None);

            OrchestrationSession session;
            using (var timeout = new CancellationTokenSource(WaitTimeout))
            {
                session = await manager.GetNextSessionAsync(entitiesOnly: false, timeout.Token);
            }

            Assert.IsNotNull(session);
            Assert.AreEqual(2, trackingStore.InstanceStatusCalls, "The instance ETag load did not run.");
            Assert.AreEqual<ETag?>(instanceETag, session.ConcurrencyTags.InstanceETag, "The session does not have the instance ETag from the retry.");
            Assert.AreEqual(1, trackingStore.HistoryCalls, "The retry loaded the history again.");
            Assert.AreSame(history.Events[0], session.RuntimeState.ExecutionStartedEvent);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static bool[] GetLiveStatesAfterFullCollection(WeakReference[] references)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            return references.Select(reference => reference != null && reference.IsAlive).ToArray();
        }

        static OrchestrationHistory CreateHistory()
        {
            var executionStarted = new ExecutionStartedEvent(-1, null)
            {
                Name = "TestOrchestration",
                Version = string.Empty,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = InstanceId, ExecutionId = ExecutionId },
            };

            return new OrchestrationHistory(new List<HistoryEvent> { executionStarted }, DateTime.UtcNow, new ETag("history-etag"));
        }

        OrchestrationSessionManager CreateManager(FakeTrackingStore trackingStore, TimeSpan? retryDelay = null)
        {
            return new OrchestrationSessionManager("testaccount", this.settings, new AzureStorageOrchestrationServiceStats(), trackingStore)
            {
                PrefetchRetryDelay = retryDelay ?? TimeSpan.FromMilliseconds(1),
            };
        }

        MessageData CreateMessage()
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = InstanceId, ExecutionId = ExecutionId },
                Event = new EventRaisedEvent(-1, null) { Name = "TestEvent" },
            };

            return new MessageData(taskMessage, Guid.NewGuid(), this.controlQueue.Name, orchestrationEpisode: null, sender: new OrchestrationInstance())
            {
                OriginalQueueMessage = QueuesModelFactory.QueueMessage(
                    messageId: Guid.NewGuid().ToString(),
                    popReceipt: "pop-receipt",
                    messageText: string.Empty,
                    dequeueCount: 1),
            };
        }

        sealed class SimulatedStorageException : Exception
        {
            public SimulatedStorageException(int attempt)
                : base($"Simulated storage failure {attempt}.")
            {
                // About 32 KB, as a large storage error response can be.
                this.Payload = new string('x', 16 * 1024);
            }

            public string Payload { get; }
        }

        sealed class FakeTrackingStore : ITrackingStore
        {
            int historyCalls;
            int instanceStatusCalls;

            public Func<int, Task<OrchestrationHistory>> OnGetHistory { get; set; }

            public Func<int, Task<InstanceStatus>> OnFetchInstanceStatus { get; set; }

            // Set to true to simulate a store that does not monitor the cancellation token.
            public bool IgnoreCancellation { get; set; }

            public bool IsMigrationActive => false;

            public int HistoryCalls => Volatile.Read(ref this.historyCalls);

            public int InstanceStatusCalls => Volatile.Read(ref this.instanceStatusCalls);

            async Task<OrchestrationHistory> ITrackingStore.GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken)
            {
                int attempt = Interlocked.Increment(ref this.historyCalls);
                if (!this.IgnoreCancellation)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                // Complete asynchronously, as a storage request does.
                await Task.Yield();
                return await this.OnGetHistory(attempt);
            }

            async Task<InstanceStatus> ITrackingStore.FetchInstanceStatusAsync(string instanceId, CancellationToken cancellationToken)
            {
                int attempt = Interlocked.Increment(ref this.instanceStatusCalls);
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                return await this.OnFetchInstanceStatus(attempt);
            }

            Task ITrackingStore.CreateAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.DeleteAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

            Task<bool> ITrackingStore.ExistsAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.StartAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

            IAsyncEnumerable<string> ITrackingStore.RewindHistoryAsync(string instanceId, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.UpdateStateAsync(OrchestrationRuntimeState newRuntimeState, OrchestrationRuntimeState oldRuntimeState, string instanceId, string executionId, OrchestrationConcurrencyTags eTags, object trackingStoreContext, CancellationToken cancellationToken) => throw new NotSupportedException();

            IAsyncEnumerable<OrchestrationState> ITrackingStore.GetStateAsync(string instanceId, bool allExecutions, bool fetchInput, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task<OrchestrationState> ITrackingStore.GetStateAsync(string instanceId, string executionId, bool fetchInput, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(string instanceId, string executionId, OrchestrationRuntimeState runtimeState, bool instanceEntityExists, long sequenceNumber, CancellationToken cancellationToken) => throw new NotSupportedException();

            IAsyncEnumerable<OrchestrationState> ITrackingStore.GetStateAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

            IAsyncEnumerable<InstanceStatus> ITrackingStore.FetchInstanceStatusAsync(IEnumerable<string> instanceIds, CancellationToken cancellationToken) => throw new NotSupportedException();

            AsyncPageable<OrchestrationState> ITrackingStore.GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken) => throw new NotSupportedException();

            AsyncPageable<OrchestrationState> ITrackingStore.GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task<bool> ITrackingStore.SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent, ETag? eTag, string inputPayloadOverride, long sequenceNumber, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.UpdateStatusForRewindAsync(string instanceId, long? sequenceNumber, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.UpdateStatusForTerminationAsync(string instanceId, ExecutionTerminatedEvent executionTerminatedEvent, long sequenceNumber, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task ITrackingStore.PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task<PurgeHistoryResult> ITrackingStore.PurgeInstanceHistoryAsync(string instanceId, CancellationToken cancellationToken) => throw new NotSupportedException();

            Task<PurgeHistoryResult> ITrackingStore.PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken) => throw new NotSupportedException();
        }
    }
}
