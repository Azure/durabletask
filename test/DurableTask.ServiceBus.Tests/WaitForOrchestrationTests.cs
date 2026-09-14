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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus.Settings;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Unit tests for <see cref="ServiceBusOrchestrationService.WaitForOrchestrationAsync"/>.
    /// These use an in-memory instance store so they do not require a live Service Bus namespace.
    /// </summary>
    [TestClass]
    public class WaitForOrchestrationTests
    {
        const string InstanceId = "instance-1";
        const string FakeConnectionString =
            "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=dGVzdGtleQ==";

        static readonly DateTime BaseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static ServiceBusOrchestrationService CreateService(FakeInstanceStore instanceStore)
        {
            return new ServiceBusOrchestrationService(
                FakeConnectionString,
                "testhub",
                instanceStore,
                null,
                new ServiceBusOrchestrationServiceSettings());
        }

        static OrchestrationState CreateState(
            string executionId,
            OrchestrationStatus status,
            DateTime createdTime,
            string output = null)
        {
            return new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = InstanceId,
                    ExecutionId = executionId
                },
                OrchestrationStatus = status,
                CreatedTime = createdTime,
                LastUpdatedTime = createdTime,
                Output = output
            };
        }

        /// <summary>
        /// The core of the fix: while waiting on a specific execution, state written by an earlier run
        /// of the same instance id must never be returned, even though it is the only readable state.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_PinnedExecution_IgnoresStateFromPreviousRun()
        {
            var store = new FakeInstanceStore();

            // A previous run of the same instance id that already completed.
            store.States.Add(CreateState("previous-run", OrchestrationStatus.Completed, BaseTime, "stale output"));

            // The new run's state only becomes readable after the first poll.
            store.OnQuery = s =>
            {
                if (s.QueryCount == 2)
                {
                    s.States.Add(CreateState("current-run", OrchestrationStatus.Completed, BaseTime.AddMinutes(5), "fresh output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "current-run",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual("current-run", state.OrchestrationInstance.ExecutionId, "Returned state from the wrong run.");
            Assert.AreEqual("fresh output", state.Output);
        }

        /// <summary>
        /// If the new run never becomes readable, the wait must time out rather than return the
        /// previous run's result.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_PinnedExecution_TimesOutRatherThanReturnPreviousRun()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("previous-run", OrchestrationStatus.Completed, BaseTime, "stale output"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "current-run",
                TimeSpan.FromSeconds(2),
                CancellationToken.None);

            Assert.IsNull(state, "State from a previous run of the same instance id must not be returned.");
        }

        /// <summary>
        /// A ContinuedAsNew row is a tombstone that is never updated again, so the wait must follow the
        /// new generation instead of polling the pinned execution forever.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_PinnedExecution_ContinuedAsNew_FollowsNextGeneration()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.ContinuedAsNew, BaseTime, "next input"));
            store.States.Add(CreateState("generation-2", OrchestrationStatus.Completed, BaseTime.AddMinutes(1), "final output"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("generation-2", state.OrchestrationInstance.ExecutionId);
            Assert.AreEqual("final output", state.Output);
        }

        /// <summary>
        /// Un-pinning after a ContinuedAsNew tombstone must not re-query within the same iteration:
        /// the tombstone was itself this iteration's status check, so a zero timeout still performs
        /// exactly one lookup rather than reaching the next generation for free.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Zero_ContinuedAsNew_ChecksOnceAndDoesNotFollowNextGeneration()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.ContinuedAsNew, BaseTime, "next input"));
            store.States.Add(CreateState("generation-2", OrchestrationStatus.Completed, BaseTime.AddMinutes(1), "final output"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.Zero,
                CancellationToken.None);

            Assert.IsNull(state, "A zero timeout must not reach the next generation after un-pinning.");
            Assert.AreEqual(1, store.QueryCount, "A zero timeout must perform exactly one lookup.");
        }

        /// <summary>
        /// After following a continue-as-new to the current generation we must still not accept state
        /// left behind by an earlier run of the same instance id.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_ContinuedAsNew_IgnoresStateFromPreviousRun()
        {
            var store = new FakeInstanceStore();

            // Previous run: completed long before the current run started.
            store.States.Add(CreateState("previous-run", OrchestrationStatus.Completed, BaseTime, "stale output"));

            // Current run, first generation, already continued as new.
            store.States.Add(CreateState("generation-1", OrchestrationStatus.ContinuedAsNew, BaseTime.AddMinutes(5), "next input"));

            // The next generation only becomes readable later.
            store.OnQuery = s =>
            {
                if (s.QueryCount == 3)
                {
                    s.States.Add(CreateState("generation-2", OrchestrationStatus.Completed, BaseTime.AddMinutes(6), "final output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual("generation-2", state.OrchestrationInstance.ExecutionId, "Returned state from the wrong run.");
            Assert.AreEqual("final output", state.Output);
        }

        /// <summary>
        /// Suspended is a pause, not a terminal state: an orchestration that is never resumed has no
        /// result, so the wait must time out rather than report it as finished.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Suspended_IsNotReturnedAsTerminal()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Suspended, BaseTime));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(2),
                CancellationToken.None);

            Assert.IsNull(state, "A suspended orchestration has not completed and must not be returned.");
        }

        /// <summary>
        /// Callers that do not supply an execution id keep the legacy behavior of following the
        /// current generation of the instance.
        /// </summary>
        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow("   ")]
        public async Task WaitForOrchestration_WithoutExecutionId_UsesCurrentGeneration(string executionId)
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.ContinuedAsNew, BaseTime, "next input"));
            store.States.Add(CreateState("generation-2", OrchestrationStatus.Completed, BaseTime.AddMinutes(1), "final output"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                executionId,
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("generation-2", state.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(0, store.PinnedQueryCount, "An empty execution id must not be queried as an exact execution.");
        }

        /// <summary>
        /// Hiding ContinuedAsNew rows from the current generation lookup is an implementation detail of
        /// AzureTableInstanceStore, not a guarantee of IOrchestrationServiceInstanceStore. A store that
        /// surfaces them must not cause a tombstone to be reported as the final state.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_StoreWithoutContinuedAsNewFilter_DoesNotReturnTombstone()
        {
            var store = new FakeInstanceStore { FilterContinuedAsNew = false };
            store.States.Add(CreateState("generation-1", OrchestrationStatus.ContinuedAsNew, BaseTime, "next input"));

            store.OnQuery = s =>
            {
                if (s.QueryCount == 2)
                {
                    s.States.Add(CreateState("generation-2", OrchestrationStatus.Completed, BaseTime.AddMinutes(1), "final output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                null,
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("generation-2", state.OrchestrationInstance.ExecutionId);
        }

        [TestMethod]
        public async Task WaitForOrchestration_PinnedExecution_ReturnsFailedState()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Failed, BaseTime, "boom"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.AreEqual(1, store.PinnedQueryCount, "The pinned lookup should have been used.");
            Assert.AreEqual(0, store.LatestQueryCount, "The current generation lookup should not have been used.");
        }

        /// <summary>
        /// A suspended orchestration that is resumed and then runs to completion must return the
        /// final state, not stop at the intermediate Suspended or Running rows.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Suspended_ResumedAndCompleted_ReturnsFinalState()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Suspended, BaseTime));

            // Suspended -> Running (resumed) -> Completed, one transition per poll.
            store.OnQuery = s =>
            {
                OrchestrationStatus? next = s.QueryCount == 2 ? OrchestrationStatus.Running
                    : s.QueryCount == 3 ? OrchestrationStatus.Completed
                    : (OrchestrationStatus?)null;

                if (next != null)
                {
                    s.States.Clear();
                    s.States.Add(CreateState(
                        "generation-1",
                        next.Value,
                        BaseTime,
                        next == OrchestrationStatus.Completed ? "final output" : null));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state, "The resumed orchestration completed and must be returned.");
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("final output", state.Output);
            Assert.AreEqual(3, store.QueryCount, "The wait should have polled through Suspended and Running.");
        }

        /// <summary>
        /// Negative timeouts are a caller bug. Timeout.InfiniteTimeSpan is itself negative (-1ms), so it
        /// must be excluded from this check; values adjacent to it must still be rejected.
        /// </summary>
        [DataTestMethod]
        [DataRow(-2)]
        [DataRow(-1000)]
        [DataRow(-60000)]
        public async Task WaitForOrchestration_Timeout_Negative_Throws(int timeoutMilliseconds)
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Completed, BaseTime, "final output"));

            ServiceBusOrchestrationService service = CreateService(store);

            await Assert.ThrowsExceptionAsync<ArgumentException>(
                () => service.WaitForOrchestrationAsync(
                    InstanceId,
                    "generation-1",
                    TimeSpan.FromMilliseconds(timeoutMilliseconds),
                    CancellationToken.None),
                $"A timeout of {timeoutMilliseconds}ms should be rejected.");

            Assert.AreEqual(0, store.QueryCount, "The timeout should be validated before any lookup.");
        }

        /// <summary>
        /// A zero timeout means "check once and return", so a state that is already terminal is returned.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Zero_ChecksOnceAndReturnsTerminalState()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Completed, BaseTime, "final output"));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.Zero,
                CancellationToken.None);

            Assert.IsNotNull(state, "A zero timeout must still perform one status check.");
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual(1, store.QueryCount);
        }

        /// <summary>
        /// A zero timeout must not wait: if the orchestration is not yet terminal it returns null after
        /// a single check rather than polling.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Zero_DoesNotPollWhenNotComplete()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.Zero,
                CancellationToken.None);

            Assert.IsNull(state);
            Assert.AreEqual(1, store.QueryCount, "A zero timeout must check exactly once and not poll.");
        }

        /// <summary>
        /// A positive timeout polls until it elapses, then gives up and returns null.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Positive_PollsThenReturnsNull()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(4),
                CancellationToken.None);

            Assert.IsNull(state, "The orchestration never completed, so the wait must time out.");
            Assert.IsTrue(store.QueryCount > 1, $"A 4 second timeout should poll more than once, polled {store.QueryCount} time(s).");
        }

        /// <summary>
        /// The whole timeout window must be polled. Charging a full polling interval against the budget
        /// before the delay is awaited would abandon the wait about halfway through and miss an
        /// orchestration that completes late in the window.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Positive_PollsForTheFullTimeout()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            // With a 2 second polling interval a 4 second timeout allows checks at roughly t=0, t=2
            // and t=4, so state that only becomes terminal on the third check is still observed.
            store.OnQuery = s =>
            {
                if (s.QueryCount == 3)
                {
                    s.States.Clear();
                    s.States.Add(CreateState("generation-1", OrchestrationStatus.Completed, BaseTime, "final output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(4),
                CancellationToken.None);

            Assert.IsNotNull(state, "The orchestration completed within the timeout and must be returned.");
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("final output", state.Output);
        }

        /// <summary>
        /// A timeout shorter than the polling interval must wait out its window rather than returning on
        /// the first check, but must not overshoot it by delaying for a whole polling interval.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_ShorterThanPollingInterval_WaitsWithoutOvershooting()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            ServiceBusOrchestrationService service = CreateService(store);

            // The timeout is kept far below the 2 second polling interval so the three possible
            // behaviours are widely separated in time and the assertions below do not depend on
            // precise scheduling: clamping the delay takes about 200ms, delaying for a whole
            // interval takes about 2s, and charging the budget before the delay returns instantly.
            TimeSpan timeout = TimeSpan.FromMilliseconds(200);

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                timeout,
                CancellationToken.None);

            stopwatch.Stop();

            Assert.IsNull(state, "The orchestration never completed, so the wait must time out.");
            Assert.AreEqual(2, store.QueryCount, "The wait must check once, wait out its window, then check again.");

            // Task.Delay never returns early, so this only fails if the wait did not delay at all.
            Assert.IsTrue(
                stopwatch.Elapsed >= TimeSpan.FromMilliseconds(150),
                $"The wait must use its window instead of returning immediately, took {stopwatch.ElapsedMilliseconds}ms.");

            // Well clear of the ~200ms a clamped delay needs, and well below the 2s a full polling
            // interval would take.
            Assert.IsTrue(
                stopwatch.Elapsed < TimeSpan.FromMilliseconds(1500),
                $"The wait must clamp its delay to the remaining timeout instead of waiting a whole polling interval, took {stopwatch.ElapsedMilliseconds}ms.");
        }

        /// <summary>
        /// Timeout.InfiniteTimeSpan is negative, so a naive remaining-time check would treat it as already
        /// elapsed and return null on the first poll instead of waiting.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Infinite_WaitsForCompletion()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            store.OnQuery = s =>
            {
                if (s.QueryCount == 2)
                {
                    s.States.Clear();
                    s.States.Add(CreateState("generation-1", OrchestrationStatus.Completed, BaseTime, "final output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                Timeout.InfiniteTimeSpan,
                CancellationToken.None);

            Assert.IsNotNull(state, "An infinite timeout must keep waiting instead of giving up immediately.");
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("final output", state.Output);
        }

        /// <summary>
        /// An infinite wait must still observe cancellation, otherwise it can never be stopped.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Timeout_Infinite_HonorsCancellation()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Running, BaseTime));

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
            {
                ServiceBusOrchestrationService service = CreateService(store);

                Task<OrchestrationState> waitTask = service.WaitForOrchestrationAsync(
                    InstanceId,
                    "generation-1",
                    Timeout.InfiniteTimeSpan,
                    cts.Token);

                // An infinite wait that ignores its token would never complete, so bound the await to
                // fail the test instead of hanging the run.
                Task finished = await Task.WhenAny(waitTask, Task.Delay(TimeSpan.FromSeconds(30)));

                Assert.AreSame(waitTask, finished, "An infinite wait must stop once its token is cancelled.");

                try
                {
                    Assert.IsNull(await waitTask, "A cancelled wait must not return a state.");
                }
                catch (OperationCanceledException)
                {
                    // Also acceptable: the polling delay observes the token directly.
                }

                // Whichever path ends the wait, it must have kept polling until cancellation rather
                // than treating the negative Timeout.InfiniteTimeSpan as an elapsed budget and
                // bailing out after the first lookup.
                Assert.IsTrue(
                    store.QueryCount > 1,
                    $"An infinite wait must poll until cancelled, polled {store.QueryCount} time(s).");
            }
        }

        /// <summary>
        /// In-memory instance store that mimics the query semantics of AzureTableInstanceStore.
        /// </summary>
        sealed class FakeInstanceStore : IOrchestrationServiceInstanceStore
        {
            public List<OrchestrationState> States { get; } = new List<OrchestrationState>();

            /// <summary>
            /// Mimics AzureTableInstanceStore, which excludes ContinuedAsNew rows from the
            /// current generation lookup.
            /// </summary>
            public bool FilterContinuedAsNew { get; set; } = true;

            /// <summary>
            /// Invoked before every state lookup so a test can make state readable after N polls.
            /// </summary>
            public Action<FakeInstanceStore> OnQuery { get; set; }

            public int PinnedQueryCount { get; private set; }

            public int LatestQueryCount { get; private set; }

            public int QueryCount => this.PinnedQueryCount + this.LatestQueryCount;

            public int MaxHistoryEntryLength => 1024;

            public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
            {
                this.LatestQueryCount++;
                this.OnQuery?.Invoke(this);

                IEnumerable<OrchestrationState> matches = this.States
                    .Where(s => s.OrchestrationInstance.InstanceId == instanceId);

                if (!allInstances && this.FilterContinuedAsNew)
                {
                    matches = matches.Where(s => s.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew);
                }

                if (allInstances)
                {
                    return Task.FromResult(matches.Select(Wrap));
                }

                OrchestrationState latest = matches.OrderByDescending(s => s.LastUpdatedTime).FirstOrDefault();

                return Task.FromResult(latest == null
                    ? Enumerable.Empty<OrchestrationStateInstanceEntity>()
                    : new[] { Wrap(latest) }.AsEnumerable());
            }

            public Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
            {
                this.PinnedQueryCount++;
                this.OnQuery?.Invoke(this);

                OrchestrationState match = this.States.FirstOrDefault(
                    s => s.OrchestrationInstance.InstanceId == instanceId &&
                         s.OrchestrationInstance.ExecutionId == executionId);

                return Task.FromResult(match == null ? null : Wrap(match));
            }

            static OrchestrationStateInstanceEntity Wrap(OrchestrationState state)
            {
                return new OrchestrationStateInstanceEntity { State = state };
            }

            public Task InitializeStoreAsync(bool recreate) => throw new NotImplementedException();

            public Task DeleteStoreAsync() => throw new NotImplementedException();

            public Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities) => throw new NotImplementedException();

            public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitiesAsync(string instanceId, string executionId) => throw new NotImplementedException();

            public Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities) => throw new NotImplementedException();

            public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId) => throw new NotImplementedException();

            public Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType) => throw new NotImplementedException();

            public Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities) => throw new NotImplementedException();

            public Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities) => throw new NotImplementedException();

            public Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitiesAsync(int top) => throw new NotImplementedException();
        }
    }
}
