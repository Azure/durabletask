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
        /// Suspended is a pause, not a terminal state: the orchestration has no result yet.
        /// </summary>
        [TestMethod]
        public async Task WaitForOrchestration_Suspended_KeepsWaiting()
        {
            var store = new FakeInstanceStore();
            store.States.Add(CreateState("generation-1", OrchestrationStatus.Suspended, BaseTime));

            store.OnQuery = s =>
            {
                if (s.QueryCount == 2)
                {
                    // Resumed and completed.
                    s.States.Clear();
                    s.States.Add(CreateState("generation-1", OrchestrationStatus.Completed, BaseTime, "final output"));
                }
            };

            ServiceBusOrchestrationService service = CreateService(store);

            OrchestrationState state = await service.WaitForOrchestrationAsync(
                InstanceId,
                "generation-1",
                TimeSpan.FromSeconds(30),
                CancellationToken.None);

            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("final output", state.Output);
        }

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
