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
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests that exercise the <c>ParentInstanceId</c> Instances-table property against a real storage
    /// account, so that the actual Azure Table update semantics (InsertOrMerge / Merge) are covered.
    /// Assertions are made on the raw stored property in addition to the public read conversion,
    /// because a merge-based write that omits the property leaves a previous value intact and would
    /// otherwise be invisible to a test that only inspects converted state.
    /// </summary>
    [TestClass]
    public class ParentInstanceIdTrackingStoreTests
    {
        const string ParentInstanceIdProperty = "ParentInstanceId";

        string taskHubName;
        AzureStorageOrchestrationServiceSettings settings;
        AzureStorageClient azureStorageClient;
        AzureTableTrackingStore trackingStore;

        [TestInitialize]
        public async Task Initialize()
        {
            // A unique task hub per test keeps the Instances/History tables isolated, so a leftover row
            // from another test cannot mask a missing write.
            this.taskHubName = "pid" + Guid.NewGuid().ToString("N").Substring(0, 12);
            this.settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            this.settings.TaskHubName = this.taskHubName;

            this.azureStorageClient = new AzureStorageClient(this.settings);
            var messageManager = new MessageManager(this.settings, this.azureStorageClient, $"{this.taskHubName}-largemessages".ToLowerInvariant());
            this.trackingStore = new AzureTableTrackingStore(this.azureStorageClient, messageManager);
            await this.trackingStore.CreateAsync();
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            // Delete the per-test tables so repeated runs do not leak storage resources.
            if (this.trackingStore != null)
            {
                await this.trackingStore.DeleteAsync();
            }
        }

        /// <summary>
        /// Verifies that a no-parent write clears a parent ID left behind by a previous row with the same
        /// instance ID. This covers the terminal-history repair write, which is unconditionally
        /// InsertOrMerge, so a helper that skips the property when there is no parent would leave the
        /// stale value in place.
        /// </summary>
        [TestMethod]
        public async Task CompletedOrchestrationRepair_ClearsStaleParentInstanceId_WithInsertOrMerge()
        {
            string instanceId = $"stale-{Guid.NewGuid():N}";

            await this.SeedInstanceRowAsync(instanceId, "stale-parent");
            Assert.AreEqual("stale-parent", await this.GetRawParentInstanceIdAsync(instanceId), "Seeded row should carry the stale parent.");

            // Drive a genuine no-parent write through the same production path that uses InsertOrMerge.
            await this.trackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(
                instanceId,
                executionId: "execution-1",
                runtimeState: CreateCompletedRuntimeState(instanceId, "execution-1", parentInstanceId: null),
                instanceEntityExists: true);

            Assert.AreEqual(
                string.Empty,
                await this.GetRawParentInstanceIdAsync(instanceId),
                "A no-parent write must clear the stored property, otherwise merge semantics retain the stale parent.");

            InstanceStatus status = await this.trackingStore.FetchInstanceStatusAsync(instanceId);
            Assert.IsNull(status.State.ParentInstance, "A cleared parent must read back as a null ParentInstance.");
        }

        /// <summary>
        /// The clearing behavior must also hold for the checkpoint write in
        /// <see cref="AzureTableTrackingStore.UpdateStateAsync"/>, which routes through
        /// UpdateInstanceTableAsync. That method picks InsertOrMerge when UseInstanceTableEtag is false and
        /// Merge (with the supplied ETag) when it is true. Both are merge operations, so an omitted
        /// property is retained and a previous parent would survive into an unrelated orchestration that
        /// reused the instance ID. Seeding a row and passing its ETag is what makes the true case reach
        /// MergeEntityAsync rather than the insert branch.
        /// </summary>
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task CheckpointWrite_ClearsStaleParentInstanceId_ForBothEtagModes(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            string instanceId = $"etag-{useInstanceTableEtag}-{Guid.NewGuid():N}";

            await this.SeedInstanceRowAsync(instanceId, "stale-parent");
            Assert.AreEqual("stale-parent", await this.GetRawParentInstanceIdAsync(instanceId), "Seeded row should carry the stale parent.");

            // Passing the seeded row's ETag forces the UseInstanceTableEtag=true case down the
            // MergeEntityAsync branch; a null ETag there would insert instead of merging.
            OrchestrationInstanceStatus seeded = await this.GetRawEntityAsync(instanceId);
            var eTags = new OrchestrationETags
            {
                InstanceETag = useInstanceTableEtag ? new ETag(seeded.ETag.ToString()) : (ETag?)null,
            };

            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(CreateExecutionStartedEvent(instanceId, "execution-1", parentInstanceId: null));

            await this.trackingStore.UpdateStateAsync(
                runtimeState,
                runtimeState,
                instanceId,
                "execution-1",
                eTags,
                await this.GetTrackingStoreContextAsync(instanceId));

            Assert.AreEqual(
                string.Empty,
                await this.GetRawParentInstanceIdAsync(instanceId),
                "The checkpoint write must clear the stored property, otherwise merge semantics retain the stale parent.");

            InstanceStatus status = await this.trackingStore.FetchInstanceStatusAsync(instanceId);
            Assert.IsNull(status.State.ParentInstance, "A cleared parent must read back as a null ParentInstance.");
        }

        /// <summary>
        /// The checkpoint write must also persist a non-null parent, for both update modes.
        /// </summary>
        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task CheckpointWrite_PersistsParentInstanceId_ForBothEtagModes(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"{parentInstanceId}:child";

            await this.SeedInstanceRowAsync(childInstanceId, parentInstanceId: null);

            OrchestrationInstanceStatus seeded = await this.GetRawEntityAsync(childInstanceId);
            var eTags = new OrchestrationETags
            {
                InstanceETag = useInstanceTableEtag ? new ETag(seeded.ETag.ToString()) : (ETag?)null,
            };

            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(CreateExecutionStartedEvent(childInstanceId, "execution-1", parentInstanceId));

            await this.trackingStore.UpdateStateAsync(
                runtimeState,
                runtimeState,
                childInstanceId,
                "execution-1",
                eTags,
                await this.GetTrackingStoreContextAsync(childInstanceId));

            Assert.AreEqual(parentInstanceId, await this.GetRawParentInstanceIdAsync(childInstanceId));

            InstanceStatus status = await this.trackingStore.FetchInstanceStatusAsync(childInstanceId);
            Assert.AreEqual(parentInstanceId, status.State.ParentInstance?.OrchestrationInstance.InstanceId);
        }

        /// <summary>
        /// Verifies that the terminal-history repair path persists the parent ID when it recreates an
        /// Instances row that no longer exists. This is the projection-repair case that runs when a worker
        /// fails after writing history but before updating the Instances table, which is common for
        /// sub-orchestrations that complete within a single execution.
        /// </summary>
        [TestMethod]
        public async Task CompletedOrchestrationRepair_PersistsParentInstanceId()
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"{parentInstanceId}:child";

            // No row is seeded: this mirrors a sub-orchestration whose Instances projection was never written.
            await this.trackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(
                childInstanceId,
                executionId: "execution-1",
                runtimeState: CreateCompletedRuntimeState(childInstanceId, "execution-1", parentInstanceId),
                instanceEntityExists: false);

            Assert.AreEqual(parentInstanceId, await this.GetRawParentInstanceIdAsync(childInstanceId));

            InstanceStatus status = await this.trackingStore.FetchInstanceStatusAsync(childInstanceId);
            Assert.AreEqual(parentInstanceId, status.State.ParentInstance?.OrchestrationInstance.InstanceId);
        }

        /// <summary>
        /// Verifies the repair path also overwrites a stale parent on an Instances row that already exists.
        /// </summary>
        [TestMethod]
        public async Task CompletedOrchestrationRepair_OverwritesStaleParentInstanceId()
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"{parentInstanceId}:child";

            await this.SeedInstanceRowAsync(childInstanceId, "stale-parent");

            await this.trackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(
                childInstanceId,
                executionId: "execution-1",
                runtimeState: CreateCompletedRuntimeState(childInstanceId, "execution-1", parentInstanceId),
                instanceEntityExists: true);

            Assert.AreEqual(parentInstanceId, await this.GetRawParentInstanceIdAsync(childInstanceId));
        }

        /// <summary>
        /// Verifies the initial instance-creation write persists a non-null parent. This is the write that
        /// happens when an orchestration is created through the client creation path with a parent supplied
        /// on the ExecutionStartedEvent.
        /// </summary>
        [TestMethod]
        public async Task SetNewExecution_PersistsNonNullParentInstanceId()
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"{parentInstanceId}:child";

            bool created = await this.trackingStore.SetNewExecutionAsync(
                CreateExecutionStartedEvent(childInstanceId, "execution-1", parentInstanceId),
                eTag: null,
                inputPayloadOverride: null);

            Assert.IsTrue(created);
            Assert.AreEqual(parentInstanceId, await this.GetRawParentInstanceIdAsync(childInstanceId));

            InstanceStatus status = await this.trackingStore.FetchInstanceStatusAsync(childInstanceId);
            Assert.AreEqual(parentInstanceId, status.State.ParentInstance?.OrchestrationInstance.InstanceId);
        }

        /// <summary>
        /// Verifies the initial creation write clears a stale parent when the new execution has none. A
        /// re-created instance reuses the partition key, and this write path can replace an earlier row.
        /// </summary>
        [TestMethod]
        public async Task SetNewExecution_ClearsStaleParentInstanceId()
        {
            string instanceId = $"recreate-{Guid.NewGuid():N}";

            await this.SeedInstanceRowAsync(instanceId, "stale-parent");
            OrchestrationInstanceStatus seeded = await this.GetRawEntityAsync(instanceId);

            bool created = await this.trackingStore.SetNewExecutionAsync(
                CreateExecutionStartedEvent(instanceId, "execution-2", parentInstanceId: null),
                eTag: new ETag(seeded.ETag.ToString()),
                inputPayloadOverride: null);

            Assert.IsTrue(created);
            Assert.AreEqual(string.Empty, await this.GetRawParentInstanceIdAsync(instanceId));
        }

        /// <summary>
        /// Seeds an Instances row that already carries a parent ID, simulating a row written by a previous
        /// orchestration that reused the same instance ID.
        /// </summary>
        async Task SeedInstanceRowAsync(string instanceId, string parentInstanceId)
        {
            var entity = new TableEntity(KeySanitation.EscapePartitionKey(instanceId), string.Empty)
            {
                ["Name"] = "SeededOrchestration",
                ["RuntimeStatus"] = OrchestrationStatus.Running.ToString(),
                ["CreatedTime"] = DateTime.UtcNow,
                ["LastUpdatedTime"] = DateTime.UtcNow,
                ["TaskHubName"] = this.taskHubName,
                ["ExecutionId"] = "execution-0",
            };

            // A null parent seeds a row with no property at all, which is also the legacy row shape.
            if (parentInstanceId != null)
            {
                entity[ParentInstanceIdProperty] = parentInstanceId;
            }

            await this.trackingStore.InstancesTable.InsertOrMergeEntityAsync(entity);
        }

        /// <summary>
        /// UpdateStateAsync casts the tracking-store context to a private type, so the only legitimate way
        /// to obtain one is from the production history read, which is exactly what the dispatcher does.
        /// </summary>
        async Task<object> GetTrackingStoreContextAsync(string instanceId)
        {
            OrchestrationHistory history = await this.trackingStore.GetHistoryEventsAsync(instanceId, expectedExecutionId: null);
            return history.TrackingStoreContext;
        }

        async Task<OrchestrationInstanceStatus> GetRawEntityAsync(string instanceId)
        {
            string filter = AzureTableQueryFilter.PartitionKeyEquals(KeySanitation.EscapePartitionKey(instanceId));
            await foreach (OrchestrationInstanceStatus entity in this.trackingStore.InstancesTable.ExecuteQueryAsync<OrchestrationInstanceStatus>(filter))
            {
                return entity;
            }

            return null;
        }

        /// <summary>
        /// Reads the stored property directly rather than the converted state, so that a value which was
        /// merely left untouched by a merge is still visible to the assertion.
        /// </summary>
        async Task<string> GetRawParentInstanceIdAsync(string instanceId)
        {
            OrchestrationInstanceStatus entity = await this.GetRawEntityAsync(instanceId);
            Assert.IsNotNull(entity, $"Expected an Instances row for '{instanceId}'.");
            return entity.ParentInstanceId;
        }

        static ExecutionStartedEvent CreateExecutionStartedEvent(string instanceId, string executionId, string parentInstanceId)
        {
            var executionStartedEvent = new ExecutionStartedEvent(-1, "input")
            {
                Name = "TestOrchestration",
                Version = string.Empty,
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                },
            };

            if (parentInstanceId != null)
            {
                executionStartedEvent.ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = parentInstanceId,
                        ExecutionId = "parent-execution",
                    },
                    Name = "ParentOrchestration",
                    Version = string.Empty,
                    TaskScheduleId = 1,
                };
            }

            return executionStartedEvent;
        }

        static OrchestrationRuntimeState CreateCompletedRuntimeState(string instanceId, string executionId, string parentInstanceId)
        {
            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(CreateExecutionStartedEvent(instanceId, executionId, parentInstanceId));
            runtimeState.AddEvent(new ExecutionCompletedEvent(-1, "output", OrchestrationStatus.Completed));
            return runtimeState;
        }
    }
}
