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
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RewindOutputTrackingStoreTests
    {
        const string PreservedProperty = "PreservedProperty";

        string taskHubName;
        AzureTableTrackingStore trackingStore;
        RecordingRequestHandler tableRequestRecorder;
        TransportClientProvider<TableServiceClient, TableClientOptions> tableClientProvider;

        [TestInitialize]
        public async Task Initialize()
        {
            this.taskHubName = "rewind" + Guid.NewGuid().ToString("N").Substring(0, 9);
            AzureStorageOrchestrationServiceSettings settings =
                TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = this.taskHubName;
            var defaultProvider = settings.StorageAccountClientProvider;
            this.tableRequestRecorder = new RecordingRequestHandler();
            this.tableClientProvider =
                new TransportClientProvider<TableServiceClient, TableClientOptions>(
                    defaultProvider.Table,
                    this.tableRequestRecorder);
            settings.StorageAccountClientProvider = new StorageAccountClientProvider(
                defaultProvider.Blob,
                defaultProvider.Queue,
                this.tableClientProvider);

            var azureStorageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(
                settings,
                azureStorageClient,
                $"{this.taskHubName}-largemessages".ToLowerInvariant());
            this.trackingStore = new AzureTableTrackingStore(azureStorageClient, messageManager);
            await this.trackingStore.CreateAsync();
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            try
            {
                if (this.trackingStore != null)
                {
                    await this.trackingStore.DeleteAsync();
                }
            }
            finally
            {
                this.tableClientProvider?.Dispose();
                this.tableRequestRecorder?.Dispose();
            }
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_RemovesPersistedOutput()
        {
            string instanceId = $"output-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: "old failure");

            TableEntity failed = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual("old failure", failed["Output"]);

            await this.trackingStore.UpdateStatusForRewindAsync(
                instanceId,
                "execution-1",
                failed.ETag);

            TableEntity rewound = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), rewound["RuntimeStatus"]);
            Assert.IsFalse(rewound.ContainsKey("Output"));
            Assert.AreEqual("preserve me", rewound[PreservedProperty]);
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_IsIdempotentWhenOutputIsMissing()
        {
            string instanceId = $"missing-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: null);

            TableEntity failed = await this.GetRawEntityAsync(instanceId);
            await this.trackingStore.UpdateStatusForRewindAsync(
                instanceId,
                "execution-1",
                failed.ETag);
            TableEntity pending = await this.GetRawEntityAsync(instanceId);
            await this.trackingStore.UpdateStatusForRewindAsync(
                instanceId,
                "execution-1",
                pending.ETag);

            TableEntity rewound = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), rewound["RuntimeStatus"]);
            Assert.IsFalse(rewound.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.Failed)]
        public async Task TerminalWriteAfterRewind_PersistsNewOutput(OrchestrationStatus terminalStatus)
        {
            string instanceId = $"complete-{Guid.NewGuid():N}";
            const string ExecutionId = "execution-1";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: "old failure");
            TableEntity failed = await this.GetRawEntityAsync(instanceId);
            await this.trackingStore.UpdateStatusForRewindAsync(instanceId, ExecutionId, failed.ETag);

            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(CreateExecutionStartedEvent(instanceId, ExecutionId));
            runtimeState.AddEvent(new ExecutionCompletedEvent(-1, "new output", terminalStatus));

            await this.trackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(
                instanceId,
                ExecutionId,
                runtimeState,
                instanceEntityExists: true);

            TableEntity completed = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(terminalStatus.ToString(), completed["RuntimeStatus"]);
            Assert.AreEqual("new output", completed["Output"]);
        }

        [TestMethod]
        public async Task SetNewExecution_ReplacesPersistedOutput()
        {
            string instanceId = $"reuse-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Completed, output: "old output");
            TableEntity existing = await this.GetRawEntityAsync(instanceId);

            bool created = await this.trackingStore.SetNewExecutionAsync(
                CreateExecutionStartedEvent(instanceId, "execution-2"),
                existing.ETag,
                inputPayloadOverride: null);

            Assert.IsTrue(created);
            TableEntity pending = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), pending["RuntimeStatus"]);
            Assert.IsFalse(pending.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Pending, true, false, false, true)]
        [DataRow(OrchestrationStatus.Pending, false, false, false, false)]
        [DataRow(OrchestrationStatus.Pending, true, false, true, false)]
        [DataRow(OrchestrationStatus.Pending, true, true, false, false)]
        [DataRow(OrchestrationStatus.Running, true, false, true, false)]
        [DataRow(OrchestrationStatus.Suspended, true, false, true, false)]
        [DataRow(OrchestrationStatus.Completed, true, false, true, false)]
        [DataRow(OrchestrationStatus.Canceled, true, false, true, false)]
        [DataRow(OrchestrationStatus.Terminated, true, false, true, false)]
        [DataRow(OrchestrationStatus.ContinuedAsNew, true, false, true, false)]
        [DataRow(OrchestrationStatus.Failed, true, false, true, true)]
        [DataRow(OrchestrationStatus.Failed, true, true, true, false)]
        public async Task RewindHistory_RediscoversOnlyCurrentRewoundChild(
            OrchestrationStatus childStatus,
            bool markerMatchesCurrentExecution,
            bool hasCurrentFailure,
            bool hasOutput,
            bool expectsChildTarget)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"child-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string ChildExecutionId = "child-execution";
            const int TaskScheduledId = 0;

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000000",
                ParentExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000001",
                ParentExecutionId,
                EventType.SubOrchestrationInstanceCreated,
                eventId: TaskScheduledId,
                childInstanceId: childInstanceId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000002",
                ParentExecutionId,
                EventType.GenericEvent,
                taskScheduledId: TaskScheduledId,
                reason: "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000003",
                ParentExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            await this.SeedInstanceRowAsync(
                childInstanceId,
                childStatus,
                output: hasOutput ? "child output" : null,
                executionId: ChildExecutionId);
            await this.SeedExecutionStartedRowAsync(
                childInstanceId,
                ChildExecutionId,
                parentInstanceId,
                ParentExecutionId,
                TaskScheduledId);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000000",
                ChildExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000001",
                markerMatchesCurrentExecution ? ChildExecutionId : "old-child-execution",
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);
            if (hasCurrentFailure)
            {
                await this.SeedHistoryRowAsync(
                    childInstanceId,
                    "0000000000000002",
                    ChildExecutionId,
                    EventType.ExecutionCompleted,
                    reason: "new failure",
                    orchestrationStatus: OrchestrationStatus.Failed);
            }

            TableEntity childBefore = await this.GetRawEntityAsync(childInstanceId);
            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();
            TableEntity childAfter = await this.GetRawEntityAsync(childInstanceId);

            CollectionAssert.AreEqual(
                expectsChildTarget ? new[] { childInstanceId } : new[] { parentInstanceId },
                targets);
            Assert.AreEqual(
                expectsChildTarget ? OrchestrationStatus.Pending.ToString() : childStatus.ToString(),
                childAfter["RuntimeStatus"]);
            Assert.AreEqual(
                expectsChildTarget,
                childBefore.ETag != childAfter.ETag,
                "Only a proven stranded child should receive a new write fence.");
            if (expectsChildTarget || !hasOutput)
            {
                Assert.IsFalse(childAfter.ContainsKey("Output"));
            }
            else
            {
                Assert.AreEqual("child output", childAfter["Output"]);
            }
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Running, false)]
        [DataRow(OrchestrationStatus.Suspended, false)]
        [DataRow(OrchestrationStatus.Failed, true)]
        public async Task RewindHistory_TraversesActiveIntermediateToPendingDescendant(
            OrchestrationStatus intermediateStatus,
            bool resetsIntermediate)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string intermediateInstanceId = $"intermediate-{Guid.NewGuid():N}";
            string leafInstanceId = $"leaf-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string IntermediateExecutionId = "intermediate-execution";
            const string LeafExecutionId = "leaf-execution";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                intermediateInstanceId);

            await this.SeedInstanceRowAsync(
                intermediateInstanceId,
                intermediateStatus,
                output: "intermediate progress",
                executionId: IntermediateExecutionId);
            await this.SeedExecutionStartedRowAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedRewoundParentHistoryAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                leafInstanceId);

            await this.SeedInstanceRowAsync(
                leafInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: LeafExecutionId);
            await this.SeedExecutionStartedRowAsync(
                leafInstanceId,
                LeafExecutionId,
                intermediateInstanceId,
                IntermediateExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                leafInstanceId,
                "0000000000000000",
                LeafExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                leafInstanceId,
                "0000000000000001",
                LeafExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            TableEntity intermediateBefore =
                await this.GetRawEntityAsync(intermediateInstanceId);
            TableEntity leafBefore = await this.GetRawEntityAsync(leafInstanceId);

            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            CollectionAssert.AreEqual(new[] { leafInstanceId }, targets);
            TableEntity parentAfter = await this.GetRawEntityAsync(parentInstanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), parentAfter["RuntimeStatus"]);
            Assert.IsFalse(parentAfter.ContainsKey("Output"));

            TableEntity intermediateAfter =
                await this.GetRawEntityAsync(intermediateInstanceId);
            Assert.AreEqual(resetsIntermediate, intermediateBefore.ETag != intermediateAfter.ETag);
            Assert.AreEqual(
                resetsIntermediate ? OrchestrationStatus.Pending.ToString() : intermediateStatus.ToString(),
                intermediateAfter["RuntimeStatus"]);
            if (resetsIntermediate)
            {
                Assert.IsFalse(intermediateAfter.ContainsKey("Output"));
            }
            else
            {
                Assert.AreEqual("intermediate progress", intermediateAfter["Output"]);
            }

            TableEntity leafAfter = await this.GetRawEntityAsync(leafInstanceId);
            Assert.AreNotEqual(leafBefore.ETag, leafAfter.ETag);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), leafAfter["RuntimeStatus"]);
            Assert.IsFalse(leafAfter.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Running)]
        [DataRow(OrchestrationStatus.Suspended)]
        public async Task RewindHistory_ActiveIntermediateWithoutRecoverableDescendantFallsBackToParent(
            OrchestrationStatus intermediateStatus)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string intermediateInstanceId = $"intermediate-{Guid.NewGuid():N}";
            string completedLeafInstanceId = $"leaf-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string IntermediateExecutionId = "intermediate-execution";
            const string LeafExecutionId = "leaf-execution";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                intermediateInstanceId);

            await this.SeedInstanceRowAsync(
                intermediateInstanceId,
                intermediateStatus,
                output: "intermediate progress",
                executionId: IntermediateExecutionId);
            await this.SeedExecutionStartedRowAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedRewoundParentHistoryAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                completedLeafInstanceId);

            await this.SeedInstanceRowAsync(
                completedLeafInstanceId,
                OrchestrationStatus.Completed,
                output: "completed output",
                executionId: LeafExecutionId);
            await this.SeedExecutionStartedRowAsync(
                completedLeafInstanceId,
                LeafExecutionId,
                intermediateInstanceId,
                IntermediateExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                completedLeafInstanceId,
                "0000000000000000",
                LeafExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                completedLeafInstanceId,
                "0000000000000001",
                LeafExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            TableEntity intermediateBefore =
                await this.GetRawEntityAsync(intermediateInstanceId);
            TableEntity completedLeafBefore =
                await this.GetRawEntityAsync(completedLeafInstanceId);

            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            CollectionAssert.AreEqual(new[] { parentInstanceId }, targets);
            TableEntity parentAfter = await this.GetRawEntityAsync(parentInstanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), parentAfter["RuntimeStatus"]);
            Assert.IsFalse(parentAfter.ContainsKey("Output"));

            TableEntity intermediateAfter =
                await this.GetRawEntityAsync(intermediateInstanceId);
            Assert.AreEqual(intermediateBefore.ETag, intermediateAfter.ETag);
            Assert.AreEqual(intermediateStatus.ToString(), intermediateAfter["RuntimeStatus"]);
            Assert.AreEqual("intermediate progress", intermediateAfter["Output"]);

            TableEntity completedLeafAfter =
                await this.GetRawEntityAsync(completedLeafInstanceId);
            Assert.AreEqual(completedLeafBefore.ETag, completedLeafAfter.ETag);
            Assert.AreEqual(OrchestrationStatus.Completed.ToString(), completedLeafAfter["RuntimeStatus"]);
            Assert.AreEqual("completed output", completedLeafAfter["Output"]);
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Running, OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Running, OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.Suspended, OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Suspended, OrchestrationStatus.Completed)]
        public async Task RewindHistory_DoesNotTraverseActiveIntermediateWithFreshCompletion(
            OrchestrationStatus intermediateStatus,
            OrchestrationStatus completionStatus)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string intermediateInstanceId = $"intermediate-{Guid.NewGuid():N}";
            string oldLeafInstanceId = $"leaf-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string IntermediateExecutionId = "intermediate-execution";
            const string LeafExecutionId = "leaf-execution";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                intermediateInstanceId);

            await this.SeedInstanceRowAsync(
                intermediateInstanceId,
                intermediateStatus,
                output: "intermediate progress",
                executionId: IntermediateExecutionId);
            await this.SeedExecutionStartedRowAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedRewoundParentHistoryAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                oldLeafInstanceId);
            await this.SeedHistoryRowAsync(
                intermediateInstanceId,
                "0000000000000004",
                IntermediateExecutionId,
                EventType.ExecutionCompleted,
                reason: "new completion",
                orchestrationStatus: completionStatus);

            await this.SeedInstanceRowAsync(
                oldLeafInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: LeafExecutionId);
            await this.SeedExecutionStartedRowAsync(
                oldLeafInstanceId,
                LeafExecutionId,
                intermediateInstanceId,
                IntermediateExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                oldLeafInstanceId,
                "0000000000000000",
                LeafExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                oldLeafInstanceId,
                "0000000000000001",
                LeafExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            TableEntity intermediateBefore =
                await this.GetRawEntityAsync(intermediateInstanceId);
            TableEntity oldLeafBefore =
                await this.GetRawEntityAsync(oldLeafInstanceId);

            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            CollectionAssert.AreEqual(new[] { parentInstanceId }, targets);
            TableEntity intermediateAfter =
                await this.GetRawEntityAsync(intermediateInstanceId);
            Assert.AreEqual(intermediateBefore.ETag, intermediateAfter.ETag);
            Assert.AreEqual(intermediateStatus.ToString(), intermediateAfter["RuntimeStatus"]);
            Assert.AreEqual("intermediate progress", intermediateAfter["Output"]);

            TableEntity oldLeafAfter = await this.GetRawEntityAsync(oldLeafInstanceId);
            Assert.AreEqual(oldLeafBefore.ETag, oldLeafAfter.ETag);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), oldLeafAfter["RuntimeStatus"]);
            Assert.IsFalse(oldLeafAfter.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.ContinuedAsNew)]
        [DataRow(OrchestrationStatus.Terminated)]
        public async Task RewindHistory_DoesNotTraverseTerminalOrReusedIntermediate(
            OrchestrationStatus intermediateStatus)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string intermediateInstanceId = $"intermediate-{Guid.NewGuid():N}";
            string strandedLeafInstanceId = $"leaf-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string IntermediateExecutionId = "intermediate-execution";
            const string LeafExecutionId = "leaf-execution";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                intermediateInstanceId);

            await this.SeedInstanceRowAsync(
                intermediateInstanceId,
                intermediateStatus,
                output: "new execution output",
                executionId: IntermediateExecutionId);
            await this.SeedExecutionStartedRowAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                parentInstanceId: "unrelated-parent",
                parentExecutionId: "unrelated-parent-execution",
                taskScheduleId: 0);
            await this.SeedRewoundParentHistoryAsync(
                intermediateInstanceId,
                "old-intermediate-execution",
                strandedLeafInstanceId);

            await this.SeedInstanceRowAsync(
                strandedLeafInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: LeafExecutionId);
            await this.SeedExecutionStartedRowAsync(
                strandedLeafInstanceId,
                LeafExecutionId,
                intermediateInstanceId,
                "old-intermediate-execution",
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                strandedLeafInstanceId,
                "0000000000000000",
                LeafExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                strandedLeafInstanceId,
                "0000000000000001",
                LeafExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            TableEntity intermediateBefore =
                await this.GetRawEntityAsync(intermediateInstanceId);
            TableEntity strandedLeafBefore =
                await this.GetRawEntityAsync(strandedLeafInstanceId);

            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            CollectionAssert.AreEqual(new[] { parentInstanceId }, targets);
            TableEntity intermediateAfter =
                await this.GetRawEntityAsync(intermediateInstanceId);
            Assert.AreEqual(intermediateBefore.ETag, intermediateAfter.ETag);
            Assert.AreEqual(intermediateStatus.ToString(), intermediateAfter["RuntimeStatus"]);
            Assert.AreEqual("new execution output", intermediateAfter["Output"]);

            TableEntity strandedLeafAfter =
                await this.GetRawEntityAsync(strandedLeafInstanceId);
            Assert.AreEqual(strandedLeafBefore.ETag, strandedLeafAfter.ETag);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), strandedLeafAfter["RuntimeStatus"]);
            Assert.IsFalse(strandedLeafAfter.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(false, true, true)]
        [DataRow(true, false, true)]
        [DataRow(true, true, false)]
        public async Task RewindHistory_DoesNotRecoverChildOwnedByDifferentParentEdge(
            bool parentInstanceMatches,
            bool parentExecutionMatches,
            bool taskScheduleMatches)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"child-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string ChildExecutionId = "reused-child-execution";
            const int TaskScheduledId = 0;

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                childInstanceId);

            await this.SeedInstanceRowAsync(
                childInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: ChildExecutionId);
            await this.SeedExecutionStartedRowAsync(
                childInstanceId,
                ChildExecutionId,
                parentInstanceMatches ? parentInstanceId : "unrelated-parent",
                parentExecutionMatches ? ParentExecutionId : "unrelated-parent-execution",
                taskScheduleMatches ? TaskScheduledId : TaskScheduledId + 1);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000000",
                ChildExecutionId,
                EventType.OrchestratorStarted);
            const string RewoundCompletionRowKey = "0000000000000001";
            await this.SeedHistoryRowAsync(
                childInstanceId,
                RewoundCompletionRowKey,
                ChildExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            TableEntity childBefore = await this.GetRawEntityAsync(childInstanceId);
            TableEntity completionBefore =
                await this.GetRawHistoryEntityAsync(childInstanceId, RewoundCompletionRowKey);

            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            CollectionAssert.AreEqual(new[] { parentInstanceId }, targets);
            TableEntity childAfter = await this.GetRawEntityAsync(childInstanceId);
            Assert.AreEqual(childBefore.ETag, childAfter.ETag);
            Assert.AreEqual(ChildExecutionId, childAfter["ExecutionId"]);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), childAfter["RuntimeStatus"]);
            Assert.IsFalse(childAfter.ContainsKey("Output"));

            TableEntity completionAfter =
                await this.GetRawHistoryEntityAsync(childInstanceId, RewoundCompletionRowKey);
            Assert.AreEqual(completionBefore.ETag, completionAfter.ETag);
            Assert.AreEqual(nameof(EventType.GenericEvent), completionAfter["EventType"]);
            Assert.AreEqual(
                "Rewound: " + nameof(EventType.ExecutionCompleted),
                completionAfter["Reason"]);
        }

        [TestMethod]
        public async Task RewindHistory_DeduplicatesRepeatedRecoveredEdgesBeforeRecursing()
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string intermediateInstanceId = $"intermediate-{Guid.NewGuid():N}";
            string leafInstanceId = $"leaf-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string IntermediateExecutionId = "intermediate-execution";
            const string LeafExecutionId = "leaf-execution";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedRewoundParentHistoryAsync(
                parentInstanceId,
                ParentExecutionId,
                intermediateInstanceId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000004",
                ParentExecutionId,
                EventType.GenericEvent,
                taskScheduledId: 0,
                reason: "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));

            await this.SeedInstanceRowAsync(
                intermediateInstanceId,
                OrchestrationStatus.Running,
                output: "intermediate progress",
                executionId: IntermediateExecutionId);
            await this.SeedExecutionStartedRowAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedRewoundParentHistoryAsync(
                intermediateInstanceId,
                IntermediateExecutionId,
                leafInstanceId);
            await this.SeedHistoryRowAsync(
                intermediateInstanceId,
                "0000000000000004",
                IntermediateExecutionId,
                EventType.GenericEvent,
                taskScheduledId: 0,
                reason: "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));

            await this.SeedInstanceRowAsync(
                leafInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: LeafExecutionId);
            await this.SeedExecutionStartedRowAsync(
                leafInstanceId,
                LeafExecutionId,
                intermediateInstanceId,
                IntermediateExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                leafInstanceId,
                "0000000000000000",
                LeafExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                leafInstanceId,
                "0000000000000001",
                LeafExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            this.tableRequestRecorder.Clear();
            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            int leafOrchestratorQueries = this.tableRequestRecorder.CountRequests(request =>
                request.Method == HttpMethod.Get &&
                request.Uri.AbsolutePath.IndexOf(
                    this.trackingStore.HistoryTable.Name,
                    StringComparison.OrdinalIgnoreCase) >= 0 &&
                Uri.UnescapeDataString(request.Uri.Query).Contains(leafInstanceId) &&
                Uri.UnescapeDataString(request.Uri.Query).Contains(nameof(EventType.OrchestratorStarted)));
            int leafResets = this.tableRequestRecorder.CountRequests(request =>
                request.Method == HttpMethod.Put &&
                request.Uri.AbsolutePath.IndexOf(
                    this.trackingStore.InstancesTable.Name,
                    StringComparison.OrdinalIgnoreCase) >= 0 &&
                Uri.UnescapeDataString(request.Uri.AbsoluteUri).Contains(leafInstanceId));

            Assert.AreEqual(
                1,
                leafOrchestratorQueries,
                $"Expected one leaf traversal, observed {leafOrchestratorQueries} queries, {leafResets} resets, and {targets.Length} targets.");
            Assert.AreEqual(1, leafResets);
            CollectionAssert.AreEqual(new[] { leafInstanceId }, targets);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task RewindHistory_DeduplicatesMixedLiveAndRecoveredEdge(bool liveFailureFirst)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"child-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string ChildExecutionId = "child-execution";
            const string FirstFailureRowKey = "0000000000000002";
            const string SecondFailureRowKey = "0000000000000003";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000000",
                ParentExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000001",
                ParentExecutionId,
                EventType.SubOrchestrationInstanceCreated,
                eventId: 0,
                childInstanceId: childInstanceId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                FirstFailureRowKey,
                ParentExecutionId,
                liveFailureFirst ? EventType.SubOrchestrationInstanceFailed : EventType.GenericEvent,
                taskScheduledId: 0,
                reason: liveFailureFirst
                    ? "current failure"
                    : "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                SecondFailureRowKey,
                ParentExecutionId,
                liveFailureFirst ? EventType.GenericEvent : EventType.SubOrchestrationInstanceFailed,
                taskScheduledId: 0,
                reason: liveFailureFirst
                    ? "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed)
                    : "current failure");
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000004",
                ParentExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            await this.SeedInstanceRowAsync(
                childInstanceId,
                OrchestrationStatus.Pending,
                output: null,
                executionId: ChildExecutionId);
            await this.SeedExecutionStartedRowAsync(
                childInstanceId,
                ChildExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000000",
                ChildExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000001",
                ChildExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            this.tableRequestRecorder.Clear();
            string[] targets = (await this.trackingStore
                .RewindHistoryAsync(parentInstanceId)
                .ToListAsync())
                .ToArray();

            int childOrchestratorQueries = this.tableRequestRecorder.CountRequests(request =>
                request.Method == HttpMethod.Get &&
                request.Uri.AbsolutePath.IndexOf(
                    this.trackingStore.HistoryTable.Name,
                    StringComparison.OrdinalIgnoreCase) >= 0 &&
                Uri.UnescapeDataString(request.Uri.Query).Contains(childInstanceId) &&
                Uri.UnescapeDataString(request.Uri.Query).Contains(nameof(EventType.OrchestratorStarted)));
            int childResets = this.tableRequestRecorder.CountRequests(request =>
                request.Method == HttpMethod.Put &&
                request.Uri.AbsolutePath.IndexOf(
                    this.trackingStore.InstancesTable.Name,
                    StringComparison.OrdinalIgnoreCase) >= 0 &&
                Uri.UnescapeDataString(request.Uri.AbsoluteUri).Contains(childInstanceId));

            int expectedChildRewinds = liveFailureFirst ? 1 : 2;
            Assert.AreEqual(expectedChildRewinds, childOrchestratorQueries);
            Assert.AreEqual(expectedChildRewinds, childResets);
            CollectionAssert.AreEqual(new[] { childInstanceId }, targets);

            string liveFailureRowKey = liveFailureFirst ? FirstFailureRowKey : SecondFailureRowKey;
            TableEntity liveFailure =
                await this.GetRawHistoryEntityAsync(parentInstanceId, liveFailureRowKey);
            Assert.AreEqual(nameof(EventType.GenericEvent), liveFailure["EventType"]);
            Assert.AreEqual(
                "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed),
                liveFailure["Reason"]);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task RewindHistory_LiveFailureProcessesFreshCheckpointAfterRecoveredEdge(
            bool recoveredEdgeProducedTarget)
        {
            string parentInstanceId = $"parent-{Guid.NewGuid():N}";
            string childInstanceId = $"child-{Guid.NewGuid():N}";
            const string ParentExecutionId = "parent-execution";
            const string ChildExecutionId = "child-execution";
            const string FreshCompletionRowKey = "0000000000000002";

            await this.SeedInstanceRowAsync(
                parentInstanceId,
                OrchestrationStatus.Failed,
                output: "parent failure",
                executionId: ParentExecutionId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000000",
                ParentExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000001",
                ParentExecutionId,
                EventType.SubOrchestrationInstanceCreated,
                eventId: 0,
                childInstanceId: childInstanceId);
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000002",
                ParentExecutionId,
                EventType.GenericEvent,
                taskScheduledId: 0,
                reason: "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000003",
                ParentExecutionId,
                EventType.SubOrchestrationInstanceFailed,
                taskScheduledId: 0,
                reason: "current failure");
            await this.SeedHistoryRowAsync(
                parentInstanceId,
                "0000000000000004",
                ParentExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            await this.SeedInstanceRowAsync(
                childInstanceId,
                recoveredEdgeProducedTarget
                    ? OrchestrationStatus.Pending
                    : OrchestrationStatus.Running,
                output: null,
                executionId: ChildExecutionId);
            await this.SeedExecutionStartedRowAsync(
                childInstanceId,
                ChildExecutionId,
                parentInstanceId,
                ParentExecutionId,
                taskScheduleId: 0);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000000",
                ChildExecutionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                childInstanceId,
                "0000000000000001",
                ChildExecutionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);

            this.tableRequestRecorder.Arm(request =>
                request.Method == HttpMethod.Put &&
                request.RequestUri.AbsolutePath.IndexOf(
                    this.trackingStore.HistoryTable.Name,
                    StringComparison.OrdinalIgnoreCase) >= 0 &&
                Uri.UnescapeDataString(request.RequestUri.AbsoluteUri).Contains(parentInstanceId));
            Task<string[]> rewind = RewindAsync();

            try
            {
                await this.tableRequestRecorder.WaitUntilBlockedAsync();

                TableEntity runningChild = await this.GetRawEntityAsync(childInstanceId);
                runningChild["RuntimeStatus"] = OrchestrationStatus.Running.ToString();
                runningChild["Output"] = "fresh in-flight output";
                await this.trackingStore.InstancesTable.ReplaceEntityAsync(
                    runningChild,
                    runningChild.ETag);
                await this.SeedHistoryRowAsync(
                    childInstanceId,
                    FreshCompletionRowKey,
                    ChildExecutionId,
                    EventType.ExecutionCompleted,
                    reason: "fresh failure",
                    orchestrationStatus: OrchestrationStatus.Failed);

                // Count only work performed after the fresh child checkpoint is visible.
                this.tableRequestRecorder.Clear();
                this.tableRequestRecorder.Release();

                string[] targets = await rewind;
                int childOrchestratorQueries = this.tableRequestRecorder.CountRequests(request =>
                    request.Method == HttpMethod.Get &&
                    request.Uri.AbsolutePath.IndexOf(
                        this.trackingStore.HistoryTable.Name,
                        StringComparison.OrdinalIgnoreCase) >= 0 &&
                    Uri.UnescapeDataString(request.Uri.Query).Contains(childInstanceId) &&
                    Uri.UnescapeDataString(request.Uri.Query).Contains(nameof(EventType.OrchestratorStarted)));
                int childResets = this.tableRequestRecorder.CountRequests(request =>
                    request.Method == HttpMethod.Put &&
                    request.Uri.AbsolutePath.IndexOf(
                        this.trackingStore.InstancesTable.Name,
                        StringComparison.OrdinalIgnoreCase) >= 0 &&
                    Uri.UnescapeDataString(request.Uri.AbsoluteUri).Contains(childInstanceId));

                Assert.AreEqual(1, childOrchestratorQueries);
                Assert.AreEqual(1, childResets);
                CollectionAssert.AreEqual(new[] { childInstanceId }, targets);

                TableEntity childAfter = await this.GetRawEntityAsync(childInstanceId);
                Assert.AreEqual(OrchestrationStatus.Pending.ToString(), childAfter["RuntimeStatus"]);
                Assert.IsFalse(childAfter.ContainsKey("Output"));

                TableEntity freshCompletion =
                    await this.GetRawHistoryEntityAsync(childInstanceId, FreshCompletionRowKey);
                Assert.AreEqual(nameof(EventType.GenericEvent), freshCompletion["EventType"]);
                Assert.AreEqual(
                    "Rewound: " + nameof(EventType.ExecutionCompleted),
                    freshCompletion["Reason"]);
            }
            finally
            {
                this.tableRequestRecorder.Release();
            }

            async Task<string[]> RewindAsync()
            {
                return (await this.trackingStore
                    .RewindHistoryAsync(parentInstanceId)
                    .ToListAsync())
                    .ToArray();
            }
        }

        async Task SeedRewoundParentHistoryAsync(
            string instanceId,
            string executionId,
            string childInstanceId)
        {
            await this.SeedHistoryRowAsync(
                instanceId,
                "0000000000000000",
                executionId,
                EventType.OrchestratorStarted);
            await this.SeedHistoryRowAsync(
                instanceId,
                "0000000000000001",
                executionId,
                EventType.SubOrchestrationInstanceCreated,
                eventId: 0,
                childInstanceId: childInstanceId);
            await this.SeedHistoryRowAsync(
                instanceId,
                "0000000000000002",
                executionId,
                EventType.GenericEvent,
                taskScheduledId: 0,
                reason: "Rewound: " + nameof(EventType.SubOrchestrationInstanceFailed));
            await this.SeedHistoryRowAsync(
                instanceId,
                "0000000000000003",
                executionId,
                EventType.GenericEvent,
                reason: "Rewound: " + nameof(EventType.ExecutionCompleted),
                orchestrationStatus: OrchestrationStatus.Failed);
        }

        async Task SeedExecutionStartedRowAsync(
            string instanceId,
            string executionId,
            string parentInstanceId,
            string parentExecutionId,
            int taskScheduleId)
        {
            var executionStarted = new ExecutionStartedEvent(-1, "input")
            {
                Name = "TestOrchestration",
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                },
                ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = parentInstanceId,
                        ExecutionId = parentExecutionId,
                    },
                    TaskScheduleId = taskScheduleId,
                },
            };
            TableEntity entity = TableEntityConverter.Serialize(executionStarted);
            entity.PartitionKey = KeySanitation.EscapePartitionKey(instanceId);
            entity.RowKey = "execution-start";
            entity[nameof(OrchestrationInstance.ExecutionId)] = executionId;
            await this.trackingStore.HistoryTable.InsertEntityAsync(entity);
        }

        async Task SeedInstanceRowAsync(
            string instanceId,
            OrchestrationStatus status,
            string output,
            string executionId = "execution-1")
        {
            var entity = new TableEntity(KeySanitation.EscapePartitionKey(instanceId), string.Empty)
            {
                ["Name"] = "TestOrchestration",
                ["RuntimeStatus"] = status.ToString(),
                ["CreatedTime"] = DateTime.UtcNow,
                ["LastUpdatedTime"] = DateTime.UtcNow,
                ["TaskHubName"] = this.taskHubName,
                ["ExecutionId"] = executionId,
                [PreservedProperty] = "preserve me",
            };

            if (output != null)
            {
                entity["Output"] = output;
            }

            await this.trackingStore.InstancesTable.InsertEntityAsync(entity);
        }

        async Task SeedHistoryRowAsync(
            string instanceId,
            string rowKey,
            string executionId,
            EventType eventType,
            int? eventId = null,
            int? taskScheduledId = null,
            string childInstanceId = null,
            string reason = null,
            OrchestrationStatus? orchestrationStatus = null)
        {
            var entity = new TableEntity(KeySanitation.EscapePartitionKey(instanceId), rowKey)
            {
                [nameof(OrchestrationInstance.ExecutionId)] = executionId,
                [nameof(HistoryEvent.EventType)] = eventType.ToString(),
            };

            if (eventId.HasValue)
            {
                entity[nameof(HistoryEvent.EventId)] = eventId.Value;
            }

            if (taskScheduledId.HasValue)
            {
                entity[nameof(TaskCompletedEvent.TaskScheduledId)] = taskScheduledId.Value;
            }

            if (childInstanceId != null)
            {
                entity[nameof(OrchestrationInstance.InstanceId)] = childInstanceId;
            }

            if (reason != null)
            {
                entity[nameof(TaskFailedEvent.Reason)] = reason;
            }

            if (orchestrationStatus.HasValue)
            {
                entity[nameof(ExecutionCompletedEvent.OrchestrationStatus)] =
                    orchestrationStatus.Value.ToString();
            }

            await this.trackingStore.HistoryTable.InsertEntityAsync(entity);
        }

        async Task<TableEntity> GetRawEntityAsync(string instanceId)
        {
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and " +
                $"{AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), string.Empty)}";
            return await this.trackingStore.InstancesTable
                .ExecuteQueryAsync<TableEntity>(filter, 1)
                .FirstOrDefaultAsync();
        }

        async Task<TableEntity> GetRawHistoryEntityAsync(string instanceId, string rowKey)
        {
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and " +
                $"{AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), rowKey)}";
            return await this.trackingStore.HistoryTable
                .ExecuteQueryAsync<TableEntity>(filter, 1)
                .FirstOrDefaultAsync();
        }

        static ExecutionStartedEvent CreateExecutionStartedEvent(string instanceId, string executionId)
        {
            return new ExecutionStartedEvent(-1, "input")
            {
                Name = "TestOrchestration",
                Version = string.Empty,
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                },
            };
        }

        sealed class TransportClientProvider<TClient, TOptions> :
            IStorageServiceClientProvider<TClient, TOptions>,
            IDisposable
            where TOptions : ClientOptions
        {
            readonly IStorageServiceClientProvider<TClient, TOptions> inner;
            readonly HttpClientTransport transport;

            public TransportClientProvider(
                IStorageServiceClientProvider<TClient, TOptions> inner,
                HttpMessageHandler handler)
            {
                this.inner = inner;
                this.transport = new HttpClientTransport(
                    new HttpClient(handler, disposeHandler: false));
            }

            public TOptions CreateOptions()
            {
                TOptions options = this.inner.CreateOptions();
                options.Transport = this.transport;
                return options;
            }

            public TClient CreateClient(TOptions options) => this.inner.CreateClient(options);

            public void Dispose()
            {
                this.transport.Dispose();
            }
        }

        sealed class RecordingRequestHandler : DelegatingHandler
        {
            readonly object sync = new object();
            readonly List<RecordedRequest> requests = new List<RecordedRequest>();
            Func<HttpRequestMessage, bool> barrierPredicate;
            TaskCompletionSource<object> blocked;
            TaskCompletionSource<object> release;
            bool barrierClaimed;

            public RecordingRequestHandler()
                : base(new HttpClientHandler())
            {
            }

            public void Clear()
            {
                lock (this.sync)
                {
                    this.requests.Clear();
                }
            }

            public void Arm(Func<HttpRequestMessage, bool> predicate)
            {
                lock (this.sync)
                {
                    this.barrierPredicate = predicate;
                    this.blocked = new TaskCompletionSource<object>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    this.release = new TaskCompletionSource<object>(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                    this.barrierClaimed = false;
                }
            }

            public async Task WaitUntilBlockedAsync()
            {
                Task blockedTask;
                lock (this.sync)
                {
                    blockedTask = this.blocked.Task;
                }

                Task completedTask = await Task.WhenAny(
                    blockedTask,
                    Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.AreSame(
                    blockedTask,
                    completedTask,
                    "The expected table request did not reach the barrier.");
                await blockedTask;
            }

            public void Release()
            {
                lock (this.sync)
                {
                    this.release?.TrySetResult(null);
                }
            }

            public int CountRequests(Func<RecordedRequest, bool> predicate)
            {
                lock (this.sync)
                {
                    return this.requests.Count(predicate);
                }
            }

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                Task releaseTask = null;
                lock (this.sync)
                {
                    if (!this.barrierClaimed && this.barrierPredicate?.Invoke(request) == true)
                    {
                        this.barrierClaimed = true;
                        this.blocked.TrySetResult(null);
                        releaseTask = this.release.Task;
                    }
                }

                if (releaseTask != null)
                {
                    await releaseTask;
                }

                HttpResponseMessage response = await base.SendAsync(request, cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    lock (this.sync)
                    {
                        this.requests.Add(new RecordedRequest(request.Method, request.RequestUri));
                    }
                }

                return response;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    this.Release();
                }

                base.Dispose(disposing);
            }

            public sealed class RecordedRequest
            {
                public RecordedRequest(HttpMethod method, Uri uri)
                {
                    this.Method = method;
                    this.Uri = uri;
                }

                public HttpMethod Method { get; }

                public Uri Uri { get; }
            }
        }
    }
}
