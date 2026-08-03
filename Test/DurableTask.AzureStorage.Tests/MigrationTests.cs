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
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Runtime.Serialization;
    using Azure.Data.Tables;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Query;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the zero-downtime Azure Storage to DTS migration feature. Every test runs with
    /// <see cref="AzureStorageOrchestrationServiceSettings.UseInstanceTableEtag"/> set to <c>true</c>, which is a
    /// requirement for any customer performing a live migration.
    /// </summary>
    [TestClass]
    public class MigrationTests
    {
        const string SequenceNumberPropertyName = "SequenceNumber";
        const string SentinelRowKey = "sentinel";
        const string ModifiedInstancesQueueSuffix = "modifiedinstances";

        /// <summary>
        /// Rewinding an orchestration while a migration is active must bump the instance's sequence number (and stamp
        /// the same value on the history sentinel row) and record the instance in the modified-instances queue so the
        /// migration process re-copies it. The rewind's effects are inspected immediately after the rewind (with the
        /// worker stopped) so the assertions are deterministic.
        /// </summary>
        [TestMethod]
        public async Task RewindDuringMigration_BumpsSequenceNumberAndEnqueuesInstance()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.UseInstanceTableEtag = true);

            await host.StartAsync(MigrationMode.MigrationStarted);

            // Run an orchestration that fails so that it can be rewound.
            RewindFailOrchestration.ShouldFail = true;
            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(RewindFailOrchestration), input: "world");
            OrchestrationState? failedStatus = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
            Assert.AreEqual(OrchestrationStatus.Failed, failedStatus?.OrchestrationStatus);

            string instanceId = client.InstanceId;

            // Stop the worker so no further work items are processed. This makes the state observed immediately after
            // the rewind deterministic (otherwise the revived orchestration would keep bumping the sequence number).
            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            var azureStorageClient = new AzureStorageClient(settings);

            long? sequenceNumberBeforeRewind = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.IsNotNull(sequenceNumberBeforeRewind, "The instance row should carry a sequence number while a migration is active.");

            // Drain all messages produced while the orchestration was running so the rewind's enqueue can be isolated.
            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Act: rewind the failed orchestration.
            RewindFailOrchestration.ShouldFail = false;
            await client.RewindAsync("rewind for migration test");

            // Assert: the rewind bumped the instance sequence number by exactly one...
            long? sequenceNumberAfterRewind = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumberBeforeRewind + 1, sequenceNumberAfterRewind, "Rewind should bump the instance sequence number by one.");

            // ...and stamped the same value on the history sentinel row so the two tables stay in sync.
            long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumberAfterRewind, sentinelSequenceNumber, "The history sentinel should carry the same sequence number as the instance row.");

            // ...and enqueued exactly one modified-instance message for this instance with a null execution ID
            // (rewind targets the latest execution, so no specific execution ID is recorded).
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Rewind should enqueue the instance exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.IsNull(enqueued[0].ExecutionId, "Rewind does not target a specific execution, so the execution ID should be null.");
        }

        /// <summary>
        /// Terminating a pending orchestration while a migration is active must bump the instance's sequence number and
        /// record the instance in the modified-instances queue. The termination is applied by the worker (there is no
        /// history to rewrite for a never-started instance), so the assertions are made after the instance reaches the
        /// terminal state and the worker is stopped.
        /// </summary>
        [TestMethod]
        public async Task TerminatePendingDuringMigration_BumpsSequenceNumberAndEnqueuesInstance()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.UseInstanceTableEtag = true);

            await host.StartAsync(MigrationMode.MigrationStarted);

            // Schedule a start time far in the future so the orchestration stays Pending (its ExecutionStarted message
            // is not yet visible) when it is terminated. This exercises the "terminate a pending orchestration" path.
            TestOrchestrationClient client = await host.StartOrchestrationAsync(
                typeof(PendingOrchestration), input: 0, startAt: DateTime.UtcNow.AddMinutes(5));
            await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Pending);

            string instanceId = client.InstanceId;

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            var azureStorageClient = new AzureStorageClient(settings);

            // Read the pending instance's sequence number (assigned when it was created) as the baseline.
            long? sequenceNumberBeforeTermination = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);

            // Drain the message enqueued when the instance was created so the termination's enqueue can be isolated.
            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Act: terminate the pending orchestration and wait for it to reach the terminal state.
            await client.TerminateAsync("terminate for migration test");
            OrchestrationState? terminatedStatus = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(OrchestrationStatus.Terminated, terminatedStatus?.OrchestrationStatus);

            // Stop the worker so the observed state is deterministic (the scheduled start message never fires).
            await host.StopAsync();

            // Assert: termination bumped the instance sequence number by one...
            long? sequenceNumberAfterTermination = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual((sequenceNumberBeforeTermination ?? 0) + 1, sequenceNumberAfterTermination, "Terminating a pending orchestration should bump the instance sequence number by one.");

            // ...and (since the instance never ran) there is no history at all.
            Assert.IsFalse(await HistoryExistsAsync(azureStorageClient, instanceId), "A terminated pending orchestration should have no history.");

            // ...and enqueued exactly one modified-instance message for this instance with a null execution ID
            // (the terminate message targets the latest generation, so no specific execution ID is recorded).
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Termination should enqueue the instance exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.IsNull(enqueued[0].ExecutionId, "The terminate message targets the latest generation, so the execution ID should be null.");
        }

        /// <summary>
        /// Creating a brand-new orchestration while a migration is active must set its sequence number to 1 and record
        /// the instance in the modified-instances queue. A future scheduled start keeps the instance Pending so that
        /// only the effect of creating it is observed (it never runs).
        /// </summary>
        [TestMethod]
        public async Task CreateDuringMigration_SetsSequenceNumberToOneAndEnqueuesInstance()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.UseInstanceTableEtag = true);

            await host.StartAsync(MigrationMode.MigrationStarted);

            // Schedule a start time far in the future so the orchestration stays Pending and never runs.
            TestOrchestrationClient client = await host.StartOrchestrationAsync(
                typeof(SimpleOrchestration), input: "world", startAt: DateTime.UtcNow.AddMinutes(5));
            await client.WaitForStatusChange(TimeSpan.FromSeconds(10), OrchestrationStatus.Pending);

            string instanceId = client.InstanceId;

            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            var azureStorageClient = new AzureStorageClient(settings);

            long? sequenceNumber = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(1, sequenceNumber, "Creating a brand-new orchestration should set the sequence number to 1.");

            // The instance was only created (it never ran), so there is no history.
            Assert.IsFalse(await HistoryExistsAsync(azureStorageClient, instanceId), "A newly created, unstarted orchestration should have no history.");

            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Creating the instance should enqueue it exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.IsNotNull(enqueued[0].ExecutionId, "Create records the new generation's execution ID.");
        }

        /// <summary>
        /// Recreating a completed orchestration (same instance ID, new generation) while a migration is active must
        /// bump the instance's sequence number by one and record the instance in the modified-instances queue. The
        /// recreate overwrites the instance row synchronously in the create call, so (with the worker stopped) its
        /// effect is observed immediately without the recreated generation running.
        /// </summary>
        [TestMethod]
        public async Task RecreateDuringMigration_BumpsSequenceNumberAndEnqueuesInstance()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.UseInstanceTableEtag = true);

            await host.StartAsync(MigrationMode.MigrationStarted);

            string instanceId = $"migration-recreate-{Guid.NewGuid():N}";

            // Run an orchestration to completion so the instance row carries a sequence number.
            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "world", instanceId: instanceId);
            OrchestrationState? status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            // Stop the worker so the recreate's synchronous effect is observed deterministically.
            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            var azureStorageClient = new AzureStorageClient(settings);

            long? sequenceNumberBeforeRecreate = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.IsNotNull(sequenceNumberBeforeRecreate, "The completed instance should carry a sequence number while a migration is active.");

            // Drain the messages produced by the first run so the recreate's enqueue can be isolated.
            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Act: recreate the completed orchestration under the same instance ID (a new generation).
            await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "again", instanceId: instanceId);

            // Assert: the recreate bumped the instance sequence number by one...
            long? sequenceNumberAfterRecreate = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumberBeforeRecreate + 1, sequenceNumberAfterRecreate, "Recreating an orchestration should bump the instance sequence number by one.");

            // ...while leaving the previous generation's history sentinel untouched (the recreate rewrites only the
            // instance row; the sentinel realigns once the new generation runs its first checkpoint).
            long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumberBeforeRecreate, sentinelSequenceNumber, "The recreate should not change the previous generation's history sentinel.");

            // ...and enqueued the instance exactly once for the new generation (which has its own execution ID).
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Recreate should enqueue the instance exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.IsNotNull(enqueued[0].ExecutionId, "A recreate targets a specific new generation, so the execution ID should be set.");
        }

        /// <summary>
        /// Running an orchestration to completion while a migration is active must produce a sequence number and an
        /// enqueue count equal to 1 (for the create) plus one per orchestration episode (each episode commits one
        /// checkpoint, which bumps the sequence number and enqueues the instance). A dedicated task hub isolates the
        /// exact enqueue count.
        /// </summary>
        [TestMethod]
        public async Task CompletedOrchestrationDuringMigration_SequenceNumberAndEnqueuesMatchCreatePlusEpisodes()
        {
            string taskHubName = $"migrepisodes{Guid.NewGuid():N}";
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings =>
                {
                    settings.UseInstanceTableEtag = true;
                    settings.TaskHubName = taskHubName;
                });

            await host.StartAsync(MigrationMode.MigrationStarted);

            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(MultiEpisodeOrchestration), input: "world");
            OrchestrationState? status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            string instanceId = client.InstanceId;

            // Stop the worker so the observed state is stable.
            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            settings.TaskHubName = taskHubName;
            var azureStorageClient = new AzureStorageClient(settings);

            // Each episode (OrchestratorStarted event) corresponds to one checkpoint.
            int episodeCount = await GetEpisodeCountAsync(azureStorageClient, instanceId);
            Assert.IsTrue(episodeCount >= 2, "The orchestration should have run for multiple episodes.");

            long expected = 1 + episodeCount;

            long? sequenceNumber = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(expected, sequenceNumber, "Sequence number should be 1 for the create plus one per episode.");

            // The history sentinel is stamped with the same sequence number as the instance row on every checkpoint.
            long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumber, sentinelSequenceNumber, "The history sentinel should carry the same sequence number as the instance row.");

            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(expected, enqueued.Count, "The instance should be enqueued once for the create and once per episode.");
            Assert.IsTrue(enqueued.All(instance => instance.InstanceId == instanceId), "Every enqueued message should be for this instance.");
        }

        /// <summary>
        /// Exercises the completed-orchestration recovery path (UpdateInstanceStatusForCompletedOrchestrationAsync):
        /// when the history table shows a terminal state but the instance table lags behind (as after a crash between
        /// the two writes), locking the next work item reconciles the instance row. During a migration this must bump
        /// the instance row's sequence number to match the history sentinel and enqueue the instance. Modeled on
        /// TestWorkerFailingDuringCompleteWorkItemCallCompletedOrchestration.
        /// </summary>
        [TestMethod]
        public async Task CompletedRecoveryDuringMigration_MatchesSentinelSequenceNumberAndEnqueuesInstance()
        {
            string taskHubName = $"migrrecovery{Guid.NewGuid():N}";
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings =>
                {
                    settings.UseInstanceTableEtag = true;
                    settings.TaskHubName = taskHubName;
                });

            await host.StartAsync(MigrationMode.MigrationStarted);

            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "world");
            OrchestrationState? status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            string instanceId = client.InstanceId;
            string executionId = status!.OrchestrationInstance.ExecutionId;

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            settings.TaskHubName = taskHubName;
            var azureStorageClient = new AzureStorageClient(settings);

            // After completion the instance row and history sentinel carry the same sequence number.
            long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.IsNotNull(sentinelSequenceNumber, "The completed instance should have a history sentinel sequence number.");
            Assert.AreEqual(sentinelSequenceNumber, await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId));

            // Clear the messages produced by the run so the recovery's enqueue can be isolated.
            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Simulate a crash between the history checkpoint and the instance-table update: the history sentinel holds
            // the final sequence number, but the instance row is stale (Running, and one sequence number behind).
            Table instanceTable = azureStorageClient.GetTableReference(settings.InstanceTableName);
            var staleInstanceEntity = new TableEntity(KeySanitation.EscapePartitionKey(instanceId), string.Empty)
            {
                ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                [SequenceNumberPropertyName] = sentinelSequenceNumber.Value - 1,
            };
            await instanceTable.MergeEntityAsync(staleInstanceEntity, Azure.ETag.All);

            // Raising an event locks the next work item, which detects the stale instance row and reconciles it.
            await client.RaiseEventAsync("Recover", "please");

            DateTime deadline = DateTime.UtcNow.AddSeconds(30);
            OrchestrationState? recovered = null;
            while (DateTime.UtcNow < deadline)
            {
                recovered = await client.GetStatusAsync();
                if (recovered?.OrchestrationStatus == OrchestrationStatus.Completed)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }

            Assert.AreEqual(OrchestrationStatus.Completed, recovered?.OrchestrationStatus, "The recovery should reconcile the instance row back to Completed.");

            await host.StopAsync();

            // The recovery bumped the instance row's sequence number back up to match the history sentinel.
            long? instanceSequenceNumberAfterRecovery = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            long? sentinelSequenceNumberAfterRecovery = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sentinelSequenceNumber, instanceSequenceNumberAfterRecovery, "Recovery should set the instance sequence number to match the sentinel.");
            Assert.AreEqual(sentinelSequenceNumberAfterRecovery, instanceSequenceNumberAfterRecovery, "The instance and history sentinel sequence numbers should match after recovery.");

            // The recovery enqueued the instance exactly once for the completed generation.
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Recovery should enqueue the instance exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.AreEqual(executionId, enqueued[0].ExecutionId, "Recovery records the completed generation's execution ID.");
        }

        /// <summary>
        /// Confirms that in a split-brain situation (two workers completing the same work item) during a migration, the
        /// instance table and history sentinel sequence numbers stay aligned. Both workers derive the same next
        /// sequence number from the same base, and the UseInstanceTableEtag guard rejects the losing worker's instance
        /// write, so the sentinel it commits still matches the instance value written by the winning worker. Uses the
        /// low-level lock/complete API, modeled on WorkerAttemptingToUpdateInstanceTableAfterStalling.
        /// </summary>
        [TestMethod]
        public async Task SplitBrainDuringMigration_InstanceAndHistorySequenceNumbersRemainAligned()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = $"migrsplit{Guid.NewGuid():N}",
                ExtendedSessionsEnabled = false,
                // A migrating customer always runs with UseInstanceTableEtag = true, which is what makes the losing
                // worker's stale instance write fail instead of silently overwriting the winner's.
                UseInstanceTableEtag = true,
            };

            AzureStorageOrchestrationService? service = null;
            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync(MigrationMode.MigrationStarted);

                var orchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance_id",
                    ExecutionId = "execution_id",
                };
                var startedEvent = new ExecutionStartedEvent(-1, string.Empty)
                {
                    Name = "orchestration",
                    Version = string.Empty,
                    OrchestrationInstance = orchestrationInstance,
                    ScheduledStartTime = DateTime.UtcNow,
                };

                await service.CreateTaskOrchestrationAsync(new TaskMessage
                {
                    OrchestrationInstance = orchestrationInstance,
                    Event = startedEvent,
                });

                // Worker A locks the first work item and prepares its checkpoint (base sequence number is 1).
                var workItem = await service.LockNextTaskOrchestrationWorkItemAsync(TimeSpan.FromMinutes(5), CancellationToken.None);
                var runtimeState = workItem.OrchestrationRuntimeState;
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(startedEvent);
                runtimeState.AddEvent(new TaskScheduledEvent(0));
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

                var azureStorageClient = new AzureStorageClient(settings);

                // Simulate a competing worker B winning the same episode: it advances the instance row to the next
                // sequence number (1 -> 2), which also changes the instance row's eTag out from under worker A.
                Table instanceTable = azureStorageClient.GetTableReference(settings.InstanceTableName);
                var winnerEntity = new TableEntity(orchestrationInstance.InstanceId, string.Empty)
                {
                    ["RuntimeStatus"] = OrchestrationStatus.Running.ToString("G"),
                    [SequenceNumberPropertyName] = 2L,
                };
                await instanceTable.MergeEntityAsync(winnerEntity, Azure.ETag.All);

                // Worker A (now stale) completes the same work item. Its history sentinel write succeeds (also computing
                // sequence number 2), but its instance write is rejected due to the stale eTag.
                SessionAbortedException exception = await Assert.ThrowsExceptionAsync<SessionAbortedException>(async () =>
                    await service.CompleteTaskOrchestrationWorkItemAsync(
                        workItem, runtimeState, new List<TaskMessage>(), new List<TaskMessage>(), new List<TaskMessage>(), null, null));
                Assert.IsInstanceOfType(exception.InnerException, typeof(DurableTaskStorageException));
                Assert.AreEqual((int)HttpStatusCode.PreconditionFailed, ((DurableTaskStorageException)exception.InnerException).HttpStatusCode);

                // Because both workers derived the same next sequence number, the history sentinel (written by the
                // losing worker) and the instance row (written by the winning worker) remain aligned.
                long? instanceSequenceNumber = await GetInstanceSequenceNumberAsync(azureStorageClient, orchestrationInstance.InstanceId);
                long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, orchestrationInstance.InstanceId);
                Assert.AreEqual(2L, instanceSequenceNumber, "The instance row should carry the winning worker's sequence number.");
                Assert.AreEqual(2L, sentinelSequenceNumber, "The history sentinel should carry the same sequence number.");
                Assert.AreEqual(instanceSequenceNumber, sentinelSequenceNumber, "The instance and history sequence numbers must remain aligned.");
            }
            finally
            {
                if (service != null)
                {
                    try
                    {
                        await service.StopAsync(isForced: true);
                    }
                    catch
                    {
                        // Ignore shutdown errors so the real test failure (if any) is not masked.
                    }
                }
            }
        }

        /// <summary>
        /// Confirms that every public API guarded by ThrowIfMigrationEnding rejects requests with
        /// <see cref="OrchestrationServiceUnavailableException"/> once the service is started in
        /// <see cref="MigrationMode.MigrationEnding"/> (so callers are redirected to the new backend).
        /// </summary>
        [TestMethod]
        public async Task MigrationEnding_RejectsGuardedPublicApiRequests()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = $"migrend{Guid.NewGuid():N}",
                ExtendedSessionsEnabled = false,
                UseInstanceTableEtag = true,
            };

            AzureStorageOrchestrationService? service = null;
            try
            {
                service = new AzureStorageOrchestrationService(settings);
                AzureStorageOrchestrationService svc = service;
                await svc.CreateAsync();
                await svc.StartAsync(MigrationMode.MigrationEnding);

                var instance = new OrchestrationInstance { InstanceId = "instance_id", ExecutionId = "execution_id" };
                var creationMessage = new TaskMessage
                {
                    OrchestrationInstance = instance,
                    Event = new ExecutionStartedEvent(-1, "input") { Name = "orchestration", Version = string.Empty, OrchestrationInstance = instance },
                };
                var message = new TaskMessage { OrchestrationInstance = instance, Event = new GenericEvent(-1, "event") };
                var workItem = new TaskOrchestrationWorkItem { InstanceId = "instance_id" };
                var activityWorkItem = new TaskActivityWorkItem { Id = "activity_id", TaskMessage = message };
                var emptyMessages = new List<TaskMessage>();
                var runtimeStatuses = new[] { OrchestrationStatus.Completed };
                var condition = new OrchestrationInstanceStatusQueryCondition();

                // Every public API that calls ThrowIfMigrationEnding.
                var guardedApis = new (string Name, Func<Task> Invoke)[]
                {
                    ("CreateTaskOrchestrationAsync(message)", () => svc.CreateTaskOrchestrationAsync(creationMessage)),
                    ("CreateTaskOrchestrationAsync(message, dedupeStatuses)", () => svc.CreateTaskOrchestrationAsync(creationMessage, null)),
                    ("SendTaskOrchestrationMessageAsync", () => svc.SendTaskOrchestrationMessageAsync(message)),
                    ("SendTaskOrchestrationMessageBatchAsync", () => svc.SendTaskOrchestrationMessageBatchAsync(message)),
                    ("GetOrchestrationStateAsync(instanceId, allExecutions)", () => svc.GetOrchestrationStateAsync("instance_id", true)),
                    ("GetOrchestrationStateAsync(instanceId, executionId)", () => svc.GetOrchestrationStateAsync("instance_id", "execution_id")),
                    ("GetOrchestrationStateAsync(instanceId, allExecutions, fetchInput)", () => svc.GetOrchestrationStateAsync("instance_id", true, true)),
                    ("GetOrchestrationStateAsync(cancellationToken)", () => svc.GetOrchestrationStateAsync(CancellationToken.None)),
                    ("GetOrchestrationStateAsync(createdTimeFrom, createdTimeTo, runtimeStatus, ct)", () => svc.GetOrchestrationStateAsync(DateTime.MinValue, DateTime.MaxValue, runtimeStatuses, CancellationToken.None)),
                    ("GetOrchestrationStateAsync(createdTimeFrom, createdTimeTo, runtimeStatus, top, continuationToken, ct)", () => svc.GetOrchestrationStateAsync(DateTime.MinValue, DateTime.MaxValue, runtimeStatuses, 10, null, CancellationToken.None)),
                    ("GetOrchestrationStateAsync(condition, top, continuationToken, ct)", () => svc.GetOrchestrationStateAsync(condition, 10, null, CancellationToken.None)),
                    ("GetOrchestrationWithQueryAsync", () => svc.GetOrchestrationWithQueryAsync(new OrchestrationQuery(), CancellationToken.None)),
                    ("ForceTerminateTaskOrchestrationAsync", () => svc.ForceTerminateTaskOrchestrationAsync("instance_id", "reason")),
                    ("RewindTaskOrchestrationAsync", () => svc.RewindTaskOrchestrationAsync("instance_id", "reason")),
                    ("GetOrchestrationHistoryAsync", () => svc.GetOrchestrationHistoryAsync("instance_id", "execution_id")),
                    ("PurgeInstanceHistoryAsync(instanceId)", () => svc.PurgeInstanceHistoryAsync("instance_id")),
                    ("PurgeInstanceHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatus)", () => svc.PurgeInstanceHistoryAsync(DateTime.MinValue, DateTime.MaxValue, runtimeStatuses)),
                    ("IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(instanceId)", () => ((IOrchestrationServicePurgeClient)svc).PurgeInstanceStateAsync("instance_id")),
                    ("IOrchestrationServicePurgeClient.PurgeInstanceStateAsync(filter)", () => ((IOrchestrationServicePurgeClient)svc).PurgeInstanceStateAsync(new PurgeInstanceFilter(DateTime.MinValue, DateTime.MaxValue, runtimeStatuses))),
                    ("WaitForOrchestrationAsync", () => svc.WaitForOrchestrationAsync("instance_id", "execution_id", TimeSpan.FromSeconds(1), CancellationToken.None)),
                    ("PurgeOrchestrationHistoryAsync", () => svc.PurgeOrchestrationHistoryAsync(DateTime.MinValue, OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter)),
                    ("CompleteTaskOrchestrationWorkItemAsync", () => svc.CompleteTaskOrchestrationWorkItemAsync(workItem, null, emptyMessages, emptyMessages, emptyMessages, null, null)),
                    ("RenewTaskOrchestrationWorkItemLockAsync", () => svc.RenewTaskOrchestrationWorkItemLockAsync(workItem)),
                    ("AbandonTaskOrchestrationWorkItemAsync", () => svc.AbandonTaskOrchestrationWorkItemAsync(workItem)),
                    ("ReleaseTaskOrchestrationWorkItemAsync", () => svc.ReleaseTaskOrchestrationWorkItemAsync(workItem)),
                    ("CompleteTaskActivityWorkItemAsync", () => svc.CompleteTaskActivityWorkItemAsync(activityWorkItem, message)),
                    ("RenewTaskActivityWorkItemLockAsync", () => svc.RenewTaskActivityWorkItemLockAsync(activityWorkItem)),
                    ("AbandonTaskActivityWorkItemAsync", () => svc.AbandonTaskActivityWorkItemAsync(activityWorkItem)),
                };

                foreach ((string name, Func<Task> invoke) in guardedApis)
                {
                    await Assert.ThrowsExceptionAsync<OrchestrationServiceUnavailableException>(
                        invoke,
                        $"{name} should reject requests with {nameof(OrchestrationServiceUnavailableException)} while the migration is ending.");
                }
            }
            finally
            {
                if (service != null)
                {
                    try
                    {
                        await service.StopAsync(isForced: true);
                    }
                    catch
                    {
                        // Ignore shutdown errors so the real test failure (if any) is not masked.
                    }
                }
            }
        }

        /// <summary>
        /// Starting the service in <see cref="MigrationMode.MigrationEnding"/> must record a durable marker in the
        /// migration table (creating the table if needed) so the ending state survives restarts.
        /// </summary>
        [TestMethod]
        public async Task MigrationEnding_RecordsMarkerInMigrationTable()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = $"migrend{Guid.NewGuid():N}",
                ExtendedSessionsEnabled = false,
                UseInstanceTableEtag = true,
            };

            AzureStorageOrchestrationService? service = null;
            try
            {
                service = new AzureStorageOrchestrationService(settings);
                await service.CreateAsync();
                await service.StartAsync(MigrationMode.MigrationEnding);

                var azureStorageClient = new AzureStorageClient(settings);
                Table migrationTable = azureStorageClient.GetTableReference(settings.MigrationTableName);

                Assert.IsTrue(await migrationTable.ExistsAsync(), "The migration table should be created when a migration is ending.");

                var markers = new List<TableEntity>();
                await foreach (TableEntity entity in migrationTable.ExecuteQueryAsync<TableEntity>())
                {
                    markers.Add(entity);
                }

                Assert.AreEqual(1, markers.Count, "The migration table should contain exactly one marker row while a migration is ending.");
                Assert.AreEqual(MigrationMode.MigrationEnding.ToString(), markers[0].GetString("State"), "The marker should record the MigrationEnding state.");
            }
            finally
            {
                if (service != null)
                {
                    try
                    {
                        await service.StopAsync(isForced: true);
                    }
                    catch
                    {
                        // Ignore shutdown errors so the real test failure (if any) is not masked.
                    }
                }
            }
        }

        /// <summary>
        /// Recreating a completed instance that was subsequently purged, then running the recreated generation to
        /// completion, while a migration is active, must carry the sequence number forward across the whole lifecycle:
        /// create (1), complete (2), purge (3), recreate-create (4), recreate-complete (5) — a final sequence number of
        /// 5, a matching history sentinel, and one enqueue at each of those five steps. This is deterministic only
        /// because the purge tombstone retains the execution ID/generation, which makes the recreate's ExecutionStarted
        /// defer until its instance row is written (rather than racing it); a regression there would flake this test.
        /// A dedicated task hub isolates the exact enqueue count.
        /// </summary>
        [TestMethod]
        public async Task RecreatePurgedCompletedInstanceDuringMigration_HasExpectedSequenceNumberAndEnqueues()
        {
            string taskHubName = $"migrrp{Guid.NewGuid():N}";
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings =>
                {
                    settings.UseInstanceTableEtag = true;
                    settings.TaskHubName = taskHubName;
                });

            await host.StartAsync(MigrationMode.MigrationStarted);

            string instanceId = $"recreate-purged-{Guid.NewGuid():N}";

            // Run to completion: create (seq 1) + one completion checkpoint (seq 2).
            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "world", instanceId: instanceId);
            Assert.AreEqual(OrchestrationStatus.Completed, (await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30)))?.OrchestrationStatus);

            // Purge the completed instance: keeps a tombstone row with the sequence number bumped to 3, and deletes all history.
            await client.PurgeInstanceHistory();

            // Recreate under the same instance ID and run to completion: the create carries the purged sequence number
            // forward (3 -> 4), then one completion checkpoint (seq 5).
            TestOrchestrationClient recreatedClient = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "again", instanceId: instanceId);
            Assert.AreEqual(OrchestrationStatus.Completed, (await recreatedClient.WaitForCompletionAsync(TimeSpan.FromSeconds(30)))?.OrchestrationStatus);

            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            settings.TaskHubName = taskHubName;
            var azureStorageClient = new AzureStorageClient(settings);

            // The sequence number carries forward across all five state changes: create (1), complete (2), purge (3),
            // recreate-create (4), recreate-complete (5).
            long? sequenceNumber = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(5L, sequenceNumber, "The sequence number should carry forward across create, complete, purge, recreate-create, and recreate-complete.");

            // The recreated generation ran to completion, so its history sentinel matches the instance row.
            long? sentinelSequenceNumber = await GetSentinelSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumber, sentinelSequenceNumber, "The history sentinel should carry the same sequence number as the instance row.");

            // The instance was enqueued exactly once at each of the five state changes.
            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(5, enqueued.Count, "The instance should be enqueued once for the create, the completion checkpoint, the purge, the recreate, and the recreate's completion checkpoint.");
            Assert.IsTrue(enqueued.All(instance => instance.InstanceId == instanceId), "Every enqueued message should be for this instance.");
        }

        /// <summary>
        /// Purging a single completed instance while a migration is active must leave the instance row behind (with an
        /// incremented sequence number) rather than deleting it, and record the instance in the modified-instances
        /// queue so the migration process can observe the purge.
        /// </summary>
        [TestMethod]
        public async Task PurgeInstanceDuringMigration_KeepsRowBumpsSequenceNumberAndEnqueuesInstance()
        {
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings => settings.UseInstanceTableEtag = true);

            await host.StartAsync(MigrationMode.MigrationStarted);

            TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: "world");
            OrchestrationState? status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

            string instanceId = client.InstanceId;

            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            var azureStorageClient = new AzureStorageClient(settings);

            long? sequenceNumberBeforePurge = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.IsNotNull(sequenceNumberBeforePurge, "The completed instance should carry a sequence number while a migration is active.");

            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Act: purge the single instance.
            await client.PurgeInstanceHistory();

            // Assert: the instance row still exists...
            Assert.IsTrue(await InstanceRowExistsAsync(azureStorageClient, instanceId), "Purge should keep the instance row while a migration is active.");

            // ...with its sequence number bumped by one...
            long? sequenceNumberAfterPurge = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
            Assert.AreEqual(sequenceNumberBeforePurge + 1, sequenceNumberAfterPurge, "Purge should bump the instance sequence number by one.");

            // ...and its history (including the sentinel) deleted.
            Assert.IsFalse(await HistoryExistsAsync(azureStorageClient, instanceId), "Purge should delete all history for the instance.");

            // ...and enqueued exactly once with a null execution ID (purge is by instance ID only).
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(1, enqueued.Count, "Purge should enqueue the instance exactly once.");
            Assert.AreEqual(instanceId, enqueued[0].InstanceId);
            Assert.IsNull(enqueued[0].ExecutionId, "Single-instance purge is by instance ID only, so the execution ID should be null.");
        }

        /// <summary>
        /// Purging multiple completed instances by filter while a migration is active must, for every matched
        /// instance, leave the instance row behind with an incremented sequence number and record it in the
        /// modified-instances queue. A dedicated task hub isolates this task-hub-wide purge from other instances.
        /// </summary>
        [TestMethod]
        public async Task PurgeByFilterDuringMigration_KeepsRowsBumpsSequenceNumbersAndEnqueuesInstances()
        {
            string taskHubName = $"migrpurge{Guid.NewGuid():N}";
            using TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
                enableExtendedSessions: false,
                modifySettingsAction: settings =>
                {
                    settings.UseInstanceTableEtag = true;
                    settings.TaskHubName = taskHubName;
                });

            await host.StartAsync(MigrationMode.MigrationStarted);

            DateTime createdTimeFrom = DateTime.UtcNow;

            const int instanceCount = 3;
            var instanceIds = new List<string>();
            for (int i = 0; i < instanceCount; i++)
            {
                TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(SimpleOrchestration), input: $"world-{i}");
                OrchestrationState? status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
                instanceIds.Add(client.InstanceId);
            }

            await host.StopAsync();

            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);
            settings.UseInstanceTableEtag = true;
            settings.TaskHubName = taskHubName;
            var azureStorageClient = new AzureStorageClient(settings);

            var sequenceNumbersBeforePurge = new Dictionary<string, long?>();
            foreach (string instanceId in instanceIds)
            {
                long? sequenceNumber = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
                Assert.IsNotNull(sequenceNumber, $"The completed instance {instanceId} should carry a sequence number while a migration is active.");
                sequenceNumbersBeforePurge[instanceId] = sequenceNumber;
            }

            Queue modifiedInstancesQueue = GetModifiedInstancesQueue(azureStorageClient, settings);
            await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);

            // Act: purge all completed instances by time range.
            await host.service.PurgeInstanceHistoryAsync(createdTimeFrom, DateTime.UtcNow, new[] { OrchestrationStatus.Completed });

            // Assert: every purged instance's row still exists with its sequence number bumped by one.
            foreach (string instanceId in instanceIds)
            {
                Assert.IsTrue(await InstanceRowExistsAsync(azureStorageClient, instanceId), $"Purge should keep the instance row for {instanceId}.");
                long? sequenceNumberAfterPurge = await GetInstanceSequenceNumberAsync(azureStorageClient, instanceId);
                Assert.AreEqual(sequenceNumbersBeforePurge[instanceId] + 1, sequenceNumberAfterPurge, $"Purge should bump the sequence number for {instanceId} by one.");
                Assert.IsFalse(await HistoryExistsAsync(azureStorageClient, instanceId), $"Purge should delete all history for {instanceId}.");
            }

            // ...and every purged instance was enqueued exactly once.
            List<OrchestrationInstance> enqueued = await DrainModifiedInstancesQueueAsync(modifiedInstancesQueue);
            Assert.AreEqual(instanceCount, enqueued.Count, "Each purged instance should be enqueued exactly once.");
            CollectionAssert.AreEquivalent(instanceIds, enqueued.Select(instance => instance.InstanceId).ToList());
        }

        static async Task<bool> InstanceRowExistsAsync(AzureStorageClient azureStorageClient, string instanceId)
        {
            Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
            string filter = AzureTableQueryFilter.PartitionKeyEquals(instanceId);
            await foreach (TableEntity _ in instanceTable.ExecuteQueryAsync<TableEntity>(filter))
            {
                return true;
            }

            return false;
        }

        static async Task<bool> HistoryExistsAsync(AzureStorageClient azureStorageClient, string instanceId)
        {
            Table historyTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.HistoryTableName);
            string filter = AzureTableQueryFilter.PartitionKeyEquals(instanceId);
            await foreach (TableEntity _ in historyTable.ExecuteQueryAsync<TableEntity>(filter))
            {
                return true;
            }

            return false;
        }

        static async Task<int> GetEpisodeCountAsync(AzureStorageClient azureStorageClient, string instanceId)
        {
            Table historyTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.HistoryTableName);
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and " +
                $"{AzureTableQueryFilter.ColumnEquals(nameof(HistoryEvent.EventType), nameof(EventType.OrchestratorStarted))}";
            int count = 0;
            await foreach (TableEntity _ in historyTable.ExecuteQueryAsync<TableEntity>(filter))
            {
                count++;
            }

            return count;
        }

        static async Task<long?> GetInstanceSequenceNumberAsync(AzureStorageClient azureStorageClient, string instanceId)
        {
            Table instanceTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.InstanceTableName);
            string filter = AzureTableQueryFilter.PartitionKeyEquals(instanceId);
            await foreach (TableEntity entity in instanceTable.ExecuteQueryAsync<TableEntity>(filter))
            {
                return entity.GetInt64(SequenceNumberPropertyName);
            }

            return null;
        }

        static async Task<long?> GetSentinelSequenceNumberAsync(AzureStorageClient azureStorageClient, string instanceId)
        {
            Table historyTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.HistoryTableName);
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and " +
                $"{AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), SentinelRowKey)}";
            await foreach (TableEntity entity in historyTable.ExecuteQueryAsync<TableEntity>(filter))
            {
                return entity.GetInt64(SequenceNumberPropertyName);
            }

            return null;
        }

        static Queue GetModifiedInstancesQueue(AzureStorageClient azureStorageClient, AzureStorageOrchestrationServiceSettings settings)
        {
            string queueName = AzureStorageOrchestrationService.GetQueueName(settings.TaskHubName, ModifiedInstancesQueueSuffix);
            return azureStorageClient.GetQueueReference(queueName);
        }

        static async Task<List<OrchestrationInstance>> DrainModifiedInstancesQueueAsync(Queue queue)
        {
            var drained = new List<OrchestrationInstance>();
            while (true)
            {
                IReadOnlyCollection<QueueMessage> messages = await queue.GetMessagesAsync(batchSize: 32, visibilityTimeout: TimeSpan.FromMinutes(1));
                if (messages.Count == 0)
                {
                    break;
                }

                foreach (QueueMessage message in messages)
                {
                    drained.Add(Utils.DeserializeFromJson<OrchestrationInstance>(message.Body.ToString()));
                    await queue.DeleteMessageAsync(message);
                }
            }

            return drained;
        }

        [KnownType(typeof(RewindFailActivity))]
        sealed class RewindFailOrchestration : TaskOrchestration<string, string>
        {
            public static bool ShouldFail = true;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = await context.ScheduleTask<string>(typeof(RewindFailActivity), input);
                if (ShouldFail)
                {
                    throw new Exception("Simulating a transient, unhandled exception for the rewind migration test.");
                }

                return result;
            }
        }

        sealed class RewindFailActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return $"Hello, {input}!";
            }
        }

        // Never actually runs in the terminate-pending test; it is terminated while still scheduled/pending.
        sealed class PendingOrchestration : TaskOrchestration<string, int>
        {
            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                return Task.FromResult("done");
            }
        }

        sealed class SimpleOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult($"Hello, {input}!");
            }
        }

        // Runs across multiple episodes (one checkpoint per episode) by awaiting two sequential activities.
        [KnownType(typeof(RewindFailActivity))]
        sealed class MultiEpisodeOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string first = await context.ScheduleTask<string>(typeof(RewindFailActivity), input);
                return await context.ScheduleTask<string>(typeof(RewindFailActivity), first);
            }
        }
    }
}
