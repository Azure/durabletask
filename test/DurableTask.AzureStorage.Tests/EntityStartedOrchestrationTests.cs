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
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EntityStartedOrchestrationTests
    {
        AzureStorageOrchestrationService service;
        TaskHubWorker worker;
        AzureStorageOrchestrationServiceSettings settings;
        PendingWritePolicy pendingWritePolicy;

        [TestInitialize]
        public async Task Initialize()
        {
            this.settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            this.settings.TaskHubName = "EntityStart" + Guid.NewGuid().ToString("N").Substring(0, 12);
            this.settings.PartitionCount = 1;
            this.settings.UseAppLease = false;
            this.settings.UseSeparateQueueForEntityWorkItems = true;
            this.settings.MaxQueuePollingInterval = TimeSpan.FromMilliseconds(100);
            this.settings.ControlQueueVisibilityTimeout = TimeSpan.FromSeconds(5);

            this.pendingWritePolicy = new PendingWritePolicy(this.settings.InstanceTableName);
            var tableOptions = new TableClientOptions();
            tableOptions.AddPolicy(this.pendingWritePolicy, HttpPipelinePosition.PerCall);
            string connectionString = TestHelpers.GetTestStorageAccountConnectionString();
            this.settings.StorageAccountClientProvider = new StorageAccountClientProvider(
                StorageServiceClientProvider.ForBlob(connectionString),
                StorageServiceClientProvider.ForQueue(connectionString),
                StorageServiceClientProvider.ForTable(connectionString, tableOptions));

            this.service = new AzureStorageOrchestrationService(this.settings);
            await this.service.CreateAsync();
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            if (this.worker != null)
            {
                await this.worker.StopAsync(isForced: true);
                this.worker.Dispose();
            }

            if (this.service != null)
            {
                await this.service.DeleteAsync();
                this.service.Dispose();
            }
        }

        [DataTestMethod]
        [DataRow(false, 16)]
        [DataRow(true, 16)]
        [DataRow(false, 36 * 1024)]
        [DataRow(true, 60 * 1024)]
        public async Task EntityStartedScheduledOrchestrationIsPendingAndCanBeTerminated(bool useInstanceTableEtag, int inputLength)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var entity = new OrchestrationInstance { InstanceId = "@pendingstarter@test" };
            string childInstanceId = Guid.NewGuid().ToString("N");
            string input = "\"" + new string('x', inputLength) + "\"";
            DateTime createdTime = DateTime.UtcNow;
            DateTime scheduledStartTime = default;
            int executions = 0;

            this.CreateEntityWorker(() =>
            {
                scheduledStartTime = DateTime.UtcNow.AddSeconds(10);
                return new StartNewOrchestrationOperationAction
                {
                    Name = typeof(ChildOrchestration).FullName,
                    InstanceId = childInstanceId,
                    Input = input,
                    ScheduledStartTime = scheduledStartTime,
                    Tags = new Dictionary<string, string> { ["source"] = "entity" },
                };
            });
            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                Interlocked.Increment(ref executions);
                return next();
            });

            await this.worker.StartAsync();
            await this.service.SendTaskOrchestrationMessageAsync(
                ClientEntityHelpers.EmitOperationSignal(entity, Guid.NewGuid(), "start", null, null).AsTaskMessage());

            var client = new TaskHubClient(this.service);
            await new TestInstance<string>(client, entity, createdTime, null).WaitForStart(TimeSpan.FromSeconds(30));

            // The entity checkpoint must not complete before its child's Pending registration.
            var child = new TestOrchestrationClient(client, typeof(ChildOrchestration), childInstanceId, createdTime);
            OrchestrationState state = await child.GetStatusAsync();
            Assert.IsNotNull(state, "An entity-started orchestration must be visible before its scheduled start.");
            Assert.AreEqual(OrchestrationStatus.Pending, state.OrchestrationStatus);
            Assert.AreEqual(scheduledStartTime, state.ScheduledStartTime);
            Assert.AreEqual(input, state.Input);
            Assert.AreEqual("entity", state.Tags["source"]);
            Assert.AreEqual(entity.InstanceId, state.ParentInstance.OrchestrationInstance.InstanceId);
            OrchestrationState listed = (await this.service.GetOrchestrationStateAsync(CancellationToken.None))
                .Single(item => item.OrchestrationInstance.InstanceId == childInstanceId);
            Assert.AreEqual(OrchestrationStatus.Pending, listed.OrchestrationStatus);
            Assert.IsTrue(DateTime.UtcNow < scheduledStartTime, "Termination must happen while the start is still delayed.");
            Assert.IsTrue(await this.service.AllControlQueues.Single().InnerQueue.GetApproximateMessagesCountAsync() > 0);

            if (inputLength >= 60 * 1024)
            {
                var table = new TableServiceClient(TestHelpers.GetTestStorageAccountConnectionString()).GetTableClient(this.settings.InstanceTableName);
                TableEntity row = await table.GetEntityAsync<TableEntity>(childInstanceId, string.Empty);
                Assert.IsTrue(Uri.TryCreate(row.GetString("Input"), UriKind.Absolute, out _),
                    "Pending must reuse the compressed queue message's blob reference for large inputs.");
            }

            await child.TerminateAsync("terminate before start");
            state = await child.WaitForCompletionAsync(TimeSpan.FromSeconds(5));
            Assert.AreEqual(OrchestrationStatus.Terminated, state?.OrchestrationStatus);

            // Wait for consumption of the actual delayed queue message, not an arbitrary sleep.
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            {
                while (await this.service.AllControlQueues.Single().InnerQueue.GetApproximateMessagesCountAsync() > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(50), timeout.Token);
                }
            }

            Assert.AreEqual(0, executions, "Terminating a Pending entity-started orchestration must prevent execution.");
            state = await child.GetStatusAsync();
            Assert.AreEqual(OrchestrationStatus.Terminated, state.OrchestrationStatus);
            Assert.AreEqual("terminate before start", state.Output);
            Assert.AreEqual(
                0,
                (await this.service.TrackingStore.GetHistoryEventsAsync(childInstanceId, null)).Events.Count);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ReusedEntityStartedScheduledOrchestrationCanBeTerminated(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            string childInstanceId = Guid.NewGuid().ToString("N");
            DateTime createdTime = DateTime.UtcNow;
            DateTime scheduledStartTime = default;
            int executions = 0;
            this.CreateEntityWorker(() =>
            {
                scheduledStartTime = DateTime.UtcNow.AddSeconds(10);
                return new StartNewOrchestrationOperationAction
                {
                    Name = typeof(ChildOrchestration).FullName,
                    InstanceId = childInstanceId,
                    Input = "\"must-not-run\"",
                    ScheduledStartTime = scheduledStartTime,
                };
            });
            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                Interlocked.Increment(ref executions);
                return next();
            });
            await this.worker.StartAsync();
            var client = new TaskHubClient(this.service);
            OrchestrationInstance previous = await client.CreateOrchestrationInstanceAsync(
                typeof(ChildOrchestration), childInstanceId, "done");
            OrchestrationState previousState = await client.WaitForOrchestrationAsync(previous, TimeSpan.FromSeconds(30));
            Assert.IsNotNull(previousState);
            Assert.AreEqual(OrchestrationStatus.Completed, previousState.OrchestrationStatus);
            Interlocked.Exchange(ref executions, 0);
            OrchestrationHistory previousHistory = await this.service.TrackingStore.GetHistoryEventsAsync(
                childInstanceId, previous.ExecutionId);

            var entity = new OrchestrationInstance { InstanceId = "@pendingstarter@test" };
            await this.service.SendTaskOrchestrationMessageAsync(
                ClientEntityHelpers.EmitOperationSignal(entity, Guid.NewGuid(), "start", null, null).AsTaskMessage());
            await new TestInstance<string>(client, entity, createdTime, null).WaitForStart(TimeSpan.FromSeconds(30));
            InstanceStatus pending = await this.service.TrackingStore.FetchInstanceStatusAsync(childInstanceId);
            Assert.AreEqual(OrchestrationStatus.Pending, pending.State.OrchestrationStatus);
            Assert.AreNotEqual(previous.ExecutionId, pending.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(previousState.Generation + 1, pending.State.Generation);
            Assert.IsTrue(DateTime.UtcNow < scheduledStartTime);

            var child = new TestOrchestrationClient(client, typeof(ChildOrchestration), childInstanceId, createdTime);
            await child.TerminateAsync("terminate the reused Pending execution");
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            {
                while (await this.service.AllControlQueues.Single().InnerQueue.GetApproximateMessagesCountAsync() > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(50), timeout.Token);
                }
            }

            OrchestrationState terminated = await child.GetStatusAsync();
            Assert.AreEqual(OrchestrationStatus.Terminated, terminated.OrchestrationStatus,
                "Old terminal history must not cause termination of the new Pending execution to be discarded.");
            Assert.AreEqual(0, executions);
            Assert.AreEqual(pending.State.OrchestrationInstance.ExecutionId, terminated.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(pending.State.Generation, terminated.Generation);
            Assert.AreEqual("terminate the reused Pending execution", terminated.Output);
            Assert.AreEqual(
                previousHistory.Events.Count,
                (await this.service.TrackingStore.GetHistoryEventsAsync(childInstanceId, previous.ExecutionId)).Events.Count);
            Assert.AreEqual(
                0,
                (await this.service.TrackingStore.GetHistoryEventsAsync(childInstanceId, terminated.OrchestrationInstance.ExecutionId)).Events.Count);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task RegisteredEntityStartRechecksTerminationAfterReconciliation(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            ControlQueue queue = this.service.AllControlQueues.Single();
            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            MessageData queued = (await queue.GetMessagesAsync(timeout.Token)).Single();
            Assert.AreEqual(1, (await this.ReconcileStartAsync(queued)).Count());

            await this.service.TrackingStore.UpdateStatusForTerminationAsync(
                start.OrchestrationInstance.InstanceId, new ExecutionTerminatedEvent(-1, "terminated after reconciliation"));
            int reads = this.pendingWritePolicy.InstanceReads;
            string decision = await this.EvaluateBatchAsync(new OrchestrationRuntimeState(), new[] { queued.TaskMessage });

            Assert.AreEqual("Instance is Terminated", decision);
            Assert.AreEqual(reads + 1, this.pendingWritePolicy.InstanceReads,
                "Only registered entity admission needs this fresh termination check.");
        }

        [DataTestMethod]
        [DataRow(null, 0)]
        [DataRow("awaited-parent", 0)]
        [DataRow("@legacy@test", null)]
        public async Task HistorylessClientAwaitedAndLegacyStartsDoNotAddStatusReads(string parentInstanceId, int? generation)
        {
            TaskMessage start = CreateStart(this.CreateEntitySession());
            var startEvent = (ExecutionStartedEvent)start.Event;
            startEvent.Generation = generation;
            startEvent.ParentInstance = parentInstanceId == null
                ? null
                : new ParentInstance { OrchestrationInstance = new OrchestrationInstance { InstanceId = parentInstanceId } };
            int reads = this.pendingWritePolicy.InstanceReads;

            Assert.IsNull(await this.EvaluateBatchAsync(new OrchestrationRuntimeState(), new[] { start }));

            Assert.AreEqual(reads, this.pendingWritePolicy.InstanceReads);
        }

        [DataTestMethod]
        [DataRow(false, OrchestrationStatus.Completed)]
        [DataRow(true, OrchestrationStatus.Completed)]
        [DataRow(false, OrchestrationStatus.Failed)]
        [DataRow(true, OrchestrationStatus.Failed)]
        [DataRow(false, OrchestrationStatus.Terminated)]
        [DataRow(true, OrchestrationStatus.Terminated)]
        [DataRow(false, OrchestrationStatus.Canceled)]
        [DataRow(true, OrchestrationStatus.Canceled)]
        public async Task ReusedPendingTerminationPreservesCurrentExecutionMetadata(bool useInstanceTableEtag, OrchestrationStatus oldStatus)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync(oldStatus);
            string reason = new string('r', 36 * 1024);
            TaskMessage termination = CreateTermination(start.OrchestrationInstance.InstanceId, reason);
            int reads = this.pendingWritePolicy.InstanceReads;

            string decision = await this.EvaluateBatchAsync(oldRuntime, new[] { termination });

            Assert.AreEqual("Instance is Terminated", decision);
            Assert.AreEqual(reads + 1, this.pendingWritePolicy.InstanceReads,
                "Reuse the terminal-history status snapshot instead of querying it twice.");
            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(OrchestrationStatus.Terminated, after.State.OrchestrationStatus);
            Assert.AreEqual(start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(pending.State.Generation, after.State.Generation);
            Assert.AreEqual(pending.State.CreatedTime, after.State.CreatedTime);
            Assert.AreEqual(pending.State.ParentInstance.OrchestrationInstance.InstanceId, after.State.ParentInstance.OrchestrationInstance.InstanceId);
            Assert.AreEqual(reason, after.State.Output);
            Assert.AreEqual(termination.Event.Timestamp, after.State.LastUpdatedTime);
            Assert.AreNotEqual(pending.ETag, after.ETag);
            Assert.AreEqual(
                oldRuntime.Events.Count,
                (await this.service.TrackingStore.GetHistoryEventsAsync(start.OrchestrationInstance.InstanceId, oldRuntime.OrchestrationInstance.ExecutionId)).Events.Count);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task OldExecutionTerminationDoesNotCancelReusedPendingExecution(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync();
            TaskMessage termination = CreateTermination(
                start.OrchestrationInstance.InstanceId, "old execution only", oldRuntime.OrchestrationInstance.ExecutionId);

            Assert.AreEqual("Instance is Completed", await this.EvaluateBatchAsync(oldRuntime, new[] { termination }));

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(pending.ETag, after.ETag);
            Assert.AreEqual(OrchestrationStatus.Pending, after.State.OrchestrationStatus);
            Assert.AreEqual(start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ExecutionIndependentTerminationIsNotHiddenByOldTargetedTermination(bool independentLast)
        {
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync();
            TaskMessage oldTermination = CreateTermination(
                start.OrchestrationInstance.InstanceId, "old execution", oldRuntime.OrchestrationInstance.ExecutionId);
            TaskMessage independent = CreateTermination(start.OrchestrationInstance.InstanceId, "current execution");
            TaskMessage[] messages = independentLast
                ? new[] { oldTermination, independent }
                : new[] { independent, oldTermination };

            Assert.AreEqual("Instance is Terminated", await this.EvaluateBatchAsync(oldRuntime, messages));

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual("current execution", after.State.Output);
            Assert.AreEqual(start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(pending.State.Generation, after.State.Generation);
        }

        [DataTestMethod]
        [DataRow(false, "replace")]
        [DataRow(true, "replace")]
        [DataRow(false, "start")]
        [DataRow(true, "start")]
        [DataRow(false, "purge")]
        [DataRow(true, "purge")]
        public async Task ReusedPendingTerminationRejectsConcurrentRowChanges(bool useInstanceTableEtag, string change)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync();
            TaskMessage replacement = CreateStart(this.CreateEntitySession(), start.OrchestrationInstance.InstanceId);
            ((ExecutionStartedEvent)replacement.Event).Generation = pending.State.Generation + 1;
            this.pendingWritePolicy.BeforeWrite = async () =>
            {
                if (change == "purge")
                {
                    await this.service.TrackingStore.PurgeInstanceHistoryAsync(start.OrchestrationInstance.InstanceId);
                }
                else if (change == "replace")
                {
                    Assert.IsTrue(await this.service.TrackingStore.SetNewExecutionAsync((ExecutionStartedEvent)replacement.Event, pending.ETag, null));
                }
                else
                {
                    OrchestrationHistory history = await this.service.TrackingStore.GetHistoryEventsAsync(start.OrchestrationInstance.InstanceId, null);
                    var running = new OrchestrationRuntimeState();
                    running.AddEvent(new OrchestratorStartedEvent(-1));
                    running.AddEvent(start.Event);
                    running.AddEvent(new OrchestratorCompletedEvent(-1));
                    await this.service.TrackingStore.UpdateStateAsync(
                        running, new OrchestrationRuntimeState(), start.OrchestrationInstance.InstanceId, start.OrchestrationInstance.ExecutionId,
                        new OrchestrationETags { HistoryETag = history.ETag, InstanceETag = pending.ETag }, history.TrackingStoreContext);
                }
            };

            DurableTaskStorageException error = await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                () => this.EvaluateBatchAsync(oldRuntime, new[] { CreateTermination(start.OrchestrationInstance.InstanceId, "racing termination") }));
            Assert.AreEqual(change == "purge" ? 404 : 412, error.HttpStatusCode);
            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            if (change == "purge")
            {
                Assert.IsNull(after);
                Assert.AreEqual(0, (await this.service.TrackingStore.GetHistoryEventsAsync(start.OrchestrationInstance.InstanceId, null)).Events.Count);
            }
            else
            {
                Assert.IsNotNull(after);
                Assert.AreEqual(change == "replace" ? replacement.OrchestrationInstance.ExecutionId : start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
                Assert.AreEqual(change == "replace" ? OrchestrationStatus.Pending : OrchestrationStatus.Running, after.State.OrchestrationStatus);
                Assert.IsNull(after.State.Output);
            }
        }

        [DataTestMethod]
        [DataRow(false, 404, "TableNotFound")]
        [DataRow(true, 404, "TableNotFound")]
        [DataRow(false, 503, "ServerBusy")]
        [DataRow(true, 503, "ServerBusy")]
        public async Task ReusedPendingTerminationPropagatesStorageFailures(bool useInstanceTableEtag, int status, string errorCode)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync();
            this.pendingWritePolicy.BeforeWrite = () => throw new RequestFailedException(status, "Injected termination failure", errorCode, null);

            DurableTaskStorageException error = await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                () => this.EvaluateBatchAsync(oldRuntime, new[] { CreateTermination(start.OrchestrationInstance.InstanceId, "terminate") }));

            Assert.AreEqual(status, error.HttpStatusCode);
            Assert.AreEqual(errorCode, error.ErrorCode);
            Assert.AreEqual(pending.ETag, (await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId)).ETag);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ReusedPendingTerminationPropagatesCancellation(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            var (oldRuntime, start, pending) = await this.CreateReusedPendingAsync();
            using var cancellation = new CancellationTokenSource();
            this.pendingWritePolicy.BeforeWrite = () =>
            {
                Assert.AreEqual(cancellation.Token, this.pendingWritePolicy.LastWriteCancellationToken);
                cancellation.Cancel();
                cancellation.Token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            };

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => this.EvaluateBatchAsync(
                    oldRuntime, new[] { CreateTermination(start.OrchestrationInstance.InstanceId, "terminate") }, cancellation.Token));

            Assert.IsTrue(cancellation.IsCancellationRequested);
            Assert.AreEqual(pending.ETag, (await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId)).ETag);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ImmediateEntityStartCanCompleteAndContinueAsNew(bool continueAsNew)
        {
            this.settings.UseInstanceTableEtag = true;
            string childInstanceId = Guid.NewGuid().ToString("N");
            this.CreateEntityWorker(() => new StartNewOrchestrationOperationAction
            {
                Name = typeof(ChildOrchestration).FullName,
                InstanceId = childInstanceId,
                Input = continueAsNew ? "\"continue\"" : "\"done\"",
            });
            await this.worker.StartAsync();
            var entity = new OrchestrationInstance { InstanceId = "@pendingstarter@test" };
            await this.service.SendTaskOrchestrationMessageAsync(
                ClientEntityHelpers.EmitOperationSignal(entity, Guid.NewGuid(), "start", null, null).AsTaskMessage());

            var client = new TaskHubClient(this.service);
            OrchestrationState completed = await client.WaitForOrchestrationAsync(
                new OrchestrationInstance { InstanceId = childInstanceId }, TimeSpan.FromSeconds(30));
            Assert.IsNotNull(completed);
            Assert.AreEqual(OrchestrationStatus.Completed, completed.OrchestrationStatus);
            Assert.AreEqual("\"done\"", completed.Output);
        }

        [TestMethod]
        public async Task EntityStartIsEnqueuedBeforePendingAndDeferredUntilRegistration()
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            ControlQueue queue = this.service.AllControlQueues.Single();
            MessageData queuedMessage = null;
            this.pendingWritePolicy.BeforeWrite = async () =>
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                queuedMessage = (await queue.GetMessagesAsync(timeout.Token)).Single();
                Assert.AreEqual(session.Instance.InstanceId, queuedMessage.Sender.InstanceId);
                Assert.AreEqual(session.Instance.ExecutionId, queuedMessage.Sender.ExecutionId);
                Assert.AreEqual(session.GetCurrentEpisode(), queuedMessage.Episode);
                Assert.IsNull(await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId));
                string receipt = queuedMessage.OriginalQueueMessage.PopReceipt;
                Assert.AreEqual(0, (await this.ReconcileStartAsync(queuedMessage)).Count());
                await TestHelpers.WaitFor(
                    () => queuedMessage.OriginalQueueMessage.PopReceipt != receipt,
                    TimeSpan.FromSeconds(10));
            };

            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);

            Assert.IsNotNull(queuedMessage);
            Assert.AreEqual(0, ((ExecutionStartedEvent)start.Event).Generation);
            Assert.AreEqual(1, (await this.ReconcileStartAsync(queuedMessage)).Count());
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Pending)]
        [DataRow(OrchestrationStatus.Running)]
        [DataRow(OrchestrationStatus.Suspended)]
        [DataRow(OrchestrationStatus.ContinuedAsNew)]
        public async Task EntityStartDoesNotReplaceActiveInstance(OrchestrationStatus status)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            TaskMessage previousStart = CreateStart(session, start.OrchestrationInstance.InstanceId);
            await this.SeedInstanceAsync((ExecutionStartedEvent)previousStart.Event, status);
            InstanceStatus before = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);

            await this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session);

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(before.ETag, after.ETag);
            Assert.AreEqual(previousStart.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(status, after.State.OrchestrationStatus);
            Assert.AreEqual(before.State.Generation, ((ExecutionStartedEvent)start.Event).Generation);
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Terminated)]
        [DataRow(OrchestrationStatus.Canceled)]
        public async Task EntityStartReusesTerminalInstanceWithNextGeneration(OrchestrationStatus status)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            TaskMessage previousStart = CreateStart(session, start.OrchestrationInstance.InstanceId);
            await this.SeedInstanceAsync((ExecutionStartedEvent)previousStart.Event, status);

            await this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session);

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(OrchestrationStatus.Pending, after.State.OrchestrationStatus);
            Assert.AreEqual(8, after.State.Generation);
            Assert.AreEqual(8, ((ExecutionStartedEvent)start.Event).Generation);
        }

        [TestMethod]
        public async Task RetriedEntityStartDoesNotResetTerminatedPendingInstance()
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            ControlQueue queue = this.service.AllControlQueues.Single();
            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            InstanceStatus pending = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            Assert.AreEqual(pending.ETag, (await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId)).ETag);

            await this.service.TrackingStore.UpdateStatusForTerminationAsync(
                start.OrchestrationInstance.InstanceId, new ExecutionTerminatedEvent(-1, "terminated"));
            InstanceStatus terminated = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);

            // Also cover a fresh envelope for the same execution, without the prepared Generation.
            TaskMessage retry = CreateStart(session, start.OrchestrationInstance.InstanceId);
            retry.OrchestrationInstance.ExecutionId = start.OrchestrationInstance.ExecutionId;
            await this.service.SendEntityOrchestrationStartAsync(queue, retry, session);
            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(terminated.ETag, after.ETag);
            Assert.AreEqual(OrchestrationStatus.Terminated, after.State.OrchestrationStatus);
            Assert.AreEqual("terminated", after.State.Output);
        }

        [TestMethod]
        public async Task RetriedEntityStartDoesNotReplaceNewerExecutionOrRecreatePurgedInstance()
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            ControlQueue queue = this.service.AllControlQueues.Single();
            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            InstanceStatus pending = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            TaskMessage newer = CreateStart(session, start.OrchestrationInstance.InstanceId);
            ((ExecutionStartedEvent)newer.Event).Generation = 1;
            Assert.IsTrue(await this.service.TrackingStore.SetNewExecutionAsync((ExecutionStartedEvent)newer.Event, pending.ETag, null));
            InstanceStatus beforeRetry = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);

            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            InstanceStatus afterRetry = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(beforeRetry.ETag, afterRetry.ETag);
            Assert.AreEqual(newer.OrchestrationInstance.ExecutionId, afterRetry.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(1, afterRetry.State.Generation);

            await this.service.TrackingStore.PurgeInstanceHistoryAsync(start.OrchestrationInstance.InstanceId);
            await this.service.SendEntityOrchestrationStartAsync(queue, start, session);
            Assert.IsNull(await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId));
        }

        [TestMethod]
        public async Task EntityBatchCanRetryAfterPendingRegistrationFails()
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            ControlQueue queue = this.service.AllControlQueues.Single();
            this.pendingWritePolicy.BeforeWrite = () => throw new RequestFailedException(503, "Injected storage failure", "ServerBusy", null);
            await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                () => this.service.SendEntityOrchestrationStartAsync(queue, start, session));
            Assert.IsNull(await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId));

            // Reprocessing an uncheckpointed entity batch emits a fresh start for the same instance ID.
            TaskMessage retry = CreateStart(session, start.OrchestrationInstance.InstanceId);
            await this.service.SendEntityOrchestrationStartAsync(queue, retry, session);

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(retry.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(OrchestrationStatus.Pending, after.State.OrchestrationStatus);
            Assert.AreEqual(0, after.State.Generation);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task PendingRegistrationDoesNotOverwriteConcurrentCheckpoint(bool useInstanceTableEtag)
        {
            this.settings.UseInstanceTableEtag = useInstanceTableEtag;
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            this.pendingWritePolicy.BeforeWrite = async () =>
            {
                string instanceId = start.OrchestrationInstance.InstanceId;
                OrchestrationHistory history = await this.service.TrackingStore.GetHistoryEventsAsync(instanceId, null);
                var runtimeState = new OrchestrationRuntimeState();
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(start.Event);
                runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));
                await this.service.TrackingStore.UpdateStateAsync(
                    runtimeState, new OrchestrationRuntimeState(), instanceId, start.OrchestrationInstance.ExecutionId,
                    new OrchestrationETags { HistoryETag = history.ETag }, history.TrackingStoreContext);
            };

            await this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session);

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            Assert.AreEqual(OrchestrationStatus.Running, after.State.OrchestrationStatus);
            Assert.AreEqual(start.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task PendingRegistrationDoesNotOverwriteReplacementOrResurrectConcurrentPurge(bool purge)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            await this.SeedInstanceAsync((ExecutionStartedEvent)CreateStart(session, start.OrchestrationInstance.InstanceId).Event, OrchestrationStatus.Completed);
            TaskMessage newer = CreateStart(session, start.OrchestrationInstance.InstanceId);
            ((ExecutionStartedEvent)newer.Event).Generation = 8;
            this.pendingWritePolicy.BeforeWrite = async () =>
            {
                if (purge)
                {
                    await this.service.TrackingStore.PurgeInstanceHistoryAsync(start.OrchestrationInstance.InstanceId);
                }
                else
                {
                    InstanceStatus previous = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
                    Assert.IsTrue(await this.service.TrackingStore.SetNewExecutionAsync((ExecutionStartedEvent)newer.Event, previous.ETag, null));
                }
            };

            await this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session);

            InstanceStatus after = await this.service.TrackingStore.FetchInstanceStatusAsync(start.OrchestrationInstance.InstanceId);
            if (purge)
            {
                Assert.IsNull(after);
            }
            else
            {
                Assert.AreEqual(newer.OrchestrationInstance.ExecutionId, after.State.OrchestrationInstance.ExecutionId);
                Assert.AreEqual(8, after.State.Generation);
            }
        }

        [DataTestMethod]
        [DataRow(404, "TableNotFound")]
        [DataRow(503, "ServerBusy")]
        public async Task PendingRegistrationPropagatesOtherStorageFailures(int status, string errorCode)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            await this.SeedInstanceAsync((ExecutionStartedEvent)CreateStart(session, start.OrchestrationInstance.InstanceId).Event, OrchestrationStatus.Completed);
            this.pendingWritePolicy.BeforeWrite = () => throw new RequestFailedException(status, "Injected storage failure", errorCode, null);

            DurableTaskStorageException error = await Assert.ThrowsExceptionAsync<DurableTaskStorageException>(
                () => this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session));
            Assert.AreEqual(status, error.HttpStatusCode);
            Assert.AreEqual(errorCode, error.ErrorCode);
        }

        [DataTestMethod]
        [DataRow("@legacy@test", null)]
        [DataRow("awaited-parent", 0)]
        public async Task LegacyEntityAndAwaitedSubOrchestrationStartsDoNotRequirePendingRegistration(string parentInstanceId, int? generation)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage start = CreateStart(session);
            var startEvent = (ExecutionStartedEvent)start.Event;
            startEvent.ParentInstance.OrchestrationInstance = new OrchestrationInstance { InstanceId = parentInstanceId };
            startEvent.Generation = generation;
            ControlQueue queue = this.service.AllControlQueues.Single();
            await queue.AddMessageAsync(start, session);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            MessageData message = (await queue.GetMessagesAsync(timeout.Token)).Single();

            Assert.AreEqual(1, (await this.ReconcileStartAsync(message)).Count());
        }

        async Task SeedInstanceAsync(ExecutionStartedEvent start, OrchestrationStatus status)
        {
            start.Generation = 7;
            Assert.IsTrue(await this.service.TrackingStore.SetNewExecutionAsync(start, null, null));
            var table = new TableServiceClient(TestHelpers.GetTestStorageAccountConnectionString()).GetTableClient(this.settings.InstanceTableName);
            await table.UpdateEntityAsync(
                new TableEntity(start.OrchestrationInstance.InstanceId, string.Empty) { ["RuntimeStatus"] = status.ToString() },
                ETag.All, TableUpdateMode.Merge);
        }

        async Task<(OrchestrationRuntimeState OldRuntime, TaskMessage PendingStart, InstanceStatus Pending)> CreateReusedPendingAsync(
            OrchestrationStatus oldStatus = OrchestrationStatus.Completed)
        {
            OrchestrationSession session = this.CreateEntitySession();
            TaskMessage previousStart = CreateStart(session);
            ((ExecutionStartedEvent)previousStart.Event).Generation = 7;
            string instanceId = previousStart.OrchestrationInstance.InstanceId;
            OrchestrationHistory history = await this.service.TrackingStore.GetHistoryEventsAsync(instanceId, null);
            var oldRuntime = new OrchestrationRuntimeState();
            oldRuntime.AddEvent(new OrchestratorStartedEvent(-1));
            oldRuntime.AddEvent(previousStart.Event);
            oldRuntime.AddEvent(new ExecutionCompletedEvent(0, "\"previous output\"", oldStatus));
            oldRuntime.AddEvent(new OrchestratorCompletedEvent(-1));
            await this.service.TrackingStore.UpdateStateAsync(
                oldRuntime, new OrchestrationRuntimeState(), instanceId, previousStart.OrchestrationInstance.ExecutionId,
                new OrchestrationETags { HistoryETag = history.ETag }, history.TrackingStoreContext);
            TaskMessage start = CreateStart(session, instanceId);
            await this.service.SendEntityOrchestrationStartAsync(this.service.AllControlQueues.Single(), start, session);
            InstanceStatus pending = await this.service.TrackingStore.FetchInstanceStatusAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending, pending.State.OrchestrationStatus);
            return (oldRuntime, start, pending);
        }

        static TaskMessage CreateTermination(string instanceId, string reason, string executionId = null)
        {
            return new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId, ExecutionId = executionId },
                Event = new ExecutionTerminatedEvent(-1, reason),
            };
        }

        void CreateEntityWorker(Func<StartNewOrchestrationOperationAction> createStart)
        {
            this.worker = new TaskHubWorker(this.service);
            this.worker.AddTaskOrchestrations(typeof(ChildOrchestration));
            this.worker.AddEntityDispatcherMiddleware((context, next) =>
            {
                var request = context.GetProperty<EntityBatchRequest>();
                context.SetProperty(new EntityBatchResult
                {
                    EntityState = "\"started\"",
                    Results = request.Operations.Select(_ => new OperationResult()).ToList(),
                    Actions = new List<OperationAction> { createStart() },
                });
                return Task.CompletedTask;
            });
        }

        OrchestrationSession CreateEntitySession()
        {
            var instance = new OrchestrationInstance { InstanceId = "@pendingstarter@test", ExecutionId = Guid.NewGuid().ToString("N") };
            var runtimeState = new OrchestrationRuntimeState(new HistoryEvent[]
            {
                new OrchestratorStartedEvent(-1),
                new ExecutionStartedEvent(-1, "\"entity-state\"") { OrchestrationInstance = instance, Name = "pendingstarter" },
                new OrchestratorCompletedEvent(-1),
            });
            return new OrchestrationSession(
                this.settings, "test-account", instance, this.service.AllControlQueues.Single(),
                new List<MessageData>(), runtimeState, new OrchestrationETags(), DateTime.MinValue,
                null, TimeSpan.Zero, CancellationToken.None, Guid.NewGuid());
        }

        static TaskMessage CreateStart(OrchestrationSession session, string instanceId = null)
        {
            var instance = new OrchestrationInstance { InstanceId = instanceId ?? Guid.NewGuid().ToString("N"), ExecutionId = Guid.NewGuid().ToString("N") };
            return new TaskMessage
            {
                OrchestrationInstance = instance,
                Event = new ExecutionStartedEvent(-1, "\"child-input\"")
                {
                    Name = typeof(ChildOrchestration).FullName,
                    Version = string.Empty,
                    OrchestrationInstance = instance,
                    ParentInstance = new ParentInstance { OrchestrationInstance = session.Instance, TaskScheduleId = 0 },
                },
            };
        }

        Task<IEnumerable<MessageData>> ReconcileStartAsync(MessageData message)
        {
            var manager = (OrchestrationSessionManager)typeof(AzureStorageOrchestrationService)
                .GetField("orchestrationSessionManager", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(this.service);
            MethodInfo reconcile = typeof(OrchestrationSessionManager)
                .GetMethod("DedupeExecutionStartedMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            return (Task<IEnumerable<MessageData>>)reconcile.Invoke(manager, new object[]
            {
                this.service.AllControlQueues.Single(), new[] { message }, Guid.NewGuid(), CancellationToken.None,
            });
        }

        Task<string> EvaluateBatchAsync(
            OrchestrationRuntimeState runtimeState,
            TaskMessage[] messages,
            CancellationToken cancellationToken = default)
        {
            MethodInfo evaluate = typeof(AzureStorageOrchestrationService)
                .GetMethod("IsExecutableInstanceAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(evaluate);
            return (Task<string>)evaluate.Invoke(this.service, new object[] { runtimeState, messages, false, cancellationToken });
        }

        sealed class PendingWritePolicy : HttpPipelinePolicy
        {
            readonly string tableName;

            public PendingWritePolicy(string tableName) => this.tableName = tableName;

            public Func<Task> BeforeWrite { get; set; }

            public int InstanceReads { get; private set; }

            public CancellationToken LastWriteCancellationToken { get; private set; }

            public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                this.BeforeWriteAsync(message).GetAwaiter().GetResult();
                ProcessNext(message, pipeline);
            }

            public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
            {
                await this.BeforeWriteAsync(message);
                await ProcessNextAsync(message, pipeline);
            }

            Task BeforeWriteAsync(HttpMessage message)
            {
                if (message.Request.Uri.ToUri().AbsolutePath.IndexOf(this.tableName, StringComparison.Ordinal) < 0)
                {
                    return Task.CompletedTask;
                }

                if (message.Request.Method == RequestMethod.Get)
                {
                    this.InstanceReads++;
                }
                else if (message.Request.Method == RequestMethod.Post ||
                    message.Request.Method == RequestMethod.Put ||
                    message.Request.Method == RequestMethod.Patch ||
                    message.Request.Method.ToString() == "MERGE")
                {
                    this.LastWriteCancellationToken = message.CancellationToken;
                    if (this.BeforeWrite != null)
                    {
                        Func<Task> beforeWrite = this.BeforeWrite;
                        this.BeforeWrite = null;
                        return beforeWrite();
                    }
                }

                return Task.CompletedTask;
            }
        }

        public class ChildOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                if (input == "continue")
                {
                    context.ContinueAsNew("done");
                }

                return Task.FromResult(input);
            }
        }
    }
}
