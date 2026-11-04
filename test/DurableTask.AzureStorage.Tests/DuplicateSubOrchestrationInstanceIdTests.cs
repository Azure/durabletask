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
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class DuplicateSubOrchestrationInstanceIdTests
    {
        const string ChildInput = "private-child-input";
        const string ReleaseEvent = "release";
        const string ScheduleNextEvent = "schedule-next";
        static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

        AzureStorageOrchestrationService service;
        TaskHubWorker worker;
        TaskHubClient client;
        OrchestrationInstance parent;
        string taskHubName;
        bool workerStarted;

        public TestContext TestContext { get; set; }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task DuplicateAwaitedIds_SameBatch_FailsWithoutStartingAnyChild(bool extendedSessions)
        {
            await this.StartAsync(extendedSessions, "Duplicate");

            // There is deliberately no release event: rejection must not depend on a child finishing.
            await this.AssertDuplicateFailureAsync();
            HistoryEvent[] history = await this.GetHistoryAsync(this.parent);
            Assert.AreEqual(0, history.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
            Assert.AreEqual(0, history.OfType<SubOrchestrationInstanceCompletedEvent>().Count());
            Assert.AreEqual(1, history.OfType<ExecutionCompletedEvent>().Single().EventId);

            await this.StopWorkerAsync();
            var trackingStore = (AzureTableTrackingStore)this.service.TrackingStore;
            TableEntity[] instances = await trackingStore.InstancesTable.ExecuteQueryAsync<TableEntity>().ToArrayAsync();
            TableEntity[] persistedHistory = await trackingStore.HistoryTable.ExecuteQueryAsync<TableEntity>().ToArrayAsync();
            CollectionAssert.AreEqual(new[] { this.parent.InstanceId }, instances.Select(e => e.PartitionKey).ToArray());
            Assert.IsTrue(persistedHistory.All(e => e.PartitionKey == this.parent.InstanceId),
                "A rejected batch must not persist history for either a duplicate child or its distinct sibling.");
            Assert.IsFalse(persistedHistory.Any(e =>
                e.TryGetValue("EventType", out object value) && value?.ToString() == nameof(EventType.SubOrchestrationInstanceCreated)));
            Assert.IsNull(await this.client.GetOrchestrationStateAsync(this.parent.InstanceId + "-child"));
            await this.AssertEmptyControlQueuesAsync();
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task DuplicateAwaitedIds_LaterEpisode_FailsWithoutReleasingPendingChild(bool extendedSessions)
        {
            await this.StartAsync(extendedSessions, "Pending");
            SubOrchestrationInstanceCreatedEvent first = await this.WaitForChildStartAsync(0);
            OrchestrationState child = await this.WaitForRunningChildAsync(first.InstanceId, ChildInput + "-0");

            await this.client.RaiseEventAsync(this.parent, ScheduleNextEvent, string.Empty);
            await this.AssertDuplicateFailureAsync();
            await this.StopWorkerAsync();

            HistoryEvent[] history = await this.GetHistoryAsync(this.parent);
            Assert.AreEqual(1, history.OfType<SubOrchestrationInstanceCreatedEvent>().Count(),
                "The child committed in the earlier episode is retained, but the new duplicate must not be committed.");
            Assert.AreEqual(0, history.OfType<SubOrchestrationInstanceCompletedEvent>().Count());
            Assert.AreEqual(0, history.OfType<SubOrchestrationInstanceFailedEvent>().Count());
            Assert.AreEqual(1, history.OfType<ExecutionCompletedEvent>().Single().EventId);

            OrchestrationState remainingChild = await this.client.GetOrchestrationStateAsync(first.InstanceId);
            Assert.AreEqual(OrchestrationStatus.Running, remainingChild.OrchestrationStatus);
            Assert.AreEqual(child.OrchestrationInstance.ExecutionId, remainingChild.OrchestrationInstance.ExecutionId);
            HistoryEvent[] childHistory = await this.GetHistoryAsync(remainingChild.OrchestrationInstance);
            Assert.AreEqual(1, childHistory.OfType<ExecutionStartedEvent>().Count());
            Assert.AreEqual(0, childHistory.OfType<EventRaisedEvent>().Count(),
                "The pending child must never receive a release event in this regression.");
            await this.AssertEmptyControlQueuesAsync();
        }

        [DataTestMethod]
        [DataRow(false, "Distinct")]
        [DataRow(true, "Distinct")]
        [DataRow(false, "Automatic")]
        [DataRow(true, "Automatic")]
        [DataRow(false, "Sequential")]
        [DataRow(true, "Sequential")]
        [DataRow(false, "SequentialAfterFailure")]
        [DataRow(true, "SequentialAfterFailure")]
        public async Task AllowedAwaitedIds_WithGuardEnabled_Completes(bool extendedSessions, string scenario)
        {
            await this.StartAsync(extendedSessions, scenario);
            bool firstChildFails = scenario == "SequentialAfterFailure";
            SubOrchestrationInstanceCreatedEvent first = await this.WaitForChildStartAsync(0);
            OrchestrationState firstChild = await this.WaitForRunningChildAsync(
                first.InstanceId, firstChildFails ? "fail" : ChildInput + "-0");
            await this.client.RaiseEventAsync(firstChild.OrchestrationInstance, ReleaseEvent, string.Empty);

            SubOrchestrationInstanceCreatedEvent second = await this.WaitForChildStartAsync(1);
            OrchestrationState secondChild = await this.WaitForRunningChildAsync(second.InstanceId, ChildInput + "-1");
            await this.client.RaiseEventAsync(secondChild.OrchestrationInstance, ReleaseEvent, string.Empty);

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(this.parent, TestTimeout);
            Assert.IsNotNull(state, "The valid parent did not finish after both children were released.");
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus, state.Output);
            Assert.AreEqual(JsonConvert.SerializeObject(
                (firstChildFails ? "caught" : ChildInput + "-0") + "," + ChildInput + "-1"), state.Output);

            HistoryEvent[] history = await this.GetHistoryAsync(this.parent);
            SubOrchestrationInstanceCreatedEvent[] starts = history.OfType<SubOrchestrationInstanceCreatedEvent>().ToArray();
            Assert.AreEqual(2, starts.Length);
            int[] completedTaskIds = history.OfType<SubOrchestrationInstanceCompletedEvent>()
                .Select(e => e.TaskScheduledId).ToArray();
            int[] failedTaskIds = history.OfType<SubOrchestrationInstanceFailedEvent>()
                .Select(e => e.TaskScheduledId).ToArray();
            CollectionAssert.AreEqual(firstChildFails ? new[] { 1 } : new[] { 0, 1 }, completedTaskIds);
            CollectionAssert.AreEqual(firstChildFails ? new[] { 0 } : Array.Empty<int>(), failedTaskIds);

            if (scenario.StartsWith("Sequential", StringComparison.Ordinal))
            {
                Assert.AreEqual(starts[0].InstanceId, starts[1].InstanceId);
                Assert.AreNotEqual(firstChild.OrchestrationInstance.ExecutionId, secondChild.OrchestrationInstance.ExecutionId);
            }
            else
            {
                Assert.AreNotEqual(starts[0].InstanceId, starts[1].InstanceId);
                if (scenario == "Distinct")
                {
                    Assert.IsTrue(string.Equals(starts[0].InstanceId, starts[1].InstanceId, StringComparison.OrdinalIgnoreCase),
                        "Distinct IDs that differ only by case must retain ordinal identity.");
                }
            }
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            if (this.service == null)
            {
                return;
            }

            try
            {
                await this.StopWorkerAsync();
                var trackingStore = (AzureTableTrackingStore)this.service.TrackingStore;
                this.TestContext.WriteLine("Task hub: " + this.taskHubName);
                if (this.parent != null)
                {
                    this.TestContext.WriteLine("Parent history: " + await this.client.GetOrchestrationHistoryAsync(this.parent));
                }

                this.TestContext.WriteLine("Persisted instances: " + JsonConvert.SerializeObject(
                    await trackingStore.InstancesTable.ExecuteQueryAsync<TableEntity>().ToArrayAsync()));
                this.TestContext.WriteLine("Persisted history: " + JsonConvert.SerializeObject(
                    await trackingStore.HistoryTable.ExecuteQueryAsync<TableEntity>().ToArrayAsync()));
            }
            finally
            {
                try
                {
                    await this.service.DeleteAsync();
                }
                finally
                {
                    this.worker?.Dispose();
                }
            }
        }

        async Task StartAsync(bool extendedSessions, string scenario)
        {
            this.taskHubName = "duplicateids" + Guid.NewGuid().ToString("N");
            AzureStorageOrchestrationServiceSettings settings =
                TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(extendedSessions, extendedSessionTimeoutInSeconds: 5);
            settings.TaskHubName = this.taskHubName;
            settings.PartitionCount = 1;
            settings.MaxQueuePollingInterval = TimeSpan.FromMilliseconds(100);
            this.service = new AzureStorageOrchestrationService(settings);
            this.client = new TaskHubClient(this.service);
            this.worker = new TaskHubWorker(this.service)
            {
                FailOnDuplicateSubOrchestrationInstanceIds = true,
            };
            this.worker.AddTaskOrchestrations(typeof(DuplicateIdParent), typeof(EventGatedChild));
            await this.service.CreateAsync();
            await this.worker.StartAsync();
            this.workerStarted = true;
            this.parent = await this.client.CreateOrchestrationInstanceAsync(
                typeof(DuplicateIdParent), "parent-" + Guid.NewGuid().ToString("N"), scenario);
        }

        async Task AssertDuplicateFailureAsync()
        {
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(this.parent, TestTimeout);
            Assert.IsNotNull(state, "The duplicate-ID parent hung instead of failing without a child release.");
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus, state.Output);
            // AzureStorage exposes the failure message in state, but retains typed details in history.
            HistoryEvent[] history = await this.GetHistoryAsync(this.parent);
            FailureDetails failure = history.OfType<ExecutionCompletedEvent>().Single().FailureDetails;
            Assert.IsNotNull(failure);
            Assert.AreEqual("DuplicateSubOrchestrationInstanceId", failure.ErrorType);
            Assert.IsTrue(failure.IsNonRetriable);
            Assert.AreEqual(failure.ToString(), state.Output);
            StringAssert.Contains(failure.ErrorMessage, this.parent.InstanceId);
            StringAssert.Contains(failure.ErrorMessage, this.parent.InstanceId + "-child");
            StringAssert.Contains(failure.ErrorMessage, "task ID 0");
            StringAssert.Contains(failure.ErrorMessage, "task ID 1");
            StringAssert.Contains(failure.ErrorMessage, "distinct instance IDs");
            Assert.IsFalse(failure.ErrorMessage.Contains(ChildInput),
                "Duplicate-ID diagnostics must not disclose child input.");
            this.TestContext.WriteLine("Failure: " + JsonConvert.SerializeObject(failure));
        }

        async Task<HistoryEvent[]> GetHistoryAsync(OrchestrationInstance instance)
        {
            OrchestrationHistory history = await this.service.TrackingStore.GetHistoryEventsAsync(
                instance.InstanceId, instance.ExecutionId, CancellationToken.None);
            return history.Events.ToArray();
        }

        async Task<SubOrchestrationInstanceCreatedEvent> WaitForChildStartAsync(int taskId)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            do
            {
                HistoryEvent[] history = await this.GetHistoryAsync(this.parent);
                SubOrchestrationInstanceCreatedEvent child = history.OfType<SubOrchestrationInstanceCreatedEvent>()
                    .SingleOrDefault(e => e.EventId == taskId);
                if (child != null)
                {
                    return child;
                }

                await Task.Delay(100);
            }
            while (stopwatch.Elapsed < TestTimeout);

            throw new TimeoutException($"Child task {taskId} was not persisted for parent {this.parent.InstanceId}.");
        }

        async Task<OrchestrationState> WaitForRunningChildAsync(string instanceId, string expectedInput)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            do
            {
                OrchestrationState state = await this.client.GetOrchestrationStateAsync(instanceId);
                if (state?.OrchestrationStatus == OrchestrationStatus.Running &&
                    state.Input == JsonConvert.SerializeObject(expectedInput))
                {
                    return state;
                }

                await Task.Delay(100);
            }
            while (stopwatch.Elapsed < TestTimeout);

            throw new TimeoutException($"Child {instanceId} did not reach its external-event wait.");
        }

        async Task StopWorkerAsync()
        {
            if (this.workerStarted)
            {
                await this.worker.StopAsync();
                this.workerStarted = false;
            }
        }

        async Task AssertEmptyControlQueuesAsync()
        {
            var queues = this.service.AllControlQueues.ToArray();
            Assert.AreEqual(1, queues.Length, "The outbound queue assertion must inspect the task hub's actual control queue.");
            foreach (var queue in queues)
            {
                int count = await queue.InnerQueue.GetApproximateMessagesCountAsync();
                this.TestContext.WriteLine($"Persisted queue {queue.Name}: {count} messages");
                Assert.AreEqual(0, count, "A rejected child start must not remain queued, including invisible messages.");
            }
        }

        public class DuplicateIdParent : TaskOrchestration<string, string>
        {
            readonly TaskCompletionSource<bool> scheduleNext = new TaskCompletionSource<bool>();

            public override async Task<string> RunTask(OrchestrationContext context, string scenario)
            {
                string childId = context.OrchestrationInstance.InstanceId + "-child";
                bool firstChildFails = scenario == "SequentialAfterFailure";
                string firstInput = firstChildFails ? "fail" : ChildInput + "-0";
                Task<string> first = scenario == "Automatic"
                    ? context.CreateSubOrchestrationInstance<string>(typeof(EventGatedChild), firstInput)
                    : context.CreateSubOrchestrationInstance<string>(typeof(EventGatedChild), childId, firstInput);

                if (scenario.StartsWith("Sequential", StringComparison.Ordinal))
                {
                    string firstResult;
                    try
                    {
                        firstResult = await first;
                    }
                    catch (SubOrchestrationFailedException) when (firstChildFails)
                    {
                        firstResult = "caught";
                    }

                    string secondResult = await context.CreateSubOrchestrationInstance<string>(
                        typeof(EventGatedChild), childId, ChildInput + "-1");
                    return firstResult + "," + secondResult;
                }

                if (scenario == "Pending")
                {
                    await this.scheduleNext.Task;
                }

                Task<string> second = scenario == "Automatic"
                    ? context.CreateSubOrchestrationInstance<string>(typeof(EventGatedChild), ChildInput + "-1")
                    : context.CreateSubOrchestrationInstance<string>(
                        typeof(EventGatedChild),
                        scenario == "Distinct" ? context.OrchestrationInstance.InstanceId + "-CHILD" : childId,
                        ChildInput + "-1");
                if (scenario == "Duplicate")
                {
                    // This unrelated start must also be discarded with the invalid decision batch.
                    Task<string> sibling = context.CreateSubOrchestrationInstance<string>(
                        typeof(EventGatedChild), childId + "-sibling", ChildInput);
                    return string.Join(",", await Task.WhenAll(first, second, sibling));
                }

                return string.Join(",", await Task.WhenAll(first, second));
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == ScheduleNextEvent)
                {
                    this.scheduleNext.TrySetResult(true);
                }
            }
        }

        public class EventGatedChild : TaskOrchestration<string, string>
        {
            readonly TaskCompletionSource<bool> release = new TaskCompletionSource<bool>();

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                await this.release.Task;
                if (input == "fail")
                {
                    throw new InvalidOperationException("Expected child failure before sequential ID reuse.");
                }

                return input;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == ReleaseEvent)
                {
                    this.release.TrySetResult(true);
                }
            }
        }
    }
}
