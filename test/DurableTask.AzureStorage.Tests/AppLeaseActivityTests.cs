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
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AppLeaseActivityTests
    {
        static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);
        static readonly TimeSpan AutomaticFailoverTimeout = TimeSpan.FromSeconds(30);

        [TestMethod]
        public async Task DifferentAppCannotDequeueActivityWhilePassive()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService owner = CreateService(taskHubName, "PrimaryApp", useAppLease: true);
            AzureStorageOrchestrationService passive = CreateService(taskHubName, "SecondaryApp", useAppLease: true);

            try
            {
                await owner.CreateAsync();
                await owner.StartAsync();
                await WaitForOwnerAsync(owner);
                await passive.StartAsync();
                await EnqueueActivityAsync(owner, "activity");

                using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                {
                    TaskActivityWorkItem passiveWorkItem = await passive.LockNextTaskActivityWorkItem(
                        TestTimeout,
                        cancellation.Token);
                    Assert.IsNull(passiveWorkItem);
                }

                TaskActivityWorkItem ownerWorkItem = await LockActivityAsync(owner);
                Assert.IsNotNull(ownerWorkItem);
                await owner.AbandonTaskActivityWorkItemAsync(ownerWorkItem);
            }
            finally
            {
                await StopAsync(passive);
                await StopAsync(owner);
            }
        }

        [TestMethod]
        public async Task SameAppWorkersCanDequeueActivitiesConcurrently()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService worker1 = CreateService(taskHubName, "SharedApp", useAppLease: true);
            AzureStorageOrchestrationService worker2 = CreateService(taskHubName, "SharedApp", useAppLease: true);

            try
            {
                await worker1.CreateAsync();
                await worker1.StartAsync();
                await WaitForOwnerAsync(worker1);
                await worker2.StartAsync();
                await EnqueueActivityAsync(worker1, "activity-1");
                await EnqueueActivityAsync(worker1, "activity-2");

                Task<TaskActivityWorkItem> lock1 = LockActivityAsync(worker1);
                Task<TaskActivityWorkItem> lock2 = LockActivityAsync(worker2);
                TaskActivityWorkItem[] workItems = await Task.WhenAll(lock1, lock2);

                Assert.IsNotNull(workItems[0]);
                Assert.IsNotNull(workItems[1]);
                Assert.AreNotEqual(workItems[0].Id, workItems[1].Id);

                await worker1.AbandonTaskActivityWorkItemAsync(workItems[0]);
                await worker2.AbandonTaskActivityWorkItemAsync(workItems[1]);
            }
            finally
            {
                await StopAsync(worker2);
                await StopAsync(worker1);
            }
        }

        [TestMethod]
        public async Task AppLeaseDisabledAllowsDifferentAppsToDequeueActivities()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService worker1 = CreateService(taskHubName, "App1", useAppLease: false);
            AzureStorageOrchestrationService worker2 = CreateService(taskHubName, "App2", useAppLease: false);

            try
            {
                await worker1.CreateAsync();
                await worker1.StartAsync();
                await worker2.StartAsync();
                await EnqueueActivityAsync(worker1, "activity-1");
                await EnqueueActivityAsync(worker1, "activity-2");

                TaskActivityWorkItem[] workItems = await Task.WhenAll(
                    LockActivityAsync(worker1),
                    LockActivityAsync(worker2));

                Assert.IsNotNull(workItems[0]);
                Assert.IsNotNull(workItems[1]);

                await worker1.AbandonTaskActivityWorkItemAsync(workItems[0]);
                await worker2.AbandonTaskActivityWorkItemAsync(workItems[1]);
            }
            finally
            {
                await StopAsync(worker2);
                await StopAsync(worker1);
            }
        }

        [TestMethod]
        public async Task ForcedFailoverCancelsOldOwnerPollAndEnablesNewOwner()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService oldOwner = CreateService(taskHubName, "OldApp", useAppLease: true);
            AzureStorageOrchestrationService newOwner = CreateService(taskHubName, "NewApp", useAppLease: true);

            try
            {
                await oldOwner.CreateAsync();
                await oldOwner.StartAsync();
                await WaitForOwnerAsync(oldOwner);
                await newOwner.StartAsync();

                Task<TaskActivityWorkItem> oldOwnerPoll = oldOwner.LockNextTaskActivityWorkItem(
                    TestTimeout,
                    CancellationToken.None);

                await newOwner.ForceChangeAppLeaseAsync();
                await WaitForOwnerAsync(newOwner);
                await EnqueueActivityAsync(newOwner, "activity");

                TaskActivityWorkItem newOwnerWorkItem = await LockActivityAsync(newOwner);
                Assert.IsNotNull(newOwnerWorkItem);
                Assert.IsNull(await WithTimeoutAsync(oldOwnerPoll));

                await newOwner.AbandonTaskActivityWorkItemAsync(newOwnerWorkItem);
            }
            finally
            {
                await StopAsync(newOwner);
                await StopAsync(oldOwner);
            }
        }

        [TestMethod]
        public async Task ForcedFailoverQuiescesOldAppBeforeNewOwnerStarts()
        {
            string taskHubName = GetTaskHubName();
            TimeSpan renewInterval = TimeSpan.FromSeconds(2);
            AzureStorageOrchestrationServiceSettings oldSettings = CreateSettings(
                taskHubName,
                "OldApp",
                useAppLease: true,
                renewInterval: renewInterval);
            AzureStorageOrchestrationServiceSettings newSettings = CreateSettings(
                taskHubName,
                "NewApp",
                useAppLease: true,
                renewInterval: renewInterval);
            AppLeaseManager oldManager = CreateAppLeaseManager(
                oldSettings,
                new TestPartitionManager());
            AppLeaseManager newManager = CreateAppLeaseManager(
                newSettings,
                new TestPartitionManager());
            try
            {
                await oldManager.CreateContainerIfNotExistsAsync();
                await oldManager.StartAsync();
                using (AppLeaseOwnershipSignal.AppLeaseOwnership oldOwnership =
                    await GetOwnershipAsync(oldManager))
                {
                    await newManager.StartAsync();

                    await newManager.ForceChangeAppLeaseAsync();
                    using (AppLeaseOwnershipSignal.AppLeaseOwnership newOwnership =
                        await GetOwnershipAsync(newManager))
                    {
                        Assert.IsTrue(oldOwnership.LostToken.IsCancellationRequested);
                        Assert.IsFalse(oldOwnership.TryBeginDispatch());
                    }
                }
            }
            finally
            {
                await newManager.StopAsync();
                await oldManager.StopAsync();
            }
        }

        [TestMethod]
        public async Task LeaseExpirationEnablesNewOwner()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService oldOwner = CreateService(taskHubName, "OldApp", useAppLease: true);
            AzureStorageOrchestrationService newOwner = CreateService(taskHubName, "NewApp", useAppLease: true);

            try
            {
                await oldOwner.CreateAsync();
                await oldOwner.StartAsync();
                await WaitForOwnerAsync(oldOwner);
                await newOwner.StartAsync();
                await oldOwner.StopAsync(isForced: true);
                oldOwner = null;
                await EnqueueActivityAsync(newOwner, "activity");

                TaskActivityWorkItem newOwnerWorkItem =
                    await LockActivityAsync(newOwner, AutomaticFailoverTimeout);

                Assert.IsNotNull(newOwnerWorkItem);
                await newOwner.AbandonTaskActivityWorkItemAsync(newOwnerWorkItem);
            }
            finally
            {
                await StopAsync(newOwner);
                await StopAsync(oldOwner);
            }
        }

        [TestMethod]
        public async Task PreviousOwnerProcessesOrchestrationAfterReacquiringAppLease()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationServiceSettings firstSettings =
                CreateSettings(taskHubName, "FirstApp", useAppLease: true);
            AzureStorageOrchestrationServiceSettings secondSettings =
                CreateSettings(taskHubName, "SecondApp", useAppLease: true);
            firstSettings.LeaseAcquireInterval = TimeSpan.FromMilliseconds(200);
            secondSettings.LeaseAcquireInterval = TimeSpan.FromMilliseconds(200);

            var firstService = new AzureStorageOrchestrationService(firstSettings);
            var secondService = new AzureStorageOrchestrationService(secondSettings);
            var firstWorker = new TaskHubWorker(firstService);
            var secondWorker = new TaskHubWorker(secondService);
            firstWorker.AddTaskOrchestrations(typeof(WaitForSignalOrchestration));
            secondWorker.AddTaskOrchestrations(typeof(WaitForSignalOrchestration));

            bool firstWorkerStarted = false;
            bool secondWorkerStarted = false;

            try
            {
                await firstService.CreateAsync();
                await firstWorker.StartAsync();
                firstWorkerStarted = true;
                await WaitForOwnerAsync(firstService);

                var client = new TaskHubClient(firstService);
                OrchestrationInstance instance =
                    await client.CreateOrchestrationInstanceAsync(
                        typeof(WaitForSignalOrchestration),
                        input: null);
                await WaitForOrchestrationStatusAsync(
                    client,
                    instance,
                    OrchestrationStatus.Running);

                await secondWorker.StartAsync();
                secondWorkerStarted = true;
                await secondService.ForceChangeAppLeaseAsync();
                await WaitForOwnerAsync(secondService, secondSettings, instance.InstanceId);

                await secondWorker.StopAsync(isForced: true);
                secondWorkerStarted = false;
                await WaitForAppLeaseOwnerAsync(firstSettings, AutomaticFailoverTimeout);
                await WaitForOwnerAsync(
                    firstService,
                    firstSettings,
                    instance.InstanceId,
                    AutomaticFailoverTimeout);
                await client.RaiseEventAsync(instance, "complete", "done");

                OrchestrationState state =
                    await client.WaitForOrchestrationAsync(instance, TestTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.AreEqual("\"done\"", state.Output);
            }
            finally
            {
                if (secondWorkerStarted)
                {
                    await secondWorker.StopAsync(isForced: true);
                }

                if (firstWorkerStarted)
                {
                    await firstWorker.StopAsync(isForced: true);
                }
            }
        }

        [TestMethod]
        public async Task ReleasedPartitionsDoNotResumeListeningBeforeFailback()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationServiceSettings firstSettings =
                CreateSettings(taskHubName, "FirstApp", useAppLease: true);
            AzureStorageOrchestrationServiceSettings secondSettings =
                CreateSettings(taskHubName, "SecondApp", useAppLease: true);
            firstSettings.PartitionCount = 4;
            firstSettings.LeaseAcquireInterval = TimeSpan.FromMilliseconds(200);
            secondSettings.PartitionCount = 4;
            secondSettings.LeaseAcquireInterval = TimeSpan.FromMilliseconds(200);
            secondSettings.ControlQueueBufferThreshold = 0;

            var firstService = new AzureStorageOrchestrationService(firstSettings);
            var secondService = new AzureStorageOrchestrationService(secondSettings);
            var firstWorker = new TaskHubWorker(firstService);
            var secondWorker = new TaskHubWorker(secondService);
            firstWorker.AddTaskOrchestrations(
                new WorkerIdentityOrchestrationCreator("FirstApp"));
            secondWorker.AddTaskOrchestrations(
                new WorkerIdentityOrchestrationCreator("SecondApp"));

            bool firstWorkerStarted = false;
            bool secondWorkerStarted = false;

            try
            {
                await firstService.CreateAsync();
                await firstWorker.StartAsync();
                firstWorkerStarted = true;
                await WaitForAppLeaseOwnerAsync(firstSettings);
                await WaitForTableOwnershipAsync(firstService, firstSettings);
                await WaitForReceivingQueueCountAsync(firstService, firstSettings.PartitionCount);

                await secondWorker.StartAsync();
                secondWorkerStarted = true;
                await secondService.ForceChangeAppLeaseAsync();
                await WaitForAppLeaseOwnerAsync(secondSettings);
                await WaitForTableOwnershipAsync(secondService, secondSettings);
                await WaitForReceivingQueueCountAsync(secondService, secondSettings.PartitionCount);

                string[] oldReceivingQueues = firstService.OwnedControlQueues
                    .Where(queue => !queue.IsReleased)
                    .Select(queue => queue.Name)
                    .ToArray();
                if (oldReceivingQueues.Length == 0)
                {
                    secondSettings.ControlQueueBufferThreshold = 1000;
                }

                var client = new TaskHubClient(firstService);
                OrchestrationInstance secondOwnerInstance =
                    await client.CreateOrchestrationInstanceAsync(
                        WorkerIdentityOrchestrationCreator.OrchestrationName,
                        version: string.Empty,
                        input: null);
                OrchestrationState secondOwnerState =
                    await client.WaitForOrchestrationAsync(secondOwnerInstance, TestTimeout);

                Assert.AreEqual(
                    0,
                    oldReceivingQueues.Length,
                    $"The previous app resumed receiving from released partitions: {string.Join(", ", oldReceivingQueues)}. " +
                    $"New orchestration output: {secondOwnerState?.Output ?? "<not completed>"}.");
                if (secondOwnerState == null)
                {
                    Assert.Fail("The new owner did not complete the orchestration.");
                }

                Assert.AreEqual(OrchestrationStatus.Completed, secondOwnerState.OrchestrationStatus);
                Assert.AreEqual("\"SecondApp\"", secondOwnerState.Output);

                await secondWorker.StopAsync(isForced: true);
                secondWorkerStarted = false;
                await WaitForAppLeaseOwnerAsync(firstSettings, AutomaticFailoverTimeout);
                await WaitForTableOwnershipAsync(
                    firstService,
                    firstSettings,
                    AutomaticFailoverTimeout);
                await WaitForReceivingQueueCountAsync(
                    firstService,
                    firstSettings.PartitionCount,
                    AutomaticFailoverTimeout);

                OrchestrationInstance firstOwnerInstance =
                    await client.CreateOrchestrationInstanceAsync(
                        WorkerIdentityOrchestrationCreator.OrchestrationName,
                        version: string.Empty,
                        input: null);
                OrchestrationState firstOwnerState =
                    await client.WaitForOrchestrationAsync(firstOwnerInstance, TestTimeout);

                Assert.IsNotNull(firstOwnerState);
                Assert.AreEqual(OrchestrationStatus.Completed, firstOwnerState.OrchestrationStatus);
                Assert.AreEqual("\"FirstApp\"", firstOwnerState.Output);
            }
            finally
            {
                if (secondWorkerStarted)
                {
                    await secondWorker.StopAsync(isForced: true);
                }

                if (firstWorkerStarted)
                {
                    await firstWorker.StopAsync(isForced: true);
                }
            }
        }

        [TestMethod]
        public async Task AppLeaseManagerCanRestartAfterPartitionManagerStopFails()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationServiceSettings settings =
                CreateSettings(taskHubName, "PrimaryApp", useAppLease: true);
            var partitionManager = new StopFailingPartitionManager();
            AppLeaseManager manager = CreateAppLeaseManager(settings, partitionManager);

            await manager.CreateContainerIfNotExistsAsync();
            await manager.StartAsync();
            await WaitForOwnershipAsync(manager);

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => manager.StopAsync());

            await manager.StartAsync();
            await WaitForOwnershipAsync(manager);

            await WithTimeoutAsync(manager.StopAsync());
            Assert.AreEqual(2, partitionManager.StartCount);
            Assert.AreEqual(2, partitionManager.StopCount);
        }

        [TestMethod]
        public async Task ForceChangeBeforeFirstOwnerAcquiresLease()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationServiceSettings settings =
                CreateSettings(taskHubName, "PrimaryApp", useAppLease: true);
            var partitionManager = new TestPartitionManager();
            AppLeaseManager manager = CreateAppLeaseManager(settings, partitionManager);

            try
            {
                await manager.CreateContainerIfNotExistsAsync();

                await manager.ForceChangeAppLeaseAsync();

                using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
                using (await manager.WaitForOwnershipAsync(cancellation.Token))
                {
                    Assert.AreEqual(1, partitionManager.StartCount);
                }
            }
            finally
            {
                await manager.StopAsync();
            }
        }

        [TestMethod]
        public async Task LeaseLossDoesNotCancelDispatchedActivity()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService oldOwner = CreateService(taskHubName, "OldApp", useAppLease: true);
            AzureStorageOrchestrationService newOwner = CreateService(taskHubName, "NewApp", useAppLease: true);

            try
            {
                await oldOwner.CreateAsync();
                await oldOwner.StartAsync();
                await WaitForOwnerAsync(oldOwner);
                await newOwner.StartAsync();
                await EnqueueActivityAsync(oldOwner, "in-flight");

                TaskActivityWorkItem inFlightWorkItem = await LockActivityAsync(oldOwner);
                await newOwner.ForceChangeAppLeaseAsync();
                await EnqueueActivityAsync(newOwner, "after-failover");
                TaskActivityWorkItem newOwnerWorkItem = await LockActivityAsync(newOwner);

                TaskActivityWorkItem renewedWorkItem =
                    await oldOwner.RenewTaskActivityWorkItemLockAsync(inFlightWorkItem);
                Assert.IsTrue(renewedWorkItem.LockedUntilUtc > DateTime.UtcNow);

                await oldOwner.AbandonTaskActivityWorkItemAsync(inFlightWorkItem);
                await newOwner.AbandonTaskActivityWorkItemAsync(newOwnerWorkItem);
            }
            finally
            {
                await StopAsync(newOwner);
                await StopAsync(oldOwner);
            }
        }

        [TestMethod]
        public async Task OwnershipLossAfterDequeueDoesNotStartProcessingTrace()
        {
            string taskHubName = GetTaskHubName();
            var loggerFactory = new RecordingLoggerFactory();
            AzureStorageOrchestrationServiceSettings oldSettings =
                CreateSettings(taskHubName, "OldApp", useAppLease: true);
            oldSettings.LoggerFactory = loggerFactory;
            AzureStorageOrchestrationService oldOwner =
                new AzureStorageOrchestrationService(oldSettings);
            AzureStorageOrchestrationService newOwner =
                CreateService(taskHubName, "NewApp", useAppLease: true);

            try
            {
                await oldOwner.CreateAsync();
                await oldOwner.StartAsync();
                await WaitForOwnerAsync(oldOwner);
                await newOwner.StartAsync();
                await EnqueueActivityAsync(oldOwner, "ownership-race");

                oldOwner.OnActivityMessageDequeued = async () =>
                {
                    oldOwner.OnActivityMessageDequeued = null;
                    await newOwner.ForceChangeAppLeaseAsync();
                    await WaitForOwnerAsync(newOwner);
                };

                TaskActivityWorkItem rejectedWorkItem = await LockActivityAsync(oldOwner);

                Assert.IsNull(rejectedWorkItem);
                Assert.IsFalse(loggerFactory.HasEvent("ProcessingMessage"));

                TaskActivityWorkItem newOwnerWorkItem = await LockActivityAsync(newOwner);
                Assert.IsNotNull(newOwnerWorkItem);
                await newOwner.AbandonTaskActivityWorkItemAsync(newOwnerWorkItem);
            }
            finally
            {
                await StopAsync(newOwner);
                await StopAsync(oldOwner);
            }
        }

        [TestMethod]
        public async Task PassiveActivityPollStopsOnCancellationAndShutdown()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService owner = CreateService(taskHubName, "PrimaryApp", useAppLease: true);
            AzureStorageOrchestrationService passive = CreateService(taskHubName, "SecondaryApp", useAppLease: true);

            try
            {
                await owner.CreateAsync();
                await owner.StartAsync();
                await WaitForOwnerAsync(owner);
                await passive.StartAsync();

                using (var cancellation = new CancellationTokenSource())
                {
                    Task<TaskActivityWorkItem> canceledPoll = passive.LockNextTaskActivityWorkItem(
                        TestTimeout,
                        cancellation.Token);
                    cancellation.Cancel();
                    Assert.IsNull(await WithTimeoutAsync(canceledPoll));
                }

                Task<TaskActivityWorkItem> shutdownPoll = passive.LockNextTaskActivityWorkItem(
                    TestTimeout,
                    CancellationToken.None);
                await passive.StopAsync(isForced: true);
                Assert.IsNull(await WithTimeoutAsync(shutdownPoll));
                passive = null;
            }
            finally
            {
                await StopAsync(passive);
                await StopAsync(owner);
            }
        }

        static AzureStorageOrchestrationService CreateService(
            string taskHubName,
            string appName,
            bool useAppLease,
            TimeSpan? renewInterval = null)
        {
            return new AzureStorageOrchestrationService(
                CreateSettings(taskHubName, appName, useAppLease, renewInterval));
        }

        static AzureStorageOrchestrationServiceSettings CreateSettings(
            string taskHubName,
            string appName,
            bool useAppLease,
            TimeSpan? renewInterval = null)
        {
            return new AzureStorageOrchestrationServiceSettings
            {
                AppName = appName,
                AppLeaseOptions = new AppLeaseOptions
                {
                    AcquireInterval = TimeSpan.FromMilliseconds(200),
                    LeaseInterval = TimeSpan.FromSeconds(15),
                    RenewInterval = renewInterval ?? TimeSpan.FromMilliseconds(200),
                },
                MaxQueuePollingInterval = TimeSpan.FromMilliseconds(50),
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(
                    TestHelpers.GetTestStorageAccountConnectionString()),
                TaskHubName = taskHubName,
                UseAppLease = useAppLease,
                WorkerId = Guid.NewGuid().ToString("N"),
            };
        }

        static string GetTaskHubName()
        {
            return "applease" + Guid.NewGuid().ToString("N").Substring(0, 16);
        }

        static async Task EnqueueActivityAsync(
            AzureStorageOrchestrationService service,
            string activityName)
        {
            var instance = new OrchestrationInstance
            {
                ExecutionId = Guid.NewGuid().ToString("N"),
                InstanceId = Guid.NewGuid().ToString("N"),
            };

            await service.WorkItemQueue.AddMessageAsync(
                new TaskMessage
                {
                    Event = new TaskScheduledEvent(0, activityName),
                    OrchestrationInstance = instance,
                },
                instance);
        }

        static async Task WaitForOwnerAsync(AzureStorageOrchestrationService service)
        {
            await TestHelpers.WaitFor(
                () => service.OwnedControlQueues.Any(),
                TestTimeout);
        }

        static async Task WaitForOwnerAsync(
            AzureStorageOrchestrationService service,
            AzureStorageOrchestrationServiceSettings settings,
            string instanceId,
            TimeSpan? timeout = null)
        {
            uint partitionIndex =
                Fnv1aHashHelper.ComputeHash(instanceId) % (uint)settings.PartitionCount;
            string queueName =
                AzureStorageOrchestrationService.GetControlQueueName(
                    settings.TaskHubName,
                    (int)partitionIndex);

            await TestHelpers.WaitFor(
                () => service.OwnedControlQueues.Any(queue => queue.Name == queueName),
                timeout ?? TestTimeout);
        }

        static async Task WaitForAppLeaseOwnerAsync(
            AzureStorageOrchestrationServiceSettings settings,
            TimeSpan? timeout = null)
        {
            string taskHubName = settings.TaskHubName.ToLowerInvariant();
            Blob appLeaseInfoBlob = new AzureStorageClient(settings)
                .GetBlobContainerReference(taskHubName + "-applease")
                .GetBlobReference(taskHubName + "-appleaseinfo");
            byte[] appLeaseIdBytes =
                BitConverter.GetBytes(Fnv1aHashHelper.ComputeHash(settings.AppName));
            Array.Resize(ref appLeaseIdBytes, 16);
            string expectedOwnerId = new Guid(appLeaseIdBytes).ToString();

            using (var cancellation = new CancellationTokenSource(timeout ?? TestTimeout))
            {
                try
                {
                    while (true)
                    {
                        string json = await appLeaseInfoBlob.DownloadTextAsync();
                        var info = Utils.DeserializeFromJson<TestAppLeaseInfo>(json);
                        if (info.OwnerId == expectedOwnerId)
                        {
                            return;
                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellation.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    Assert.Fail("The previous owner did not reacquire the app lease.");
                }
            }
        }

        static async Task WaitForTableOwnershipAsync(
            AzureStorageOrchestrationService service,
            AzureStorageOrchestrationServiceSettings settings,
            TimeSpan? timeout = null)
        {
            using (var cancellation = new CancellationTokenSource(timeout ?? TestTimeout))
            {
                try
                {
                    while (true)
                    {
                        List<TablePartitionLease> leases =
                            await service.ListTableLeasesAsync().ToListAsync();
                        if (leases.Count == settings.PartitionCount &&
                            leases.All(
                                lease =>
                                    lease.CurrentOwner == settings.WorkerId &&
                                    !lease.IsDraining))
                        {
                            return;
                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellation.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    Assert.Fail(
                        $"Worker {settings.WorkerId} did not own all non-draining table partitions.");
                }
            }
        }

        static Task WaitForReceivingQueueCountAsync(
            AzureStorageOrchestrationService service,
            int expectedCount,
            TimeSpan? timeout = null)
        {
            return TestHelpers.WaitFor(
                () => service.OwnedControlQueues.Count(queue => !queue.IsReleased) == expectedCount,
                timeout ?? TestTimeout);
        }

        static async Task WaitForOrchestrationStatusAsync(
            TaskHubClient client,
            OrchestrationInstance instance,
            OrchestrationStatus expectedStatus)
        {
            using (var cancellation = new CancellationTokenSource(TestTimeout))
            {
                try
                {
                    while (true)
                    {
                        OrchestrationState state =
                            await client.GetOrchestrationStateAsync(instance);
                        if (state?.OrchestrationStatus == expectedStatus)
                        {
                            return;
                        }

                        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellation.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    Assert.Fail(
                        $"The orchestration did not reach {expectedStatus} before the test timeout.");
                }
            }
        }

        static async Task WaitForOwnershipAsync(AppLeaseManager manager)
        {
            using (await GetOwnershipAsync(manager))
            {
            }
        }

        static async Task<AppLeaseOwnershipSignal.AppLeaseOwnership> GetOwnershipAsync(
            AppLeaseManager manager)
        {
            using (var cancellation = new CancellationTokenSource(TestTimeout))
            {
                return await manager.WaitForOwnershipAsync(cancellation.Token);
            }
        }

        static AppLeaseManager CreateAppLeaseManager(
            AzureStorageOrchestrationServiceSettings settings,
            IPartitionManager partitionManager)
        {
            string taskHubName = settings.TaskHubName.ToLowerInvariant();
            return new AppLeaseManager(
                new AzureStorageClient(settings),
                partitionManager,
                taskHubName + "-applease",
                taskHubName + "-appleaseinfo",
                settings.AppLeaseOptions);
        }

        static async Task<TaskActivityWorkItem> LockActivityAsync(
            AzureStorageOrchestrationService service,
            TimeSpan? timeout = null)
        {
            TimeSpan effectiveTimeout = timeout ?? TestTimeout;
            using (var cancellation = new CancellationTokenSource(effectiveTimeout))
            {
                return await service.LockNextTaskActivityWorkItem(effectiveTimeout, cancellation.Token);
            }
        }

        static async Task<T> WithTimeoutAsync<T>(Task<T> task)
        {
            Task completedTask = await Task.WhenAny(task, Task.Delay(TestTimeout));
            Assert.AreSame(task, completedTask, "The operation did not complete before the test timeout.");
            return await task;
        }

        static async Task WithTimeoutAsync(Task task)
        {
            Task completedTask = await Task.WhenAny(task, Task.Delay(TestTimeout));
            Assert.AreSame(task, completedTask, "The operation did not complete before the test timeout.");
            await task;
        }

        static async Task StopAsync(AzureStorageOrchestrationService service)
        {
            if (service != null)
            {
                await service.StopAsync(isForced: true);
            }
        }

        sealed class StopFailingPartitionManager : IPartitionManager
        {
            public int StartCount { get; private set; }

            public int StopCount { get; private set; }

            public Task StartAsync()
            {
                this.StartCount++;
                return Task.CompletedTask;
            }

            public Task StopAsync()
            {
                this.StopCount++;
                return this.StopCount == 1
                    ? Task.FromException(new InvalidOperationException("Simulated stop failure."))
                    : Task.CompletedTask;
            }

            public Task CreateLeaseStore() => Task.CompletedTask;

            public Task CreateLease(string leaseName) => Task.CompletedTask;

            public Task DeleteLeases() => Task.CompletedTask;

            public async IAsyncEnumerable<BlobPartitionLease> GetOwnershipBlobLeasesAsync(
                [EnumeratorCancellation]
                CancellationToken cancellationToken = default)
            {
                await Task.CompletedTask;
                yield break;
            }
        }

        sealed class WorkerIdentityOrchestrationCreator : ObjectCreator<TaskOrchestration>
        {
            public const string OrchestrationName = "AppLeaseWorkerIdentity";

            readonly string workerIdentity;

            public WorkerIdentityOrchestrationCreator(string workerIdentity)
            {
                this.workerIdentity = workerIdentity;
                this.Name = OrchestrationName;
                this.Version = string.Empty;
            }

            public override TaskOrchestration Create()
            {
                return new WorkerIdentityOrchestration(this.workerIdentity);
            }
        }

        sealed class WorkerIdentityOrchestration : TaskOrchestration<string, string>
        {
            readonly string workerIdentity;

            public WorkerIdentityOrchestration(string workerIdentity)
            {
                this.workerIdentity = workerIdentity;
            }

            public override Task<string> RunTask(
                OrchestrationContext context,
                string input)
            {
                return Task.FromResult(this.workerIdentity);
            }
        }

        sealed class TestPartitionManager : IPartitionManager
        {
            public int StartCount { get; private set; }

            public Task StartAsync()
            {
                this.StartCount++;
                return Task.CompletedTask;
            }

            public Task StopAsync() => Task.CompletedTask;

            public Task CreateLeaseStore() => Task.CompletedTask;

            public Task CreateLease(string leaseName) => Task.CompletedTask;

            public Task DeleteLeases() => Task.CompletedTask;

            public async IAsyncEnumerable<BlobPartitionLease> GetOwnershipBlobLeasesAsync(
                [EnumeratorCancellation]
                CancellationToken cancellationToken = default)
            {
                await Task.CompletedTask;
                yield break;
            }
        }

        sealed class RecordingLoggerFactory : ILoggerFactory
        {
            readonly ConcurrentQueue<EventId> events = new ConcurrentQueue<EventId>();

            public void AddProvider(ILoggerProvider provider)
            {
            }

            public ILogger CreateLogger(string categoryName)
            {
                return new RecordingLogger(this.events);
            }

            public void Dispose()
            {
            }

            public bool HasEvent(string name)
            {
                return this.events.Any(e => e.Name == name);
            }

            sealed class RecordingLogger : ILogger
            {
                readonly ConcurrentQueue<EventId> events;

                public RecordingLogger(ConcurrentQueue<EventId> events)
                {
                    this.events = events;
                }

                public IDisposable BeginScope<TState>(TState state) => null;

                public bool IsEnabled(LogLevel logLevel) => true;

                public void Log<TState>(
                    LogLevel logLevel,
                    EventId eventId,
                    TState state,
                    Exception exception,
                    Func<TState, Exception, string> formatter)
                {
                    this.events.Enqueue(eventId);
                }
            }
        }

        sealed class WaitForSignalOrchestration : TaskOrchestration<string, string>
        {
            TaskCompletionSource<string> signal;

            public override async Task<string> RunTask(
                OrchestrationContext context,
                string input)
            {
                this.signal = new TaskCompletionSource<string>();
                return await this.signal.Task;
            }

            public override void OnEvent(
                OrchestrationContext context,
                string name,
                string input)
            {
                if (name == "complete")
                {
                    this.signal?.TrySetResult(input);
                }
            }
        }

        sealed class TestAppLeaseInfo
        {
            public string OwnerId { get; set; }
        }
    }
}
