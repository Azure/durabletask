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
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    [DoNotParallelize]
    public class AppLeaseActivityTests
    {
        static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(15);

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

                TaskActivityWorkItem[] workItems = await Task.WhenAll(
                    LockActivityAsync(worker1),
                    LockActivityAsync(worker2));

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
        public async Task ClosedGatePreventsNewActivityReceive()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService service =
                CreateService(taskHubName, "PrimaryApp", useAppLease: true);

            try
            {
                await service.CreateAsync();
                await service.StartAsync();
                await WaitForOwnerAsync(service);
                await EnqueueActivityAsync(service, "blocked");
                GetAppLeaseManager(service).SetActivityOwnership(ownsLease: false);

                using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(250)))
                {
                    TaskActivityWorkItem blockedWorkItem =
                        await service.LockNextTaskActivityWorkItem(TestTimeout, cancellation.Token);
                    Assert.IsNull(blockedWorkItem);
                }

                GetAppLeaseManager(service).SetActivityOwnership(ownsLease: true);
                TaskActivityWorkItem recoveredWorkItem = await LockActivityAsync(service);
                Assert.IsNotNull(recoveredWorkItem);
                await service.AbandonTaskActivityWorkItemAsync(recoveredWorkItem);
            }
            finally
            {
                await StopAsync(service);
            }
        }

        [TestMethod]
        public async Task OwnershipLossDoesNotCancelPendingReceive()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService service =
                CreateService(taskHubName, "PrimaryApp", useAppLease: true);

            try
            {
                await service.CreateAsync();
                await service.StartAsync();
                await WaitForOwnerAsync(service);

                Task<TaskActivityWorkItem> pendingReceive =
                    service.LockNextTaskActivityWorkItem(TestTimeout, CancellationToken.None);
                Assert.IsFalse(pendingReceive.IsCompleted);

                GetAppLeaseManager(service).SetActivityOwnership(ownsLease: false);

                Task completionAfterLoss = await Task.WhenAny(
                    pendingReceive,
                    Task.Delay(TimeSpan.FromMilliseconds(500)));
                Assert.AreNotSame(
                    pendingReceive,
                    completionAfterLoss,
                    "Ownership loss alone must not cancel a receive that already started.");

                await EnqueueActivityAsync(service, "rejected-while-closed");
                Assert.IsNull(await WithTimeoutAsync(pendingReceive));

                GetAppLeaseManager(service).SetActivityOwnership(ownsLease: true);
                TaskActivityWorkItem recoveredWorkItem = await LockActivityAsync(service);
                Assert.IsNotNull(recoveredWorkItem);
                await service.AbandonTaskActivityWorkItemAsync(recoveredWorkItem);
            }
            finally
            {
                await StopAsync(service);
            }
        }

        [TestMethod]
        public async Task OwnershipRegainAllowsPendingReceiveToDispatch()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService service =
                CreateService(taskHubName, "PrimaryApp", useAppLease: true);

            try
            {
                await service.CreateAsync();
                await service.StartAsync();
                await WaitForOwnerAsync(service);

                Task<TaskActivityWorkItem> pendingReceive =
                    service.LockNextTaskActivityWorkItem(TestTimeout, CancellationToken.None);
                Assert.IsFalse(pendingReceive.IsCompleted);

                AppLeaseManager appLeaseManager = GetAppLeaseManager(service);
                appLeaseManager.SetActivityOwnership(ownsLease: false);
                appLeaseManager.SetActivityOwnership(ownsLease: true);

                await EnqueueActivityAsync(service, "accepted-after-regain");
                TaskActivityWorkItem workItem = await WithTimeoutAsync(pendingReceive);
                Assert.IsNotNull(workItem);
                await service.AbandonTaskActivityWorkItemAsync(workItem);
            }
            finally
            {
                await StopAsync(service);
            }
        }

        [TestMethod]
        public async Task OwnershipLossAfterDequeueAbandonsWithoutStartingTrace()
        {
            string taskHubName = GetTaskHubName();
            var loggerFactory = new RecordingLoggerFactory();
            AzureStorageOrchestrationServiceSettings ownerSettings =
                CreateSettings(taskHubName, "PrimaryApp", useAppLease: true);
            ownerSettings.LoggerFactory = loggerFactory;
            AzureStorageOrchestrationService owner =
                new AzureStorageOrchestrationService(ownerSettings);
            AzureStorageOrchestrationService recoveryReader =
                CreateService(taskHubName, "RecoveryReader", useAppLease: false);
            CorrelationSettings previousCorrelationSettings = CorrelationSettings.Current;

            try
            {
                CorrelationSettings.Current = new CorrelationSettings
                {
                    EnableDistributedTracing = true,
                    Protocol = Protocol.W3CTraceContext,
                };
                CorrelationTraceContext.Current = null;

                await owner.CreateAsync();
                await owner.StartAsync();
                await WaitForOwnerAsync(owner);
                await EnqueueActivityAsync(owner, "ownership-race");

                owner.OnActivityMessageDequeued = async () =>
                {
                    owner.OnActivityMessageDequeued = null;
                    await StopAsync(owner);
                };

                TaskActivityWorkItem rejectedWorkItem = await LockActivityAsync(owner);
                owner = null;

                Assert.IsNull(rejectedWorkItem);
                Assert.IsNull(CorrelationTraceContext.Current);
                Assert.IsFalse(loggerFactory.HasEvent("ReceivedMessage"));
                Assert.IsFalse(loggerFactory.HasEvent("ProcessingMessage"));

                await recoveryReader.StartAsync();
                TaskActivityWorkItem recoveredWorkItem = await LockActivityAsync(recoveryReader);
                Assert.IsNotNull(recoveredWorkItem);
                await recoveryReader.AbandonTaskActivityWorkItemAsync(recoveredWorkItem);
            }
            finally
            {
                CorrelationTraceContext.Current = null;
                CorrelationSettings.Current = previousCorrelationSettings;
                await StopAsync(recoveryReader);
                await StopAsync(owner);
            }
        }

        [TestMethod]
        public async Task ClosingGateDoesNotCancelDispatchedActivity()
        {
            string taskHubName = GetTaskHubName();
            AzureStorageOrchestrationService owner =
                CreateService(taskHubName, "PrimaryApp", useAppLease: true);

            try
            {
                await owner.CreateAsync();
                await owner.StartAsync();
                await WaitForOwnerAsync(owner);
                await EnqueueActivityAsync(owner, "in-flight");

                TaskActivityWorkItem inFlightWorkItem = await LockActivityAsync(owner);
                await owner.StopAsync(isForced: true);

                TaskActivityWorkItem renewedWorkItem =
                    await owner.RenewTaskActivityWorkItemLockAsync(inFlightWorkItem);
                Assert.IsTrue(renewedWorkItem.LockedUntilUtc > DateTime.UtcNow);

                await owner.AbandonTaskActivityWorkItemAsync(inFlightWorkItem);
                owner = null;
            }
            finally
            {
                await StopAsync(owner);
            }
        }

        [TestMethod]
        public async Task PassiveWaitHonorsCallerCancellationAndServiceShutdown()
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
            bool useAppLease)
        {
            return new AzureStorageOrchestrationService(
                CreateSettings(taskHubName, appName, useAppLease));
        }

        static AzureStorageOrchestrationServiceSettings CreateSettings(
            string taskHubName,
            string appName,
            bool useAppLease)
        {
            return new AzureStorageOrchestrationServiceSettings
            {
                AppName = appName,
                AppLeaseOptions = new AppLeaseOptions
                {
                    AcquireInterval = TimeSpan.FromMilliseconds(200),
                    LeaseInterval = TimeSpan.FromSeconds(15),
                    RenewInterval = TimeSpan.FromMilliseconds(200),
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
            return "activitygate" + Guid.NewGuid().ToString("N").Substring(0, 12);
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

        static AppLeaseManager GetAppLeaseManager(AzureStorageOrchestrationService service)
        {
            FieldInfo field = typeof(AzureStorageOrchestrationService).GetField(
                "appLeaseManager",
                BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(field);

            var appLeaseManager = field.GetValue(service) as AppLeaseManager;
            Assert.IsNotNull(appLeaseManager);
            return appLeaseManager;
        }

        static async Task<TaskActivityWorkItem> LockActivityAsync(
            AzureStorageOrchestrationService service)
        {
            using (var cancellation = new CancellationTokenSource(TestTimeout))
            {
                return await service.LockNextTaskActivityWorkItem(
                    TestTimeout,
                    cancellation.Token);
            }
        }

        static async Task<T> WithTimeoutAsync<T>(Task<T> task)
        {
            Task completedTask = await Task.WhenAny(task, Task.Delay(TestTimeout));
            Assert.AreSame(task, completedTask, "The operation did not complete before the test timeout.");
            return await task;
        }

        static async Task StopAsync(AzureStorageOrchestrationService service)
        {
            if (service != null)
            {
                Task stopTask = service.StopAsync(isForced: true);
                Task completedTask = await Task.WhenAny(stopTask, Task.Delay(TestTimeout));
                Assert.AreSame(stopTask, completedTask, "Service shutdown did not complete before the test timeout.");
                await stopTask;
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
    }
}
