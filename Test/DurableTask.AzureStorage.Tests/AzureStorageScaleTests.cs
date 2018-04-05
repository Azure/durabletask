﻿//  ----------------------------------------------------------------------------------
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
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Validates the following requirements:
    /// https://github.com/Azure/azure-functions-durable-extension/issues/1
    /// </summary>
    [TestClass]
    public class AzureStorageScaleTests
    {
        /// <summary>
        /// Basic validation of task hub creation.
        /// </summary>
        [TestMethod]
        public async Task CreateTaskHub()
        {
            await this.EnsureTaskHubAsync(nameof(CreateTaskHub), testDeletion: false);
        }

        /// <summary>
        /// Basic validation of task hub deletion.
        /// </summary>
        [TestMethod]
        public async Task DeleteTaskHub()
        {
            await this.EnsureTaskHubAsync(nameof(DeleteTaskHub), testDeletion: true);
        }

        async Task<AzureStorageOrchestrationService> EnsureTaskHubAsync(
            string testName, 
            bool testDeletion,
            bool deleteBeforeCreate = true,
            string workerId = "test")
        {
            string storageConnectionString = TestHelpers.GetTestStorageAccountConnectionString();
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            string taskHubName = testName;
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = taskHubName,
                StorageConnectionString = storageConnectionString,
                WorkerId = workerId,
            };

            Trace.TraceInformation($"Task Hub name: {taskHubName}");

            var service = new AzureStorageOrchestrationService(settings);

            if (deleteBeforeCreate)
            {
                await service.CreateAsync();
            }
            else
            {
                await service.CreateIfNotExistsAsync();
            }

            // Control queues
            Assert.IsNotNull(service.AllControlQueues, "Control queue collection was not initialized.");
            CloudQueue[] controlQueues = service.AllControlQueues.ToArray();
            Assert.AreEqual(4, controlQueues.Length, "Expected to see the default four control queues created.");
            foreach (CloudQueue queue in controlQueues)
            {
                Assert.IsTrue(queue.Exists(), $"Queue {queue.Name} was not created.");
            }

            // Work-item queue
            CloudQueue workItemQueue = service.WorkItemQueue;
            Assert.IsNotNull(workItemQueue, "Work-item queue client was not initialized.");
            Assert.IsTrue(workItemQueue.Exists(), $"Queue {workItemQueue.Name} was not created.");

            // History table
            CloudTable historyTable = service.HistoryTable;
            Assert.IsNotNull(historyTable, "History table was not initialized.");
            Assert.IsTrue(historyTable.Exists(), $"History table {historyTable.Name} was not created.");

            // Instances table
            CloudTable instancesTable = service.InstancesTable;
            Assert.IsNotNull(instancesTable, "Instances table was not initialized.");
            Assert.IsTrue(instancesTable.Exists(), $"Instances table {instancesTable.Name} was not created.");

            string expectedContainerName = taskHubName.ToLowerInvariant() + "-leases";
            CloudBlobContainer taskHubContainer = storageAccount.CreateCloudBlobClient().GetContainerReference(expectedContainerName);
            Assert.IsTrue(taskHubContainer.Exists(), $"Task hub blob container {expectedContainerName} was not created.");

            // Task Hub config blob
            CloudBlob infoBlob = taskHubContainer.GetBlobReference("taskhub.json");
            Assert.IsTrue(infoBlob.Exists(), $"The blob {infoBlob.Name} was not created.");

            // Task Hub lease container
            CloudBlobDirectory leaseDirectory = taskHubContainer.GetDirectoryReference("default");
            IListBlobItem[] leaseBlobs = leaseDirectory.ListBlobs().ToArray();
            Assert.AreEqual(controlQueues.Length, leaseBlobs.Length, "Expected to see the same number of control queues and lease blobs.");

            foreach (IListBlobItem blobItem in leaseBlobs)
            {
                string path = blobItem.Uri.AbsolutePath;
                Assert.IsTrue(
                    controlQueues.Where(q => path.Contains(q.Name)).Any(),
                    $"Could not find any known control queue name in the lease name {path}");
            }

            if (testDeletion)
            {
                await service.DeleteAsync();

                foreach (CloudQueue queue in controlQueues)
                {
                    Assert.IsFalse(queue.Exists(), $"Queue {queue.Name} was not deleted.");
                }

                Assert.IsFalse(workItemQueue.Exists(), $"Queue {workItemQueue.Name} was not deleted.");
                Assert.IsFalse(historyTable.Exists(), $"History table {historyTable.Name} was not deleted.");
                Assert.IsFalse(instancesTable.Exists(), $"Instances table {instancesTable.Name} was not deleted.");
                Assert.IsFalse(taskHubContainer.Exists(), $"Task hub blob container {taskHubContainer.Name} was not deleted.");
            }

            return service;
        }

        /// <summary>
        /// REQUIREMENT: Workers can be added or removed at any time and control-queue partitions are load-balanced automatically.
        /// REQUIREMENT: No two workers will ever process the same control queue.
        /// </summary>
        [TestMethod]
        public async Task MultiWorkerLeaseMovement()
        {
            const int MaxWorkerCount = 4;

            var services = new AzureStorageOrchestrationService[MaxWorkerCount];
            var workerIds = new string[MaxWorkerCount];
            int currentWorkerCount = 0;

            // Gradually scale out to four workers and then scale back down to one.
            // Partitions and queues should be load balanced equally across partitions through every step.
            for (int i = 0; i < (MaxWorkerCount * 2) - 1; i++)
            {
                if (i < MaxWorkerCount)
                {
                    Trace.TraceInformation($"Starting task hub service #{i}...");
                    workerIds[i] = $"worker{i}";
                    services[i] = await this.EnsureTaskHubAsync(
                        nameof(MultiWorkerLeaseMovement),
                        testDeletion: false,
                        deleteBeforeCreate: i == 0,
                        workerId: workerIds[i]);
                    await services[i].StartAsync();
                    currentWorkerCount++;
                }
                else
                {
                    int workerIndex = i % MaxWorkerCount;
                    Trace.TraceInformation($"Stopping task hub service #{workerIndex}...");
                    await services[workerIndex].StopAsync();
                    currentWorkerCount--;
                }

                TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30);
                Trace.TraceInformation($"Waiting for all leases to become balanced. Timeout = {timeout}.");

                bool isBalanced = false;

                Stopwatch sw = Stopwatch.StartNew();
                while (sw.Elapsed < timeout)
                {
                    Trace.TraceInformation($"Checking current lease distribution across {currentWorkerCount} workers...");
                    var leases = (await services[0].ListBlobLeasesAsync())
                        .Select(
                            lease => new
                            {
                                Name = lease.Blob.Name,
                                State = lease.Blob.Properties.LeaseState,
                                Owner = lease.Owner,
                            })
                        .Where(lease => !string.IsNullOrEmpty(lease.Owner))
                        .ToArray();

                    Array.ForEach(leases, lease => Trace.TraceInformation(
                        $"Blob: {lease.Name}, State: {lease.State}, Owner: {lease.Owner}"));

                    isBalanced = false;
                    var workersWithLeases = leases.GroupBy(l => l.Owner).ToArray();
                    if (workersWithLeases.Count() == currentWorkerCount)
                    {
                        int maxLeaseCount = workersWithLeases.Max(owned => owned.Count());
                        int minLeaseCount = workersWithLeases.Min(owned => owned.Count());
                        int totalLeaseCount = workersWithLeases.Sum(owned => owned.Count());

                        isBalanced = maxLeaseCount - minLeaseCount <= 1 && totalLeaseCount == 4;
                        if (isBalanced)
                        {
                            Trace.TraceInformation($"Success: Leases are balanced across {currentWorkerCount} workers.");

                            var allQueueNames = new HashSet<string>();

                            // Make sure the control queues are also assigned to the correct workers
                            for (int j = 0; j < services.Length; j++)
                            {
                                AzureStorageOrchestrationService service = services[j];
                                if (service == null)
                                {
                                    continue;
                                }

                                foreach (CloudQueue controlQueue in service.OwnedControlQueues)
                                {
                                    Assert.IsTrue(allQueueNames.Add(controlQueue.Name));
                                }
                                
                                Trace.TraceInformation(
                                    "Queues owned by {0}: {1}",
                                    service.WorkerId,
                                    string.Join(", ", service.OwnedControlQueues.Select(q => q.Name)));

                                var ownedLeases = leases.Where(l => l.Owner == service.WorkerId);
                                Assert.AreEqual(
                                    ownedLeases.Count(),
                                    service.OwnedControlQueues.Count(),
                                    $"Mismatch between control queue count and lease count for {service.WorkerId}");
                                Assert.IsTrue(
                                    service.OwnedControlQueues.All(q => ownedLeases.Any(l => l.Name.Contains(q.Name))),
                                    "Mismatch between queue assignment and lease ownership.");
                                Assert.IsTrue(
                                    service.OwnedControlQueues.All(q => q.Exists()),
                                    $"One or more control queues owned by {service.WorkerId} do not exist");
                            }

                            Assert.AreEqual(totalLeaseCount, allQueueNames.Count, "Unexpected number of queues!");

                            break;
                        }
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(5));
                }

                Assert.IsTrue(isBalanced, "Failed to acquire all leases.");
            }
        }

        /// <summary>
        /// REQUIREMENT: Orchestration history is equally distributed across table storage partitions.
        /// REQUIREMENT: Function processing is automatically load-balanced across all available workers.
        /// </summary>
        [TestMethod]
        public async Task TestInstanceAndMessageDistribution()
        {
            const int InstanceCount = 50;

            // Create a service and enqueue N messages.
            // Make sure each partition has messages in it.
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = nameof(TestInstanceAndMessageDistribution),
                PartitionCount = 4,
            };

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync();

            var client = new TaskHubClient(service);

            Trace.TraceInformation($"Starting {InstanceCount} orchestrations...");

            var createTasks = new Task<OrchestrationInstance>[InstanceCount];
            for (int i = 0; i < InstanceCount; i++)
            {
                createTasks[i] = client.CreateOrchestrationInstanceAsync(typeof(NoOpOrchestration), input: null);
            }

            OrchestrationInstance[] instances = await Task.WhenAll(createTasks);

            CloudQueue[] controlQueues = service.AllControlQueues.ToArray();
            Assert.AreEqual(settings.PartitionCount, controlQueues.Length, "Unexpected number of control queues");

            foreach (CloudQueue cloudQueue in controlQueues)
            {
                await cloudQueue.FetchAttributesAsync();
                int messageCount = cloudQueue.ApproximateMessageCount.GetValueOrDefault(-1);

                Trace.TraceInformation($"Queue {cloudQueue.Name} has {messageCount} message(s).");
                Assert.IsTrue(messageCount > 0, $"Queue {cloudQueue.Name} didn't receive any messages");
            }

            Trace.TraceInformation("Success. All queue partitions have orchestration start messages.");

            // Start the service and let it process the previously enqueued messages.
            // Check that there are exactly N unique partition keys in the table
            Trace.TraceInformation("Starting the worker to consume the messages and run the orchestrations...");
            var worker = new TaskHubWorker(service);
            worker.AddTaskOrchestrations(typeof(NoOpOrchestration));
            await worker.StartAsync();

            try
            {
                // Wait for the instances to run and complete
                OrchestrationState[] states = await Task.WhenAll(
                    instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
                Assert.IsTrue(
                    Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                    "Not all orchestrations completed successfully!");

                DynamicTableEntity[] entities = service.HistoryTable.ExecuteQuery(new TableQuery()).ToArray();
                int uniquePartitions = entities.GroupBy(e => e.PartitionKey).Count();
                Trace.TraceInformation($"Found {uniquePartitions} unique partition(s) in table storage.");
                Assert.AreEqual(InstanceCount, uniquePartitions, "Unexpected number of table partitions.");
            }
            finally
            {
                await worker.StopAsync(isForced: true);
            }
        }

        [TestMethod]
        public async Task MonitorIdleTaskHubDisconnected()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = nameof(MonitorIdleTaskHubDisconnected),
                PartitionCount = 4,
            };

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync();

            var monitor = new DisconnectedPerformanceMonitor(settings.StorageConnectionString, settings.TaskHubName);

            PerformanceHeartbeat heartbeat;
            ScaleRecommendation recommendation;

            for (int i = 0; i < 10; i++)
            {
                heartbeat = await monitor.PulseAsync(currentWorkerCount: 0);
                Assert.IsNotNull(heartbeat);
                Assert.AreEqual(settings.PartitionCount, heartbeat.PartitionCount);
                Assert.AreEqual(0, heartbeat.AggregateControlQueueLength);
                Assert.AreEqual(0.0, heartbeat.AggregateControlQueueLengthTrend);
                Assert.AreEqual(0, heartbeat.WorkItemQueueLength);
                Assert.AreEqual(0.0, heartbeat.WorkItemQueueLengthTrend);

                recommendation = heartbeat.ScaleRecommendation;
                Assert.IsNotNull(recommendation);
                Assert.AreEqual(ScaleAction.None, recommendation.Action);
                Assert.AreEqual(false, recommendation.KeepWorkersAlive);
                Assert.IsNotNull(recommendation.Reason);
            }

            // If any workers are assigned, the recommendation should be to have them removed.
            heartbeat = await monitor.PulseAsync(currentWorkerCount: 1);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.IsNotNull(recommendation);
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
            Assert.AreEqual(false, recommendation.KeepWorkersAlive);
            Assert.IsNotNull(recommendation.Reason);
        }

        [TestMethod]
        public async Task MonitorIncreasingControlQueueLoadDisconnected()
        {
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = nameof(MonitorIncreasingControlQueueLoadDisconnected),
                PartitionCount = 4,
            };

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync();

            var client = new TaskHubClient(service);
            var monitor = new DisconnectedPerformanceMonitor(settings.StorageConnectionString, settings.TaskHubName);
            int simulatedWorkerCount = 0;

            for (int i = 1; i < settings.PartitionCount + 10; i++)
            {
                await client.CreateOrchestrationInstanceAsync(typeof(NoOpOrchestration), input: null);
                PerformanceHeartbeat heartbeat = await monitor.PulseAsync(simulatedWorkerCount);
                Assert.IsNotNull(heartbeat);

                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsNotNull(recommendation);
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                Assert.AreEqual(settings.PartitionCount, heartbeat.PartitionCount);
                Assert.AreEqual(i, heartbeat.AggregateControlQueueLength);

                if (i < DisconnectedPerformanceMonitor.QueueLengthSampleSize)
                {
                    Assert.AreEqual(0.0, heartbeat.AggregateControlQueueLengthTrend);

                    ScaleAction expectedScaleAction = simulatedWorkerCount == 0 ? ScaleAction.AddWorker : ScaleAction.None;
                    Assert.AreEqual(expectedScaleAction, recommendation.Action);
                }
                else
                {
                    Assert.IsTrue(heartbeat.AggregateControlQueueLengthTrend > 0);

                    if (simulatedWorkerCount < settings.PartitionCount)
                    {
                        Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
                    }
                    else
                    {
                        // We should not add more workers than the number of partitions
                        Assert.AreEqual(ScaleAction.None, recommendation.Action);
                    }
                }

                Assert.AreEqual(0, heartbeat.WorkItemQueueLength);
                Assert.AreEqual(0.0, heartbeat.WorkItemQueueLengthTrend);

                if (recommendation.Action == ScaleAction.AddWorker)
                {
                    simulatedWorkerCount++;
                }
            }

            Assert.AreEqual(settings.PartitionCount, simulatedWorkerCount);
        }

        [TestMethod]
        public async Task ScaleDecisionForIncreasingWorkItemCount()
        {
            var fakePerformanceMonitor = GetFakePerformanceMonitor();
            fakePerformanceMonitor.AddWorkItemQueueLengths(1, 2, 3, 3, 5, 6);
            fakePerformanceMonitor.AddControlQueueLengths(0, 0, 0, 0, 0, 0);

            PerformanceHeartbeat heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecisionForIncreasingControlItemCount()
        {
            var fakePerformanceMonitor = GetFakePerformanceMonitor();
            fakePerformanceMonitor.AddWorkItemQueueLengths(0, 0, 0, 0, 0, 0);
            fakePerformanceMonitor.AddControlQueueLengths(1, 2, 3, 4, 5, 6);

            PerformanceHeartbeat heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecisionForDecreasingWorkItemCount()
        {
            var fakePerformanceMonitor = GetFakePerformanceMonitor();
            fakePerformanceMonitor.AddWorkItemQueueLengths(6, 5, 4, 3, 2, 1);
            fakePerformanceMonitor.AddControlQueueLengths(0, 0, 0, 0, 0, 0);

            // Case with multiple workers: should scale in
            PerformanceHeartbeat heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 2);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            // Case with no workers: should add one
            heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 0);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecisionForDecreasingControlItemCount()
        {
            var fakePerformanceMonitor = GetFakePerformanceMonitor();
            fakePerformanceMonitor.AddWorkItemQueueLengths(0, 0, 0, 0, 0, 0);
            fakePerformanceMonitor.AddControlQueueLengths(6, 5, 4, 3, 2, 1);

            // Case with multiple workers: should scale in
            PerformanceHeartbeat heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 2);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            // Case with no workers: should add one
            heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount: 0);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecisionForMessageCountThresholds()
        {
            var fakePerformanceMonitor = new FakePerformanceMonitor(
                TestHelpers.GetTestStorageAccountConnectionString(),
                "taskHub",
                partitionCount: 10);

            PerformanceHeartbeat heartbeat;
            ScaleRecommendation recommendation;

            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 20; simulatedWorkerCount++)
            {
                int messageCount = 1 + (DisconnectedPerformanceMonitor.MaxMessagesPerWorkerRatio * simulatedWorkerCount);
                fakePerformanceMonitor.AddWorkItemQueueLengths(0, 0, 0, 0, messageCount);
                fakePerformanceMonitor.AddControlQueueLengths(0, 0, 0, 0, 0);
                heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount);
                recommendation = heartbeat.ScaleRecommendation;
                Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                fakePerformanceMonitor.AddWorkItemQueueLengths(1, 1, 1, 1, 1);
                fakePerformanceMonitor.AddControlQueueLengths(0, 0, 0, 0, messageCount);
                heartbeat = await fakePerformanceMonitor.PulseAsync(simulatedWorkerCount);
                recommendation = heartbeat.ScaleRecommendation;

                if (simulatedWorkerCount < fakePerformanceMonitor.PartitionCount)
                {
                    Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
                }
                else
                {
                    // Can't add more workers than partitions when the load is purely control queue-driven.
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }

                Assert.IsTrue(recommendation.KeepWorkersAlive);
            }
        }

        static FakePerformanceMonitor GetFakePerformanceMonitor()
        {
            return new FakePerformanceMonitor(TestHelpers.GetTestStorageAccountConnectionString(), "taskHub");
        }

        class NoOpOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult(string.Empty);
            }
        }

        class FakePerformanceMonitor : DisconnectedPerformanceMonitor
        {
            int[] testWorkItemCounts;
            int[] testControlItemCounts;

            public FakePerformanceMonitor(
                string storageConnectionString,
                string taskHub,
                int partitionCount = AzureStorageOrchestrationServiceSettings.DefaultPartitionCount) 
                : base(storageConnectionString, taskHub)
            {
                this.PartitionCount = partitionCount;
            }

            public int PartitionCount { get; }

            public void AddControlQueueLengths(params int[] lengths)
            {
                for (int i = 0; i < lengths.Length; i++)
                {
                    base.AddControlQueueLength(lengths[i]);
                }

                this.testControlItemCounts = lengths;
            }

            public void AddWorkItemQueueLengths(params int[] lengths)
            {
                for (int i = 0; i < lengths.Length; i++)
                {
                    base.AddWorkItemQueueLength(lengths[i]);
                }

                this.testWorkItemCounts = lengths;
            }

            protected override void AddControlQueueLength(int aggregateQueueLength)
            {
                // no-op
            }

            protected override void AddWorkItemQueueLength(int queueLength)
            {
                // no-op
            }

            protected override Task<ControlQueueData> GetAggregateControlQueueLengthAsync()
            {
                // Not used
                return Task.FromResult(new ControlQueueData
                {
                    AggregateQueueLength = 0,
                    PartitionCount = this.PartitionCount
                });
            }

            protected override Task<int> GetWorkItemQueueLengthAsync()
            {
                // no-op
                return Task.FromResult(0);
            }

            public override async Task<PerformanceHeartbeat> PulseAsync(int simulatedWorkerCount)
            {
                Trace.TraceInformation(
                    "PULSE INPUT: Worker count: {0}; control items: {1}; work items: {2}.",
                    simulatedWorkerCount,
                    "[" + string.Join(",", this.testControlItemCounts) + "]",
                    "[" + string.Join(",", this.testWorkItemCounts) + "]");

                PerformanceHeartbeat heartbeat = await base.PulseAsync(simulatedWorkerCount);
                Trace.TraceInformation($"PULSE OUTPUT: {heartbeat}");
                return heartbeat;
            }
        }
    }
}
