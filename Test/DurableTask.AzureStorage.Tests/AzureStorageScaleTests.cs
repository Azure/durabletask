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
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
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
            ControlQueue[] controlQueues = service.AllControlQueues.ToArray();
            Assert.AreEqual(4, controlQueues.Length, "Expected to see the default four control queues created.");
            foreach (ControlQueue queue in controlQueues)
            {
                Assert.IsTrue(await queue.InnerQueue.ExistsAsync(), $"Queue {queue.Name} was not created.");
            }

            // Work-item queue
            WorkItemQueue workItemQueue = service.WorkItemQueue;
            Assert.IsNotNull(workItemQueue, "Work-item queue client was not initialized.");
            Assert.IsTrue(await workItemQueue.InnerQueue.ExistsAsync(), $"Queue {workItemQueue.Name} was not created.");

            // TrackingStore
            ITrackingStore trackingStore = service.TrackingStore;
            Assert.IsNotNull(trackingStore, "Tracking Store was not initialized.");

            try
            {
                Assert.IsTrue(trackingStore.ExistsAsync().Result, $"Tracking Store was not created.");
            }
            catch (NotSupportedException)
            { }

            string expectedContainerName = taskHubName.ToLowerInvariant() + "-leases";
            CloudBlobContainer taskHubContainer = storageAccount.CreateCloudBlobClient().GetContainerReference(expectedContainerName);
            Assert.IsTrue(await taskHubContainer.ExistsAsync(), $"Task hub blob container {expectedContainerName} was not created.");

            // Task Hub config blob
            CloudBlob infoBlob = taskHubContainer.GetBlobReference("taskhub.json");
            Assert.IsTrue(await infoBlob.ExistsAsync(), $"The blob {infoBlob.Name} was not created.");

            // Task Hub lease container
            CloudBlobDirectory leaseDirectory = taskHubContainer.GetDirectoryReference("default");
            IListBlobItem[] leaseBlobs = (await this.ListBlobsAsync(leaseDirectory)).ToArray();
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

                foreach (ControlQueue queue in controlQueues)
                {
                    Assert.IsFalse(await queue.InnerQueue.ExistsAsync(), $"Queue {queue.Name} was not deleted.");
                }

                Assert.IsFalse(await workItemQueue.InnerQueue.ExistsAsync(), $"Queue {workItemQueue.Name} was not deleted.");

                try
                {
                    Assert.IsFalse(trackingStore.ExistsAsync().Result, $"Tracking Store was not deleted.");
                }
                catch (NotSupportedException)
                { }

                Assert.IsFalse(await taskHubContainer.ExistsAsync(), $"Task hub blob container {taskHubContainer.Name} was not deleted.");
            }

            return service;
        }

        public async Task<List<IListBlobItem>> ListBlobsAsync(CloudBlobDirectory client)
        {
            BlobContinuationToken continuationToken = null;
            var results = new List<IListBlobItem>();
            do
            {
                BlobResultSegment response = await client.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);
            }
            while (continuationToken != null);
            return results;
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

                                foreach (ControlQueue controlQueue in service.OwnedControlQueues)
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
                                    service.OwnedControlQueues.All(q => q.InnerQueue.ExistsAsync().GetAwaiter().GetResult()),
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

            ControlQueue[] controlQueues = service.AllControlQueues.ToArray();
            Assert.AreEqual(settings.PartitionCount, controlQueues.Length, "Unexpected number of control queues");

            foreach (ControlQueue cloudQueue in controlQueues)
            {
                await cloudQueue.InnerQueue.FetchAttributesAsync();
                int messageCount = cloudQueue.InnerQueue.ApproximateMessageCount.GetValueOrDefault(-1);

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

                var tableTrackingStore = service.TrackingStore as AzureTableTrackingStore;

                if (tableTrackingStore != null)
                {
                    DynamicTableEntity[] entities = (await tableTrackingStore.HistoryTable.ExecuteQuerySegmentedAsync(new TableQuery(), new TableContinuationToken())).ToArray();
                    int uniquePartitions = entities.GroupBy(e => e.PartitionKey).Count();
                    Trace.TraceInformation($"Found {uniquePartitions} unique partition(s) in table storage.");
                    Assert.AreEqual(InstanceCount, uniquePartitions, "Unexpected number of table partitions.");
                }
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
            var monitor = new DisconnectedPerformanceMonitor(settings.StorageConnectionString, settings.TaskHubName);

            await service.DeleteAsync();

            // A null heartbeat is expected when the task hub does not exist.
            PerformanceHeartbeat heartbeat = await monitor.PulseAsync(currentWorkerCount: 0);
            Assert.IsNull(heartbeat);

            await service.CreateAsync();

            ScaleRecommendation recommendation;

            for (int i = 0; i < 10; i++)
            {
                heartbeat = await monitor.PulseAsync(currentWorkerCount: 0);
                Assert.IsNotNull(heartbeat);
                Assert.AreEqual(settings.PartitionCount, heartbeat.PartitionCount);
                Assert.AreEqual(settings.PartitionCount, heartbeat.ControlQueueLengths.Count);
                Assert.AreEqual(settings.PartitionCount, heartbeat.ControlQueueLatencies.Count);
                Assert.AreEqual(0, heartbeat.ControlQueueLengths.Count(l => l != 0));
                Assert.AreEqual(0, heartbeat.ControlQueueLatencies.Count(l => l != TimeSpan.Zero));
                Assert.AreEqual(0, heartbeat.WorkItemQueueLength);
                Assert.AreEqual(0.0, heartbeat.WorkItemQueueLatencyTrend);
                Assert.AreEqual(TimeSpan.Zero, heartbeat.WorkItemQueueLatency);

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

            var monitor = new DisconnectedPerformanceMonitor(settings.StorageConnectionString, settings.TaskHubName);
            int simulatedWorkerCount = 0;
            await service.CreateAsync();

            // A heartbeat should come back with no recommendation since there is no data.
            PerformanceHeartbeat heartbeat = await monitor.PulseAsync(simulatedWorkerCount);
            Assert.IsNotNull(heartbeat);
            Assert.IsNotNull(heartbeat.ScaleRecommendation);
            Assert.AreEqual(ScaleAction.None, heartbeat.ScaleRecommendation.Action);
            Assert.IsFalse(heartbeat.ScaleRecommendation.KeepWorkersAlive);

            var client = new TaskHubClient(service);
            var previousTotalLatency = TimeSpan.Zero;
            for (int i = 1; i < settings.PartitionCount + 10; i++)
            {
                await client.CreateOrchestrationInstanceAsync(typeof(NoOpOrchestration), input: null);
                heartbeat = await monitor.PulseAsync(simulatedWorkerCount);
                Assert.IsNotNull(heartbeat);

                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsNotNull(recommendation);
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                Assert.AreEqual(settings.PartitionCount, heartbeat.PartitionCount);
                Assert.AreEqual(settings.PartitionCount, heartbeat.ControlQueueLengths.Count);
                Assert.AreEqual(i, heartbeat.ControlQueueLengths.Sum());
                Assert.AreEqual(0, heartbeat.WorkItemQueueLength);
                Assert.AreEqual(TimeSpan.Zero, heartbeat.WorkItemQueueLatency);

                TimeSpan currentTotalLatency = TimeSpan.FromTicks(heartbeat.ControlQueueLatencies.Sum(ts => ts.Ticks));
                Assert.IsTrue(currentTotalLatency > previousTotalLatency);

                if (i + 1 < DisconnectedPerformanceMonitor.QueueLengthSampleSize)
                {
                    int queuesWithNonZeroLatencies = heartbeat.ControlQueueLatencies.Count(t => t > TimeSpan.Zero);
                    Assert.IsTrue(queuesWithNonZeroLatencies > 0 && queuesWithNonZeroLatencies <= i);

                    int queuesWithAtLeastOneMessage = heartbeat.ControlQueueLengths.Count(l => l > 0);
                    Assert.IsTrue(queuesWithAtLeastOneMessage > 0 && queuesWithAtLeastOneMessage <= i);

                    ScaleAction expectedScaleAction = simulatedWorkerCount == 0 ? ScaleAction.AddWorker : ScaleAction.None;
                    Assert.AreEqual(expectedScaleAction, recommendation.Action);
                }
                else
                {
                    // Validate that control queue latencies are going up with each iteration.
                    Assert.IsTrue(currentTotalLatency.Ticks > previousTotalLatency.Ticks);
                    previousTotalLatency = currentTotalLatency;
                }

                Assert.AreEqual(0, heartbeat.WorkItemQueueLength);
                Assert.AreEqual(0.0, heartbeat.WorkItemQueueLatencyTrend);

                if (recommendation.Action == ScaleAction.AddWorker)
                {
                    simulatedWorkerCount++;
                }

                // The high-latency threshold is 1 second
                Thread.Sleep(TimeSpan.FromSeconds(1.1));
            }
        }

        #region Work Item Queue Scaling
        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_High()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(500, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(600, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(700, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(800, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(900, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(1000, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_Moderate()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(500, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(600, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(700, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(800, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(900, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_Low()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            var random = new Random();

            // Scale down for low latency is semi-random, so need to take a lot of samples
            var recommendations = new ScaleRecommendation[500];
            for (int i = 0; i < recommendations.Length; i++)
            {
                mock.AddLatencies(random.Next(50), new[] { 0, 0, 0, 0 });

                heartbeat = await mock.PulseAsync(simulatedWorkerCount: 2);
                recommendations[i] = heartbeat.ScaleRecommendation;
            }

            int scaleOutCount = recommendations.Count(r => r.Action == ScaleAction.AddWorker);
            int scaleInCount = recommendations.Count(r => r.Action == ScaleAction.RemoveWorker);
            int noScaleCount = recommendations.Count(r => r.Action == ScaleAction.None);
            int keepAliveCount = recommendations.Count(r => r.KeepWorkersAlive);

            Trace.TraceInformation($"Scale-out count  : {scaleOutCount}.");
            Trace.TraceInformation($"Scale-in count   : {scaleInCount}.");
            Trace.TraceInformation($"No-scale count   : {noScaleCount}.");
            Trace.TraceInformation($"Keep-alive count : {keepAliveCount}.");

            // It is expected that we scale-in only a small percentage of the time and never scale-out.
            Assert.AreEqual(0, scaleOutCount);
            Assert.AreNotEqual(0, scaleInCount);
            Assert.IsTrue(noScaleCount > scaleInCount, "Should have more no-scale decisions");
            Assert.IsTrue(keepAliveCount > recommendations.Length * 0.9, "Almost all should be keep-alive");
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_Idle()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(30000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
            Assert.IsFalse(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_NotIdle()
        {
            var mock = GetFakePerformanceMonitor();

            for (int i = 0; i < 100; i++)
            {
                mock.AddLatencies(1, new[] { 0, 0, 0, 0 });
                mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
                mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
                mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
                mock.AddLatencies(0, new[] { 0, 0, 0, 0 });

                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;

                // Should never scale to zero when there was a message in a queue
                // within the last 5 samples.
                Assert.AreEqual(ScaleAction.None, recommendation.Action);
                Assert.IsTrue(recommendation.KeepWorkersAlive);
            }
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_MaxPollingDelay1()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(9999, new[] { 0, 0, 0, 0 });

            // When queue is idle, first non-zero latency must be > max polling interval
            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }


        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_MaxPollingDelay2()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(100, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_QuickDrain()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(30000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(30000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(30000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(30000, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(3, new[] { 0, 0, 0, 0 });

            // Something happened and we immediately drained the work-item queue
            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_WorkItemLatency_NotMaxPollingDelay()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(10, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(9999, new[] { 0, 0, 0, 0 });

            // Queue was not idle, so we consider high threshold but not max polling latency
            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }
        #endregion

        #region Control Queue Scaling
        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_High1()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 600 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 700 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 800 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 900 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 1000 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action, "Only one hot partition");
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_High2()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 600, 600 });
            mock.AddLatencies(0, new[] { 0, 0, 700, 700 });
            mock.AddLatencies(0, new[] { 0, 0, 800, 800 });
            mock.AddLatencies(0, new[] { 0, 0, 900, 900 });
            mock.AddLatencies(0, new[] { 0, 0, 1000, 1000 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action, "Two hot partitions");
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            heartbeat = await mock.PulseAsync(simulatedWorkerCount: 2);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action, "Only two hot partitions");
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_High4()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 600, 600, 600, 600 });
            mock.AddLatencies(0, new[] { 700, 700, 700, 700 });
            mock.AddLatencies(0, new[] { 800, 800, 800, 800 });
            mock.AddLatencies(0, new[] { 900, 900, 900, 900 });
            mock.AddLatencies(0, new[] { 1000, 1000, 1000, 1000 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 3);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action, "Four hot partitions");
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            heartbeat = await mock.PulseAsync(simulatedWorkerCount: 4);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action, "Only four hot partitions");
            Assert.IsTrue(recommendation.KeepWorkersAlive);

            heartbeat = await mock.PulseAsync(simulatedWorkerCount: 5);
            recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action, "No work items and only four hot partitions");
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_Moderate()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 500, 500, 500, 500 });
            mock.AddLatencies(0, new[] { 500, 500, 500, 500 });
            mock.AddLatencies(0, new[] { 500, 500, 500, 500 });
            mock.AddLatencies(0, new[] { 500, 500, 500, 500 });
            mock.AddLatencies(0, new[] { 500, 500, 500, 500 });

            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 10; simulatedWorkerCount++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                if (simulatedWorkerCount > 4)
                {
                    Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
                }
                else
                {
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }
            }
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_Idle1()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 1, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 1, 1, 1 });

            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 10; simulatedWorkerCount++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                if (simulatedWorkerCount > 3)
                {
                    Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
                }
                else
                {
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }
            }
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_Idle2()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 1, 1, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 1, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 1, 1 });

            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 10; simulatedWorkerCount++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                if (simulatedWorkerCount > 2)
                {
                    Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
                }
                else
                {
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }
            }
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_Idle4()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
            Assert.IsFalse(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_NotIdle()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 1 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });

            for (int i = 0; i < 100; i++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;

                // We should never scale to zero unless all control queues are idle.
                Assert.AreEqual(ScaleAction.None, recommendation.Action);
                Assert.IsTrue(recommendation.KeepWorkersAlive);
            }
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_MaxPollingDelay1()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 9999, 9999, 9999, 9999 });

            // When queue is idle, first non-zero latency must be > max polling interval
            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_MaxPollingDelay2()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 10000, 10000, 10000, 10000 });
            mock.AddLatencies(0, new[] { 100, 100, 100, 100 });

            PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount: 1);
            ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
            Assert.AreEqual(ScaleAction.None, recommendation.Action);
            Assert.IsTrue(recommendation.KeepWorkersAlive);
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_QuickDrain()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 30000, 30000, 30000, 30000 });
            mock.AddLatencies(0, new[] { 30000, 30000, 30000, 30000 });
            mock.AddLatencies(0, new[] { 30000, 30000, 30000, 30000 });
            mock.AddLatencies(0, new[] { 30000, 30000, 30000, 30000 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });

            // Something happened and we immediately drained the work-item queue
            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 10; simulatedWorkerCount++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                if (simulatedWorkerCount > 4)
                {
                    Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
                }
                else
                {
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }
            }
        }

        [TestMethod]
        public async Task ScaleDecision_ControlQueueLatency_NotMaxPollingDelay()
        {
            var mock = GetFakePerformanceMonitor();
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 0, 0, 0 });
            mock.AddLatencies(0, new[] { 0, 10, 10, 10 });
            mock.AddLatencies(0, new[] { 9999, 9999, 9999, 9999 });

            // Queue was not idle, so we consider high threshold but not max polling latency
            for (int simulatedWorkerCount = 1; simulatedWorkerCount < 10; simulatedWorkerCount++)
            {
                PerformanceHeartbeat heartbeat = await mock.PulseAsync(simulatedWorkerCount);
                ScaleRecommendation recommendation = heartbeat.ScaleRecommendation;
                Assert.IsTrue(recommendation.KeepWorkersAlive);

                if (simulatedWorkerCount < 3)
                {
                    Assert.AreEqual(ScaleAction.AddWorker, recommendation.Action);
                }
                else if (simulatedWorkerCount <= 4)
                {
                    Assert.AreEqual(ScaleAction.None, recommendation.Action);
                }
                else
                {
                    Assert.AreEqual(ScaleAction.RemoveWorker, recommendation.Action);
                }
            }
        }
        #endregion

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
            public FakePerformanceMonitor(
                string storageConnectionString,
                string taskHub,
                int partitionCount = AzureStorageOrchestrationServiceSettings.DefaultPartitionCount) 
                : base(storageConnectionString, taskHub)
            {
                this.PartitionCount = partitionCount;
                for (int i = 0; i < partitionCount; i++)
                {
                    this.ControlQueueLatencies.Add(new QueueMetricHistory(5));
                }
            }

            internal override int PartitionCount { get; }

            internal override Task<bool> UpdateQueueMetrics()
            {
                return Task.FromResult(true);
            }

            public void AddLatencies(int workItemQueueLatency, params int[] controlQueueLatencies)
            {
                if (controlQueueLatencies.Length != this.PartitionCount)
                {
                    throw new ArgumentException(string.Format(
                        "Wrong number of control queue latencies. Expected {0}. Actual: {1}.",
                        this.PartitionCount,
                        controlQueueLatencies.Length));
                }

                this.WorkItemQueueLatencies.Add(workItemQueueLatency);
                for (int i = 0; i < this.ControlQueueLatencies.Count; i++)
                {
                    this.ControlQueueLatencies[i].Add(controlQueueLatencies[i]);
                }
            }

            public void AddControlQueueLatencies(params int[][] latencies)
            {
                for (int i = 0; i < latencies.Length; i++)
                {
                    for (int j = 0; j < latencies[i].Length; j++)
                    {
                        this.ControlQueueLatencies[i].Add(j);
                    }
                }
            }

            public void AddControlQueueLatencies2(params Tuple<int, int, int, int>[] latencies)
            {
                for (int i = 0; i < latencies.Length; i++)
                {
                    this.ControlQueueLatencies[0].Add(latencies[i].Item1);
                    this.ControlQueueLatencies[1].Add(latencies[i].Item2);
                    this.ControlQueueLatencies[2].Add(latencies[i].Item3);
                    this.ControlQueueLatencies[3].Add(latencies[i].Item4);
                }
            }

            public void AddWorkItemQueueLatencies(params int[] latencies)
            {
                for (int i = 0; i < latencies.Length; i++)
                {
                    this.WorkItemQueueLatencies.Add(latencies[i]);
                }
            }

            public override async Task<PerformanceHeartbeat> PulseAsync(int simulatedWorkerCount)
            {
                Trace.TraceInformation(
                    "PULSE INPUT: Worker count: {0}; work items: {1}; control items: {2}  {3}.",
                    simulatedWorkerCount,
                    this.WorkItemQueueLatencies,
                    Environment.NewLine,
                    string.Join(Environment.NewLine + "  ", this.ControlQueueLatencies));

                PerformanceHeartbeat heartbeat = await base.PulseAsync(simulatedWorkerCount);
                Trace.TraceInformation($"PULSE OUTPUT: {heartbeat}");
                return heartbeat;
            }
        }
    }
}
