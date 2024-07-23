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
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;

    [TestClass]
    public class TestTablePartitionManager
    {
        readonly string connection = TestHelpers.GetTestStorageAccountConnectionString();

        // Start with one worker and four partitions.
        // Test the worker could claim all the partitions in 5 seconds.
        [TestMethod]
        public async Task TestOneWorkerWithFourPartition()
        {
            string testName = nameof(TestOneWorkerWithFourPartition);
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                WorkerId = "0",
                AppName = testName,
                UseTablePartitionManagement = true,
            };
            var service = new AzureStorageOrchestrationService(settings);
            await service.StartAsync();

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(5),
                condition: async t =>
                {
                    var partitions = await service.ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return partitions.All(p => p.CurrentOwner == "0");
                });

            await service.StopAsync();
            await service.DeleteAsync();
        }

        // Starts with two workers and four partitions.
        // Test that one worker can acquire two partitions,
        // since two workers can't start at the same time, and one will start earlier than the other one.
        // There should be a steal process, and test that the lease transfer will take no longer than 30 sec.
        [TestMethod]
        public async Task TestTwoWorkerWithFourPartitions()
        {
            var services = new AzureStorageOrchestrationService[2];
            string testName = nameof(TestTwoWorkerWithFourPartitions);

            for (int i = 0; i < 2; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 2) &&
                            (partitions.Count(p => p.CurrentOwner == "1") == 2));
                });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // Starts with four workers and four partitions.
        // Test that each worker can acquire one partition. 
        // Since workers can't start at the same time, there should be a steal lease process.
        // Test that the lease transfer will take no longer than 30 sec.
        [TestMethod]
        public async Task TestFourWorkerWithFourPartitions()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestFourWorkerWithFourPartitions);

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });


            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // Starts with one worker and four partitions, then add three more workers.
        // Test that each worker can acquire one partitions. 
        // Test that the lease transfer will take no longer than 30 sec.
        [TestMethod]
        public async Task TestAddThreeWorkersWithOneWorkerAndFourPartitions()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestAddThreeWorkers";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            await services[0].StartAsync();
            // Wait for worker[0] to acquire all the partitions. Then start the other three workers.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(5),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return partitions.All(p => p.CurrentOwner == "0");
                });
            await services[1].StartAsync();
            await services[2].StartAsync();
            await services[3].StartAsync();
            
            // Check that each worker has acquired one partition.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // Starts with four workers and four partitions. And then add four more workers.
        // Test that the added workers will do nothing. 
        [TestMethod]
        public async Task TestAddFourWorkersWithFourWorkersAndFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(15);
            Stopwatch stopwatch = new Stopwatch();
            var services = new AzureStorageOrchestrationService[8];
            string testName = "TestAddFourWorkers";

            for (int i = 0; i < 8; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            for (int i = 0; i < 4; i++)
            {
                await services[i].StartAsync();
            }
            
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return
                        (partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                        (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                        (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                        (partitions.Count(p => p.CurrentOwner == "3") == 1);
                });

            for (int i = 4; i < 8; i++)
            {
                await services[i].StartAsync();
            }

            var oldDistribution = await services[0].ListTableLeasesAsync().ToListAsync();
            stopwatch.Start();
            bool isDistributionChanged = false;

            while (stopwatch.Elapsed < timeout)
            {
                var newDistribution = await services[0].ListTableLeasesAsync().ToListAsync();
                Assert.AreEqual(oldDistribution.Count, newDistribution.Count);
                isDistributionChanged = !(oldDistribution.Zip(newDistribution, (p1, p2) =>
                                        p1.CurrentOwner == p2.CurrentOwner && p1.NextOwner== p2.NextOwner).All(result => result));
                
                Assert.IsFalse(isDistributionChanged);
            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // Start with four workers and four partitions. And then scale down to three workers.
        // Test that partitions will be rebalanced between the three workers: one worker will have two, and the other two both have one.
        [TestMethod]
        public async Task TestScalingDownToThreeWorkers()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestScalingDownToThree";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            // wait for the partitions to be distributed equally among four workers.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });
            await services[3].StopAsync();

            bool isBalanced = false;

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(10),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);

                    // Assert that two partitions have the same CurrentOwner value and the other two have unique CurrentOwner values
                    int distinctCurrentOwnersCount = partitions.Select(x => x.CurrentOwner).Distinct().Count();
                    isBalanced = distinctCurrentOwnersCount == 3;
                    return isBalanced;
                });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // Start with four workers and four partitions. And then scale down to one worker.
        // Test that the left one worker will take the four partitions.
        [TestMethod]
        public async Task TestScalingDownToOneWorkers()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestScalingDownToOne";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            // wait for the partitions to be distributed equally among four workers.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });

            IList<Task> tasks = new List<Task>
            {
                services[1].StopAsync(),
                services[2].StopAsync(),
                services[3].StopAsync()
            };
            await Task.WhenAll(tasks);


            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(10),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return partitions.All(p => p.CurrentOwner == "0");
                });

            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        [TestMethod]
        // Start with four workers and four partitions. Then kill one worker.
        // Test that the partitions will be rebalanced among the three remaining workers.
        public async Task TestKillOneWorker()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestKillOneWorker);

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            // wait for the partitions to be distributed equally among four workers.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });

            services[3].KillPartitionManagerLoop();

            bool isBalanced = false;

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(40),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);

                    // Assert that two partitions have the same CurrentOwner value and the other two have unique CurrentOwner values
                    int distinctCurrentOwnersCount = partitions.Select(x => x.CurrentOwner).Distinct().Count();
                    isBalanced = distinctCurrentOwnersCount == 3;
                    return isBalanced;
                });

            IList<Task> tasks = new List<Task>();
            tasks.Add(services[0].StopAsync());
            tasks.Add(services[1].StopAsync());
            tasks.Add(services[2].StopAsync());
            await Task.WhenAll(tasks);
            await services[0].DeleteAsync();
        }

        // Start with four workers and four partitions. Then kill three workers.
        // Test that the remaining worker will take all the partitions.
        [TestMethod]
        public async Task TestKillThreeWorker()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestKillThreeWorker);

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            // wait for the partitions to be distributed equally among four workers.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(30),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return  ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                (partitions.Count(p => p.CurrentOwner == "3") == 1));
                });

            services[3].KillPartitionManagerLoop();
            services[2].KillPartitionManagerLoop();
            services[1].KillPartitionManagerLoop();

            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(40),
                condition: async t =>
                {
                    var partitions = await services[0].ListTableLeasesAsync(t).ToListAsync(t);
                    Assert.AreEqual(4, partitions.Count);
                    return partitions.All(p => p.CurrentOwner == "0");
                });


            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        /// <summary>
        /// End to end test to simulate two workers with one partition.
        /// Simulate that one worker becomes unhealthy, make sure the other worker will take over the partition.
        /// After that the unhealthy worker becomes healthy again, make sure it will not take the partition back and also
        /// it won't dequeue the control queue of the stolen partition.
        /// </summary>
        [TestMethod]
        public async Task TestUnhealthyWorker()
        {
            const int WorkerCount = 2;
            const int InstanceCount = 50;
            var services = new AzureStorageOrchestrationService[WorkerCount];
            var taskHubWorkers = new TaskHubWorker[WorkerCount];

            for (int i = 0; i < WorkerCount; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings()
                {
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    TaskHubName = "TestUnhealthyWorker",
                    PartitionCount = 1,
                    WorkerId = i.ToString(),
                    UseTablePartitionManagement = true,
                };
                services[i] = new AzureStorageOrchestrationService(settings);
                taskHubWorkers[i] = new TaskHubWorker(services[i]);
                taskHubWorkers[i].AddTaskOrchestrations(typeof(LongRunningOrchestrator));
            }

            // Create 50 orchestration instances.
            var client = new TaskHubClient(services[0]);
            var createInstanceTasks = new Task<OrchestrationInstance>[InstanceCount];
            for (int i = 0; i < InstanceCount; i++)
            {
                createInstanceTasks[i] = client.CreateOrchestrationInstanceAsync(typeof(LongRunningOrchestrator), input: null);
            }
            OrchestrationInstance[] instances = await Task.WhenAll(createInstanceTasks);

            await taskHubWorkers[0].StartAsync();
            // Ensure worker 0 acquired the partition.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(2),
                condition: async t =>
                {
                    TablePartitionLease lease = await services[0].ListTableLeasesAsync(t).SingleAsync(t);
                    return lease.CurrentOwner == "0";
                });
            await taskHubWorkers[1].StartAsync();

            using var cts = new CancellationTokenSource();
            {
                // Kill worker[0] and start a new worker. 
                // The new worker will take over the partitions of worker[0].
                services[0].KillPartitionManagerLoop();
                await WaitForConditionAsync(
                    timeout: TimeSpan.FromSeconds(40),
                    condition: async t =>
                    {
                        TablePartitionLease lease = await services[0].ListTableLeasesAsync(t).SingleAsync(t);
                        return lease.CurrentOwner == "1";
                    });

                // After worker[1] takes over the lease, restart the worker[0].
                services[0].SimulateUnhealthyWorker(cts.Token);

                // Wait one second for worker0 to remove the lost partitions' control queue.
                await Task.Delay(1000);
                Assert.AreEqual(1, services[1].OwnedControlQueues.Count());
                Assert.AreEqual(0, services[0].OwnedControlQueues.Count());

                // Check all the instances could be processed successfully. 
                OrchestrationState[] states = await Task.WhenAll(
                    instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
                Assert.IsTrue(
                    Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                    "Not all orchestrations completed successfully!");
            }
            await taskHubWorkers[1].StopAsync();
        }

        /// <summary>
        /// End to End test with four workers and four partitions.
        /// Ensure that no worker listens to the same queue at the same time during the balancing process.
        /// Ensure that all instances should be processed sucessfully.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task EnsureOwnedQueueExclusive()
        {
            const int WorkerCount = 4;
            const int InstanceCount = 100;
            var services = new AzureStorageOrchestrationService[WorkerCount];
            var taskHubWorkers = new TaskHubWorker[WorkerCount];

            // Create 4 task hub workers.
            for (int i = 0; i < WorkerCount; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings()
                {
                    StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                    TaskHubName = nameof(EnsureOwnedQueueExclusive),
                    PartitionCount = 4,
                    WorkerId = i.ToString(),
                    UseTablePartitionManagement = true,
                };
                services[i] = new AzureStorageOrchestrationService(settings);
                taskHubWorkers[i] = new TaskHubWorker(services[i]);
                taskHubWorkers[i].AddTaskOrchestrations(typeof(HelloOrchestrator));
                taskHubWorkers[i].AddTaskActivities(typeof(Hello));
            }

            // Create 100 orchestration instances.
            var client = new TaskHubClient(services[0]);
            var createInstanceTasks = new Task<OrchestrationInstance>[InstanceCount];
            for (int i = 0; i < InstanceCount; i++)
            {
                createInstanceTasks[i] = client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input: i.ToString());
            }
            OrchestrationInstance[] instances = await Task.WhenAll(createInstanceTasks);

            var taskHubWorkerTasks = taskHubWorkers.Select(worker => worker.StartAsync());
            await Task.WhenAll(taskHubWorkerTasks);

            // Check all the workers are not listening to the same queue at the same time during the balancing.
            bool isBalanced = false;
            var timeout = TimeSpan.FromSeconds(30);
            var sw = Stopwatch.StartNew();
            while (!isBalanced)
            {
                Assert.IsTrue(sw.Elapsed <= timeout, "Timeout expired!");
                var partitions = await services[0].ListTableLeasesAsync().ToListAsync();
                Assert.AreEqual(4, partitions.Count);
                isBalanced = (partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "3") == 1);

                string[] array0 = services[0].OwnedControlQueues.Select(i => i.Name).ToArray();
                string[] array1 = services[1].OwnedControlQueues.Select(i => i.Name).ToArray();
                string[] array2 = services[2].OwnedControlQueues.Select(i => i.Name).ToArray();
                string[] array3 = services[3].OwnedControlQueues.Select(i => i.Name).ToArray();
                bool allUnique = CheckAllArraysUnique(array1, array2, array3, array0);
                await Task.Delay(1000);

                Assert.IsTrue(allUnique, "Multiple workers lsiten to the same queue at the same time.");
            }
            
            // Check all the instances could be processed successfully. 
            OrchestrationState[] states = await Task.WhenAll(
                instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
            Assert.IsTrue(
                Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                "Not all orchestrations completed successfully!");
            
            var stopServiceTasks = taskHubWorkers.Select(worker => worker.StopAsync());
            await Task.WhenAll(stopServiceTasks);
        }

        /// <summary>
        /// End to end test to simulate one worker with one partition.
        /// Simulate that one worker becomes unhealthy, and then back to a healthy again before the owned lease expires.
        /// Make sure the worker can still listen to owned control queue without claiming the lease.
        /// </summary>
        [TestMethod]
        public async Task TestWorkerRestartBeforeLeaseExpired()
        {

            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(this.connection),
                TaskHubName = "WorkerRestartBeforeLeaseExpired",
                PartitionCount = 1,
                LeaseInterval = TimeSpan.FromMinutes(5),
                WorkerId = "0",
                UseTablePartitionManagement = true,
            };

            var service = new AzureStorageOrchestrationService(settings);
            var worker  = new TaskHubWorker(service);

            // start worker, so TaskHub resources are created
            await worker.StartAsync();

            // Ensure worker acquired the partition.
            await WaitForConditionAsync(
                timeout: TimeSpan.FromSeconds(2),
                condition: async t =>
                {
                    TablePartitionLease lease = await service.ListTableLeasesAsync(t).SingleAsync(t);
                    return lease.CurrentOwner == "0";
                });
            
            // quickly stop worker
            await worker.StopAsync(isForced: true);

            // create a local table client to manipulate the partitions table
            var azureStorageClient = new AzureStorageClient(settings);
            Table partitionTable = azureStorageClient.GetTableReference(azureStorageClient.Settings.PartitionTableName);

            // read the partition table
            var results = partitionTable.ExecuteQueryAsync<TablePartitionLease>();
            var numResults = results.CountAsync().Result;
            Assert.AreEqual(numResults, 1); // there should only be 1 partition

            // We want to test that worker 0 starts listening to the control queue without claiming the lease.
            // Therefore, we force the table to be in a state where worker 0 is still the current owner of the partition.
            var partitionData = results.FirstAsync().Result;
            partitionData.NextOwner = null;
            partitionData.IsDraining = false;
            partitionData.CurrentOwner = "0";
            partitionData.ExpiresAt = DateTime.UtcNow.AddMinutes(5);
            await partitionTable.InsertOrMergeEntityAsync(partitionData);

            // guarantee table is corrrectly updated
            results = partitionTable.ExecuteQueryAsync<TablePartitionLease>();
            numResults = results.CountAsync().Result;
            Assert.AreEqual(numResults, 1); // there should only be 1 partition
            Assert.AreEqual(results.FirstAsync().Result.CurrentOwner, "0"); // ensure current owner is partition "0"

            // create and start new worker with the same settings, ensure it is actively listening to the queue
            worker = new TaskHubWorker(service);
            await worker.StartAsync();
            await Task.Delay(TimeSpan.FromSeconds(10));
            Assert.AreEqual(1, service.OwnedControlQueues.Count());
        }

        [KnownType(typeof(Hello))]
        internal class HelloOrchestrator : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                return await context.ScheduleTask<string>(typeof(Hello), "world");
            }
        }

        internal class Hello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    throw new ArgumentNullException(nameof(input));
                }

                Console.WriteLine($"Activity: Hello {input}");
                return $"Hello, {input}!";
            }
        }

        internal class LongRunningOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
                return Task.FromResult(string.Empty);
            }
        }

        // Check if all the arrays elements are unique.
        static bool CheckAllArraysUnique(params string[][] arrays)
        {
            int totalArrays = arrays.Length;

            for (int i = 0; i < totalArrays - 1; i++)
            {
                for (int j = i + 1; j < totalArrays; j++)
                {
                    if (arrays[i].Intersect(arrays[j]).Any())
                    {
                        return false; // Found at least one common element, arrays are not unique
                    }
                }
            }

            return true; // No common elements found, arrays are unique
        }

        static async ValueTask WaitForConditionAsync(TimeSpan timeout, Func<CancellationToken, ValueTask<bool>> condition)
        {
            if (Debugger.IsAttached)
            {
                // Give more time for debugging so we can step through the code
                timeout = TimeSpan.FromMinutes(3);
            }

            using var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(timeout);

            try
            {
                while (!await condition(tokenSource.Token))
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), tokenSource.Token);
                }
            }
            catch (OperationCanceledException)
            {
                Assert.Fail("Timeout expired");
            }
        }
    }
}
