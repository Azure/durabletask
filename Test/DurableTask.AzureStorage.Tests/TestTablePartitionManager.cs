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
    using DurableTask.AzureStorage.Messaging;
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
    public class TestTablePartitionManager: IDisposable
    {

        string connection = TestHelpers.GetTestStorageAccountConnectionString();
        //string connection = "UseDevelopmentStorage=true";

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        // Start with one worker and four partitions.
        // Test the worker could claim all the partitions in 5 seconds.
        public async Task TestOneWorkerWithFourPartition()
        {
            //string storageConnectionString = TestHelpers.GetTestStorageAccountConnectionString();
            //var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            string testName = "TestOneWorkerWithFourPartition";
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageConnectionString = this.connection,
                WorkerId = "0",
                AppName = testName,
                UseTablePartitionManagement = true,
            };

            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();
            var service = new AzureStorageOrchestrationService(settings);
            await service.StartAsync();


            bool isAllPartitionClaimed = false;
            stopwatch.Start();
            while (!isAllPartitionClaimed)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = service.ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isAllPartitionClaimed = partitions.All(p => p.CurrentOwner == "0");
            }

            stopwatch.Stop();
            await service.StopAsync();
            await service.DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with two workers and four partitions.
        //Test that one worker can acquire two partitions. 
        //Since two worker can not start at the same time, and one will start earlier than the another one.
        //There would be a steal process, and test that the lease tranfer will take no longer than 30 sec.
        public async Task TestTwoWorkerWithFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[2];
            string testName = "TestTwoWorkerWithFourPartitions";

            for (int i = 0; i < 2; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isBalanced = (partitions.Count(p => p.CurrentOwner == "0") == 2) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 2);

            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with four workers and four partitions.
        //Test that each worker can acquire four partitions. 
        //Since workers can not start at the same time, there would be a steal lease process.
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestFourWorkerWithFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestFourWorkerWithFourPartitions";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isBalanced = (partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "3") == 1);

            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        // [TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with one workers and four partitions.And then add three more workers.
        //Test that each worker can acquire four partitions. 
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestAddThreeWorkersWithOneWorkerAndFourPartitions()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(30);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestAddThreeWorkers";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            await services[0].StartAsync();
            await Task.Delay(1000);
            await services[1].StartAsync();
            await services[2].StartAsync();
            await services[3].StartAsync();

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isBalanced = (partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "3") == 1);

            }
            stopwatch.Stop();
            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Starts with four workers and four partitions. And then add four more workers.
        //Test that the added workers will do nothing. 
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
                    StorageConnectionString = this.connection,
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
            await Task.Delay(30000);

            for (int i = 4; i < 8; i++)
            {
                await services[i].StartAsync();
            }

            stopwatch.Start();
            bool isDistributionChanged = false;

            while (stopwatch.Elapsed < timeout)
            {
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isDistributionChanged = (partitions.Count(p => p.CurrentOwner == "4" || p.NextOwner == "4") != 0) &&
                             (partitions.Count(p => p.CurrentOwner == "5" || p.NextOwner == "5") != 0) &&
                             (partitions.Count(p => p.CurrentOwner == "6" || p.NextOwner == "6") != 0) &&
                             (partitions.Count(p => p.CurrentOwner == "7" || p.NextOwner == "7") != 0);
                Assert.IsFalse(isDistributionChanged);
            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to three workers.
        //Test that partitions will be rebalance between the three workers, which is one worker will have two, and the other two both have one. 
        public async Task TestScalingDownToThreeWorkers()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestScalingDownToThree";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            Task.Delay(30000).Wait();
            await services[3].StopAsync();

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                // One of the three active workers will have two leases, others remain one.
                isBalanced = ((partitions.Count(p => p.CurrentOwner == "0") == 2) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1))
                             ||((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 2) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1))
                             || ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 2));
            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to one workers.
        //Test that the left one worker will take the four partitions.
        public async Task TestScalingDownToOneWorkers()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(5);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestScalingDownToOne";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            Task.Delay(30000).Wait();
            IList<Task> tasks = new List<Task>();
            tasks.Add(services[1].StopAsync());
            tasks.Add(services[2].StopAsync());
            tasks.Add(services[3].StopAsync());
            await Task.WhenAll(tasks);


            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                // The left worker 0 will take all leases.
                isBalanced = partitions.All(p => p.CurrentOwner == "0");
            }
            stopwatch.Stop();
            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill one worker.
        //Test that the partitions will be rebalanced among he three left workers.
        public async Task TestKillOneWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestKillOne";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            Task.Delay(30000).Wait();

            services[3].KillPartitionManagerLoop();

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                // One of the three active workers will have two leases, others remain one.
                isBalanced = ((partitions.Count(p => p.CurrentOwner == "0") == 2) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1))
                             || ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 2) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1))
                             || ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 2));
            }
            stopwatch.Stop();

            IList<Task> tasks = new List<Task>();
            tasks.Add(services[0].StopAsync());
            tasks.Add(services[1].StopAsync());
            tasks.Add(services[2].StopAsync());
            await Task.WhenAll(tasks);
            await services[0].DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill three workers.
        //Test that the left worker will take all the partitions.
        public async Task TestKillThreeWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestKillThree";

            for (int i = 0; i < 4; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings);
            }

            var startTasks = services.Select(service => service.StartAsync());
            await Task.WhenAll(startTasks);

            Task.Delay(30000).Wait();

            services[3].KillPartitionManagerLoop();
            services[2].KillPartitionManagerLoop();
            services[1].KillPartitionManagerLoop();

            stopwatch.Start();
            bool isBalanced = false;

            while (!isBalanced)
            {
                Assert.IsTrue(stopwatch.Elapsed < timeout, "Timeout expired!");
                var partitions = services[0].ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                isBalanced = partitions.All(p => p.CurrentOwner == "0");
            }
            stopwatch.Stop();
            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        /// <summary>
        /// End to end test to simulate two workers with one partition.
        /// Simulate that one worker becomes unhealthy, make sure the other worker will take over the partition.
        /// After that the unhethy worker becomes healthy again, make sure it will not take the partition back and also
        /// it won't dequeue the control queue of the stolen partition.
        /// </summary>
        /// <returns></returns>
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
                    StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                    TaskHubName = "TestUnhealthyWorker3",
                    PartitionCount = 1,
                    WorkerId = i.ToString(),
                    //ControlQueueBufferThreshold = 10,
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

            ControlQueue[] controlQueues = services[0].AllControlQueues.ToArray();

            foreach (ControlQueue cloudQueue in controlQueues)
            {
                await cloudQueue.InnerQueue.FetchAttributesAsync();
            }
            await taskHubWorkers[0].StartAsync();
            await Task.Delay(1000);// Ensure the partition is acquired by worker[0].
            var partitions = services[0].ListTableLeases();
            Assert.AreEqual("0", partitions.Single().CurrentOwner);
            await taskHubWorkers[1].StartAsync();

            var source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            //Kill worker[0] and start a new worker. 
            //The new worker will take over the partitions of worker[0].
            services[0].KillPartitionManagerLoop();
            bool isLeaseReclaimed = false;
            while(!isLeaseReclaimed)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                partitions = services[0].ListTableLeases();
                isLeaseReclaimed = partitions.All(p => p.CurrentOwner == "1");
            }

            //After worker[1] takes over the lease, restart the worker[0].
            services[0].SimulateUnhealthyWorker(token);
            
            //Wait one second for worker0 to remove the lost partitions' control queue.
            await Task.Delay(1000);
            Assert.AreEqual(1, services[1].OwnedControlQueues.Count());
            Assert.AreEqual(0, services[0].OwnedControlQueues.Count());

            //Check all the instances could be processed successfully. 
            OrchestrationState[] states = await Task.WhenAll(
                instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
            Assert.IsTrue(
                Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                "Not all orchestrations completed successfully!");

            source.Cancel();
            await taskHubWorkers[1].StopAsync();
            source.Dispose();
        }

        /// <summary>
        /// End to End test with four workers and four partitions.
        /// Ensure that no worker listens to the same queue at the same time during the balancing process.
        /// Ensure that all instances could be process sucessfully.
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
                    StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                    TaskHubName = nameof(EnsureOwnedQueueExclusive),
                    PartitionCount = 4,
                    WorkerId = i.ToString(),
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

            ControlQueue[] controlQueues = services[0].AllControlQueues.ToArray();

            foreach (ControlQueue cloudQueue in controlQueues)
            {
                await cloudQueue.InnerQueue.FetchAttributesAsync();
            }

            var taskHubWorkerTasks = taskHubWorkers.Select(worker => worker.StartAsync());
            await Task.WhenAll(taskHubWorkerTasks);

            //Check all the workers are not listening to the same queue at the same time during the balancing.
            bool isBalanced = false;
            while (!isBalanced)
            {
                var partitions = services[0].ListTableLeases();
                
                Assert.AreEqual(4, partitions.Count());
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
            
            //Check all the instances could be processed successfully. 
            OrchestrationState[] states = await Task.WhenAll(
                instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
            Assert.IsTrue(
                Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                "Not all orchestrations completed successfully!");
            
            var stopServiceTasks = taskHubWorkers.Select(worker => worker.StopAsync());
            await Task.WhenAll(stopServiceTasks);
            await services[0].DeleteAsync();
        }

        /// <summary>
        /// End to End test to simulate one worker with 50 instances.
        /// Test that it can process all 50 instances.
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task TestMessageProcess()
        {
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = nameof(TestMessageProcess),
                PartitionCount = 4,
                WorkerId = "0",
            };
            var service = new AzureStorageOrchestrationService(settings);
            var worker = new TaskHubWorker(service);
            var client = new TaskHubClient(service);

            var createInstanceTasks = new Task<OrchestrationInstance>[50];
            for (int i = 0; i < 50; i++)
            {
                createInstanceTasks[i] = client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input: i.ToString());
            }
            OrchestrationInstance[] instances = await Task.WhenAll(createInstanceTasks);

            worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
            worker.AddTaskActivities(typeof(Hello));

            ControlQueue[] controlQueues = service.AllControlQueues.ToArray();

            foreach (ControlQueue cloudQueue in controlQueues)
            {
                await cloudQueue.InnerQueue.FetchAttributesAsync();
            }

            await worker.StartAsync();
            //It needs time for the worker to start listening to the queues.
            await Task.Delay(1500);
            Assert.AreEqual(4, service.OwnedControlQueues.Count());

            OrchestrationState[] states = await Task.WhenAll(
                   instances.Select(i => client.WaitForOrchestrationAsync(i, TimeSpan.FromSeconds(30))));
            Assert.IsTrue(
                Array.TrueForAll(states, s => s?.OrchestrationStatus == OrchestrationStatus.Completed),
                "Not all orchestrations completed successfully!");

            await worker.StopAsync();
            await service.DeleteAsync();

        }

        class NoOpOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult(string.Empty);
            }
        }

        [KnownType(typeof(Hello))]
        internal class HelloOrchestrator : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                //  await contextBase.ScheduleTask<string>(typeof(Hello), "world");
                //   if you pass an empty string it throws an error
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

        //Check if all the arrays elements are unique.
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

        public void Dispose()
        {
        }

    }
}
