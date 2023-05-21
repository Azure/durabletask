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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class TestTablePartitionManager
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
            var settings = new AzureStorageOrchestrationServiceSettings[2];

            for (int i = 0; i < 2; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[8];

            for (int i = 0; i < 8; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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

        //[TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill three workers.
        //Test that the left worker will take all the partitions.
        public async Task TestKillThreeWorker()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);
            Stopwatch stopwatch = new Stopwatch();

            var services = new AzureStorageOrchestrationService[4];
            string testName = "TestKillThree";
            var settings = new AzureStorageOrchestrationServiceSettings[4];

            for (int i = 0; i < 4; i++)
            {
                settings[i] = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageConnectionString = this.connection,
                    AppName = testName,
                    UseTablePartitionManagement = true,
                    WorkerId = i.ToString(),
                };
                services[i] = new AzureStorageOrchestrationService(settings[i]);
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



    }
}
