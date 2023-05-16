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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using System.Linq;

    [TestClass]
    public class TestTablePartitionManager
    {

        string connection = TestHelpers.GetTestStorageAccountConnectionString(); 

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
            while (stopwatch.Elapsed < timeout && !isAllPartitionClaimed)
            {
                var partitions = service.ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    Assert.AreEqual("0", partition.CurrentOwner);
                    if (partition.RowKey == "controlqueue-03") isAllPartitionClaimed = true;
                }
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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                var partitions = services[0].ListTableLeases();
                bool ifLoopCompleted = true;
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null)
                    {
                        ifLoopCompleted = false;
                        break;
                    }
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                }

                if (ifLoopCompleted && (worker0PartitionNum == worker1PartitionNum))
                {
                    isBalanced = true;
                    Assert.AreEqual(2, worker1PartitionNum);
                    Assert.AreEqual(2, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;
                int worker3PartitionNum = 0;
                bool ifLoopCompleted = true;
                var partitions = services[0].ListTableLeases();
                
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null)
                        {
                            ifLoopCompleted = false;
                            break;
                        }
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                    if (partition.CurrentOwner == "3") worker3PartitionNum++;
                }
                
                if (ifLoopCompleted && (worker0PartitionNum == worker1PartitionNum) && (worker2PartitionNum == worker3PartitionNum) && (worker0PartitionNum == worker2PartitionNum))
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;
                int worker3PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                    if (partition.CurrentOwner == "3") worker3PartitionNum++;
                    if ((worker0PartitionNum == worker1PartitionNum) && (worker2PartitionNum == worker3PartitionNum) && (worker0PartitionNum == worker2PartitionNum))
                    {
                        isBalanced = true;
                        Assert.AreEqual(1, worker0PartitionNum);
                    }
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
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

            while (stopwatch.Elapsed < timeout)
            {
                int worker4PartitionNum = 0;
                int worker5PartitionNum = 0;
                int worker6PartitionNum = 0;
                int worker7PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "4" || partition.NextOwner == "4") worker4PartitionNum++;
                    if (partition.CurrentOwner == "5" || partition.NextOwner == "5") worker5PartitionNum++;
                    if (partition.CurrentOwner == "6" || partition.NextOwner == "6") worker6PartitionNum++;
                    if (partition.CurrentOwner == "7" || partition.NextOwner == "7") worker7PartitionNum++;
                }

                Assert.AreEqual(0, worker4PartitionNum);
                Assert.AreEqual(0, worker5PartitionNum);
                Assert.AreEqual(0, worker6PartitionNum);
                Assert.AreEqual(0, worker7PartitionNum);
            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to three workers.
        //Test that partitions will be rebalance between the three workers, which is one worker will have two, and the other two both have one. 
        public async Task TestScalingDownToThreeWorkers()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(10);
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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++; 
                }

                if (worker0PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker1PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker2PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to one workers.
        //Test that the left one worker will take the four partitions.
        public async Task TestScalingDownToOneWorkers()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(10);
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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                }
                if (worker0PartitionNum == 4)
                {
                    isBalanced = true;
                }


            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        [TestCategory("DisabledInCI")]
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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;
                int worker1PartitionNum = 0;
                int worker2PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                    if (partition.CurrentOwner == "1") worker1PartitionNum++;
                    if (partition.CurrentOwner == "2") worker2PartitionNum++;
                }

                if (worker0PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker1PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker0PartitionNum);
                    Assert.AreEqual(1, worker2PartitionNum);
                }

                if (worker2PartitionNum == 2)
                {
                    isBalanced = true;
                    Assert.AreEqual(1, worker1PartitionNum);
                    Assert.AreEqual(1, worker0PartitionNum);
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
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

            while (stopwatch.Elapsed < timeout && !isBalanced)
            {
                int worker0PartitionNum = 0;

                var partitions = services[0].ListTableLeases();
                foreach (var partition in partitions)
                {
                    if (partition.CurrentOwner == null) break;
                    if (partition.CurrentOwner == "0") worker0PartitionNum++;
                }

                if (worker0PartitionNum == 4)
                {
                    isBalanced = true;
                }
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.Elapsed <= timeout);
            Assert.IsTrue(isBalanced);

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }



    }
}
