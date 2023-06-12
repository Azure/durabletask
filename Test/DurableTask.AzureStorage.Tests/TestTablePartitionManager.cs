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

        [TestMethod]
        // Start with one worker and four partitions.
        // Test the worker could claim all the partitions in 5 seconds.
        public async Task TestOneWorkerWithFourPartition()
        {
            //string storageConnectionString = TestHelpers.GetTestStorageAccountConnectionString();
            //var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            string testName = nameof(TestOneWorkerWithFourPartition);
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageConnectionString = this.connection,
                WorkerId = "0",
                AppName = testName,
                UseTablePartitionManagement = true,
            };
            var service = new AzureStorageOrchestrationService(settings);
            await service.StartAsync();


            WaitForCondition(
                timeout: TimeSpan.FromSeconds(5),
                condition: () =>
                {
                var partitions = service.ListTableLeases();
                Assert.AreEqual(4, partitions.Count());
                return partitions.All(p => p.CurrentOwner == "0");
                });

            await service.StopAsync();
            await service.DeleteAsync();
        }

        [TestMethod]
        //Starts with two workers and four partitions.
        //Test that one worker can acquire two partitions. 
        //Since two worker can not start at the same time, and one will start earlier than the another one.
        //There would be a steal process, and test that the lease tranfer will take no longer than 30 sec.
        public async Task TestTwoWorkerWithFourPartitions()
        {
            var services = new AzureStorageOrchestrationService[2];
            string testName = nameof(TestTwoWorkerWithFourPartitions);

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

            WaitForCondition(
                timeout: TimeSpan.FromSeconds(30),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());
                    return ((partitions.Count(p => p.CurrentOwner == "0") == 2) &&
                            (partitions.Count(p => p.CurrentOwner == "1") == 2));
                });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestMethod]
        //Starts with four workers and four partitions.
        //Test that each worker can acquire four partitions. 
        //Since workers can not start at the same time, there would be a steal lease process.
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestFourWorkerWithFourPartitions()
        {

            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestFourWorkerWithFourPartitions);

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
            
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });


            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestMethod]
        //Starts with one workers and four partitions.And then add three more workers.
        //Test that each worker can acquire four partitions. 
        //Test that the lease tranfer will take no longer than 30 sec.
        public async Task TestAddThreeWorkersWithOneWorkerAndFourPartitions()
        {

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
            //Wait for worker[0] to acquire all the partitions. Then start the other three workers.
            WaitForCondition(
                timeout: TimeSpan.FromSeconds(5),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());
                    return partitions.All(p => p.CurrentOwner == "0");
                });
            await services[1].StartAsync();
            await services[2].StartAsync();
            await services[3].StartAsync();

            //Check that each worker has acquired one partitions.
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

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
            
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });

            for (int i = 4; i < 8; i++)
            {
                await services[i].StartAsync();
            }

            var oldDistribution = services[0].ListTableLeases();
            stopwatch.Start();
            bool isDistributionChanged = false;

            while (stopwatch.Elapsed < timeout)
            {
                var newDistribution = services[0].ListTableLeases();
                Assert.AreEqual(oldDistribution.Count(), newDistribution.Count());
                isDistributionChanged = !(oldDistribution.Zip(newDistribution, (p1, p2) =>
                                        p1.CurrentOwner == p2.CurrentOwner && p1.NextOwner== p2.NextOwner).All(result => result));
                
                Assert.IsFalse(isDistributionChanged);
            }
            stopwatch.Stop();

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to three workers.
        //Test that partitions will be rebalance between the three workers, which is one worker will have two, and the other two both have one. 
        public async Task TestScalingDownToThreeWorkers()
        {

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

            // wait for the partitions to be distributed equally among four workers.
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });
            await services[3].StopAsync();

            bool isBalanced = false;

            WaitForCondition(
                timeout: TimeSpan.FromSeconds(10),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());

                    // Assert that two partitions have the same CurrentOwner value and the other two have unique CurrentOwner values
                    int distinctCurrentOwnersCount = partitions.Select(x => x.CurrentOwner).Distinct().Count();
                    isBalanced = distinctCurrentOwnersCount == 3;
                    return isBalanced;
                });

            var stopTasks = services.Select(service => service.StopAsync());
            await Task.WhenAll(stopTasks);
            await services[0].DeleteAsync();
        }

        [TestMethod]
        //Start with four workers and four partitions. And then sacle down to one workers.
        //Test that the left one worker will take the four partitions.
        public async Task TestScalingDownToOneWorkers()
        {
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

            // wait for the partitions to be distributed equally among four workers.
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });

            IList<Task> tasks = new List<Task>();
            tasks.Add(services[1].StopAsync());
            tasks.Add(services[2].StopAsync());
            tasks.Add(services[3].StopAsync());
            await Task.WhenAll(tasks);


            WaitForCondition(
                timeout: TimeSpan.FromSeconds(10),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());
                    return partitions.All(p => p.CurrentOwner == "0");
                });

            await services[0].StopAsync();
            await services[0].DeleteAsync();
        }

        [TestMethod]
        //Start with four workers and four partitions. Then kill one worker.
        //Test that the partitions will be rebalanced among he three left workers.
        public async Task TestKillOneWorker()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestKillOneWorker);

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

            // wait for the partitions to be distributed equally among four workers.
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                        var partitions = services[0].ListTableLeases();
                        Assert.AreEqual(4, partitions.Count());
                        return ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                                 (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });

            services[3].KillPartitionManagerLoop();

            bool isBalanced = false;

            WaitForCondition(
                timeout: TimeSpan.FromSeconds(40),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());

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

        [TestCategory("DisabledInCI")]
        [TestMethod]
        //Start with four workers and four partitions. Then kill three workers.
        //Test that the left worker will take all the partitions.
        public async Task TestKillThreeWorker()
        {
            var services = new AzureStorageOrchestrationService[4];
            string testName = nameof(TestKillThreeWorker);

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

            // wait for the partitions to be distributed equally among four workers.
            WaitForCondition(
                    timeout: TimeSpan.FromSeconds(30),
                    condition: () =>
                    {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());
                    return  ((partitions.Count(p => p.CurrentOwner == "0") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "1") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "2") == 1) &&
                             (partitions.Count(p => p.CurrentOwner == "3") == 1));
                    });

            services[3].KillPartitionManagerLoop();
            services[2].KillPartitionManagerLoop();
            services[1].KillPartitionManagerLoop();

            WaitForCondition(
                timeout: TimeSpan.FromSeconds(40),
                condition: () =>
                {
                    var partitions = services[0].ListTableLeases();
                    Assert.AreEqual(4, partitions.Count());
                    return partitions.All(p => p.CurrentOwner == "0");
                });


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
                    StorageConnectionString = this.connection,
                    TaskHubName = "TestUnhealthyWorker",
                    PartitionCount = 1,
                    WorkerId = i.ToString(),
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
            await Task.Delay(1000);// Ensure the partition is acquired by worker[0].
            var partitions = services[0].ListTableLeases();
            Assert.AreEqual("0", partitions.Single().CurrentOwner);
            await taskHubWorkers[1].StartAsync();

            using var cts = new CancellationTokenSource();
            {
                //Kill worker[0] and start a new worker. 
                //The new worker will take over the partitions of worker[0].
                services[0].KillPartitionManagerLoop();
                WaitForCondition(
                    timeout: TimeSpan.FromSeconds(40),
                    condition: () => services[0].ListTableLeases().Single().CurrentOwner == "1");

                //After worker[1] takes over the lease, restart the worker[0].
                services[0].SimulateUnhealthyWorker(cts.Token);

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
            }
            await taskHubWorkers[1].StopAsync();
        }

        /// <summary>
        /// End to End test with four workers and four partitions.
        /// Ensure that no worker listens to the same queue at the same time during the balancing process.
        /// Ensure that all instances could be process sucessfully.
        /// </summary>
        /// <returns></returns>
        [TestCategory("DisabledInCI")]
        [TestMethod]
        public async Task EnsureOwnedQueueExclusive()
        {
            const int WorkerCount = 4;
            const int InstanceCount = 50;
            var services = new AzureStorageOrchestrationService[WorkerCount];
            var taskHubWorkers = new TaskHubWorker[WorkerCount];

            // Create 4 task hub workers.
            for (int i = 0; i < WorkerCount; i++)
            {
                var settings = new AzureStorageOrchestrationServiceSettings()
                {
                    StorageConnectionString = this.connection,
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

            var taskHubWorkerTasks = taskHubWorkers.Select(worker => worker.StartAsync());
            await Task.WhenAll(taskHubWorkerTasks);

            //Check all the workers are not listening to the same queue at the same time during the balancing.
            bool isBalanced = false;
            var timeout = TimeSpan.FromSeconds(30);
            var sw = Stopwatch.StartNew();
            while (!isBalanced)
            {
                Assert.IsTrue(sw.Elapsed <= timeout, "Timeout expired!");
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

        static void WaitForCondition(TimeSpan timeout, Func<bool> condition)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                Assert.IsTrue(sw.Elapsed < timeout, "Timeout expired");
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
        }


    }
}
