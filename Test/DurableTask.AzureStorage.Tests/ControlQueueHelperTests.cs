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
    using System.Data;
    using System.Data.SqlTypes;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.ControlQueueHeartbeat;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class ControlQueueHelperTests
    {
        IControlQueueHelper controlQueueHelper;
        AzureStorageOrchestrationService azureStorageOrchestrationService;
        AzureStorageOrchestrationServiceSettings settings;
        int partitionCount = 4;
        Dictionary<string, int> controlQueueNumberToNameMap;

        [TestInitialize]
        public void Initialize()
        {
            settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = TestHelpers.GetTestTaskHubName(),
                PartitionCount = partitionCount,
                ControlQueueHearbeatOrchestrationInterval = TimeSpan.FromSeconds(5),
                ControlQueueOrchHeartbeatDetectionInterval = TimeSpan.FromSeconds(5),
                ControlQueueOrchHeartbeatDetectionThreshold = TimeSpan.FromSeconds(5),
            };

            azureStorageOrchestrationService = new AzureStorageOrchestrationService(settings);
            controlQueueHelper = azureStorageOrchestrationService;

            controlQueueNumberToNameMap = new Dictionary<string, int>();

            for (int i = 0; i < partitionCount; i++)
            {
                var controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, i);
                controlQueueNumberToNameMap[controlQueueName] = i;
            }
        }

        [TestMethod]
        public async Task StartControlQueueHeartbeatMonitorAsync()
        {
            var detectionCount = new Dictionary<string, int>();

            var taskHubClient = new TaskHubClient(azureStorageOrchestrationService);
            var taskHubWorker = new Core.TaskHubWorker(azureStorageOrchestrationService);
            var controlQueueToInstanceInfo = azureStorageOrchestrationService.GetControlQueueToInstanceIdInfo();

            // Creating dictionary to calculate stuck issue detection count for each control-queue.
            foreach (var controlQueueName in controlQueueToInstanceInfo.Keys)
            {
                detectionCount[controlQueueName] = 0;
            }

            var cancellationTokenSrc = new CancellationTokenSource();

            await controlQueueHelper.StartControlQueueHeartbeatMonitorAsync(
                taskHubClient,
                taskHubWorker,
                async (x, y, z, cancelationToken) => { await Task.CompletedTask; },
                async (workerId, ownerId, isOwner, controlQueueName, instanceid, orchDetectionInfo, cancellationToken) =>
                {
                    detectionCount[controlQueueName]++;
                    await Task.CompletedTask;
                },
                cancellationTokenSrc.Token);

            // Scheduling of all orchestrator should happen.
            foreach (var instanceId in controlQueueToInstanceInfo.Values)
            {
                var orchIsntance = await taskHubClient.GetOrchestrationStateAsync(instanceId);
                Assert.IsNotNull(orchIsntance);
            }

            // Orchestrator registration completed.
            var objectCreator = new NameValueObjectCreator<TaskOrchestration>(
                ControlQueueHeartbeatTaskOrchestratorV1.OrchestrationName,
                ControlQueueHeartbeatTaskOrchestratorV1.OrchestrationVersion,
                typeof(ControlQueueHeartbeatTaskOrchestratorV1));

            Assert.ThrowsException<InvalidOperationException>(() => { taskHubWorker.AddTaskOrchestrations(objectCreator); });

            await Task.Delay(this.settings.ControlQueueOrchHeartbeatDetectionInterval + this.settings.ControlQueueOrchHeartbeatDetectionThreshold);

            var detectionCountDuplicate = new Dictionary<string, int>();

            // Should trigger delegate for control-queue stuck.
            foreach (var controlQueueName in controlQueueToInstanceInfo.Keys)
            {
                detectionCountDuplicate[controlQueueName] = detectionCount[controlQueueName];
                Assert.IsTrue(detectionCount[controlQueueName] > 0);
            }


            await Task.Delay(this.settings.ControlQueueOrchHeartbeatDetectionInterval + this.settings.ControlQueueOrchHeartbeatDetectionThreshold);
            cancellationTokenSrc.Cancel();

            // Give it some time to cancel the ongoing operations.
            await Task.Delay(this.settings.ControlQueueOrchHeartbeatDetectionInterval);

            // Should trigger delegate for control-queue stuck.
            foreach (var controlQueueName in controlQueueToInstanceInfo.Keys)
            {
                Assert.IsFalse(detectionCountDuplicate[controlQueueName] == detectionCount[controlQueueName]);
                detectionCountDuplicate[controlQueueName] = detectionCount[controlQueueName];
            }

            await Task.Delay(this.settings.ControlQueueOrchHeartbeatDetectionInterval + this.settings.ControlQueueOrchHeartbeatDetectionThreshold);

            // Should trigger delegate for control-queue stuck.
            foreach (var controlQueueName in controlQueueToInstanceInfo.Keys)
            {
                Assert.IsTrue(detectionCountDuplicate[controlQueueName] == detectionCount[controlQueueName]);
            }
        }

        [TestMethod]
        public async Task ScheduleControlQueueHeartbeatOrchestrations()
        {
            var utcBefore = DateTime.UtcNow;

            var taskHubClient = new TaskHubClient(azureStorageOrchestrationService);
            await controlQueueHelper.ScheduleControlQueueHeartbeatOrchestrationsAsync(taskHubClient, true);

            var controlQueueToInstanceInfo = azureStorageOrchestrationService.GetControlQueueToInstanceIdInfo();

            var utcNow = DateTime.UtcNow;

            foreach (var instanceId in controlQueueToInstanceInfo.Values)
            {
                var orchIsntance = await taskHubClient.GetOrchestrationStateAsync(instanceId);

                Assert.IsTrue(orchIsntance.CreatedTime >= utcBefore && orchIsntance.CreatedTime <= utcNow);
            }

            await controlQueueHelper.ScheduleControlQueueHeartbeatOrchestrationsAsync(taskHubClient, false);

            foreach (var instanceId in controlQueueToInstanceInfo.Values)
            {
                var orchIsntance = await taskHubClient.GetOrchestrationStateAsync(instanceId);

                Assert.IsTrue(orchIsntance.CreatedTime >= utcBefore && orchIsntance.CreatedTime <= utcNow);
            }
        }

        [TestMethod]
        public void ScheduleControlQueueHeartbeatOrchestrations_InvalidInput()
        {
            var settingsMod = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = TestHelpers.GetTestTaskHubName(),
                PartitionCount = partitionCount + 1,
            };

            var azureStorageOrchestrationServiceMod = new AzureStorageOrchestrationService(settingsMod);

            var taskHubClient = new TaskHubClient(azureStorageOrchestrationServiceMod);

            Assert.ThrowsExceptionAsync<ArgumentNullException>(async () =>
            {
                await controlQueueHelper.ScheduleControlQueueHeartbeatOrchestrationsAsync(null);
            });

            IOrchestrationServiceClient orchestrationService = new Mock<IOrchestrationServiceClient>().Object;
            var taskHubClientDiff = new TaskHubClient(orchestrationService);

            Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await controlQueueHelper.ScheduleControlQueueHeartbeatOrchestrationsAsync(taskHubClientDiff);
            });

            Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
            {
                await controlQueueHelper.ScheduleControlQueueHeartbeatOrchestrationsAsync(taskHubClient);
            });
        }

        [TestMethod]
        public void RegisterControlQueueHeartbeatOrchestration()
        {
            var taskHubWorker = new Core.TaskHubWorker(azureStorageOrchestrationService);
            controlQueueHelper.RegisterControlQueueHeartbeatOrchestration(taskHubWorker, async (x, y, z, cancellationToken) => { await Task.CompletedTask; });

            var objectCreator = new NameValueObjectCreator<TaskOrchestration>(
                ControlQueueHeartbeatTaskOrchestratorV1.OrchestrationName,
                ControlQueueHeartbeatTaskOrchestratorV1.OrchestrationVersion,
                typeof(ControlQueueHeartbeatTaskOrchestratorV1));

            Assert.ThrowsException<InvalidOperationException>(() => { taskHubWorker.AddTaskOrchestrations(objectCreator); });
        }

        [TestMethod]
        public void RegisterControlQueueHeartbeatOrchestration_InvalidInput()
        {
            var settingsMod = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = TestHelpers.GetTestTaskHubName(),
                PartitionCount = partitionCount + 1,
            };

            var azureStorageOrchestrationServiceMod = new AzureStorageOrchestrationService(settingsMod);

            var taskHubWorker = new TaskHubWorker(azureStorageOrchestrationServiceMod);

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                controlQueueHelper.RegisterControlQueueHeartbeatOrchestration(null, async (x, y, z, cancellationToken) => { await Task.CompletedTask; });
            });

            IOrchestrationService orchestrationService = new Mock<IOrchestrationService>().Object;
            var taskHubWorkerDiff = new TaskHubWorker(orchestrationService);

            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                controlQueueHelper.RegisterControlQueueHeartbeatOrchestration(taskHubWorkerDiff, async (x, y, z, cancellationToken) => { await Task.CompletedTask; });
            });

            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                controlQueueHelper.RegisterControlQueueHeartbeatOrchestration(taskHubWorker, async (x, y, z, cancellationToken) => { await Task.CompletedTask; });
            });
        }

        [TestMethod]
        [DataRow(new int[] { 0, 1, 2, 3 })]
        [DataRow(new int[] { 2, 3 })]
        [DataRow(new int[] { 1, 3 })]
        [DataRow(new int[] { 0, 1 })]
        [DataRow(new int[] { 0, 2 })]
        [DataRow(new int[] { 0, 3 })]
        [DataRow(new int[] { 0 })]
        [DataRow(new int[] { 1 })]
        [DataRow(new int[] { 3 })]
        public async Task GetControlQueueInstanceId(int[] controlQueueNumbers)
        {
            Dictionary<int, List<string>> controlQueueNumberToInstanceIds = new Dictionary<int, List<string>>();

            var controlQueueNumbersHashSet = new HashSet<int>();

            foreach (var cQN in controlQueueNumbers)
            {
                controlQueueNumbersHashSet.Add(cQN);
                controlQueueNumberToInstanceIds[cQN] = new List<string>();
            }


            for (int i = 0; i < 100; i++)
            {
                var instanceId = controlQueueHelper.GetControlQueueInstanceId(controlQueueNumbersHashSet, $"prefix{Guid.NewGuid()}_");

                var controlQueue = await azureStorageOrchestrationService.GetControlQueueAsync(instanceId);
                var controlQueueNumber = controlQueueNumberToNameMap[controlQueue.Name];

                controlQueueNumberToInstanceIds[controlQueueNumber].Add(instanceId);

                Assert.IsTrue(controlQueueNumbers.Any(x => x == controlQueueNumber));
            }

            foreach (var cQN in controlQueueNumbers)
            {
                Assert.IsTrue(controlQueueNumberToInstanceIds[cQN].Count > 0);
            }
        }

        [TestMethod]
        public void GetControlQueueInstanceId_InvalidInput()
        {
            Assert.ThrowsException<ArgumentNullException>(() => { controlQueueHelper.GetControlQueueInstanceId(null); });
            Assert.ThrowsException<ArgumentException>(() => { controlQueueHelper.GetControlQueueInstanceId(new HashSet<int>()); });
            Assert.ThrowsException<ArgumentException>(() => { controlQueueHelper.GetControlQueueInstanceId(new HashSet<int>() { -4 }); });
            Assert.ThrowsException<ArgumentException>(() => { controlQueueHelper.GetControlQueueInstanceId(new HashSet<int>() { partitionCount }); });
            Assert.ThrowsException<ArgumentException>(() => { controlQueueHelper.GetControlQueueInstanceId(new HashSet<int>() { partitionCount + 4 }); });
        }

    }
}
