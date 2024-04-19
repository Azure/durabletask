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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ControlQueueHelperTests
    {
        private IControlQueueHelper controlQueueHelper;
        private AzureStorageOrchestrationService azureStorageOrchestrationService;
        private AzureStorageOrchestrationServiceSettings settings;
        private int partitionCount = 4;
        private Dictionary<string, int> controlQueueNumberToNameMap;
        private CancellationTokenSource cancellationTokenSource;
        private const string TaskHub = "taskHubName";

        [TestInitialize]
        public void Initialize()
        {
            cancellationTokenSource = new CancellationTokenSource();

            settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = TestHelpers.GetTestStorageAccountConnectionString(),
                TaskHubName = TaskHub,
                PartitionCount = partitionCount
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
        public void GetControlQueueInstanceId_NullArgument()
        {
            Action a1 = () =>
            {
                controlQueueHelper.GetControlQueueInstanceId((HashSet<int>)null, $"prefix{Guid.NewGuid()}_");
            };

            Assert.ThrowsException<ArgumentNullException>(a1);

            Action a2 = () =>
            {
                controlQueueHelper.GetControlQueueInstanceId((HashSet<string>)null, $"prefix{Guid.NewGuid()}_");
            };

            Assert.ThrowsException<ArgumentNullException>(a2);
        }

        [TestMethod]
        [DataRow(new string[] { "taskHubName-control-00", "taskHubName-control-01", "taskHubName-control-02", "taskHubName-control-03", "taskHubName-control-04", "taskHubName-control-05" })]
        [DataRow(new string[] { "taskHubName-control-02", "taskHubName-control--1" })]
        [DataRow(new string[] { "taskHubName-control-01", "taskHubName-control-07" })]
        [DataRow(new string[] { "taskHubName-control-09" })]
        [DataRow(new string[] { "taskHubName-09" })]
        [DataRow(new string[] { "TaskHubName-control-9" })]
        [DataRow(new string[] { })]
        public void GetControlQueueInstanceId_ControlQueueNames_InvalidArgument(string[] controlQueueNumbers)
        {
            var controlQueueNumbersHashSet = new HashSet<string>();

            foreach (var cQN in controlQueueNumbers)
            {
                controlQueueNumbersHashSet.Add(cQN);
            }

            Action a = () =>
            {
                controlQueueHelper.GetControlQueueInstanceId(controlQueueNumbersHashSet, $"prefix{Guid.NewGuid()}_");
            };

            Assert.ThrowsException<ArgumentException>(a);
        }

        [TestMethod]
        [DataRow(new int[] { 0, 1, 2, 3, 4, 5 })]
        [DataRow(new int[] { 2, -1 })]
        [DataRow(new int[] { 1, 7 })]
        [DataRow(new int[] { 9 })]
        [DataRow(new int[] { })]
        public void GetControlQueueInstanceId_ControlQueueNumbers_InvalidArgument(int[] controlQueueNumbers)
        {
            var controlQueueNumbersHashSet = new HashSet<int>();

            foreach (var cQN in controlQueueNumbers)
            {
                controlQueueNumbersHashSet.Add(cQN);
            }

            Action a = () =>
            {
                controlQueueHelper.GetControlQueueInstanceId(controlQueueNumbersHashSet, $"prefix{Guid.NewGuid()}_");
            };

            Assert.ThrowsException<ArgumentException>(a);
        }

        [TestMethod]
        [DataRow(new string[] { "taskhubname-control-00", "taskhubname-control-01", "taskhubname-control-02", "taskhubname-control-03" })]
        [DataRow(new string[] { "taskhubname-control-02", "taskhubname-control-03" })]
        [DataRow(new string[] { "taskhubname-control-01", "taskhubname-control-03" })]
        [DataRow(new string[] { "taskhubname-control-00", "taskhubname-control-01" })]
        [DataRow(new string[] { "taskhubname-control-00", "taskhubname-control-02" })]
        [DataRow(new string[] { "taskhubname-control-00", "taskhubname-control-03" })]
        [DataRow(new string[] { "taskhubname-control-00" })]
        [DataRow(new string[] { "taskhubname-control-01" })]
        [DataRow(new string[] { "taskHubname-control-03" })]
        public async Task GetControlQueueInstanceId_ControlQueueNames(string[] controlQueueNames)
        {
            Dictionary<string, List<string>> controlQueueNumberToInstanceIds = new Dictionary<string, List<string>>();

            var controlQueueNumbersHashSet = new HashSet<string>();

            foreach (var cQN in controlQueueNames)
            {
                controlQueueNumbersHashSet.Add(cQN);
                controlQueueNumberToInstanceIds[cQN.ToLowerInvariant()] = new List<string>();
            }

            for (int i = 0; i < 100; i++)
            {
                var instanceId = controlQueueHelper.GetControlQueueInstanceId(controlQueueNumbersHashSet, $"prefix{Guid.NewGuid()}_");

                var controlQueue = await azureStorageOrchestrationService.GetControlQueueAsync(instanceId);
                var controlQueueNumber = controlQueueNumberToNameMap[controlQueue.Name];

                controlQueueNumberToInstanceIds[controlQueue.Name.ToLowerInvariant()].Add(instanceId);

                Assert.IsTrue(controlQueueNames.Any(x => x.Equals(controlQueue.Name, StringComparison.OrdinalIgnoreCase)));
            }

            foreach (var cQN in controlQueueNames)
            {
                Assert.IsTrue(controlQueueNumberToInstanceIds[cQN.ToLowerInvariant()].Count > 0);
            }
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
        public async Task GetControlQueueInstanceId_ControlQueueNumbers(int[] controlQueueNumbers)
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
    }
}
