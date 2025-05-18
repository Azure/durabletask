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
    using System.Collections.Generic;
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TestPartitionIndex
    {
        private AzureStorageOrchestrationService azureStorageOrchestrationService;
        private AzureStorageOrchestrationServiceSettings settings;
        private int partitionCount = 4;
        private CancellationTokenSource cancellationTokenSource;
        private const string TaskHub = "taskHubName";

        private Dictionary<uint, string> partitionToInstanceId = new Dictionary<uint, string>()
        {
            { 0, "sampleinstanceid!13"},
            { 1, "sampleinstanceid!3"},
            { 2, "sampleinstanceid!1!"},
            { 3, "sampleinstanceid!1"}
        };

        private Dictionary<uint, string> partitionToInstanceIdWithExplicitPartitionPlacement = new Dictionary<uint, string>()
        {
            { 0, "sampleinstanceid!0"},
            { 1, "sampleinstanceid!2!1"},
            { 2, "sampleinstanceid!2"},
            { 3, "sampleinstanceid!3"}
        };

        private Dictionary<uint, string> partitionToInstanceIdWithExplicitPartitionPlacementEndingWithExclamation = new Dictionary<uint, string>()
        {
            { 0, "sampleinstanceid!3!"},
            { 1, "sampleinstanceid!2!"},
            { 2, "sampleinstanceid!1!"},
            { 3, "sampleinstanceid!0!"}
        };

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

            string str = "sampleinstanceid!0!";
            var index = azureStorageOrchestrationService.GetPartitionIndex(str);
        }

        [TestMethod]
        public void GetPartitionIndexTest_EnableExplicitPartitionPlacement_False()
        {
            settings.EnableExplicitPartitionPlacement = false;

            foreach (var kvp in partitionToInstanceId)
            {
                var instanceId = kvp.Value;
                var expectedPartitionIndex = kvp.Key;
                var partitionIndex = azureStorageOrchestrationService.GetPartitionIndex(instanceId);

                Assert.AreEqual(expectedPartitionIndex, partitionIndex);
            }
        }

        [TestMethod]
        public void GetPartitionIndexTest_EnableExplicitPartitionPlacement_True()
        {
            settings.EnableExplicitPartitionPlacement = true;

            foreach (var kvp in partitionToInstanceIdWithExplicitPartitionPlacement)
            {
                var instanceId = kvp.Value;
                var expectedPartitionIndex = kvp.Key;
                var partitionIndex = azureStorageOrchestrationService.GetPartitionIndex(instanceId);

                Assert.AreEqual(expectedPartitionIndex, partitionIndex);
            }
        }

        [TestMethod]
        public void GetPartitionIndexTest_EndingWithExclamation_EnableExplicitPartitionPlacement_True()
        {
            settings.EnableExplicitPartitionPlacement = true;

            foreach (var kvp in partitionToInstanceIdWithExplicitPartitionPlacementEndingWithExclamation)
            {
                var instanceId = kvp.Value;
                var expectedPartitionIndex = kvp.Key;
                var partitionIndex = azureStorageOrchestrationService.GetPartitionIndex(instanceId);

                Assert.AreEqual(expectedPartitionIndex, partitionIndex);
            }
        }

        [TestMethod]
        public void GetPartitionIndexTest_EndingWithExclamation_EnableExplicitPartitionPlacement_False()
        {
            settings.EnableExplicitPartitionPlacement = false;

            foreach (var kvp in partitionToInstanceIdWithExplicitPartitionPlacementEndingWithExclamation)
            {
                var instanceId = kvp.Value;
                var expectedPartitionIndex = kvp.Key;
                var partitionIndex = azureStorageOrchestrationService.GetPartitionIndex(instanceId);

                Assert.AreEqual(expectedPartitionIndex, partitionIndex);
            }
        }
    }
}
