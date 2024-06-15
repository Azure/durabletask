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
    using System.Threading;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TestPartitionIndex
    {
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

            controlQueueNumberToNameMap = new Dictionary<string, int>();

            for (int i = 0; i < partitionCount; i++)
            {
                var controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, i);
                controlQueueNumberToNameMap[controlQueueName] = i;
            }
        }

        [TestMethod]
        [DataRow(20, false)]
        [DataRow(20, true)]
        public void GetPartitionIndexTest(int maxInstanceIdCount, bool enableExplicitPartitionPlacement)
        {
            settings.EnableExplicitPartitionPlacement = enableExplicitPartitionPlacement;

            for (uint instanceIdSuffix = 0; instanceIdSuffix < settings.PartitionCount * 4; instanceIdSuffix++)
            {
                Dictionary<uint, int> indexNumberToCount = new Dictionary<uint, int>();

                for (uint indexCount = 0; indexCount < settings.PartitionCount; indexCount++)
                {
                    indexNumberToCount[indexCount] = 0;
                }

                for (int instanceCount = 0; instanceCount < maxInstanceIdCount; instanceCount++)
                {
                    var instanceIdPrefix = Guid.NewGuid().ToString();

                    var instanceId = $"{instanceIdPrefix}!{instanceIdSuffix}";

                    var partitionIndex = azureStorageOrchestrationService.GetPartitionIndex(instanceId);

                    indexNumberToCount[partitionIndex]++;
                }

                if (enableExplicitPartitionPlacement)
                {
                    Assert.AreEqual(indexNumberToCount[(uint)(instanceIdSuffix % settings.PartitionCount)], maxInstanceIdCount);
                }
                else
                {
                    Assert.AreNotEqual(indexNumberToCount[(uint)(instanceIdSuffix % settings.PartitionCount)], maxInstanceIdCount);
                }
            }
        }
    }
}
