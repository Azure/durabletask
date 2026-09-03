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
    using System.Linq;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class RewindOutputTrackingStoreTests
    {
        const string PreservedProperty = "PreservedProperty";

        string taskHubName;
        AzureTableTrackingStore trackingStore;

        [TestInitialize]
        public async Task Initialize()
        {
            this.taskHubName = "rewind" + Guid.NewGuid().ToString("N").Substring(0, 9);
            AzureStorageOrchestrationServiceSettings settings =
                TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = this.taskHubName;

            var azureStorageClient = new AzureStorageClient(settings);
            var messageManager = new MessageManager(
                settings,
                azureStorageClient,
                $"{this.taskHubName}-largemessages".ToLowerInvariant());
            this.trackingStore = new AzureTableTrackingStore(azureStorageClient, messageManager);
            await this.trackingStore.CreateAsync();
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            if (this.trackingStore != null)
            {
                await this.trackingStore.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_RemovesPersistedOutput()
        {
            string instanceId = $"output-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: "old failure");

            TableEntity failed = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual("old failure", failed["Output"]);

            await this.trackingStore.UpdateStatusForRewindAsync(instanceId);

            TableEntity rewound = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), rewound["RuntimeStatus"]);
            Assert.IsFalse(rewound.ContainsKey("Output"));
            Assert.AreEqual("preserve me", rewound[PreservedProperty]);
        }

        [TestMethod]
        public async Task UpdateStatusForRewind_IsIdempotentWhenOutputIsMissing()
        {
            string instanceId = $"missing-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: null);

            await this.trackingStore.UpdateStatusForRewindAsync(instanceId);
            await this.trackingStore.UpdateStatusForRewindAsync(instanceId);

            TableEntity rewound = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), rewound["RuntimeStatus"]);
            Assert.IsFalse(rewound.ContainsKey("Output"));
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.Failed)]
        public async Task TerminalWriteAfterRewind_PersistsNewOutput(OrchestrationStatus terminalStatus)
        {
            string instanceId = $"complete-{Guid.NewGuid():N}";
            const string ExecutionId = "execution-1";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Failed, output: "old failure");
            await this.trackingStore.UpdateStatusForRewindAsync(instanceId);

            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(CreateExecutionStartedEvent(instanceId, ExecutionId));
            runtimeState.AddEvent(new ExecutionCompletedEvent(-1, "new output", terminalStatus));

            await this.trackingStore.UpdateInstanceStatusForCompletedOrchestrationAsync(
                instanceId,
                ExecutionId,
                runtimeState,
                instanceEntityExists: true);

            TableEntity completed = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(terminalStatus.ToString(), completed["RuntimeStatus"]);
            Assert.AreEqual("new output", completed["Output"]);
        }

        [TestMethod]
        public async Task SetNewExecution_ReplacesPersistedOutput()
        {
            string instanceId = $"reuse-{Guid.NewGuid():N}";
            await this.SeedInstanceRowAsync(instanceId, OrchestrationStatus.Completed, output: "old output");
            TableEntity existing = await this.GetRawEntityAsync(instanceId);

            bool created = await this.trackingStore.SetNewExecutionAsync(
                CreateExecutionStartedEvent(instanceId, "execution-2"),
                new ETag(existing.ETag.ToString()),
                inputPayloadOverride: null);

            Assert.IsTrue(created);
            TableEntity pending = await this.GetRawEntityAsync(instanceId);
            Assert.AreEqual(OrchestrationStatus.Pending.ToString(), pending["RuntimeStatus"]);
            Assert.IsFalse(pending.ContainsKey("Output"));
        }

        async Task SeedInstanceRowAsync(string instanceId, OrchestrationStatus status, string output)
        {
            var entity = new TableEntity(KeySanitation.EscapePartitionKey(instanceId), string.Empty)
            {
                ["Name"] = "TestOrchestration",
                ["RuntimeStatus"] = status.ToString(),
                ["CreatedTime"] = DateTime.UtcNow,
                ["LastUpdatedTime"] = DateTime.UtcNow,
                ["TaskHubName"] = this.taskHubName,
                ["ExecutionId"] = "execution-1",
                [PreservedProperty] = "preserve me",
            };

            if (output != null)
            {
                entity["Output"] = output;
            }

            await this.trackingStore.InstancesTable.InsertEntityAsync(entity);
        }

        async Task<TableEntity> GetRawEntityAsync(string instanceId)
        {
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and " +
                $"{AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), string.Empty)}";
            return await this.trackingStore.InstancesTable
                .ExecuteQueryAsync<TableEntity>(filter, 1)
                .FirstOrDefaultAsync();
        }

        static ExecutionStartedEvent CreateExecutionStartedEvent(string instanceId, string executionId)
        {
            return new ExecutionStartedEvent(-1, "input")
            {
                Name = "TestOrchestration",
                Version = string.Empty,
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = instanceId,
                    ExecutionId = executionId,
                },
            };
        }
    }
}
