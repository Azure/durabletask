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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Query;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationQueryTests
    {
        AzureStorageOrchestrationService service;

        [TestInitialize]
        public async Task Initialize()
        {
            var settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(enableExtendedSessions: false);
            settings.TaskHubName = "query" + Guid.NewGuid().ToString("N");
            this.service = new AzureStorageOrchestrationService(settings);
            await this.service.CreateIfNotExistsAsync();

            TableClient table = settings.StorageAccountClientProvider.Table.CreateClient(new TableClientOptions()).GetTableClient(settings.TaskHubName + "Instances");
            foreach (string instanceId in new[]
            {
                "@counter@one", "@counter@two", "@other@one", "0order@one",
                "order-one", "order@one", "order@two", "order'/\\#?^x-one", "z-other",
                "order-failed", "z-failed",
            })
            {
                await table.AddEntityAsync(new OrchestrationInstanceStatus
                {
                    PartitionKey = KeySanitation.EscapePartitionKey(instanceId),
                    RowKey = string.Empty,
                    ExecutionId = "execution",
                    Name = "Test",
                    CreatedTime = DateTime.UtcNow,
                    LastUpdatedTime = DateTime.UtcNow,
                    CustomStatus = instanceId.StartsWith("@", StringComparison.Ordinal) ? "{\"entityExists\":true}" : null,
                    RuntimeStatus = instanceId.EndsWith("failed", StringComparison.Ordinal)
                        ? OrchestrationStatus.Failed.ToString()
                        : instanceId == "order@two" ? OrchestrationStatus.Completed.ToString() : OrchestrationStatus.Running.ToString(),
                });
            }
        }

        [TestCleanup]
        public async Task Cleanup()
        {
            if (this.service != null)
            {
                await this.service.DeleteAsync();
                this.service.Dispose();
            }
        }

        [DataTestMethod]
        [DataRow(null, new[] { "@counter@one", "@counter@two", "@other@one" })]
        [DataRow("counter", new[] { "@counter@one", "@counter@two" })]
        [DataRow("@counter@", new[] { "@counter@one", "@counter@two" })]
        public async Task EntityQuery_StillListsEntities(string prefix, string[] expectedInstanceIds)
        {
            EntityBackendQueries queries = ((IEntityOrchestrationService)this.service).EntityBackendQueries;
            var query = new EntityBackendQueries.EntityQuery { InstanceIdStartsWith = prefix, PageSize = 2 };
            var instanceIds = new List<string>();
            var tokens = new HashSet<string>();
            do
            {
                EntityBackendQueries.EntityQueryResult page = await queries.QueryEntitiesAsync(query, CancellationToken.None);
                instanceIds.AddRange(page.Results.Select(entity => entity.EntityId.ToString()));
                query.ContinuationToken = page.ContinuationToken;
                if (query.ContinuationToken != null)
                {
                    Assert.IsTrue(tokens.Add(query.ContinuationToken), "Continuation tokens must make progress.");
                }
            }
            while (query.ContinuationToken != null);

            CollectionAssert.AreEquivalent(expectedInstanceIds, instanceIds);
        }

        [DataTestMethod]
        [DataRow(null, false, new[] { "@counter@one", "@counter@two", "@other@one", "0order@one", "order-one", "order@one", "order@two", "order'/\\#?^x-one", "z-other" })]
        [DataRow("", false, new[] { "@counter@one", "@counter@two", "@other@one", "0order@one", "order-one", "order@one", "order@two", "order'/\\#?^x-one", "z-other" })]
        [DataRow(null, true, new[] { "0order@one", "order-one", "order@one", "order@two", "order'/\\#?^x-one", "z-other" })]
        [DataRow("", true, new[] { "0order@one", "order-one", "order@one", "order@two", "order'/\\#?^x-one", "z-other" })]
        [DataRow("order", false, new[] { "order-one", "order@one", "order@two", "order'/\\#?^x-one" })]
        [DataRow("order", true, new[] { "order-one", "order@one", "order@two", "order'/\\#?^x-one" })]
        [DataRow("@", false, new[] { "@counter@one", "@counter@two", "@other@one" })]
        [DataRow("@", true, new string[0])]
        [DataRow("@counter@", false, new[] { "@counter@one", "@counter@two" })]
        [DataRow("@counter@", true, new string[0])]
        [DataRow("order@", false, new[] { "order@one", "order@two" })]
        [DataRow("order@", true, new[] { "order@one", "order@two" })]
        [DataRow("order'/\\#?^x", false, new[] { "order'/\\#?^x-one" })]
        [DataRow("order'/\\#?^x", true, new[] { "order'/\\#?^x-one" })]
        public async Task InstanceQuery_AppliesPrefixAndEntityExclusionBeforePaging(
            string prefix,
            bool excludeEntities,
            string[] expectedInstanceIds)
        {
            foreach (bool useCoreQuery in new[] { false, true })
            {
                var instanceIds = new List<string>();
                var tokens = new HashSet<string>();
                string continuationToken = null;
                var statuses = new[] { OrchestrationStatus.Running, OrchestrationStatus.Completed };
                do
                {
                    IEnumerable<OrchestrationState> states;
                    if (useCoreQuery)
                    {
                        OrchestrationQueryResult page = await this.service.GetOrchestrationWithQueryAsync(
                            new OrchestrationQuery
                            {
                                InstanceIdPrefix = prefix,
                                ExcludeEntities = excludeEntities,
                                RuntimeStatus = statuses,
                                PageSize = 2,
                                ContinuationToken = continuationToken,
                            },
                            CancellationToken.None);
                        states = page.OrchestrationState;
                        continuationToken = page.ContinuationToken;
                    }
                    else
                    {
                        DurableStatusQueryResult page = await this.service.GetOrchestrationStateAsync(
                            new OrchestrationInstanceStatusQueryCondition
                            {
                                InstanceIdPrefix = prefix,
                                ExcludeEntities = excludeEntities,
                                RuntimeStatus = statuses,
                            },
                            top: 2,
                            continuationToken);
                        states = page.OrchestrationState;
                        continuationToken = page.ContinuationToken;
                    }

                    string[] pageIds = states.Select(s => s.OrchestrationInstance.InstanceId).ToArray();
                    Assert.IsTrue(pageIds.Length <= 2, "The requested page size must be preserved.");
                    CollectionAssert.IsSubsetOf(pageIds, expectedInstanceIds, $"Unexpected storage result (Core query: {useCoreQuery}).");
                    instanceIds.AddRange(pageIds);
                    if (continuationToken != null)
                    {
                        Assert.IsTrue(tokens.Add(continuationToken), "Continuation tokens must make progress.");
                    }
                }
                while (continuationToken != null);

                CollectionAssert.AreEquivalent(expectedInstanceIds, instanceIds, $"Results must survive paging without loss or duplication (Core query: {useCoreQuery}).");
                if (expectedInstanceIds.Length > 2)
                {
                    Assert.IsTrue(tokens.Count > 0, "Multiple pages must be retrieved using continuation tokens.");
                }
            }
        }
    }
}
