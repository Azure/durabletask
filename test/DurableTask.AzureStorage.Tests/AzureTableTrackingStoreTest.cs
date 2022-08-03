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
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Newtonsoft.Json;

    [TestClass]
    public class AzureTableTrackingStoreTest
    {
        [TestMethod]
        public async Task QueryStatus_WithContinuationToken_NoInputToken()
        {
            const string ConnectionString = "UseDevelopmentStorage=true";

            using var tokenSource = new CancellationTokenSource();

            var azureStorageClient = new AzureStorageClient(new AzureStorageOrchestrationServiceSettings(ConnectionString));
            var tableServiceClient = new TableServiceClient(ConnectionString);
            var tableMock = new Mock<Table>(MockBehavior.Strict, azureStorageClient, tableServiceClient, "MockTable");
            var stats = new AzureStorageOrchestrationServiceStats();
            var trackingStore = new AzureTableTrackingStore(stats, tableMock.Object);

            DateTime expectedCreatedDateFrom = DateTime.UtcNow;
            DateTime expectedCreatedDateTo = expectedCreatedDateFrom.AddHours(1);
            var inputState = new List<OrchestrationStatus>
            {
                OrchestrationStatus.Running,
                OrchestrationStatus.Completed,
                OrchestrationStatus.Failed,
            };
            var expected = new List<OrchestrationInstanceStatus>
            {
                new OrchestrationInstanceStatus
                {
                    Name = "foo",
                    RuntimeStatus = "Running"
                },
                new OrchestrationInstanceStatus
                {
                    Name = "bar",
                    RuntimeStatus = "Completed"
                },
                new OrchestrationInstanceStatus
                {
                    Name = "baz",
                    RuntimeStatus = "Failed"
                }
            };

            tableMock
                .Setup(t => t.ExecuteQueryAsync<OrchestrationInstanceStatus>(
                    $"{nameof(OrchestrationInstanceStatus.CreatedTime)} ge datetime'{expectedCreatedDateFrom:O} and {nameof(OrchestrationInstanceStatus.CreatedTime)} le datetime'{expectedCreatedDateTo:O}'",
                    new string[] { "foo", "bar", "baz" },
                    tokenSource.Token))
                .Returns(
                    new TableQueryResponse<OrchestrationInstanceStatus>(
                        AsyncPageable<OrchestrationInstanceStatus>.FromPages(
                            new[] { Page<OrchestrationInstanceStatus>.FromValues(expected, null, new Mock<Response>().Object) })));

            // .ExecuteQueryAsync<OrchestrationInstanceStatus>(filter, select, cancellationToken)
            var actual = await trackingStore
                .GetStateAsync(expectedCreatedDateFrom, expectedCreatedDateTo, inputState)
                .ToListAsync();

            Assert.Equals(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.AreSame(expected[i], actual[i]);
            }
        }
    }
}
