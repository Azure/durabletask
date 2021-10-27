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
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Moq;
    using Newtonsoft.Json;

    [TestClass]
    public class AzureTableTrackingStoreTest
    {
        [TestMethod]
        public async Task QueryStatus_WithContinuationToken_NoInputToken()
        {
            var fixture = new QueryFixture();
            fixture.SetUpQueryStateWithPagerWithoutInputToken();

            var inputState = new List<OrchestrationStatus>();
            inputState.Add(OrchestrationStatus.Running);
            inputState.Add(OrchestrationStatus.Completed);
            inputState.Add(OrchestrationStatus.Failed);

            var result = await fixture.TrackingStore.GetStateAsync(fixture.ExpectedCreatedDateFrom, fixture.ExpectedCreatedDateTo, inputState, 3, fixture.InputToken);

            Assert.AreEqual("", fixture.ActualPassedTokenString);

            Assert.AreEqual(fixture.ExpectedResult.ContinuationToken, result.ContinuationToken);
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.Count(), result.OrchestrationState.Count());
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.FirstOrDefault().Name, result.OrchestrationState.FirstOrDefault().Name);
            fixture.VerifyQueryStateWithPager();
        }

        [TestMethod]
        public async Task QueryStatus_WithContinuationToken_InputToken()
        {
            var fixture = new QueryFixture();
            var inputToken = new TableContinuationToken()
            {
                NextPartitionKey = "qux",
                NextRowKey = "quux",
                NextTableName = "corge",
            };

            var inputTokenString = JsonConvert.SerializeObject(inputToken);

            fixture.SetupQueryStateWithPagerWithInputToken(inputTokenString);

            var inputState = new List<OrchestrationStatus>();
            inputState.Add(OrchestrationStatus.Running);
            inputState.Add(OrchestrationStatus.Completed);
            inputState.Add(OrchestrationStatus.Failed);

            var result = await fixture.TrackingStore.GetStateAsync(fixture.ExpectedCreatedDateFrom, fixture.ExpectedCreatedDateTo, inputState, 3, fixture.InputToken);

            Assert.AreEqual(inputTokenString, fixture.ActualPassedTokenString);

            Assert.AreEqual(fixture.ExpectedResult.ContinuationToken, result.ContinuationToken);
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.Count(), result.OrchestrationState.Count());
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.FirstOrDefault().Name, result.OrchestrationState.FirstOrDefault().Name);
            fixture.VerifyQueryStateWithPager();
        }

        private class QueryFixture
        {
            private readonly Mock<Table> tableMock;

            public AzureTableTrackingStore TrackingStore { get; set; }

            public Table TableMock => this.tableMock.Object;

            public DateTime ExpectedCreatedDateFrom { get; set; }

            public DateTime ExpectedCreatedDateTo { get; set; }

            public int ExpectedTop { get; set; }

            public DurableStatusQueryResult ExpectedResult { get; set; }

            public string ExpectedNextPartitionKey { get; set; }

            public string ExpectedTokenObject { get; set; }

            public string InputToken { get; set; }

            public string ExpectedPassedTokenObject { get; set; }

            public string ActualPassedTokenString { get; set; }

            public List<OrchestrationInstanceStatus> InputStatus { get; set; }

            public List<OrchestrationStatus> InputState { get; set; }

            public QueryFixture()
            {
                var azureStorageClient = new AzureStorageClient(new AzureStorageOrchestrationServiceSettings() { StorageConnectionString = "UseDevelopmentStorage=true"});
                var cloudStorageAccount = CloudStorageAccount.Parse(azureStorageClient.Settings.StorageConnectionString);
                var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

                this.tableMock = new Mock<Table>(azureStorageClient, cloudTableClient, "MockTable");
            }

            private void SetUpQueryStateWithPager(string inputToken)
            {
                this.ExpectedCreatedDateFrom = DateTime.UtcNow;
                this.ExpectedCreatedDateTo = DateTime.UtcNow;
                this.ExpectedTop = 3;

                this.InputToken = inputToken;
                SetupQueryStateWithPagerInputStatus();
                SetUpQueryStateWithPagerResult();
                SetupExecuteQuerySegmentMock();
                SetupTrackingStore();
            }

            public void SetUpQueryStateWithPagerWithoutInputToken()
            {
                SetUpQueryStateWithPager("");
            }

            public void SetupQueryStateWithPagerWithInputToken(string serializedInputToken)
            {
                this.ExpectedPassedTokenObject = serializedInputToken;
                SetUpQueryStateWithPager(serializedInputToken);
            }

            public void VerifyQueryStateWithPager()
            {
                this.tableMock.Verify(t => t.ExecuteQuerySegmentAsync<OrchestrationInstanceStatus>(
                    It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                        It.IsAny<CancellationToken>(),
                        It.IsAny<string>()));
            }

            private void SetupExecuteQuerySegmentMock()
            {
                var segment = (TableEntitiesResponseInfo<OrchestrationInstanceStatus>)System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(typeof(TableEntitiesResponseInfo<OrchestrationInstanceStatus>));
                segment.GetType().GetProperty("ReturnedEntities").SetValue(segment, this.InputStatus);
                segment.GetType().GetProperty("ContinuationToken").SetValue(segment, this.ExpectedTokenObject);

                this.tableMock.Setup(t => t.ExecuteQuerySegmentAsync<OrchestrationInstanceStatus>(
                        It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                        It.IsAny<CancellationToken>(),
                        It.IsAny<string>()))
                    .Returns(Task.FromResult(segment))
                    .Callback<TableQuery<OrchestrationInstanceStatus>, CancellationToken, string>(
                        (q, cancelToken, token) =>
                        {
                            this.ActualPassedTokenString = token;
                            Assert.AreEqual(this.ExpectedTop, q.TakeCount);
                        });
            }

            private void SetupQueryStateWithPagerInputStatus()
            {
                this.InputStatus = new List<OrchestrationInstanceStatus>()
                {
                    new OrchestrationInstanceStatus()
                    {
                        Name = "foo",
                        RuntimeStatus = "Running"
                    },
                    new OrchestrationInstanceStatus()
                    {
                        Name = "bar",
                        RuntimeStatus = "Completed"
                    },
                    new OrchestrationInstanceStatus()
                    {
                        Name = "baz",
                        RuntimeStatus = "Failed"
                    }
                };
            }

            private void SetUpQueryStateWithPagerResult()
            {
                this.ExpectedResult = new DurableStatusQueryResult();

                this.ExpectedNextPartitionKey = "foo";
                var token = new TableContinuationToken()
                {
                    NextPartitionKey = ExpectedNextPartitionKey,
                    NextRowKey = "bar",
                    NextTableName = "baz",
                };
                this.ExpectedTokenObject = JsonConvert.SerializeObject(token);
                this.ExpectedResult.ContinuationToken = this.ExpectedTokenObject;
                this.ExpectedResult.OrchestrationState = new List<OrchestrationState>()
                {
                    new OrchestrationState()
                    {
                        Name = "foo",
                        OrchestrationStatus = OrchestrationStatus.Running
                    },
                    new OrchestrationState()
                    {
                        Name = "bar",
                        OrchestrationStatus = OrchestrationStatus.Completed
                    },
                    new OrchestrationState()
                    {
                        Name = "baz",
                        OrchestrationStatus = OrchestrationStatus.Failed
                    }
                };
            }

            private void SetupTrackingStore()
            {
                var stats = new AzureStorageOrchestrationServiceStats();
                this.TrackingStore = new AzureTableTrackingStore(stats, this.TableMock);
            }
        }
    }
}
