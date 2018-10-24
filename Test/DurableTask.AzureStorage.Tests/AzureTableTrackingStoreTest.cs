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
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
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

            Assert.IsNull(fixture.ActualPassedTokenObject);

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

            fixture.SetupQueryStateWithPagerWithInputToken(inputToken);

            var inputState = new List<OrchestrationStatus>();
            inputState.Add(OrchestrationStatus.Running);
            inputState.Add(OrchestrationStatus.Completed);
            inputState.Add(OrchestrationStatus.Failed);

            var result = await fixture.TrackingStore.GetStateAsync(fixture.ExpectedCreatedDateFrom, fixture.ExpectedCreatedDateTo, inputState, 3, fixture.InputToken);

            Assert.AreEqual(inputToken.NextPartitionKey, fixture.ActualPassedTokenObject.NextPartitionKey);

            Assert.AreEqual(fixture.ExpectedResult.ContinuationToken, result.ContinuationToken);
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.Count(), result.OrchestrationState.Count());
            Assert.AreEqual(fixture.ExpectedResult.OrchestrationState.FirstOrDefault().Name, result.OrchestrationState.FirstOrDefault().Name);
            fixture.VerifyQueryStateWithPager();
        }

        private class QueryFixture
        {
            private readonly Mock<CloudTable> cloudTableMock;

            public AzureTableTrackingStore TrackingStore { get; set; }

            public CloudTable CloudTableMock => this.cloudTableMock.Object;

            public DateTime ExpectedCreatedDateFrom { get; set; }

            public DateTime ExpectedCreatedDateTo { get; set; }

            public int ExpectedTop { get; set; }

            public DurableStatusQueryResult ExpectedResult { get; set; }

            public string ExpectedNextPartitionKey { get; set; }

            public TableContinuationToken ExpectedTokenObject { get; set; }

            public string InputToken { get; set; }

            public TableContinuationToken ExpectedPassedTokenObject { get; set; }

            public TableContinuationToken ActualPassedTokenObject { get; set; }

            public List<OrchestrationInstanceStatus> InputStatus { get; set; }

            public List<OrchestrationStatus> InputState { get; set; }

            public QueryFixture()
            {
                this.cloudTableMock = new Mock<CloudTable>(new Uri("https://microsoft.com"));
            }

            private void SetUpQueryStateWithPager(string inputToken, Action setupMock)
            {
                this.ExpectedCreatedDateFrom = DateTime.UtcNow;
                this.ExpectedCreatedDateTo = DateTime.UtcNow;
                this.ExpectedTop = 3;

                this.InputToken = inputToken;
                SetupQueryStateWithPagerInputStatus();
                SetUpQueryStateWithPagerResult();
                setupMock();
                SetupTrackingStore();
            }

            public void SetUpQueryStateWithPagerWithoutInputToken()
            {
                SetUpQueryStateWithPager("", SetupQueryStateWithPagerMock);
            }

            public void SetupQueryStateWithPagerWithInputToken(TableContinuationToken inputTokenObject)
            {
                this.ExpectedPassedTokenObject = inputTokenObject;
                var tokenJson = JsonConvert.SerializeObject(ExpectedPassedTokenObject);
                this.InputToken = Convert.ToBase64String(Encoding.UTF8.GetBytes(tokenJson));
                SetUpQueryStateWithPager(this.InputToken, SetupQueryStateWithPagerMock_WithInputToken);
            }

            public void VerifyQueryStateWithPager()
            {
                this.cloudTableMock.Verify(t => t.ExecuteQuerySegmentedAsync<OrchestrationInstanceStatus>(
                    It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                    It.IsAny<TableContinuationToken>()));
            }

            private void SetupQueryStateWithPagerMock()
            {
                var segment = (TableQuerySegment<OrchestrationInstanceStatus>)System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(typeof(TableQuerySegment<OrchestrationInstanceStatus>));
                segment.GetType().GetProperty("Results").SetValue(segment, this.InputStatus);
                segment.GetType().GetProperty("ContinuationToken").SetValue(segment, this.ExpectedTokenObject);

                this.cloudTableMock.Setup(t => t.ExecuteQuerySegmentedAsync<OrchestrationInstanceStatus>(
                        It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                        It.IsAny<TableContinuationToken>()))
                    .ReturnsAsync(segment)
                    .Callback<TableQuery<OrchestrationInstanceStatus>, TableContinuationToken>(
                        (q, token) =>
                        {
                            this.ActualPassedTokenObject = token;
                            Assert.AreEqual(this.ExpectedTop, q.TakeCount);
                        });
            }

            private void SetupQueryStateWithPagerMock_WithInputToken()
            {
                var segment = (TableQuerySegment<OrchestrationInstanceStatus>)System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(typeof(TableQuerySegment<OrchestrationInstanceStatus>));
                segment.GetType().GetProperty("Results").SetValue(segment, this.InputStatus);
                segment.GetType().GetProperty("ContinuationToken").SetValue(segment, this.ExpectedTokenObject);

                this.cloudTableMock.Setup(t => t.ExecuteQuerySegmentedAsync<OrchestrationInstanceStatus>(
                        It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                        It.IsAny<TableContinuationToken>()))
                    .ReturnsAsync(segment)
                    .Callback<TableQuery<OrchestrationInstanceStatus>, TableContinuationToken>(
                        (q, token) =>
                        {
                            this.ActualPassedTokenObject = token;
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
                this.ExpectedTokenObject = new TableContinuationToken()
                {
                    NextPartitionKey = ExpectedNextPartitionKey,
                    NextRowKey = "bar",
                    NextTableName = "baz",
                };
                var tokenJson = JsonConvert.SerializeObject(ExpectedTokenObject);
                var tokenBase64String = Convert.ToBase64String(Encoding.UTF8.GetBytes(tokenJson));
                this.ExpectedResult.ContinuationToken = tokenBase64String;
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
                this.TrackingStore = new AzureTableTrackingStore(stats, this.CloudTableMock);
            }
        }
    }
}
