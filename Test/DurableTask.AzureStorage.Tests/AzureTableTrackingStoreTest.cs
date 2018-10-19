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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Tests
{
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Table;
    using Moq;
    using System.Linq;
    using System.Reflection;
    using Newtonsoft.Json;

    [TestClass]
    public class AzureTableTrackingStoreTest
    {
        [TestMethod]
        public async Task QueryStatus_WithContinuationToken()
        {
            var ExpectedCreatedDateFrom = DateTime.Now;
            var ExpectedCreatedDateTo = DateTime.Now;
            var ExpectedTop = 3;
            var InputToken = "";

            var ExpectedResult = new DurableStatusQueryResult();
            var InputStatus = new List<OrchestrationInstanceStatus>()
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

            var ExpectedNextPartitionKey = "foo";
            var ExpectedTokenObject = new TableContinuationToken()
            {
                NextPartitionKey = ExpectedNextPartitionKey,
                NextRowKey = "bar",
                NextTableName = "baz",
            };
            var tokenJson = JsonConvert.SerializeObject(ExpectedTokenObject);
            var tokenBase64String = Convert.ToBase64String(Encoding.UTF8.GetBytes(tokenJson));
            ExpectedResult.ContinuationToken = tokenBase64String;
            ExpectedResult.OrchestrationState = new List<OrchestrationState>()
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

            var stats = new AzureStorageOrchestrationServiceStats();

            var cloudTableMock = new Mock<CloudTable>(new Uri("https://microsoft.com"));
            var segment = (TableQuerySegment<OrchestrationInstanceStatus>)System.Runtime.Serialization.FormatterServices.GetSafeUninitializedObject(typeof(TableQuerySegment<OrchestrationInstanceStatus>));
            segment.GetType().GetProperty("Results").SetValue(segment, InputStatus);
            segment.GetType().GetProperty("ContinuationToken").SetValue(segment, ExpectedTokenObject);

            cloudTableMock.Setup(t => t.ExecuteQuerySegmentedAsync<OrchestrationInstanceStatus>(
                    It.IsAny<TableQuery<OrchestrationInstanceStatus>>(), 
                    It.IsAny<TableContinuationToken>()))
                .ReturnsAsync(segment)
                .Callback<TableQuery<OrchestrationInstanceStatus>, TableContinuationToken>(
                    (q, token) =>
                    {
                        Assert.IsNull(token);
                        Assert.AreEqual(ExpectedTop, q.TakeCount);
                    });
            var trakingStore = new AzureTableTrackingStore(stats, cloudTableMock.Object);

            var InputState = new List<OrchestrationStatus>();
            InputState.Add(OrchestrationStatus.Running);
            InputState.Add(OrchestrationStatus.Completed);
            InputState.Add(OrchestrationStatus.Failed);

            var result = await trakingStore.GetStateAsync(ExpectedCreatedDateFrom, ExpectedCreatedDateTo, InputState, 3, InputToken);
            Assert.AreEqual(ExpectedResult.ContinuationToken, result.ContinuationToken);
            Assert.AreEqual(ExpectedResult.OrchestrationState.Count(), result.OrchestrationState.Count());
            Assert.AreEqual(ExpectedResult.OrchestrationState.FirstOrDefault().Name, result.OrchestrationState.FirstOrDefault().Name);
            cloudTableMock.Verify(t => t.ExecuteQuerySegmentedAsync<OrchestrationInstanceStatus>(
                It.IsAny<TableQuery<OrchestrationInstanceStatus>>(),
                It.IsAny<TableContinuationToken>()));
        }
    }


}
