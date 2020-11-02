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

namespace DurableTask.AzureStorage.Tests.Correlation
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ListExtensionsTest
    {
        [TestMethod]
        public async Task CorrelationSortAsync()
        {
            var operations = new List<OperationTelemetry>();
            var timeStamps = await GetDateTimeOffsetsAsync(6);

            operations.Add(CreateDependencyTelemetry(id: "02", parentId: "01", timeStamps[3]));
            operations.Add(CreateRequestTelemetry(id: "01", parentId: null, timeStamps[0]));
            operations.Add(CreateRequestTelemetry(id:"04", parentId: "03", timeStamps[2]));
            operations.Add(CreateDependencyTelemetry(id:"05", parentId: "04", timeStamps[4]));
            operations.Add(CreateDependencyTelemetry(id: "06", parentId: "05", timeStamps[5]));
            operations.Add(CreateRequestTelemetry(id: "03", parentId: "02", timeStamps[1]));

            var actual = operations.CorrelationSort();
            Assert.AreEqual(6, actual.Count);
            Assert.AreEqual("01", actual[0].Id);
            Assert.AreEqual("02", actual[1].Id);
            Assert.AreEqual("03", actual[2].Id);
            Assert.AreEqual("04", actual[3].Id);
            Assert.AreEqual("05", actual[4].Id);
            Assert.AreEqual("06", actual[5].Id);
        }

        [TestMethod]
        public async Task CorrelationSortWithTwoChildrenAsync()
        {
            var operations = new List<OperationTelemetry>();
            var timeStamps = await GetDateTimeOffsetsAsync(8);
            operations.Add(CreateRequestTelemetry(id: "01", parentId: null, timeStamps[0]));
            operations.Add(CreateDependencyTelemetry(id: "02", parentId: "01", timeStamps[1]));
            operations.Add(CreateRequestTelemetry(id: "05", parentId: "03", timeStamps[3]));
            operations.Add(CreateRequestTelemetry(id: "04", parentId: "03", timeStamps[2]));
            operations.Add(CreateDependencyTelemetry(id: "06", parentId: "04", timeStamps[4]));
            operations.Add(CreateDependencyTelemetry(id: "08", parentId: "03", timeStamps[6]));
            operations.Add(CreateDependencyTelemetry(id: "07", parentId: "05", timeStamps[5]));
            operations.Add(CreateRequestTelemetry(id: "03", parentId: "02", timeStamps[7]));

            var actual = operations.CorrelationSort();
            Assert.AreEqual(8, actual.Count);
            Assert.AreEqual("01", actual[0].Id);
            Assert.AreEqual("02", actual[1].Id);
            Assert.AreEqual("03", actual[2].Id);
            Assert.AreEqual("04", actual[3].Id);
            Assert.AreEqual("06", actual[4].Id);  // Since the tree structure, 04 child comes before 05.
            Assert.AreEqual("05", actual[5].Id); 
            Assert.AreEqual("07", actual[6].Id);
            Assert.AreEqual("08", actual[7].Id);
        }

        [TestMethod]
        public void CorrelationSortWithZero()
        {
            var operations = new List<OperationTelemetry>();
            var actual = operations.CorrelationSort();
            Assert.AreEqual(0, actual.Count);
        }

        async Task<List<DateTimeOffset>> GetDateTimeOffsetsAsync(int count)
        {
            List<DateTimeOffset> result = new List<DateTimeOffset>();
            for (var i = 0; i < count; i++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1));
                result.Add(DateTimeOffset.UtcNow);
            }

            return result;
        }

        RequestTelemetry CreateRequestTelemetry(string id, string parentId, DateTimeOffset timeStamp)
        {
            return (RequestTelemetry)SetIdAndParentId(new RequestTelemetry(), id, parentId, timeStamp);
        }

        DependencyTelemetry CreateDependencyTelemetry(string id, string parentId, DateTimeOffset timeStamp)
        {
            return (DependencyTelemetry)SetIdAndParentId(new DependencyTelemetry(), id, parentId, timeStamp);
        }

        OperationTelemetry SetIdAndParentId(OperationTelemetry telemetry, string id, string parentId, DateTimeOffset timeStamp)
        {
            telemetry.Id = id;
            telemetry.Context.Operation.ParentId = parentId;
            telemetry.Timestamp = timeStamp;
            return telemetry;
        }
    }
}
