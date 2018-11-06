using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Tests.Correlation
{
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

            operations.Add(CreateDependencyTelemetry("02", "01", timeStamps[3]));
            operations.Add(CreateRequestTelemetry("01", null, timeStamps[0]));
            operations.Add(CreateRequestTelemetry("04", "03", timeStamps[2]));
            operations.Add(CreateDependencyTelemetry("05", "04", timeStamps[4]));
            operations.Add(CreateDependencyTelemetry("06", "05", timeStamps[5]));
            operations.Add(CreateRequestTelemetry("03", "02", timeStamps[1]));

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
            operations.Add(CreateRequestTelemetry("01", null, timeStamps[0]));
            operations.Add(CreateDependencyTelemetry("02", "01", timeStamps[1]));
            operations.Add(CreateRequestTelemetry("05", "03", timeStamps[3]));
            operations.Add(CreateRequestTelemetry("04", "03", timeStamps[2]));
            operations.Add(CreateDependencyTelemetry("06", "04", timeStamps[4]));
            operations.Add(CreateDependencyTelemetry("08", "03", timeStamps[6]));
            operations.Add(CreateDependencyTelemetry("07", "05", timeStamps[5]));
            operations.Add(CreateRequestTelemetry("03", "02", timeStamps[7]));

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
