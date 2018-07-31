using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask.AzureStorage.Tracking;
using DurableTask.ServiceBus.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.AzureStorage.Tests
{
    [TestClass]
    public class OrchestrationInstanceStatusQueryBuilderTest
    {
        [TestMethod]
        public void OrchestrationInstanceQuery_RuntimeStatus()
        {
            var queryBuilder = new OrchestrationInstanceStatusQueryBuilder();
            queryBuilder.AddRuntimeStatus("Runnning");
            var query = queryBuilder.Build<OrchestrationInstanceStatus>();
            Assert.AreEqual("RuntimeStatus eq 'Runnning'", query.FilterString);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTime()
        {
            var queryBuilder = new OrchestrationInstanceStatusQueryBuilder();
            var createdTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10);
            var createdTimeTo = new DateTime(2018, 1, 10, 10, 10, 50);

            queryBuilder.AddCreatedTime(createdTimeFrom, createdTimeTo);
            var result = queryBuilder.Build<OrchestrationInstanceStatus>().FilterString;
            Assert.AreEqual("(CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')", queryBuilder.Build<OrchestrationInstanceStatus>().FilterString);

        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTimeVariations()
        {
            var queryBuilder = new OrchestrationInstanceStatusQueryBuilder();
            var createdTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10);
            queryBuilder.AddCreatedTime(createdTimeFrom, default(DateTime));
            Assert.AreEqual("CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z'", queryBuilder.Build<OrchestrationInstanceStatus>().FilterString);

            var createdTimeTo = new DateTime(2018, 1, 10, 10, 10, 50);

            queryBuilder.AddCreatedTime(default(DateTime), createdTimeTo);
            Assert.AreEqual("CreatedTime le datetime'2018-01-10T01:10:50.0000000Z'", queryBuilder.Build<OrchestrationInstanceStatus>().FilterString);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_Combination()
        {
            var queryBuilder = new OrchestrationInstanceStatusQueryBuilder();
            queryBuilder.AddRuntimeStatus("Runnning");
            var createdTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10);
            var createdTimeTo = new DateTime(2018, 1, 10, 10, 10, 50);

            queryBuilder.AddCreatedTime(createdTimeFrom, createdTimeTo);
            var result = queryBuilder.Build<OrchestrationInstanceStatus>().FilterString;
            Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and (RuntimeStatus eq 'Runnning')", queryBuilder.Build<OrchestrationInstanceStatus>().FilterString);

        }

        [TestMethod]
        public void OrchestrationInstanceQuery_LastUpdatedTime()
        {
            var queryBuilder = new OrchestrationInstanceStatusQueryBuilder();
            var lastUpdatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10);
            var lastUpdatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50);
            queryBuilder.AddLastUpdatedTime(lastUpdatedTimeFrom, lastUpdatedTimeTo);
            Assert.AreEqual("(LastUpdatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (LastUpdatedTime le datetime'2018-01-10T01:10:50.0000000Z')", queryBuilder.Build<OrchestrationInstanceStatus>().FilterString);
        }


    }
}
