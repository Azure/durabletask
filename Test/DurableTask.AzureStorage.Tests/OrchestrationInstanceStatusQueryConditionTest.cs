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
    public class OrchestrationInstanceStatusQueryConditionTest
    {
        [TestMethod]
        public void OrchestrationInstanceQuery_RuntimeStatus()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = "Running"
            };

            var query = condition.ToTableQuery<OrchestrationInstanceStatus>();
            Assert.AreEqual("RuntimeStatus eq 'Running'", query.FilterString);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTime()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)

            };

            var result = condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString;
            Assert.AreEqual("(CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTimeVariations()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10)
            };
            Assert.AreEqual("CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z'", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

            condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)
            };
            Assert.AreEqual("CreatedTime le datetime'2018-01-10T01:10:50.0000000Z'", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_Combination()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition {
                RuntimeStatus = "Running",
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)
            };
             Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and (RuntimeStatus eq 'Running')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

        }

        [TestMethod]
        public void OrchestrationInstanceQuery_LastUpdatedTime()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                LastUpdatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                LastUpdatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)
            };

            Assert.AreEqual("(LastUpdatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (LastUpdatedTime le datetime'2018-01-10T01:10:50.0000000Z')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);
        }


    }
}
