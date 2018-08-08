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
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = runtimeStatus
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
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            var condition = new OrchestrationInstanceStatusQueryCondition {
                RuntimeStatus = runtimeStatus,
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)
            };
             Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and (RuntimeStatus eq 'Running')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

        }
        [TestMethod]
        public void OrchestrationInstanceQuery_NoParameter()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition();
            var query = condition.ToTableQuery<OrchestrationInstanceStatus>();
            Assert.IsNull(query.Expression);
        }

        [TestMethod]
        public void OrchestrationInstance_MultipleRuntimeStatus()
        {
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            runtimeStatus.Add("Completed");
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = runtimeStatus,
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50)
            };
            Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and ((RuntimeStatus eq 'Running') or (RuntimeStatus eq 'Completed'))", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);
        }

        [TestMethod]
        public void OrchestrationInstance_Parse()
        {
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(new DateTime(2018, 1, 10, 10, 10, 10), new DateTime(2018, 1, 10, 10, 10, 50), runtimeStatus);
            Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and (RuntimeStatus eq 'Running')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

        }
    }
}
