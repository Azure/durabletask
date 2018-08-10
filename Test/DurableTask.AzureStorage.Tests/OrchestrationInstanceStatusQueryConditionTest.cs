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
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        public void OrchestrationInstanceQuery_CreatedTimeOnly()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10),
                CreatedTimeTo = default(DateTime),
                RuntimeStatus = new List<string>(),
            };

            var result = condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString;
            Assert.AreEqual("CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z'", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

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
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
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
        public void OrchestrationInstanceQuery_MultipleRuntimeStatus()
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
        public void OrchestrationInstanceQuery_Parse()
        {
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(new DateTime(2018, 1, 10, 10, 10, 10), new DateTime(2018, 1, 10, 10, 10, 50), runtimeStatus);
            Assert.AreEqual("((CreatedTime ge datetime'2018-01-10T01:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T01:10:50.0000000Z')) and (RuntimeStatus eq 'Running')", condition.ToTableQuery<OrchestrationInstanceStatus>().FilterString);

        }

        [TestMethod]
        public void OrchestrationInstanceQuery_ParseOptional()
        {
            var runtimeStatus = new List<string>();
            runtimeStatus.Add("Running");
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(default(DateTime), null, runtimeStatus);
            var query = condition.ToTableQuery<OrchestrationInstanceStatus>();
            Assert.AreEqual("RuntimeStatus eq 'Running'", query.FilterString);
        }
    }
}
