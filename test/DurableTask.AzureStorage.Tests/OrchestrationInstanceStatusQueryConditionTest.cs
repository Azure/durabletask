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
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationInstanceStatusQueryConditionTest
    {
        [TestMethod]
        public void OrchestrationInstanceQuery_RuntimeStatus()
        {
            var runtimeStatus = new OrchestrationStatus[] { OrchestrationStatus.Running };
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = runtimeStatus
            };

            Assert.AreEqual("RuntimeStatus eq 'Running'", condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTime()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50, DateTimeKind.Utc)
            };

            Assert.AreEqual(
                "(CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T10:10:50.0000000Z')",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTimeOnly()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc),
                CreatedTimeTo = default(DateTime),
                RuntimeStatus = new List<OrchestrationStatus>(),
            };

            Assert.AreEqual(
                "CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z'",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_CreatedTimeVariations()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc)
            };

            Assert.AreEqual(
                "CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z'",
                condition.ToOData().Filter);

            condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50, DateTimeKind.Utc)
            };

            Assert.AreEqual(
                "CreatedTime le datetime'2018-01-10T10:10:50.0000000Z'",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_Combination()
        {
            var runtimeStatus = new OrchestrationStatus[] { OrchestrationStatus.Running };
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = runtimeStatus,
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50, DateTimeKind.Utc)
            };

            Assert.AreEqual(
                "(CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T10:10:50.0000000Z') and (RuntimeStatus eq 'Running')",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_NoParameter()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition();
            Assert.IsTrue(string.IsNullOrWhiteSpace(condition.ToOData().Filter));
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_MultipleRuntimeStatus()
        {
            var runtimeStatus = new OrchestrationStatus[] { OrchestrationStatus.Running , OrchestrationStatus.Completed };
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = runtimeStatus,
                CreatedTimeFrom = new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc),
                CreatedTimeTo = new DateTime(2018, 1, 10, 10, 10, 50, DateTimeKind.Utc),
                TaskHubNames = new string[] {"FooProduction", "BarStaging"}
            };

            Assert.AreEqual(
                "(CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T10:10:50.0000000Z') and (RuntimeStatus eq 'Running' or RuntimeStatus eq 'Completed') and (TaskHubName eq 'FooProduction' or TaskHubName eq 'BarStaging')",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_WithAppId()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition()
            {
                TaskHubNames = new string[] { "FooProduction" }
            };
            Assert.AreEqual("TaskHubName eq 'FooProduction'",
                condition.ToOData().Filter
            );
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_Parse()
        {
            var runtimeStatus = new List<OrchestrationStatus>();
            runtimeStatus.Add(OrchestrationStatus.Running);
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(
                new DateTime(2018, 1, 10, 10, 10, 10, DateTimeKind.Utc),
                new DateTime(2018, 1, 10, 10, 10, 50, DateTimeKind.Utc),
                runtimeStatus);

            Assert.AreEqual(
                "(CreatedTime ge datetime'2018-01-10T10:10:10.0000000Z') and (CreatedTime le datetime'2018-01-10T10:10:50.0000000Z') and (RuntimeStatus eq 'Running')",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_ParseOptional()
        {
            var runtimeStatus = new List<OrchestrationStatus>();
            runtimeStatus.Add(OrchestrationStatus.Running);
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(default(DateTime), null, runtimeStatus);
            Assert.AreEqual("RuntimeStatus eq 'Running'", condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_InstanceIdPrefix()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = "aaab",
            };

            Assert.AreEqual(
                "(PartitionKey ge 'aaab') and (PartitionKey lt 'aaac')",
                condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_InstanceId()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceId = "abc123",
            };

            Assert.AreEqual("PartitionKey eq 'abc123'", condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_EmptyInstanceId()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceId = "", // This is technically legal
            };

            string result = condition.ToOData().Filter;
            Assert.AreEqual("PartitionKey eq ''", result);
        }
    }
}
