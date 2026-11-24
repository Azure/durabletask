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
            Assert.IsFalse(condition.ExcludeEntities);
            Assert.IsTrue(string.IsNullOrWhiteSpace(condition.ToOData().Filter));
        }

        /// <summary>
        /// When inputs and outputs are excluded, the query switches from "select everything" to an explicit
        /// column projection. ParentInstanceId must stay in that projection, otherwise status queries that
        /// omit inputs/outputs would silently return a null ParentInstance.
        /// </summary>
        [TestMethod]
        public void OrchestrationInstanceQuery_ProjectionRetainsParentInstanceId()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                RuntimeStatus = new OrchestrationStatus[] { OrchestrationStatus.Running },
                FetchInput = false,
                FetchOutput = false,
            };

            IEnumerable<string> select = condition.ToOData().Select;

            Assert.IsNotNull(select, "Excluding input and output should produce an explicit projection.");
            CollectionAssert.Contains(select.ToList(), nameof(OrchestrationInstanceStatus.ParentInstanceId));
            CollectionAssert.DoesNotContain(select.ToList(), nameof(OrchestrationInstanceStatus.Input));
            CollectionAssert.DoesNotContain(select.ToList(), nameof(OrchestrationInstanceStatus.Output));
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

        [DataTestMethod]
        [DataRow("aaab", false, "(PartitionKey ge 'aaab') and (PartitionKey lt 'aaac')")]
        [DataRow("aaab", true, "(PartitionKey ge 'aaab') and (PartitionKey lt 'aaac') and (PartitionKey lt '@' or PartitionKey ge 'A')")]
        [DataRow("@", false, "(PartitionKey ge '@') and (PartitionKey lt 'A')")]
        [DataRow("@", true, "(PartitionKey ge '@') and (PartitionKey lt 'A') and (PartitionKey lt '@' or PartitionKey ge 'A')")]
        [DataRow("@counter@", false, "(PartitionKey ge '@counter@') and (PartitionKey lt '@counterA')")]
        [DataRow("@counter@", true, "(PartitionKey ge '@counter@') and (PartitionKey lt '@counterA') and (PartitionKey lt '@' or PartitionKey ge 'A')")]
        [DataRow("order@", false, "(PartitionKey ge 'order@') and (PartitionKey lt 'orderA')")]
        [DataRow("order@", true, "(PartitionKey ge 'order@') and (PartitionKey lt 'orderA') and (PartitionKey lt '@' or PartitionKey ge 'A')")]
        public void OrchestrationInstanceQuery_InstanceIdPrefix(string prefix, bool excludeEntities, string expectedFilter)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = prefix,
                ExcludeEntities = excludeEntities,
            };

            Assert.AreEqual(expectedFilter, condition.ToOData().Filter);
        }

        [DataTestMethod]
        [DataRow(null, false, null)]
        [DataRow("", false, null)]
        [DataRow(null, true, "PartitionKey lt '@' or PartitionKey ge 'A'")]
        [DataRow("", true, "PartitionKey lt '@' or PartitionKey ge 'A'")]
        public void OrchestrationInstanceQuery_ExcludeEntitiesWithoutPrefix(string prefix, bool excludeEntities, string expectedFilter)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = prefix,
                ExcludeEntities = excludeEntities,
            };

            Assert.AreEqual(expectedFilter, condition.ToOData().Filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_ExcludeEntitiesWithPrefixAndRuntimeStatus()
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = "@counter@",
                ExcludeEntities = true,
                RuntimeStatus = new[] { OrchestrationStatus.Running, OrchestrationStatus.Completed },
            };

            Assert.AreEqual(
                "(RuntimeStatus eq 'Running' or RuntimeStatus eq 'Completed') and (PartitionKey ge '@counter@') and (PartitionKey lt '@counterA') and (PartitionKey lt '@' or PartitionKey ge 'A')",
                condition.ToOData().Filter);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void OrchestrationInstanceQuery_ExcludeEntitiesWithEscapedPrefix(bool excludeEntities)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = "prefix'/\\#?^x",
                ExcludeEntities = excludeEntities,
            };

            string expectedFilter = "(PartitionKey ge 'prefix''^0^1^2^3^^x') and (PartitionKey lt 'prefix''^0^1^2^3^^y')";
            if (excludeEntities)
            {
                expectedFilter += " and (PartitionKey lt '@' or PartitionKey ge 'A')";
            }

            Assert.AreEqual(expectedFilter, condition.ToOData().Filter);
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

        [TestMethod]
        public void OrchestrationInstanceQuery_TaskHubName_SingleQuoteEscaped()
        {
            // Verifies that a single quote in TaskHubName is properly escaped,
            // preventing OData filter injection.
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                TaskHubNames = new string[] { "Hub'Injected" }
            };

            string filter = condition.ToOData().Filter;
            Assert.AreEqual("TaskHubName eq 'Hub''Injected'", filter);
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_InstanceIdPrefix_SingleQuoteEscaped()
        {
            // Verifies that a single quote in InstanceIdPrefix is properly escaped,
            // preventing OData filter injection.
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceIdPrefix = "prefix'inject",
            };

            string filter = condition.ToOData().Filter;
            // The prefix goes through EscapePartitionKey (which doesn't touch quotes)
            // then through CreateQueryFilter (which escapes the quote for OData).
            Assert.IsTrue(filter.Contains("prefix''inject"), $"Expected escaped quote in filter: {filter}");
            Assert.IsTrue(filter.Contains("PartitionKey ge"), $"Expected ge condition in filter: {filter}");
            Assert.IsTrue(filter.Contains("PartitionKey lt"), $"Expected lt condition in filter: {filter}");
        }

        [TestMethod]
        public void OrchestrationInstanceQuery_InstanceId_SingleQuoteEscaped()
        {
            // Verifies that a single quote in InstanceId is properly escaped,
            // preventing OData filter injection.
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceId = "instance'id",
            };

            string filter = condition.ToOData().Filter;
            Assert.AreEqual("PartitionKey eq 'instance''id'", filter);
        }
    }
}
