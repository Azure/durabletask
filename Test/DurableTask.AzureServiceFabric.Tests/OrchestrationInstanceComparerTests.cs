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

namespace DurableTask.AzureServiceFabric.Tests
{
    using DurableTask.Core;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class OrchestrationInstanceComparerTests
    {
        [TestMethod]
        public void OrchestrationInstanceComparer_NullInstanceTests()
        {
            Assert.IsTrue(OrchestrationInstanceComparer.Default.Equals(null, null));
            Assert.IsFalse(OrchestrationInstanceComparer.Default.Equals(null, new OrchestrationInstance()));
            Assert.IsFalse(OrchestrationInstanceComparer.Default.Equals(new OrchestrationInstance(), null));
        }

        [TestMethod]
        public void OrchestrationInstanceComparer_SameOrchestrationInstances()
        {
            var first = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
                ExecutionId = "ExecutionId"
            };

            var second = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
                ExecutionId = "ExecutionId"
            };

            Assert.IsTrue(OrchestrationInstanceComparer.Default.Equals(first, first));
            Assert.IsTrue(OrchestrationInstanceComparer.Default.Equals(first, second));
        }

        [TestMethod]
        public void OrchestrationInstanceComparer_DifferentExecutionIdInstances()
        {
            var first = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
                ExecutionId = "ExecutionId1"
            };

            var second = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
                ExecutionId = "ExecutionId2"
            };

            Assert.IsFalse(OrchestrationInstanceComparer.Default.Equals(first, second));
        }

        [TestMethod]
        public void OrchestrationInstanceComparer_DifferentOrchestrationInstances()
        {
            var first = new OrchestrationInstance()
            {
                InstanceId = "InstanceId1",
                ExecutionId = "ExecutionId1"
            };

            var second = new OrchestrationInstance()
            {
                InstanceId = "InstanceId2",
                ExecutionId = "ExecutionId2"
            };

            Assert.IsFalse(OrchestrationInstanceComparer.Default.Equals(first, second));
        }
    }
}
