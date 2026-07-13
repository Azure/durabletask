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
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AzureTableQueryFilterTests
    {
        // PartitionKeyEquals applies KeySanitation.EscapePartitionKey (storage-key characters) and then
        // OData quote-escaping (single quotes doubled).
        [DataTestMethod]
        [DataRow("instance1", "PartitionKey eq 'instance1'")]
        [DataRow("inst'ance", "PartitionKey eq 'inst''ance'")]
        [DataRow("in#st'ance", "PartitionKey eq 'in^2st''ance'")]
        public void PartitionKeyEquals(string instanceId, string expectedFilter)
        {
            Assert.AreEqual(expectedFilter, AzureTableQueryFilter.PartitionKeyEquals(instanceId));
        }

        // ColumnEquals OData-escapes the value (single quotes doubled); the column name is literal text.
        [DataTestMethod]
        [DataRow("ExecutionId", "abc", "ExecutionId eq 'abc'")]
        [DataRow("ExecutionId", "a'b", "ExecutionId eq 'a''b'")]
        [DataRow("RowKey", "", "RowKey eq ''")]
        public void ColumnEquals(string columnName, string value, string expectedFilter)
        {
            Assert.AreEqual(expectedFilter, AzureTableQueryFilter.ColumnEquals(columnName, value));
        }

        [DataTestMethod]
        [DataRow("prefix", "PartitionKey ge 'prefix'")]
        [DataRow("pre'fix", "PartitionKey ge 'pre''fix'")]
        public void PartitionKeyGreaterOrEqual(string sanitizedPartitionKey, string expectedFilter)
        {
            Assert.AreEqual(expectedFilter, AzureTableQueryFilter.PartitionKeyGreaterOrEqual(sanitizedPartitionKey));
        }

        [DataTestMethod]
        [DataRow("prefix", "PartitionKey lt 'prefix'")]
        [DataRow("pre'fix", "PartitionKey lt 'pre''fix'")]
        public void PartitionKeyLessThan(string sanitizedPartitionKey, string expectedFilter)
        {
            Assert.AreEqual(expectedFilter, AzureTableQueryFilter.PartitionKeyLessThan(sanitizedPartitionKey));
        }
    }
}
