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

namespace DurableTask.ServiceBus.Tests
{
    using DurableTask.Core;
    using DurableTask.ServiceBus.Tracking;
    using Azure.Data.Tables;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AzureTableClientTest
    {
        const string ConnectionString = "UseDevelopmentStorage=true";

        [TestMethod]
        public void CreateQueryWithoutFilter()
        {
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();

           var query = tableClient.CreateQueryInternal(stateQuery, false);

            Assert.AreEqual("(PartitionKey eq 'IS')", query);
        }

        [TestMethod]
        public void CreateQueryWithPrimaryFilter()
        {
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();
            stateQuery.AddInstanceFilter("myInstance");

            var query = tableClient.CreateQueryInternal(stateQuery, false);

            Assert.AreEqual("(PartitionKey eq 'IS') and (RowKey ge 'ID_EID_myInstance') and (RowKey lt 'ID_EID_myInstancf')", query);
        }

        [TestMethod]
        public void CreateQueryWithPrimaryAndSecondaryFilter()
        {
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();
            stateQuery.AddInstanceFilter("myInstance");
            stateQuery.AddNameVersionFilter("myName");

            var query = tableClient.CreateQueryInternal(stateQuery, false);

            Assert.AreEqual("(PartitionKey eq 'IS') and (RowKey ge 'ID_EID_myInstance') and (RowKey lt 'ID_EID_myInstancf') and (InstanceId eq 'myInstance') and (Name eq 'myName')", query);
        }

        [TestMethod]
        public void CreateQueryWithInstanceId_SingleQuoteEscaped()
        {
            // Verifies that a single quote in InstanceId is properly escaped,
            // preventing OData filter injection.
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();
            stateQuery.AddInstanceFilter("instance'inject");

            var query = tableClient.CreateQueryInternal(stateQuery, false);

            // The single quote must be doubled in the OData filter
            Assert.IsTrue(query.Contains("instance''inject"), $"Expected escaped quote in query: {query}");
        }

        [TestMethod]
        public void CreateQueryWithInstanceIdAndExecutionId_SingleQuoteEscaped()
        {
            // Verifies that single quotes in InstanceId and ExecutionId are properly escaped,
            // preventing OData filter injection.
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();
            stateQuery.AddInstanceFilter("inst'ance", "exec'ution");

            var query = tableClient.CreateQueryInternal(stateQuery, false);

            // Both values must have their single quotes escaped
            Assert.IsTrue(query.Contains("inst''ance"), $"Expected escaped instance quote in query: {query}");
            Assert.IsTrue(query.Contains("exec''ution"), $"Expected escaped execution quote in query: {query}");
        }

        [TestMethod]
        public void CreateQueryWithNameFilter_SingleQuoteEscaped()
        {
            // Verifies that a single quote in orchestration Name is properly escaped.
            var tableClient = new AzureTableClient("myHub", ConnectionString);
            var stateQuery = new OrchestrationStateQuery();
            stateQuery.AddInstanceFilter("myInstance");
            stateQuery.AddNameVersionFilter("my'Name");

            var query = tableClient.CreateQueryInternal(stateQuery, false);

            Assert.IsTrue(query.Contains("my''Name"), $"Expected escaped name quote in query: {query}");
        }
    }
}
