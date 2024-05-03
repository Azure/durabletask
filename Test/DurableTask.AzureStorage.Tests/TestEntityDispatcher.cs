using DurableTask.Core;
using DurableTask.Core.Entities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using DurableTask.Core.Logging;
using DurableTask.Core.Entities.OperationFormat;
using System.Text;
using Microsoft.Extensions.Azure;
using DurableTask.Core.History;

namespace DurableTask.AzureStorage.Tests
{
    [TestClass]
    public class TestEntityDispatcher
    {
        readonly string connection = TestHelpers.GetTestStorageAccountConnectionString();

        
        [TestMethod]
        public void TestScheduleOrchestration()
        {
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                StorageConnectionString = this.connection,
                TaskHubName = "EntityScheduleOrch",
            };
            var service = new AzureStorageOrchestrationService(settings);
            var entityTestHelper = new EntityTestHelper(service);
            var testEvent = entityTestHelper.TestScheduleOrchestrationTags();
            
            Assert.IsInstanceOfType(testEvent, typeof(ExecutionStartedEvent));
            var testEventTags = (ExecutionStartedEvent)testEvent;
            bool containsKey = testEventTags.Tags.ContainsKey(OrchestrationTags.FireAndForget);
            Assert.IsTrue(containsKey);
        }

    }
}
