using DurableTask.Core.Entities;
using DurableTask.Core.History;
using DurableTask.Emulator;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.Core.Tests
{
    [TestClass]
    public class TestTaskEntityDispatcher
    {
        [TestMethod]
        public void TestScheduleOrchestration()
        {
           var service = new LocalOrchestrationService();

            var entityTestHelper = new EntityTestHelper(service);
            var testEvent = entityTestHelper.TestScheduleOrchestrationTags();

            Assert.IsInstanceOfType(testEvent, typeof(ExecutionStartedEvent));
            var testEventTags = (ExecutionStartedEvent)testEvent;
            bool containsKey = testEventTags.Tags.ContainsKey(OrchestrationTags.FireAndForget);
            Assert.IsTrue(containsKey);
        }
    }
}
