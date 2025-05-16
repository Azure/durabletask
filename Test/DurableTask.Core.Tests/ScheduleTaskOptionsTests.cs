// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ScheduleTaskOptionsTests
    {
        /// <summary>
        /// Tests that scheduled orchestrations can be created.
        /// </summary>
        [TestMethod]
        public async Task CanCreateScheduledOrchestrations2()
        {
            // create test orchestration service that allows us to inspect the generated HistoryEvents
            TestLocalOrchestrationService service = new TestLocalOrchestrationService();
            TaskHubClient client = new TaskHubClient(service);

            // orchestrator should fire in 5 minutes
            DateTime fireAt = DateTime.UtcNow.AddMinutes(5);

            // invoke client method
            await client.CreateOrchestrationInstanceAsync(
                name: "TestOrchestration", version: "", instanceId: null, input: null, tags: null, dedupeStatuses: null, startAt: fireAt);

            var messages = service.Messages;
            Assert.IsTrue(messages.Count == 1);
            
            TaskMessage startMessage = messages[0];
            HistoryEvent durableEvent = startMessage.Event;
            Assert.IsInstanceOfType(durableEvent, typeof(ExecutionStartedEvent));

            // we expect the generated history event to have a populated ScheduledStartTime with value provided to the client.
            ExecutionStartedEvent startEvent = (ExecutionStartedEvent)durableEvent;
            Assert.IsTrue(startEvent.ScheduledStartTime == fireAt);
        }
    }
} 