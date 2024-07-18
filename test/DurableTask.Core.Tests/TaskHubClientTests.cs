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

#nullable enable
namespace DurableTask.Core.Tests
{
    using DurableTask.Core.History;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// A test implementation of <see cref="LocalOrchestrationService"/> that captures messages for later inspection.
    /// </summary>
    public class TestLocalOrchestrationService : LocalOrchestrationService
    {
        public List<TaskMessage> Messages { get; } = new List<TaskMessage>();

        public override Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            // capture message for later inspection
            this.Messages.Add(creationMessage);

            // call base implementation
            return base.CreateTaskOrchestrationAsync(creationMessage, dedupeStatuses);
        }
    }

    [TestClass]
    public class TaskHubClientTests
    {
        /// <summary>
        /// Tests that scheduled orchestrations can be created.
        /// </summary>
        [TestMethod]
        public async Task CanCreateScheduledOrchestrations()
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
