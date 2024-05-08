using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using DurableTask.Core.History;
using DurableTask.Core.Logging;
using DurableTask.Core.Middleware;
using DurableTask.Emulator;
using DurableTask.Test.Orchestrations;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using static DurableTask.Core.TaskEntityDispatcher;

namespace DurableTask.Core.Tests
{
    [TestClass]
    public class TestTaskEntityDispatcher
    {
        /// <summary>
        /// Utiliy function to create a TaskEntityDispatcher instance. To be expanded upon as per testing needs.
        /// </summary>
        /// <returns></returns>
        private TaskEntityDispatcher GetTaskEntityDispatcher()
        {
            // TODO: these should probably be injectable parameters to this method,
            // initialized with sensible defaults if not provided
            var service = new LocalOrchestrationService();
            ILoggerFactory loggerFactory = null;
            var entityManager = new NameVersionObjectManager<TaskEntity>();
            var entityMiddleware = new DispatchMiddlewarePipeline();
            var logger = new LogHelper(loggerFactory?.CreateLogger("DurableTask.Core"));

            TaskEntityDispatcher dispatcher = new TaskEntityDispatcher(
                service, entityManager, entityMiddleware, logger, ErrorPropagationMode.UseFailureDetails);
            return dispatcher;
        }

        /// <summary>
        /// See: https://github.com/Azure/durabletask/pull/1080
        /// This test is motivated by a regression where Entities
        /// that scheduled sub-orchestrators would incorrectly set
        /// the FireAndForget tag on the ExecutionStartedEvent, causing
        /// them to then receive a SubOrchestrationCompleted event, which
        /// they did not know how to handle. Eventually, this led to them deleting
        /// their own state. This test checks against that case.
        /// </summary>
        [TestMethod]
        public void TestEntityDoesNotSetFireAndForgetTags()
        {
            TaskEntityDispatcher dispatcher = GetTaskEntityDispatcher();

            // Prepare effects
            var effects = new WorkItemEffects();
            effects.taskIdCounter = 0;
            effects.InstanceMessages = new List<TaskMessage>();

            // Prepare runtime state
            var mockEntityStartEvent = new ExecutionStartedEvent(-1, null)
            {
                OrchestrationInstance = new OrchestrationInstance(),
                Name = "testentity",
                Version = "1.0",
            };
            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(mockEntityStartEvent);

            // Prepare action.
            // This mocks starting a new orchestration from an entity.
            var action = new StartNewOrchestrationOperationAction()
            {
                InstanceId = "testsample",
                Name = "test",
                Version = "1.0",
                Input = null,
            };

            // Invoke the dispatcher and obtain resulting event
            dispatcher.ProcessSendStartMessage(effects, runtimeState, action);
            HistoryEvent resultEvent = effects.InstanceMessages[0].Event;

            Assert.IsInstanceOfType(resultEvent, typeof(ExecutionStartedEvent));
            var executionStartedEvent = (ExecutionStartedEvent)resultEvent;

            // The resulting event should contain a fire and forget tag
            bool hasFireAndForgetTag = executionStartedEvent.Tags.ContainsKey(OrchestrationTags.FireAndForget);
            Assert.IsTrue(hasFireAndForgetTag);
        }
    }
}
