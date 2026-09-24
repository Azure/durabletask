// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Settings;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public sealed class ContinueAsNewTerminationTests
    {
        const string TerminationReason = "Stop the orchestration";
        static readonly DateTime StartTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        static readonly DateTime FireAt = StartTime.AddMinutes(1);

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public void Execute_TerminationAndTimerInSameBatch_OnlyTerminates(bool terminationFirst, bool extendedSession)
        {
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();
            var executor = new TaskOrchestrationExecutor(
                runtimeState, new TimerOrchestration(), BehaviorOnContinueAsNew.Carryover);

            if (extendedSession)
            {
                Assert.AreEqual(0, executor.Execute().Actions.Count());
            }

            runtimeState.AddEvent(new OrchestratorStartedEvent(-1) { Timestamp = FireAt });
            foreach (HistoryEvent historyEvent in CreateTerminationBatch(terminationFirst))
            {
                runtimeState.AddEvent(historyEvent);
            }

            OrchestratorExecutionResult result = extendedSession ? executor.ExecuteNewEvents() : executor.Execute();
            OrchestrationCompleteOrchestratorAction completion = AssertSingleCompletion(result, OrchestrationStatus.Terminated);

            Assert.AreEqual(TerminationReason, completion.Result);
            Assert.IsFalse(result.Actions.OfType<OrchestrationCompleteOrchestratorAction>()
                .Any(action => action.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew));
        }

        [TestMethod]
        public void Execute_TerminationWhileTimerIsPending_OnlyTerminates()
        {
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();
            runtimeState.AddEvent(new ExecutionTerminatedEvent(-1, TerminationReason));
            var executor = new TaskOrchestrationExecutor(
                runtimeState, new TimerOrchestration(), BehaviorOnContinueAsNew.Carryover);

            OrchestrationCompleteOrchestratorAction completion = AssertSingleCompletion(
                executor.Execute(), OrchestrationStatus.Terminated);

            Assert.AreEqual(TerminationReason, completion.Result);
            Assert.IsFalse(executor.IsCompleted, "Termination must not require the pending timer to fire.");
        }

        [DataTestMethod]
        [DataRow(true, false, OrchestrationStatus.ContinuedAsNew)]
        [DataRow(false, false, OrchestrationStatus.Completed)]
        [DataRow(true, true, OrchestrationStatus.Failed)]
        public void Execute_TimerWithoutTermination_PreservesCompletion(
            bool continueAsNew, bool fail, OrchestrationStatus expectedStatus)
        {
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();
            runtimeState.AddEvent(new OrchestratorStartedEvent(-1) { Timestamp = FireAt });
            runtimeState.AddEvent(new TimerFiredEvent(-1, FireAt) { TimerId = 0 });
            var executor = new TaskOrchestrationExecutor(
                runtimeState, new TimerOrchestration(continueAsNew, fail),
                BehaviorOnContinueAsNew.Carryover, ErrorPropagationMode.UseFailureDetails);

            OrchestrationCompleteOrchestratorAction completion = AssertSingleCompletion(executor.Execute(), expectedStatus);

            if (expectedStatus == OrchestrationStatus.ContinuedAsNew)
            {
                Assert.AreEqual("1", completion.Result);
            }
            else if (expectedStatus == OrchestrationStatus.Completed)
            {
                Assert.AreEqual("\"completed\"", completion.Result);
            }
            else
            {
                Assert.AreEqual("Failure after ContinueAsNew", completion.FailureDetails.ErrorMessage);
                Assert.IsTrue(completion.Tags.ContainsKey(OrchestrationTags.CompleteOrchestrationLogWarning));
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task Dispatch_TerminationAndTimerInSameBatch_DoesNotStartNewExecution(bool terminationFirst)
        {
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();
            var orchestration = new TimerOrchestration();
            using var service = new CapturingOrchestrationService();
            var dispatcher = new TestOrchestrationDispatcher(service, orchestration);
            TaskOrchestrationWorkItem workItem = CreateWorkItem(runtimeState, CreateTerminationBatch(terminationFirst));

            bool completed = await dispatcher.ProcessAsync(workItem);

            Assert.AreEqual(1, orchestration.ExecutionCount, "Termination must not execute a new generation.");
            Assert.IsTrue(completed);
            Assert.AreSame(runtimeState, service.RuntimeState, "Termination must not replace the runtime state.");
            Assert.AreEqual(runtimeState.OrchestrationInstance.ExecutionId, service.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(OrchestrationStatus.Terminated, service.State.OrchestrationStatus);
            Assert.AreEqual(TerminationReason, service.State.Output);
            Assert.AreEqual(1, service.RuntimeState.Events.OfType<ExecutionCompletedEvent>().Count());
            Assert.AreEqual(OrchestrationStatus.Terminated, service.RuntimeState.ExecutionCompletedEvent.OrchestrationStatus);
            Assert.IsFalse(service.RuntimeState.Events.OfType<ContinueAsNewEvent>().Any());
            Assert.AreEqual(0, service.Messages.Count);
            Assert.IsNull(service.ContinuedAsNewMessage);
        }

        [TestMethod]
        public async Task Dispatch_TimerWithoutTermination_StartsNewExecution()
        {
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();
            var orchestration = new TimerOrchestration();
            using var service = new CapturingOrchestrationService();
            var dispatcher = new TestOrchestrationDispatcher(service, orchestration);
            TaskOrchestrationWorkItem workItem = CreateWorkItem(
                runtimeState, new HistoryEvent[] { new TimerFiredEvent(-1, FireAt) { TimerId = 0 } });

            bool completed = await dispatcher.ProcessAsync(workItem);

            Assert.IsTrue(completed);
            Assert.AreEqual(2, orchestration.ExecutionCount);
            Assert.AreNotSame(runtimeState, service.RuntimeState);
            Assert.AreNotEqual(runtimeState.OrchestrationInstance.ExecutionId, service.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, runtimeState.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, service.State.OrchestrationStatus);
            Assert.AreEqual("\"completed\"", service.State.Output);
        }

        static OrchestrationCompleteOrchestratorAction AssertSingleCompletion(
            OrchestratorExecutionResult result, OrchestrationStatus expectedStatus)
        {
            OrchestratorAction[] actions = result.Actions.ToArray();
            Assert.AreEqual(1, actions.Length, "Expected one authoritative completion action.");
            Assert.IsInstanceOfType(actions[0], typeof(OrchestrationCompleteOrchestratorAction));
            var completion = (OrchestrationCompleteOrchestratorAction)actions[0];
            Assert.AreEqual(expectedStatus, completion.OrchestrationStatus);
            return completion;
        }

        static OrchestrationRuntimeState CreateRuntimeState()
        {
            return new OrchestrationRuntimeState(new HistoryEvent[]
            {
                new OrchestratorStartedEvent(-1) { Timestamp = StartTime },
                new ExecutionStartedEvent(-1, "0")
                {
                    Name = nameof(TimerOrchestration),
                    Version = "",
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = "termination-race",
                        ExecutionId = "original-execution",
                    },
                    Timestamp = StartTime,
                },
                new TimerCreatedEvent(0, FireAt),
                new OrchestratorCompletedEvent(-1),
            });
        }

        static IEnumerable<HistoryEvent> CreateTerminationBatch(bool terminationFirst)
        {
            var timer = new TimerFiredEvent(-1, FireAt) { TimerId = 0 };
            var termination = new ExecutionTerminatedEvent(-1, TerminationReason);
            return terminationFirst
                ? new HistoryEvent[] { termination, timer }
                : new HistoryEvent[] { timer, termination };
        }

        static TaskOrchestrationWorkItem CreateWorkItem(
            OrchestrationRuntimeState runtimeState, IEnumerable<HistoryEvent> events)
        {
            return new TaskOrchestrationWorkItem
            {
                InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = DateTime.MaxValue,
                NewMessages = events.Select(historyEvent => new TaskMessage
                {
                    OrchestrationInstance = runtimeState.OrchestrationInstance,
                    Event = historyEvent,
                }).ToList(),
            };
        }

        sealed class TimerOrchestration : TaskOrchestration<string, int>
        {
            readonly bool continueAsNew;
            readonly bool fail;

            public TimerOrchestration(bool continueAsNew = true, bool fail = false)
            {
                this.continueAsNew = continueAsNew;
                this.fail = fail;
            }

            public int ExecutionCount { get; private set; }

            public override async Task<string> RunTask(OrchestrationContext context, int input)
            {
                this.ExecutionCount++;
                if (input == 0)
                {
                    await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(1), true);
                    if (this.continueAsNew)
                    {
                        context.ContinueAsNew(input + 1);
                    }

                    if (this.fail)
                    {
                        throw new InvalidOperationException("Failure after ContinueAsNew");
                    }
                }

                return "completed";
            }
        }

        sealed class TestOrchestrationDispatcher : TaskOrchestrationDispatcher
        {
            public TestOrchestrationDispatcher(IOrchestrationService service, TaskOrchestration orchestration)
                : base(service, CreateObjectManager(orchestration), new DispatchMiddlewarePipeline(),
                    new LogHelper(null), ErrorPropagationMode.UseFailureDetails, new VersioningSettings(), null)
            {
            }

            public Task<bool> ProcessAsync(TaskOrchestrationWorkItem workItem) => this.OnProcessWorkItemAsync(workItem);

            static NameVersionObjectManager<TaskOrchestration> CreateObjectManager(TaskOrchestration orchestration)
            {
                var manager = new NameVersionObjectManager<TaskOrchestration>();
                manager.Add(new TestObjectCreator<TaskOrchestration>(nameof(TimerOrchestration), "", () => orchestration));
                return manager;
            }
        }

        sealed class CapturingOrchestrationService : LocalOrchestrationService, IOrchestrationService
        {
            public OrchestrationRuntimeState RuntimeState { get; private set; }
            public OrchestrationState State { get; private set; }
            public IList<TaskMessage> Messages { get; private set; }
            public TaskMessage ContinuedAsNewMessage { get; private set; }

            Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
                TaskOrchestrationWorkItem workItem,
                OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage> outboundMessages,
                IList<TaskMessage> orchestratorMessages,
                IList<TaskMessage> timerMessages,
                TaskMessage continuedAsNewMessage,
                OrchestrationState state)
            {
                this.RuntimeState = newOrchestrationRuntimeState;
                this.State = state;
                this.Messages = outboundMessages.Concat(orchestratorMessages).Concat(timerMessages).ToList();
                this.ContinuedAsNewMessage = continuedAsNewMessage;
                return Task.CompletedTask;
            }
        }
    }
}
