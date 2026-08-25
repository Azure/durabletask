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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the history scrubbing performed by
    /// <see cref="TaskOrchestrationDispatcher.ProcessRewindOrchestrationDecision"/>.
    /// </summary>
    /// <remarks>
    /// Each test drives a real <see cref="TaskOrchestration"/> through real episodes with
    /// <see cref="TaskOrchestrationExecutor"/> until it fails, rewinds the resulting history, then
    /// replays the rewound history and asserts on the actions the orchestrator produces. A rewound
    /// history that is not replayable surfaces as a fail-orchestration action carrying a
    /// "Non-Deterministic workflow detected" message, because
    /// <see cref="TaskOrchestrationExecutor.ExecuteCore"/> converts
    /// <see cref="Exceptions.NonDeterministicOrchestrationException"/> into that action.
    /// </remarks>
    [TestClass]
    public class RewindTests
    {
        const string FailingActivity = "FailingActivity";

        /// <summary>
        /// Regression test for https://github.com/Azure/azure-functions-durable-extension/issues/444.
        /// The orchestrator catches the activity failure and schedules another activity before
        /// rethrowing. That second activity only exists because the orchestrator observed the
        /// failure, so it must not survive the rewind.
        /// </summary>
        [TestMethod]
        public void Rewind_CatchBlockSchedulesActivity_ProducesReplayableHistory()
        {
            RewindResult result = RunRewindTest(() => new CatchAndScheduleActivityOrchestration());

            AssertReplayable(result);

            // 'Cleanup' was scheduled from the catch block, so neither it nor its result may survive.
            Assert.IsFalse(
                result.RewoundHistory.OfType<TaskScheduledEvent>().Any(e => e.Name == "Cleanup"),
                "The activity scheduled from the catch block should have been removed from the history.");
            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<TaskCompletedEvent>().Count(e => e.TaskScheduledId == 3),
                "The result of the activity scheduled from the catch block should have been removed too.");

            // The two activities that succeeded before the failure are retained, so only the failed
            // activity is rescheduled.
            CollectionAssert.AreEqual(
                new[] { "First", "Second" },
                result.RewoundHistory.OfType<TaskScheduledEvent>().Select(e => e.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);
        }

        /// <summary>
        /// An activity scheduled through <see cref="OrchestrationContext.ScheduleWithRetry{T}(string, string, RetryOptions, object[])"/>
        /// always leaves a delay timer in the history (see <see cref="RetryInterceptor"/>), including
        /// one created after the final failed attempt. Those timers are created only because the
        /// orchestrator observed a failure, so they must not survive the rewind either.
        /// </summary>
        [TestMethod]
        public void Rewind_ScheduleWithRetry_RemovesRetryTimers()
        {
            RewindResult result = RunRewindTest(() => new RetryOrchestration());

            AssertReplayable(result);

            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<TimerCreatedEvent>().Count(),
                "Retry timers should have been removed from the history.");
            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<TimerFiredEvent>().Count(),
                "Retry timer results should have been removed from the history.");
            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<TaskScheduledEvent>().Count(),
                "Every attempt of the failed activity should have been removed from the history.");
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);
        }

        /// <summary>
        /// Same as <see cref="Rewind_CatchBlockSchedulesActivity_ProducesReplayableHistory"/>, but the
        /// catch block creates a sub-orchestration rather than scheduling an activity.
        /// </summary>
        [TestMethod]
        public void Rewind_CatchBlockCreatesSubOrchestration_ProducesReplayableHistory()
        {
            RewindResult result = RunRewindTest(() => new CatchAndCreateSubOrchestrationOrchestration());

            AssertReplayable(result);

            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<SubOrchestrationInstanceCreatedEvent>().Count(),
                "The sub-orchestration created from the catch block should have been removed.");
            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<SubOrchestrationInstanceCompletedEvent>().Count(),
                "The result of the sub-orchestration created from the catch block should have been removed.");
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);

            // No failed sub-orchestration means this is a terminal leaf, so a dummy rewind message is
            // emitted to force the orchestration to rerun.
            Assert.AreEqual(1, result.RewindMessages.Count);
            Assert.IsInstanceOfType(result.RewindMessages[0].Event, typeof(ExecutionRewoundEvent));
        }

        /// <summary>
        /// Same as <see cref="Rewind_CatchBlockSchedulesActivity_ProducesReplayableHistory"/>, but the
        /// catch block sends an external event, which is also matched against the orchestrator's
        /// sequence-ID counter during replay.
        /// </summary>
        [TestMethod]
        public void Rewind_CatchBlockSendsEvent_ProducesReplayableHistory()
        {
            RewindResult result = RunRewindTest(() => new CatchAndSendEventOrchestration());

            AssertReplayable(result);

            Assert.AreEqual(
                0,
                result.RewoundHistory.OfType<EventSentEvent>().Count(),
                "The event sent from the catch block should have been removed from the history.");
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);
        }

        /// <summary>
        /// Control case: an orchestration that does no work in response to the failure must rewind
        /// exactly as it did before this change.
        /// </summary>
        [TestMethod]
        public void Rewind_SimpleActivityFailure_ReschedulesOnlyTheFailedActivity()
        {
            RewindResult result = RunRewindTest(() => new SimpleFailureOrchestration());

            AssertReplayable(result);

            CollectionAssert.AreEqual(
                new[] { "First" },
                result.RewoundHistory.OfType<TaskScheduledEvent>().Select(e => e.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);
        }

        /// <summary>
        /// Fan-out/fan-in: the parallel branches are all scheduled in an episode before the failure is
        /// observed, so they must be retained and only the failed branch rescheduled.
        /// </summary>
        [TestMethod]
        public void Rewind_FanOutFanIn_RetainsSuccessfulBranches()
        {
            RewindResult result = RunRewindTest(() => new FanOutFanInOrchestration());

            AssertReplayable(result);

            CollectionAssert.AreEquivalent(
                new[] { "Branch1", "Branch3" },
                result.RewoundHistory.OfType<TaskScheduledEvent>().Select(e => e.Name).ToArray());
            CollectionAssert.AreEqual(
                new[] { FailingActivity },
                result.ReplayScheduledActivities);
        }

        /// <summary>
        /// A sub-orchestration is always created in an episode before the one that delivers its
        /// failure, so its creation event is retained and a rewind message is emitted for the child.
        /// </summary>
        [TestMethod]
        public void Rewind_FailedSubOrchestration_RetainsCreationAndEmitsChildRewindMessage()
        {
            RewindResult result = RunRewindTest(() => new SubOrchestrationFailureOrchestration());

            AssertReplayable(result);

            SubOrchestrationInstanceCreatedEvent createdEvent =
                result.RewoundHistory.OfType<SubOrchestrationInstanceCreatedEvent>().SingleOrDefault();
            Assert.IsNotNull(createdEvent, "The failed sub-orchestration's creation event should be retained.");
            Assert.AreEqual("ChildInstance", createdEvent.InstanceId);

            Assert.AreEqual(1, result.RewindMessages.Count);
            Assert.AreEqual("ChildInstance", result.RewindMessages[0].OrchestrationInstance.InstanceId);
            Assert.IsInstanceOfType(result.RewindMessages[0].Event, typeof(ExecutionRewoundEvent));

            // The parent replays up to the sub-orchestration and then waits for the rewound child.
            Assert.AreEqual(0, result.ReplayScheduledActivities.Count);
        }

        /// <summary>
        /// The rewound history must always carry a fresh execution ID.
        /// </summary>
        [TestMethod]
        public void Rewind_AssignsNewExecutionId()
        {
            RewindResult result = RunRewindTest(() => new SimpleFailureOrchestration());

            ExecutionStartedEvent executionStartedEvent =
                result.RewoundHistory.OfType<ExecutionStartedEvent>().Single();
            Assert.AreNotEqual(
                InitialExecutionId,
                executionStartedEvent.OrchestrationInstance.ExecutionId,
                "The rewound history should carry a new execution ID.");
            Assert.AreEqual(0, result.RewoundHistory.OfType<ExecutionCompletedEvent>().Count());
        }

        #region Test orchestrations

        class SimpleFailureOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                await context.ScheduleTask<string>("First", string.Empty);
                await context.ScheduleTask<string>(FailingActivity, string.Empty);
                return "done";
            }
        }

        class CatchAndScheduleActivityOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                await context.ScheduleTask<string>("First", string.Empty);
                await context.ScheduleTask<string>("Second", string.Empty);
                try
                {
                    await context.ScheduleTask<string>(FailingActivity, string.Empty);
                }
                catch (Exception)
                {
                    await context.ScheduleTask<string>("Cleanup", string.Empty);
                    throw;
                }

                return "done";
            }
        }

        class CatchAndCreateSubOrchestrationOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                try
                {
                    await context.ScheduleTask<string>(FailingActivity, string.Empty);
                }
                catch (Exception)
                {
                    await context.CreateSubOrchestrationInstance<string>("Notify", string.Empty, "NotifyInstance", null);
                    throw;
                }

                return "done";
            }
        }

        class CatchAndSendEventOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                try
                {
                    await context.ScheduleTask<string>(FailingActivity, string.Empty);
                }
                catch (Exception)
                {
                    context.SendEvent(
                        new OrchestrationInstance { InstanceId = "Listener" },
                        "Failed",
                        "payload");
                    throw;
                }

                return "done";
            }
        }

        class RetryOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var retryOptions = new RetryOptions(TimeSpan.FromSeconds(1), 2);
                return await context.ScheduleWithRetry<string>(FailingActivity, string.Empty, retryOptions);
            }
        }

        class FanOutFanInOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var tasks = new List<Task<string>>
                {
                    context.ScheduleTask<string>("Branch1", string.Empty),
                    context.ScheduleTask<string>(FailingActivity, string.Empty),
                    context.ScheduleTask<string>("Branch3", string.Empty),
                };

                await Task.WhenAll(tasks);
                return "done";
            }
        }

        class SubOrchestrationFailureOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                return await context.CreateSubOrchestrationInstance<string>(
                    FailingActivity,
                    string.Empty,
                    "ChildInstance",
                    null);
            }
        }

        #endregion

        #region Harness

        const string InstanceId = "TestInstance";
        const string InitialExecutionId = "TestExecution";

        class RewindResult
        {
            public List<HistoryEvent> HistoryAtFailure { get; set; }

            public List<HistoryEvent> RewoundHistory { get; set; }

            public List<TaskMessage> RewindMessages { get; set; }

            public IReadOnlyList<OrchestratorAction> ReplayActions { get; set; }

            public List<string> ReplayScheduledActivities { get; set; }

            public string ReplayFailureReason { get; set; }
        }

        static void AssertReplayable(RewindResult result)
        {
            Assert.IsNull(
                result.ReplayFailureReason,
                "Replaying the rewound history should not fail the orchestration. Actual failure: "
                    + result.ReplayFailureReason);
        }

        static RewindResult RunRewindTest(Func<TaskOrchestration> orchestrationFactory)
        {
            List<HistoryEvent> historyAtFailure = RunToFailure(orchestrationFactory());

            var runtimeState = new OrchestrationRuntimeState(historyAtFailure);
            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
            runtimeState.AddEvent(new ExecutionRewoundEvent(-1, "rewind requested"));

            TaskOrchestrationDispatcher.ProcessRewindOrchestrationDecision(
                runtimeState,
                out List<TaskMessage> rewindMessages,
                out OrchestrationRuntimeState rewoundState);

            List<HistoryEvent> rewoundHistory = rewoundState.Events.ToList();

            // The dispatcher persists the rewound history and the orchestration is then picked up by a
            // fresh work item.
            var replayState = new OrchestrationRuntimeState(rewoundHistory);
            replayState.AddEvent(new OrchestratorStartedEvent(-1));

            var executor = new TaskOrchestrationExecutor(
                replayState,
                orchestrationFactory(),
                BehaviorOnContinueAsNew.Carryover);
            OrchestratorExecutionResult replayResult = executor.Execute();

            OrchestrationCompleteOrchestratorAction failureAction = replayResult.Actions
                .OfType<OrchestrationCompleteOrchestratorAction>()
                .FirstOrDefault(a => a.OrchestrationStatus == OrchestrationStatus.Failed);

            return new RewindResult
            {
                HistoryAtFailure = historyAtFailure,
                RewoundHistory = rewoundHistory,
                RewindMessages = rewindMessages,
                ReplayActions = replayResult.Actions.ToList(),
                ReplayScheduledActivities = replayResult.Actions
                    .OfType<ScheduleTaskOrchestratorAction>()
                    .Select(a => a.Name)
                    .ToList(),
                ReplayFailureReason = failureAction == null
                    ? null
                    : failureAction.FailureDetails?.ErrorMessage ?? failureAction.Result,
            };
        }

        /// <summary>
        /// Drives the orchestration through real episodes until it completes, failing every task named
        /// <see cref="FailingActivity"/> and completing everything else.
        /// </summary>
        static List<HistoryEvent> RunToFailure(TaskOrchestration orchestration)
        {
            var instance = new OrchestrationInstance
            {
                InstanceId = InstanceId,
                ExecutionId = InitialExecutionId,
            };

            var history = new List<HistoryEvent>();
            var inbox = new List<HistoryEvent>
            {
                new ExecutionStartedEvent(-1, "\"input\"")
                {
                    OrchestrationInstance = instance,
                    Name = "TestOrchestration",
                    Version = string.Empty,
                },
            };

            for (int episode = 0; episode < 25; episode++)
            {
                var runtimeState = new OrchestrationRuntimeState(history);
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                foreach (HistoryEvent inboundEvent in inbox)
                {
                    runtimeState.AddEvent(inboundEvent);
                }

                inbox = new List<HistoryEvent>();

                var executor = new TaskOrchestrationExecutor(
                    runtimeState,
                    orchestration,
                    BehaviorOnContinueAsNew.Carryover);
                OrchestratorExecutionResult result = executor.Execute();

                bool completed = false;
                foreach (OrchestratorAction action in result.Actions)
                {
                    switch (action)
                    {
                        case ScheduleTaskOrchestratorAction scheduleTask:
                            runtimeState.AddEvent(new TaskScheduledEvent(
                                scheduleTask.Id,
                                scheduleTask.Name,
                                scheduleTask.Version,
                                scheduleTask.Input));
                            inbox.Add(scheduleTask.Name == FailingActivity
                                ? (HistoryEvent)new TaskFailedEvent(-1, scheduleTask.Id, "failure", "details")
                                : new TaskCompletedEvent(-1, scheduleTask.Id, "\"ok\""));
                            break;

                        case CreateSubOrchestrationAction createSubOrchestration:
                            runtimeState.AddEvent(new SubOrchestrationInstanceCreatedEvent(createSubOrchestration.Id)
                            {
                                Name = createSubOrchestration.Name,
                                Version = createSubOrchestration.Version,
                                InstanceId = createSubOrchestration.InstanceId,
                                Input = createSubOrchestration.Input,
                            });
                            inbox.Add(createSubOrchestration.Name == FailingActivity
                                ? (HistoryEvent)new SubOrchestrationInstanceFailedEvent(
                                    -1,
                                    createSubOrchestration.Id,
                                    "failure",
                                    "details")
                                : new SubOrchestrationInstanceCompletedEvent(-1, createSubOrchestration.Id, "\"ok\""));
                            break;

                        case CreateTimerOrchestratorAction createTimer:
                            runtimeState.AddEvent(new TimerCreatedEvent(createTimer.Id)
                            {
                                FireAt = createTimer.FireAt,
                            });
                            inbox.Add(new TimerFiredEvent(-1, createTimer.FireAt)
                            {
                                TimerId = createTimer.Id,
                            });
                            break;

                        case SendEventOrchestratorAction sendEvent:
                            runtimeState.AddEvent(new EventSentEvent(sendEvent.Id)
                            {
                                InstanceId = sendEvent.Instance.InstanceId,
                                Name = sendEvent.EventName,
                                Input = sendEvent.EventData,
                            });
                            break;

                        case OrchestrationCompleteOrchestratorAction complete:
                            runtimeState.AddEvent(new ExecutionCompletedEvent(
                                -1,
                                complete.Result,
                                complete.OrchestrationStatus,
                                complete.FailureDetails));
                            completed = true;
                            break;

                        default:
                            throw new InvalidOperationException(
                                "Unexpected orchestrator action: " + action.GetType().Name);
                    }
                }

                history = runtimeState.Events.ToList();
                if (completed)
                {
                    return history;
                }
            }

            throw new InvalidOperationException("The test orchestration never completed.");
        }

        #endregion
    }
}
