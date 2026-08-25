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
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for <see cref="TaskOrchestrationExecutor"/> lifetime management.
    /// </summary>
    /// <remarks>
    /// An orchestrator parks on a <see cref="TaskCompletionSource{TResult}"/> for every activity,
    /// sub-orchestration, and timer it is waiting on, and those tasks are abandoned in a pending state when
    /// the episode ends. When a debugger is attached, the CLR keeps every awaited task in the process-wide
    /// <c>Task.s_currentActiveTasks</c> dictionary until the task completes, so abandoned awaits permanently
    /// root the orchestration object graph. Disposing the executor cancels the open tasks, which lets those
    /// awaiter continuations run and unregister themselves.
    /// Regression coverage for https://github.com/Azure/azure-functions-durable-extension/issues/340.
    /// </remarks>
    [TestClass]
    public class TaskOrchestrationExecutorTests
    {
        const string ActivityName = "SayHello";

        [TestMethod]
        public void Dispose_ResumesAbandonedOrchestratorContinuations()
        {
            var orchestration = new FanOutOrchestration(fanOut: 3);
            using (var executor = CreateExecutor(orchestration))
            {
                executor.Execute();

                Assert.AreEqual(3, orchestration.StartedTaskCount, "The orchestrator should have scheduled 3 activities.");
                Assert.AreEqual(0, orchestration.ReleasedTaskCount, "Abandoned awaits should still be pending at the end of the episode.");
            }

            Assert.AreEqual(
                3,
                orchestration.ReleasedTaskCount,
                "Disposing the executor should resume every abandoned await so its continuation can unregister itself.");
        }

        [TestMethod]
        public void Dispose_WithAsyncDebuggingEnabled_DoesNotLeakActiveTasks()
        {
            // This is the actual bug: with a debugger attached, every abandoned await stays in
            // Task.s_currentActiveTasks forever, which roots the entire orchestration object graph.
            //
            // Task.s_currentActiveTasks is process-wide, so the test host's own async plumbing shows up in
            // it as well. Rather than trying to subtract that noise, this measures how the growth scales
            // with the number of abandoned awaits: a leak is proportional to the fan-out, while unrelated
            // noise is not.
            const int Episodes = 20;
            const int SmallFanOut = 1;
            const int LargeFanOut = 25;

            using (AsyncDebuggingScope.Enable())
            {
                // Warm up so that one-time allocations aren't counted against the measurement.
                RunEpisodes(Episodes, SmallFanOut, dispose: true);

                int leakySensitivity = MeasureFanOutSensitivity(Episodes, SmallFanOut, LargeFanOut, dispose: false);
                Assert.IsTrue(
                    leakySensitivity > Episodes * (LargeFanOut - SmallFanOut),
                    "Undisposed executors are expected to leak one entry per abandoned await; if they no longer " +
                    $"do, this test can no longer detect the regression. Measured {leakySensitivity}.");

                int fixedSensitivity = MeasureFanOutSensitivity(Episodes, SmallFanOut, LargeFanOut, dispose: true);
                Assert.IsTrue(
                    fixedSensitivity * 20 < leakySensitivity,
                    "Disposing the executor must stop Task.s_currentActiveTasks from growing with the number of " +
                    $"abandoned awaits, but growth was still {fixedSensitivity} against {leakySensitivity} when " +
                    "the executors were left undisposed.");
            }
        }

        /// <summary>
        /// Returns how much more the active task table grows for <paramref name="largeFanOut"/> abandoned
        /// awaits per episode than it does for <paramref name="smallFanOut"/>. Constant per-episode overhead
        /// and unrelated test host activity cancel out, leaving only growth caused by abandoned awaits.
        /// </summary>
        static int MeasureFanOutSensitivity(int episodes, int smallFanOut, int largeFanOut, bool dispose)
        {
            int small = RunEpisodes(episodes, smallFanOut, dispose);
            int large = RunEpisodes(episodes, largeFanOut, dispose);
            return large - small;
        }

        static int RunEpisodes(int episodes, int fanOut, bool dispose)
        {
            int before = AsyncDebuggingScope.ActiveTaskCount;
            for (int i = 0; i < episodes; i++)
            {
                TaskOrchestrationExecutor executor = CreateExecutor(new FanOutOrchestration(fanOut));
                executor.Execute();
                if (dispose)
                {
                    executor.Dispose();
                }
            }

            return AsyncDebuggingScope.ActiveTaskCount - before;
        }

        [TestMethod]
        public void Dispose_DoesNotChangeTheDecisionsAlreadyProduced()
        {
            var orchestration = new FanOutOrchestration(fanOut: 3);
            using (var executor = CreateExecutor(orchestration))
            {
                OrchestratorExecutionResult result = executor.Execute();
                List<OrchestratorAction> before = result.Actions.ToList();

                executor.Dispose();

                CollectionAssert.AreEqual(
                    before,
                    result.Actions.ToList(),
                    "Releasing abandoned tasks must not add or remove orchestrator actions.");
            }
        }

        [TestMethod]
        public void Dispose_OrchestratorThatSwallowsCancellation_CannotScheduleMoreWork()
        {
            var orchestration = new SwallowsCancellationOrchestration();
            using (var executor = CreateExecutor(orchestration))
            {
                executor.Execute();
            }

            Assert.IsInstanceOfType(
                orchestration.RescheduleFailure,
                typeof(OperationCanceledException),
                "A released context must refuse to open new tasks, otherwise resumed orchestrator code can leak again.");
        }

        [TestMethod]
        public void Dispose_IsIdempotent()
        {
            var orchestration = new FanOutOrchestration(fanOut: 2);
            var executor = CreateExecutor(orchestration);
            executor.Execute();

            executor.Dispose();
            executor.Dispose();

            Assert.AreEqual(2, orchestration.ReleasedTaskCount, "Repeated disposal should not resume continuations more than once.");
        }

        [TestMethod]
        public void ExtendedSession_OpenTasksSurviveBetweenEpisodes()
        {
            // Extended sessions reuse the executor across episodes, so open tasks must stay pending
            // until their results arrive. Only the end of the session may release them.
            var orchestration = new FanOutOrchestration(fanOut: 1);
            OrchestrationRuntimeState runtimeState = CreateRuntimeState();

            using (var executor = new TaskOrchestrationExecutor(runtimeState, orchestration, BehaviorOnContinueAsNew.Carryover))
            {
                OrchestratorExecutionResult firstEpisode = executor.Execute();
                Assert.AreEqual(1, firstEpisode.Actions.Count(), "The first episode should schedule the activity.");
                Assert.IsFalse(executor.IsCompleted);

                runtimeState.NewEvents.Clear();
                runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
                runtimeState.AddEvent(new TaskCompletedEvent(-1, taskScheduledId: 0, result: JsonDataConverter.Default.Serialize("Hello")));

                OrchestratorExecutionResult secondEpisode = executor.ExecuteNewEvents();

                Assert.IsTrue(executor.IsCompleted, "The activity result should have been delivered to the still-open task.");
                Assert.AreEqual(1, orchestration.ReleasedTaskCount, "The await should have been resumed by its result, not by cancellation.");
                Assert.IsTrue(
                    secondEpisode.Actions.OfType<OrchestrationCompleteOrchestratorAction>().Any(),
                    "The orchestration should have completed on the second episode.");
            }
        }

        static TaskOrchestrationExecutor CreateExecutor(TaskOrchestration orchestration) =>
            new TaskOrchestrationExecutor(CreateRuntimeState(), orchestration, BehaviorOnContinueAsNew.Carryover);

        static OrchestrationRuntimeState CreateRuntimeState()
        {
            var runtimeState = new OrchestrationRuntimeState();
            runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
            runtimeState.AddEvent(new ExecutionStartedEvent(-1, null)
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = Guid.NewGuid().ToString("N"),
                    ExecutionId = Guid.NewGuid().ToString("N"),
                },
                Name = "TestOrchestration",
                Version = string.Empty,
            });

            return runtimeState;
        }

        /// <summary>
        /// Fans out to several activities and then parks, which is the state an orchestrator is in at the
        /// end of a typical episode. Each await records whether it was ever resumed.
        /// </summary>
        class FanOutOrchestration : TaskOrchestration<string, string>
        {
            readonly int fanOut;

            public FanOutOrchestration(int fanOut)
            {
                this.fanOut = fanOut;
            }

            public int StartedTaskCount { get; private set; }

            public int ReleasedTaskCount { get; private set; }

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var tasks = new List<Task<string>>(this.fanOut);
                for (int i = 0; i < this.fanOut; i++)
                {
                    this.StartedTaskCount++;
                    tasks.Add(this.AwaitActivityAsync(context));
                }

                await Task.WhenAll(tasks);
                return string.Empty;
            }

            async Task<string> AwaitActivityAsync(OrchestrationContext context)
            {
                try
                {
                    return await context.ScheduleTask<string>(ActivityName, string.Empty);
                }
                finally
                {
                    this.ReleasedTaskCount++;
                }
            }
        }

        /// <summary>
        /// Mimics orchestrator code with a catch-all handler: it swallows the cancellation raised while the
        /// executor is being released and then tries to schedule more work.
        /// </summary>
        class SwallowsCancellationOrchestration : TaskOrchestration<string, string>
        {
            public Exception RescheduleFailure { get; private set; }

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                try
                {
                    return await context.ScheduleTask<string>(ActivityName, string.Empty);
                }
                catch (Exception)
                {
                    try
                    {
                        return await context.ScheduleTask<string>(ActivityName, string.Empty);
                    }
                    catch (Exception e)
                    {
                        this.RescheduleFailure = e;
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Turns on the CLR's async debugging bookkeeping for the duration of a test, which is what a
        /// attached debugger does, and exposes the size of the tracking dictionary.
        /// </summary>
        sealed class AsyncDebuggingScope : IDisposable
        {
            const BindingFlags StaticFlags = BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public;

            static readonly FieldInfo EnabledField = typeof(Task).GetField("s_asyncDebuggingEnabled", StaticFlags);
            static readonly FieldInfo ActiveTasksField = typeof(Task).GetField("s_currentActiveTasks", StaticFlags);

            readonly bool previousValue;

            AsyncDebuggingScope(bool previousValue)
            {
                this.previousValue = previousValue;
            }

            public static AsyncDebuggingScope Enable()
            {
                if (EnabledField == null || ActiveTasksField == null)
                {
                    Assert.Inconclusive("This runtime does not expose the async debugging state that this test relies on.");
                }

                var scope = new AsyncDebuggingScope((bool)EnabledField.GetValue(null));
                EnabledField.SetValue(null, true);
                return scope;
            }

            /// <summary>
            /// The size of the process-wide <c>Task.s_currentActiveTasks</c> table.
            /// </summary>
            public static int ActiveTaskCount
            {
                get
                {
                    object activeTasks = ActiveTasksField.GetValue(null);
                    if (activeTasks == null)
                    {
                        return 0;
                    }

                    PropertyInfo count = activeTasks.GetType().GetProperty("Count");
                    lock (activeTasks)
                    {
                        return (int)count.GetValue(activeTasks);
                    }
                }
            }

            public void Dispose() => EnabledField.SetValue(null, this.previousValue);
        }
    }
}
