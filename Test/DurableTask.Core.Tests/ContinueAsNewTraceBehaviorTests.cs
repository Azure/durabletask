// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------
#nullable enable
namespace DurableTask.Core.Tests
{
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Tests for the ContinueAsNew fresh-trace feature.
    ///
    /// Background: Long-running periodic orchestrations that use ContinueAsNew accumulate all
    /// generations into a single distributed trace, making individual cycles hard to observe.
    /// This feature adds an opt-in <see cref="ContinueAsNewTraceBehavior.StartNewTrace"/> option
    /// that starts the next generation in a fresh trace while preserving the default lineage
    /// behavior for existing users.
    ///
    /// The trace identity lifecycle:
    ///   1. Orchestrator calls ContinueAsNew(version, input, options) with StartNewTrace.
    ///   2. Dispatcher creates the next ExecutionStartedEvent with GenerateNewTrace = true
    ///      and does NOT copy the old ParentTraceContext.
    ///   3. TraceHelper.StartTraceActivityForOrchestrationExecution sees GenerateNewTrace,
    ///      creates a fresh root producer span, stores its identity in ParentTraceContext,
    ///      and resets GenerateNewTrace = false.
    ///   4. On subsequent replays, GenerateNewTrace is false and the persisted ParentTraceContext
    ///      identity is used — ensuring stable span identity across replays.
    /// </summary>
    [TestClass]
    public class ContinueAsNewTraceBehaviorTests
    {
        private ActivityListener? listener;

        [TestInitialize]
        public void Setup()
        {
            // Set up an ActivityListener so System.Diagnostics.Activity spans are actually created.
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "DurableTask.Core",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(listener);
        }

        [TestCleanup]
        public void Cleanup()
        {
            DistributedTraceActivity.Current?.Stop();
            DistributedTraceActivity.Current = null;
            listener?.Dispose();
        }

        #region ExecutionStartedEvent.GenerateNewTrace property

        [TestMethod]
        public void GenerateNewTrace_DefaultIsFalse()
        {
            // A new event should default to false so existing behavior is unchanged.
            var evt = new ExecutionStartedEvent(-1, "input");
            Assert.IsFalse(evt.GenerateNewTrace);
        }

        [TestMethod]
        public void GenerateNewTrace_CopyConstructorPreservesValue()
        {
            var original = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            var copy = new ExecutionStartedEvent(original);
            Assert.IsTrue(copy.GenerateNewTrace, "Copy constructor should preserve GenerateNewTrace = true");
        }

        [TestMethod]
        public void GenerateNewTrace_SurvivesJsonSerialization()
        {
            // GenerateNewTrace must survive serialization because the event is persisted
            // to storage and must signal TraceHelper on the first execution.
            var original = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            var serializer = new DataContractJsonSerializer(typeof(ExecutionStartedEvent));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, original);

            stream.Position = 0;
            var deserialized = (ExecutionStartedEvent?)serializer.ReadObject(stream);

            Assert.IsNotNull(deserialized);
            Assert.IsTrue(deserialized.GenerateNewTrace, "GenerateNewTrace should survive JSON round-trip");
        }

        [TestMethod]
        public void GenerateNewTrace_FalseByDefault_BackwardCompatible()
        {
            // An event serialized without GenerateNewTrace should deserialize with false.
            // This simulates loading a pre-upgrade event from storage.
            var currentEvent = new ExecutionStartedEvent(-1, "input")
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            var serializer = new DataContractJsonSerializer(typeof(ExecutionStartedEvent));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, currentEvent);

            string json = Encoding.UTF8.GetString(stream.ToArray())
                .Replace(",\"GenerateNewTrace\":false", string.Empty)
                .Replace("\"GenerateNewTrace\":false,", string.Empty);

            using var oldPayload = new MemoryStream(Encoding.UTF8.GetBytes(json));
            var deserialized = (ExecutionStartedEvent?)serializer.ReadObject(oldPayload);

            Assert.IsNotNull(deserialized);
            Assert.IsFalse(deserialized.GenerateNewTrace, "Pre-upgrade events should default to false");
        }

        #endregion

        #region Tag isolation — GenerateNewTrace does NOT leak through tags

        [TestMethod]
        public void GenerateNewTrace_DoesNotAppearInTags()
        {
            // The property-based approach should never pollute the customer-facing Tags dictionary.
            var evt = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                Tags = new Dictionary<string, string> { { "user-tag", "value" } },
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            Assert.IsFalse(evt.Tags.ContainsKey("MS_CreateTrace"),
                "GenerateNewTrace should use a typed property, not a tag");
            Assert.IsFalse(evt.Tags.ContainsKey("GenerateNewTrace"),
                "GenerateNewTrace should not appear as a tag");
        }

        [TestMethod]
        public void GenerateNewTrace_DoesNotLeakThroughTagCloning()
        {
            // This is the key test for the tag-leak bug found during review.
            // GenerateNewTrace is a property, not a tag, so it cannot leak through tag cloning.
            var genNTags = new Dictionary<string, string> { { "app-tag", "hello" } };

            // Simulate dispatcher creating Gen N+1's event with StartNewTrace
            var genN1Event = new ExecutionStartedEvent(-1, "input")
            {
                Tags = new Dictionary<string, string>(genNTags), // clone of Gen N's tags
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            // Simulate Gen N+1 doing a default ContinueAsNew (no StartNewTrace).
            // Dispatcher clones Gen N+1's tags but sets GenerateNewTrace from the action (false).
            var genN2Tags = new Dictionary<string, string>(genN1Event.Tags);
            var genN2Event = new ExecutionStartedEvent(-1, "input")
            {
                Tags = genN2Tags,
                GenerateNewTrace = false, // from the action, not inherited
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec2" },
                Name = "TestOrch",
            };

            Assert.IsFalse(genN2Event.GenerateNewTrace,
                "GenerateNewTrace must not leak to subsequent generations through tag cloning");
            Assert.AreEqual(1, genN2Event.Tags.Count, "Only application tags should be present");
        }

        #endregion

        #region OrchestrationCompleteOrchestratorAction

        [TestMethod]
        public void Action_ContinueAsNewTraceBehavior_DefaultIsPreserve()
        {
            var action = new OrchestrationCompleteOrchestratorAction();
            Assert.AreEqual(ContinueAsNewTraceBehavior.PreserveTraceContext, action.ContinueAsNewTraceBehavior);
        }

        [TestMethod]
        public void Action_ContinueAsNewTraceBehavior_CanBeSetToStartNewTrace()
        {
            var action = new OrchestrationCompleteOrchestratorAction
            {
                ContinueAsNewTraceBehavior = ContinueAsNewTraceBehavior.StartNewTrace,
            };
            Assert.AreEqual(ContinueAsNewTraceBehavior.StartNewTrace, action.ContinueAsNewTraceBehavior);
        }

        #endregion

        #region TraceHelper — GenerateNewTrace consumption and trace creation

        [TestMethod]
        public void TraceHelper_GenerateNewTrace_CreatesNewRootTrace()
        {
            // When GenerateNewTrace=true and no ParentTraceContext, TraceHelper should create
            // a fresh producer span (new root trace) and then create the orchestration span.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);

            Assert.IsNotNull(activity, "Should create an orchestration activity for fresh trace");
            Assert.IsFalse(startEvent.GenerateNewTrace, "GenerateNewTrace should be reset after consumption");
            Assert.IsNotNull(startEvent.ParentTraceContext, "ParentTraceContext should be set by the producer span");
            Assert.IsNotNull(startEvent.ParentTraceContext.Id, "Durable Id should be stored for replay");
            Assert.IsNotNull(startEvent.ParentTraceContext.SpanId, "Durable SpanId should be stored for replay");

            activity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_GenerateNewTrace_ReplayUsesPersistedIdentity()
        {
            // Simulates: first execution creates a fresh trace and persists identity.
            // Subsequent replay loads the event with GenerateNewTrace=false and persisted
            // Id/SpanId. The orchestration span should restore the same identity.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            // First execution — creates fresh trace
            Activity? firstActivity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);
            Assert.IsNotNull(firstActivity);

            string firstTraceId = firstActivity.TraceId.ToString();
            string firstSpanId = firstActivity.SpanId.ToString();

            firstActivity.Stop();
            DistributedTraceActivity.Current = null;

            // Simulate replay — GenerateNewTrace was reset, Id/SpanId persisted
            Assert.IsFalse(startEvent.GenerateNewTrace);

            Activity? replayActivity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);
            Assert.IsNotNull(replayActivity);
            Assert.AreEqual(firstTraceId, replayActivity.TraceId.ToString(),
                "Replay should use the same trace ID from the persisted identity");
            Assert.AreEqual(firstSpanId, replayActivity.SpanId.ToString(),
                "Replay should use the same span ID from the persisted identity");

            replayActivity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_PreserveTrace_NullParentReturnsNull()
        {
            // Default behavior: GenerateNewTrace=false and no ParentTraceContext → no activity.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = false,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);
            Assert.IsNull(activity, "No activity should be created when there's no parent trace context");
        }

        [TestMethod]
        public void TraceHelper_GenerateNewTrace_ProducesNewTraceId_NotInheritedFromAmbient()
        {
            // Verify the fresh trace gets a genuinely new trace ID, not inherited from ambient.
            using var ambientActivity = new Activity("ambient-parent");
            ambientActivity.SetIdFormat(ActivityIdFormat.W3C);
            ambientActivity.Start();
            string ambientTraceId = ambientActivity.TraceId.ToString();

            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test", ExecutionId = "exec1" },
                Name = "TestOrch",
            };

            // Stop ambient before calling TraceHelper (mirrors real dispatcher behavior
            // where the previous generation's activity is stopped before the next starts)
            ambientActivity.Stop();

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);
            Assert.IsNotNull(activity);
            Assert.AreNotEqual(ambientTraceId, activity.TraceId.ToString(),
                "Fresh trace should NOT inherit the ambient trace ID");

            activity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_LegacyTag_CreatesNewRootTrace()
        {
            // Backward compatibility: when the legacy CreateTraceForNewOrchestration tag is set
            // (as done by durabletask-dotnet's ShimDurableTaskClient), TraceHelper should create
            // a fresh root trace — same behavior as GenerateNewTrace=true.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = false,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test-legacy", ExecutionId = "exec1" },
                Name = "TestOrch",
                Tags = new Dictionary<string, string>
                {
                    [OrchestrationTags.CreateTraceForNewOrchestration] = "true",
                },
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);

            Assert.IsNotNull(activity, "Should create an orchestration activity via legacy tag");
            Assert.IsFalse(startEvent.Tags.ContainsKey(OrchestrationTags.CreateTraceForNewOrchestration),
                "Legacy tag should be consumed (removed) after use");
            Assert.IsNotNull(startEvent.ParentTraceContext, "ParentTraceContext should be set by the producer span");

            activity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_LegacyTag_PreservesOtherTags()
        {
            // Ensure consuming the legacy tag does not affect other user-defined tags.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = false,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test-tags", ExecutionId = "exec1" },
                Name = "TestOrch",
                Tags = new Dictionary<string, string>
                {
                    [OrchestrationTags.CreateTraceForNewOrchestration] = "true",
                    ["user-tag"] = "my-value",
                },
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);

            Assert.IsNotNull(activity);
            Assert.IsFalse(startEvent.Tags.ContainsKey(OrchestrationTags.CreateTraceForNewOrchestration),
                "Legacy tag should be removed");
            Assert.AreEqual("my-value", startEvent.Tags["user-tag"],
                "User tags should be preserved");

            activity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_BothGenerateNewTraceAndLegacyTag_WorksTogether()
        {
            // When both GenerateNewTrace and the legacy tag are set, both should be consumed
            // to prevent the tag from triggering a second fresh trace on replay.
            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test-both", ExecutionId = "exec1" },
                Name = "TestOrch",
                Tags = new Dictionary<string, string>
                {
                    [OrchestrationTags.CreateTraceForNewOrchestration] = "true",
                },
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);

            Assert.IsNotNull(activity, "Should create an orchestration activity");
            Assert.IsFalse(startEvent.GenerateNewTrace, "GenerateNewTrace should be reset");
            Assert.IsFalse(startEvent.Tags.ContainsKey(OrchestrationTags.CreateTraceForNewOrchestration),
                "Legacy tag should also be consumed to prevent double trace on replay");

            activity.Stop();
            DistributedTraceActivity.Current = null;
        }

        [TestMethod]
        public void TraceHelper_DoesNotConsumeFreshTraceSignals_WhenProducerActivityIsSuppressed()
        {
            listener?.Dispose();
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "DurableTask.Core",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.None,
            };
            ActivitySource.AddActivityListener(listener);

            var startEvent = new ExecutionStartedEvent(-1, "input")
            {
                GenerateNewTrace = true,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test-suppressed", ExecutionId = "exec1" },
                Name = "TestOrch",
                Tags = new Dictionary<string, string>
                {
                    [OrchestrationTags.CreateTraceForNewOrchestration] = "true",
                },
            };

            Activity? activity = TraceHelper.StartTraceActivityForOrchestrationExecution(startEvent);

            Assert.IsNull(activity, "No activity should be created when the producer span is suppressed");
            Assert.IsTrue(startEvent.GenerateNewTrace, "GenerateNewTrace should remain for replay");
            Assert.IsTrue(startEvent.Tags.ContainsKey(OrchestrationTags.CreateTraceForNewOrchestration),
                "Legacy trace-creation tag should remain for replay");
            Assert.IsNull(startEvent.ParentTraceContext, "No trace identity should be persisted when no producer span exists");
        }

        #endregion

        #region TaskOrchestrationContext — ContinueAsNew overloads

        [TestMethod]
        public void Context_ContinueAsNew_WithStartNewTrace_SetsTraceBehavior()
        {
            var instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            var context = new TestableTaskOrchestrationContext(instance, TaskScheduler.Default);

            context.ContinueAsNew(null, "test-input", new ContinueAsNewOptions
            {
                TraceBehavior = ContinueAsNewTraceBehavior.StartNewTrace,
            });

            // Verify the pending action has the correct trace behavior
            var actions = context.GetActions();
            Assert.AreEqual(1, actions.Count);
            var completeAction = (OrchestrationCompleteOrchestratorAction)actions[0];
            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, completeAction.OrchestrationStatus);
            Assert.AreEqual(ContinueAsNewTraceBehavior.StartNewTrace, completeAction.ContinueAsNewTraceBehavior);
        }

        [TestMethod]
        public void Context_ContinueAsNew_Default_PreservesTrace()
        {
            var instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            var context = new TestableTaskOrchestrationContext(instance, TaskScheduler.Default);

            context.ContinueAsNew("input");

            var actions = context.GetActions();
            Assert.AreEqual(1, actions.Count);
            var completeAction = (OrchestrationCompleteOrchestratorAction)actions[0];
            Assert.AreEqual(ContinueAsNewTraceBehavior.PreserveTraceContext, completeAction.ContinueAsNewTraceBehavior);
        }

        [TestMethod]
        public void Context_ContinueAsNew_WithVersion_SetsTraceBehavior()
        {
            var instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            var context = new TestableTaskOrchestrationContext(instance, TaskScheduler.Default);

            context.ContinueAsNew("2.0", "test-input", new ContinueAsNewOptions
            {
                TraceBehavior = ContinueAsNewTraceBehavior.StartNewTrace,
            });

            var actions = context.GetActions();
            Assert.AreEqual(1, actions.Count);
            var completeAction = (OrchestrationCompleteOrchestratorAction)actions[0];
            Assert.AreEqual("2.0", completeAction.NewVersion);
            Assert.AreEqual(ContinueAsNewTraceBehavior.StartNewTrace, completeAction.ContinueAsNewTraceBehavior);
        }

        [TestMethod]
        public void Context_ContinueAsNew_LastCallWins()
        {
            // When ContinueAsNew is called multiple times, the last call's options win.
            var instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            var context = new TestableTaskOrchestrationContext(instance, TaskScheduler.Default);

            context.ContinueAsNew(null, "input1", new ContinueAsNewOptions
            {
                TraceBehavior = ContinueAsNewTraceBehavior.StartNewTrace,
            });

            // Second call with default behavior should overwrite
            context.ContinueAsNew(null, "input2", new ContinueAsNewOptions
            {
                TraceBehavior = ContinueAsNewTraceBehavior.PreserveTraceContext,
            });

            var actions = context.GetActions();
            Assert.AreEqual(1, actions.Count);
            var completeAction = (OrchestrationCompleteOrchestratorAction)actions[0];
            Assert.AreEqual(ContinueAsNewTraceBehavior.PreserveTraceContext, completeAction.ContinueAsNewTraceBehavior,
                "Last ContinueAsNew call should win");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Context_ContinueAsNew_NullOptions_Throws()
        {
            var instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            var context = new TestableTaskOrchestrationContext(instance, TaskScheduler.Default);
            context.ContinueAsNew(null, "input", (ContinueAsNewOptions)null!);
        }

        #endregion

        #region Base class — NotSupportedException for unsupported implementations

        [TestMethod]
        [ExpectedException(typeof(NotSupportedException))]
        public void BaseClass_ContinueAsNewWithOptions_ThrowsNotSupported()
        {
            var ctx = new MinimalOrchestrationContext();
            ctx.ContinueAsNew("1.0", "input", new ContinueAsNewOptions());
        }

        #endregion

        #region ContinueAsNewOptions defaults

        [TestMethod]
        public void ContinueAsNewOptions_DefaultTraceBehavior_IsPreserve()
        {
            var options = new ContinueAsNewOptions();
            Assert.AreEqual(ContinueAsNewTraceBehavior.PreserveTraceContext, options.TraceBehavior);
        }

        #endregion

        #region Test helpers

        /// <summary>
        /// A minimal OrchestrationContext subclass that does NOT override the options overload.
        /// Used to verify the base class throws NotSupportedException.
        /// </summary>
        private class MinimalOrchestrationContext : OrchestrationContext
        {
            public override void ContinueAsNew(object input) { }
            public override void ContinueAsNew(string newVersion, object input) { }
            public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId, object input)
                => throw new NotImplementedException();
            public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId, object input, IDictionary<string, string> tags)
                => throw new NotImplementedException();
            public override Task<T> CreateSubOrchestrationInstance<T>(string name, string version, object input)
                => throw new NotImplementedException();
            public override Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters)
                => throw new NotImplementedException();
            public override Task<T> CreateTimer<T>(DateTime fireAt, T state)
                => throw new NotImplementedException();
            public override Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
                => throw new NotImplementedException();
            public override void SendEvent(OrchestrationInstance orchestrationInstance, string eventName, object eventData)
                => throw new NotImplementedException();
        }

        /// <summary>
        /// A testable TaskOrchestrationContext that exposes the pending actions.
        /// ContinueAsNew is stored internally until CompleteOrchestration is called,
        /// at which point it becomes visible through OrchestratorActions.
        /// </summary>
        private class TestableTaskOrchestrationContext : TaskOrchestrationContext
        {
            public TestableTaskOrchestrationContext(OrchestrationInstance instance, TaskScheduler scheduler)
                : base(instance, scheduler)
            {
                CurrentUtcDateTime = DateTime.UtcNow;
            }

            public IReadOnlyList<OrchestratorAction> GetActions()
            {
                // Trigger the completion path that moves continueAsNew into the actions map
                CompleteOrchestration("result", null, OrchestrationStatus.Completed);
                return OrchestratorActions.ToList();
            }

            public override Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters)
                => base.ScheduleTask<TResult>(name, version, parameters);

            public override Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
                => Task.FromResult(state);
        }

        #endregion
    }
}
