// Copyright (c) Microsoft Corporation. All rights reserved.

#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class RetryMetadataTests
    {
        [DataTestMethod]
        [DataRow(0, 3)]
        [DataRow(-1, 3)]
        [DataRow(1, 0)]
        [DataRow(1, -1)]
        [DataRow(4, 3)]
        public void ConstructorRejectsInvalidAttempts(int attempt, int max)
        {
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => new RetryMetadata(attempt, max, "group"));
        }

        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow(" ")]
        public void ConstructorRejectsMissingGroup(string group)
        {
            Assert.ThrowsException<ArgumentException>(() => new RetryMetadata(1, 3, group));
        }

        [DataTestMethod]
        [DataRow(1, 1)]
        [DataRow(int.MaxValue, int.MaxValue)]
        public void MetadataRoundTripsAcrossSerializers(int attempt, int max)
        {
            var metadata = new RetryMetadata(attempt, max, "execution:42");
            string json = JsonConvert.SerializeObject(metadata);
            AssertMetadata(metadata, JsonConvert.DeserializeObject<RetryMetadata>(json));
            AssertMetadata(metadata, System.Text.Json.JsonSerializer.Deserialize<RetryMetadata>(json));
            AssertMetadata(metadata, JsonDataConverter.Default.Deserialize<RetryMetadata>(JsonDataConverter.Default.Serialize(metadata)));

            var serializer = new DataContractSerializer(typeof(RetryMetadata));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, metadata);
            stream.Position = 0;
            AssertMetadata(metadata, (RetryMetadata)serializer.ReadObject(stream)!);
        }

        [DataTestMethod]
        [DataRow("{}", typeof(ArgumentOutOfRangeException))]
        [DataRow("{\"AttemptNumber\":0,\"MaxAttempts\":3,\"RetryGroupId\":\"g\"}", typeof(ArgumentOutOfRangeException))]
        [DataRow("{\"AttemptNumber\":4,\"MaxAttempts\":3,\"RetryGroupId\":\"g\"}", typeof(ArgumentOutOfRangeException))]
        [DataRow("{\"AttemptNumber\":1,\"MaxAttempts\":3,\"RetryGroupId\":\"\"}", typeof(ArgumentException))]
        [DataRow("{\"AttemptNumber\":2147483648,\"MaxAttempts\":3,\"RetryGroupId\":\"g\"}", typeof(JsonReaderException))]
        public void JsonRejectsInvalidMetadata(string json, Type exceptionType)
        {
            AssertContractError(() => JsonConvert.DeserializeObject<RetryMetadata>(json), exceptionType);
            AssertContractError(() => JsonConvert.DeserializeObject<TaskScheduledEvent>("{\"RetryMetadata\":" + json + "}"), exceptionType);
        }

        static void AssertContractError(Action deserialize, Type exceptionType)
        {
            if (exceptionType == typeof(ArgumentOutOfRangeException))
            {
                Assert.ThrowsException<ArgumentOutOfRangeException>(deserialize);
            }
            else if (exceptionType == typeof(ArgumentException))
            {
                Assert.ThrowsException<ArgumentException>(deserialize);
            }
            else
            {
                Assert.ThrowsException<JsonReaderException>(deserialize);
            }
        }

        [TestMethod]
        public void HistoryAndActionsRoundTripOptionalMetadata()
        {
            var metadata = new RetryMetadata(2, 3, "execution:42");
            var action = new ScheduleTaskOrchestratorAction
            {
                Id = 42, Name = "work", RetryMetadata = metadata, RetryFailureType = "Captured.Error",
            };
            Assert.IsFalse(JsonConvert.SerializeObject(action).Contains("Captured.Error"));
            AssertMetadata(metadata, JsonConvert.DeserializeObject<ScheduleTaskOrchestratorAction>(
                JsonConvert.SerializeObject(action))!.RetryMetadata);
            AssertMetadata(metadata, System.Text.Json.JsonSerializer.Deserialize<ScheduleTaskOrchestratorAction>(
                System.Text.Json.JsonSerializer.Serialize(action))!.RetryMetadata);
            var scheduled = new TaskScheduledEvent(42, "work") { RetryMetadata = metadata, RetryFailureType = "Captured.Error" };
            Assert.IsFalse(JsonDataConverter.Default.Serialize(scheduled).Contains("Captured.Error"));
            AssertMetadata(metadata, JsonDataConverter.Default.Deserialize<TaskScheduledEvent>(
                JsonDataConverter.Default.Serialize(scheduled)).RetryMetadata);

            var serializer = new DataContractSerializer(typeof(TaskMessage));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, new TaskMessage { Event = scheduled });
            stream.Position = 0;
            AssertMetadata(metadata, ((TaskScheduledEvent)((TaskMessage)serializer.ReadObject(stream)!).Event).RetryMetadata);

            scheduled.RetryMetadata = null;
            Assert.IsFalse(JsonConvert.SerializeObject(scheduled).Contains("RetryMetadata"));
            Assert.IsNull(JsonConvert.DeserializeObject<TaskScheduledEvent>("{\"EventId\":42,\"Name\":\"work\"}")!.RetryMetadata);
            Assert.IsNull(new TaskContext(new OrchestrationInstance()).RetryMetadata);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task RetrySchedulingIsReplayStableAndIsolated(bool withOptions)
        {
            string firstGroup = await RunAttempts(withOptions, replay: false);
            string replayGroup = await RunAttempts(withOptions, replay: true);
            Assert.AreEqual(firstGroup, replayGroup);
        }

        static async Task<string> RunAttempts(bool withOptions, bool replay)
        {
            var context = new ImmediateTimerContext { IsReplaying = replay };
            using var span = new Activity("orchestration").Start();
            Activity? previous = DistributedTraceActivity.Current;
            DistributedTraceActivity.Current = span;
            try
            {
                var options = new RetryOptions(TimeSpan.FromMilliseconds(1), 3);
                Task<string> result = withOptions
                    ? context.ScheduleTask<string>("work", "", ScheduleTaskOptions.CreateBuilder().WithRetryOptions(options).Build())
                    : context.ScheduleWithRetry<string>("work", "", options);
                ScheduleTaskOrchestratorAction first = context.Actions.Single();
                Assert.AreEqual(1, first.RetryMetadata!.AttemptNumber);
                Assert.AreEqual(3, first.RetryMetadata.MaxAttempts);
                Assert.AreEqual(0, span.Events.Count());
                string group = first.RetryMetadata.RetryGroupId;

                Task<string> ordinary = context.ScheduleTask<string>("ordinary", "");
                Assert.IsNull(context.Actions.Single(a => a.Name == "ordinary").RetryMetadata);
                context.Complete(context.Actions.Single(a => a.Name == "ordinary"));
                await ordinary;
                context.Fail(first);
                ScheduleTaskOrchestratorAction second = context.Actions.Single();
                Assert.AreEqual(2, second.RetryMetadata!.AttemptNumber);
                Assert.AreEqual(group, second.RetryMetadata.RetryGroupId);
                context.Fail(second);
                ScheduleTaskOrchestratorAction third = context.Actions.Single();
                Assert.AreEqual(3, third.RetryMetadata!.AttemptNumber);
                Assert.AreEqual(group, third.RetryMetadata.RetryGroupId);
                context.Complete(third);
                Assert.AreEqual("done", await result);
                Assert.AreEqual(2, context.TimerCalls);

                Assert.AreEqual(0, span.Events.Count(), "Retry events are projected from persisted history only at completion.");
                var history = new HistoryEvent[]
                {
                    ToScheduled(first),
                    new TaskFailedEvent(-1, first.Id, "secret", null, new FailureDetails("Example.Failure", "secret", null, null, false)),
                    ToScheduled(second),
                    new TaskFailedEvent(-1, second.Id, "secret", null, new FailureDetails("Example.Failure", "secret", null, null, false)),
                    ToScheduled(third),
                };
                TraceHelper.AddTaskRetryEvents(span, history);
                TraceHelper.AddTaskRetryEvents(span, history);
                ActivityEvent[] events = span.Events.ToArray();
                Assert.AreEqual(2, events.Length);
                for (int i = 0; i < events.Length; i++)
                {
                    Assert.AreEqual(Schema.Task.RetryScheduled, events[i].Name);
                    var tags = events[i].Tags.ToDictionary(t => t.Key, t => t.Value);
                    Assert.AreEqual(6, tags.Count);
                    Assert.AreEqual("work", tags[Schema.Task.Name]);
                    Assert.AreEqual(i == 0 ? second.Id : third.Id, tags[Schema.Task.TaskId]);
                    Assert.AreEqual(i + 2, tags[Schema.Task.RetryAttempt]);
                    Assert.AreEqual(3, tags[Schema.Task.RetryMaxAttempts]);
                    Assert.AreEqual(group, tags[Schema.Task.RetryGroupId]);
                    Assert.AreEqual("Example.Failure", tags[Schema.Task.FailureType]);
                    Assert.IsFalse(tags.Values.Any(v => v?.ToString()?.Contains("secret") == true));
                }

                return group;
            }

            finally
            {
                DistributedTraceActivity.Current = previous!;
            }
        }

        static TaskScheduledEvent ToScheduled(ScheduleTaskOrchestratorAction action) =>
            new TaskScheduledEvent(action.Id, action.Name!)
            {
                RetryMetadata = action.RetryMetadata,
                RetryFailureType = action.RetryFailureType,
            };

        [TestMethod]
        public async Task ConcurrentRetryGroupsDoNotLeakAndOldHistoryIsAccepted()
        {
            var context = new ImmediateTimerContext();
            var options = new RetryOptions(TimeSpan.FromMilliseconds(1), 2);
            Task<string> leftResult = context.ScheduleWithRetry<string>("left", "", options);
            Task<string> rightResult = context.ScheduleWithRetry<string>("right", "", options);
            ScheduleTaskOrchestratorAction left = context.Actions.Single(a => a.Name == "left");
            ScheduleTaskOrchestratorAction right = context.Actions.Single(a => a.Name == "right");
            Assert.AreNotEqual(left.RetryMetadata!.RetryGroupId, right.RetryMetadata!.RetryGroupId);
            context.Fail(right);
            ScheduleTaskOrchestratorAction rightRetry = context.Actions.Single(a => a.Name == "right");
            Assert.AreEqual(right.RetryMetadata.RetryGroupId, rightRetry.RetryMetadata!.RetryGroupId);
            context.Fail(left);
            ScheduleTaskOrchestratorAction leftRetry = context.Actions.Single(a => a.Name == "left");
            Assert.AreEqual(left.RetryMetadata.RetryGroupId, leftRetry.RetryMetadata!.RetryGroupId);
            context.Complete(rightRetry);
            context.Complete(leftRetry);
            await Task.WhenAll(leftResult, rightResult);
        }

        [DataTestMethod]
        [DataRow("exhausted")]
        [DataRow("filter")]
        [DataRow("timeout")]
        [DataRow("nonretriable")]
        public async Task EarlyStopDoesNotScheduleAnotherAttempt(string stop)
        {
            var context = new ImmediateTimerContext();
            using var span = new Activity("orchestration").Start();
            Activity? previous = DistributedTraceActivity.Current;
            DistributedTraceActivity.Current = span;
            try
            {
                var options = new RetryOptions(TimeSpan.FromMilliseconds(1), stop == "exhausted" ? 1 : 3)
                {
                    Handle = _ => stop != "filter",
                    RetryTimeout = stop == "timeout" ? TimeSpan.Zero : TimeSpan.MaxValue,
                };
                Task<string> result = context.ScheduleWithRetry<string>("work", "", options);
                context.Fail(context.Actions.Single(), nonRetriable: stop == "nonretriable");
                await Assert.ThrowsExceptionAsync<TaskFailedException>(async () => await result);
                Assert.AreEqual(0, context.Actions.Count());
                Assert.AreEqual(0, span.Events.Count());
                Assert.AreEqual(stop == "exhausted" ? 1 : 0, context.TimerCalls);
            }
            finally
            {
                DistributedTraceActivity.Current = previous!;
            }
        }

        [TestMethod]
        public async Task AsyncRetryDelegateRetainsItsScopeWithoutLeakingToCaller()
        {
            var context = new ImmediateTimerContext();
            var gate = new TaskCompletionSource<bool>();
            var interceptor = new RetryInterceptor<string>(context, new RetryOptions(TimeSpan.FromMilliseconds(1), 1), async () =>
            {
                await gate.Task;
                return await context.ScheduleTask<string>("retry", "");
            });
            Task<string?> result = interceptor.Invoke();
            Task<string> ordinary = context.ScheduleTask<string>("ordinary", "");
            ScheduleTaskOrchestratorAction action = context.Actions.Single();
            Assert.IsNull(action.RetryMetadata);
            context.Complete(action);
            await ordinary;
            gate.SetResult(true);
            action = context.Actions.Single();
            Assert.AreEqual(1, action.RetryMetadata!.AttemptNumber);
            context.Complete(action);
            await result;
        }

        [TestMethod]
        public async Task RetryableProxySchedulesMetadata()
        {
            var context = new ImmediateTimerContext();
            var proxy = context.CreateRetryableClient<IRetryActivity>(new RetryOptions(TimeSpan.FromMilliseconds(1), 2));
            Task<string> result = proxy.Run("input");
            ScheduleTaskOrchestratorAction action = context.Actions.Single();
            Assert.AreEqual(1, action.RetryMetadata!.AttemptNumber);
            context.Fail(action);
            action = context.Actions.Single();
            Assert.AreEqual(2, action.RetryMetadata!.AttemptNumber);
            context.Complete(action);
            await result;
        }

        [TestMethod]
        public async Task SubOrchestrationRetriesDoNotAnnotateUnrelatedActivities()
        {
            var context = new ImmediateTimerContext();
            Task<string> child = context.CreateSubOrchestrationInstanceWithRetry<string>(
                "child", "", new RetryOptions(TimeSpan.FromMilliseconds(1), 2), null);
            var action = context.OrchestratorActions.OfType<CreateSubOrchestrationAction>().Single();
            context.HandleSubOrchestrationCreatedEvent(new SubOrchestrationInstanceCreatedEvent(action.Id)
            {
                Name = action.Name,
                InstanceId = action.InstanceId,
            });
            context.HandleSubOrchestrationInstanceCompletedEvent(new SubOrchestrationInstanceCompletedEvent(-1, action.Id, "\"child result\""));
            await child;
            Task<string> ordinary = context.ScheduleTask<string>("ordinary", "");
            ScheduleTaskOrchestratorAction scheduled = context.Actions.Single();
            Assert.IsNull(scheduled.RetryMetadata);
            context.Complete(scheduled);
            await ordinary;
        }

        [TestMethod]
        public async Task ExhaustedRetryPreservesTheCompatibilityTimer()
        {
            var context = new ImmediateTimerContext();
            Task<string> result = context.ScheduleWithRetry<string>("work", "",
                new RetryOptions(TimeSpan.FromMilliseconds(1), 3));
            for (int attempt = 1; attempt <= 3; attempt++)
            {
                ScheduleTaskOrchestratorAction scheduled = context.Actions.Single();
                Assert.AreEqual(attempt, scheduled.RetryMetadata!.AttemptNumber);
                context.Fail(scheduled);
            }

            await Assert.ThrowsExceptionAsync<TaskFailedException>(async () => await result);
            Assert.AreEqual(0, context.Actions.Count());
            Assert.AreEqual(3, context.TimerCalls);
        }

        public interface IRetryActivity
        {
            Task<string> Run(string input);
        }

        internal static void AssertMetadata(RetryMetadata expected, RetryMetadata? actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.AttemptNumber, actual!.AttemptNumber);
            Assert.AreEqual(expected.MaxAttempts, actual.MaxAttempts);
            Assert.AreEqual(expected.RetryGroupId, actual.RetryGroupId);
        }

        sealed class ImmediateTimerContext : TaskOrchestrationContext
        {
            internal ImmediateTimerContext()
                : base(new OrchestrationInstance { InstanceId = "instance", ExecutionId = "execution" }, TaskScheduler.Default,
                      errorPropagationMode: ErrorPropagationMode.UseFailureDetails)
            {
                this.CurrentUtcDateTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            }

            internal System.Collections.Generic.IEnumerable<ScheduleTaskOrchestratorAction> Actions =>
                this.OrchestratorActions.OfType<ScheduleTaskOrchestratorAction>();

            internal int TimerCalls { get; private set; }

            public override Task<T> CreateTimer<T>(DateTime fireAt, T state)
            {
                this.TimerCalls++;
                this.CurrentUtcDateTime = fireAt;
                return Task.FromResult(state);
            }

            internal void Fail(ScheduleTaskOrchestratorAction action, bool nonRetriable = false)
            {
                this.HandleTaskScheduledEvent(new TaskScheduledEvent(action.Id, action.Name!));
                this.HandleTaskFailedEvent(new TaskFailedEvent(-1, action.Id, "secret message", "secret stack",
                    new FailureDetails("Example.Failure", "secret message", "secret stack", null, nonRetriable)));
            }

            internal void Complete(ScheduleTaskOrchestratorAction action)
            {
                this.HandleTaskScheduledEvent(new TaskScheduledEvent(action.Id, action.Name!));
                this.HandleTaskCompletedEvent(new TaskCompletedEvent(-1, action.Id, "\"done\""));
            }
        }
    }
}
