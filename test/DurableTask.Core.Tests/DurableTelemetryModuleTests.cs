// Copyright (c) Microsoft Corporation. All rights reserved.

#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.ApplicationInsights;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    [DoNotParallelize]
    public class DurableTelemetryModuleTests
    {
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public async Task ProjectedRetryEventsReachApplicationInsightsWithOriginalTimeAndCorrelation(bool structuredFailure)
        {
            using var channel = new CapturingTelemetryChannel();
            using var configuration = CreateConfiguration(channel);
            await using var module = new DurableTelemetryModule();
            module.Initialize(configuration);
            using var source = new ActivitySource("DurableTask.Core");
            Activity span = source.StartActivity("orchestration:work", ActivityKind.Server)!;
            Assert.IsNotNull(span);
            span.SetTag(Schema.Task.Type, "orchestration");
            DateTime scheduledAt = DateTime.UtcNow.AddMinutes(-5);
            var history = new HistoryEvent[]
            {
                new TaskScheduledEvent(10, "work") { RetryMetadata = new RetryMetadata(1, 3, "group") },
                new TaskFailedEvent(-1, 10, "secret failure message", "opaque secret stack",
                    structuredFailure ? new FailureDetails("Example.Error", "secret", "secret stack", null, false) : null),
                new TaskScheduledEvent(20, "work")
                {
                    Timestamp = scheduledAt,
                    Input = "secret input",
                    RetryMetadata = new RetryMetadata(2, 3, "group"),
                },
            };

            TraceHelper.AddTaskRetryEvents(span, history);
            TraceHelper.AddTaskRetryEvents(span, history);
            Assert.AreEqual(0, channel.Items.Count, "Telemetry must not be sent before the orchestration span stops.");
            span.Stop();
            span.Stop();

            EventTelemetry retryEvent = channel.Items.OfType<EventTelemetry>().Single();
            Assert.AreEqual(2, channel.Items.Count);
            Assert.AreEqual(1, channel.Items.OfType<RequestTelemetry>().Count(), "Do not duplicate orchestration spans.");
            Assert.AreEqual(Schema.Task.RetryScheduled, retryEvent.Name);
            Assert.AreEqual(new DateTimeOffset(scheduledAt), retryEvent.Timestamp);
            Assert.AreEqual(span.TraceId.ToString(), retryEvent.Context.Operation.Id);
            Assert.AreEqual(span.SpanId.ToString(), retryEvent.Context.Operation.ParentId);
            Assert.AreEqual(6, retryEvent.Properties.Count);
            Assert.AreEqual("work", retryEvent.Properties[Schema.Task.Name]);
            Assert.AreEqual("20", retryEvent.Properties[Schema.Task.TaskId]);
            Assert.AreEqual("2", retryEvent.Properties[Schema.Task.RetryAttempt]);
            Assert.AreEqual("3", retryEvent.Properties[Schema.Task.RetryMaxAttempts]);
            Assert.AreEqual("group", retryEvent.Properties[Schema.Task.RetryGroupId]);
            Assert.AreEqual(structuredFailure ? "Example.Error" : typeof(TaskFailedException).FullName,
                retryEvent.Properties[Schema.Task.FailureType]);
            Assert.IsFalse(retryEvent.Properties.Values.Any(value => value.Contains("secret")));
        }

        [TestMethod]
        public async Task RetryEventMappingOnlyExportsAllowlistedFieldsFromOrchestrationEvents()
        {
            using var channel = new CapturingTelemetryChannel();
            using var configuration = CreateConfiguration(channel);
            await using var module = new DurableTelemetryModule();
            module.Initialize(configuration);
            using var source = new ActivitySource("DurableTask.Core");
            using (Activity span = source.StartActivity("orchestration:work", ActivityKind.Server)!)
            {
                span.SetTag(Schema.Task.Type, "orchestration");
                span.AddBaggage("input", "secret baggage");
                span.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
                {
                    { "exception.message", "secret message" },
                }));
                span.AddEvent(new ActivityEvent(Schema.Task.RetryScheduled, tags: new ActivityTagsCollection
                {
                    { Schema.Task.Name, "work" },
                    { Schema.Task.TaskId, 20 },
                    { Schema.Task.RetryAttempt, 2 },
                    { Schema.Task.RetryGroupId, "group" },
                    { Schema.Task.RetryMaxAttempts, 3 },
                    { Schema.Task.FailureType, "Example.Error" },
                    { "exception.message", "secret message" },
                    { "exception.stacktrace", "secret stack" },
                    { "input", "secret input" },
                }));
            }

            using (Activity span = source.StartActivity("activity:work", ActivityKind.Server)!)
            {
                span.SetTag(Schema.Task.Type, "activity");
                span.AddEvent(new ActivityEvent(Schema.Task.RetryScheduled));
            }

            EventTelemetry retryEvent = channel.Items.OfType<EventTelemetry>().Single();
            Assert.AreEqual(6, retryEvent.Properties.Count);
            Assert.IsFalse(retryEvent.Properties.Values.Any(value => value.Contains("secret")));
            Assert.AreEqual(2, channel.Items.OfType<RequestTelemetry>().Count(), "Existing request export is unchanged.");
        }

        [TestMethod]
        public async Task StartingFreshTraceExportsTheCompletedGenerationsRetryEvents()
        {
            using var channel = new CapturingTelemetryChannel();
            using var configuration = CreateConfiguration(channel);
            await using var module = new DurableTelemetryModule();
            module.Initialize(configuration);
            using var source = new ActivitySource("DurableTask.Core");
            Activity span = source.StartActivity("orchestration:work", ActivityKind.Server)!;
            span.SetTag(Schema.Task.Type, "orchestration");
            var state = new OrchestrationRuntimeState(new HistoryEvent[]
            {
                new ExecutionStartedEvent(-1, null)
                {
                    Name = "work",
                    OrchestrationInstance = new OrchestrationInstance { InstanceId = "instance", ExecutionId = "generation1" },
                },
                new TaskScheduledEvent(10, "work") { RetryMetadata = new RetryMetadata(1, 3, "group") },
                new TaskFailedEvent(-1, 10, "secret", null, new FailureDetails("Example.Error", "secret", null, null, false)),
                new TaskScheduledEvent(20, "work") { RetryMetadata = new RetryMetadata(2, 3, "group") },
                new ContinueAsNewEvent(-1, null),
            });
            var nextGeneration = new ExecutionStartedEvent(-1, null)
            {
                GenerateNewTrace = true,
                Name = "work",
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "instance", ExecutionId = "generation2" },
            };

            Activity next = TaskOrchestrationDispatcher.RestartTraceActivityForContinueAsNewIfNeeded(
                span, state, nextGeneration)!;
            try
            {
                EventTelemetry retryEvent = channel.Items.OfType<EventTelemetry>().Single();
                Assert.AreEqual(span.TraceId.ToString(), retryEvent.Context.Operation.Id);
                Assert.AreNotEqual(span.TraceId, next.TraceId);
                Assert.AreEqual(0, next.Events.Count());
                Assert.AreEqual(1, channel.Items.OfType<RequestTelemetry>().Count(), "Only the completed generation is stopped.");
            }
            finally
            {
                next.Stop();
                DistributedTraceActivity.Current = null!;
            }
        }

        [TestMethod]
        public async Task ProjectionKeepsConcurrentGroupsSeparateAndDoesNotInventMissingRetries()
        {
            using var channel = new CapturingTelemetryChannel();
            using var configuration = CreateConfiguration(channel);
            await using var module = new DurableTelemetryModule();
            module.Initialize(configuration);
            using var source = new ActivitySource("DurableTask.Core");
            Activity span = source.StartActivity("orchestration:work", ActivityKind.Server)!;
            span.SetTag(Schema.Task.Type, "orchestration");
            var history = new HistoryEvent[]
            {
                new TaskScheduledEvent(1, "left") { RetryMetadata = new RetryMetadata(1, 3, "left") },
                new TaskScheduledEvent(2, "right") { RetryMetadata = new RetryMetadata(1, 3, "right") },
                new TaskFailedEvent(-1, 2, "secret", null, new FailureDetails("Right.Error", "secret", null, null, false)),
                new TaskFailedEvent(-1, 1, "secret", null, new FailureDetails("Left.Error", "secret", null, null, false)),
                new TaskScheduledEvent(3, "right") { RetryMetadata = new RetryMetadata(2, 3, "right") },
                new TaskScheduledEvent(4, "left") { RetryMetadata = new RetryMetadata(2, 3, "left") },
                new TaskScheduledEvent(5, "old"),
                new TaskFailedEvent(-1, 5, "secret", null),
                new TaskScheduledEvent(6, "old"),
                new TaskScheduledEvent(7, "initial") { RetryMetadata = new RetryMetadata(1, 1, "single") },
                new TaskFailedEvent(-1, 7, "secret", null),
            };
            TraceHelper.AddTaskRetryEvents(span, history);
            TraceHelper.AddTaskRetryEvents(span, history);
            span.Stop();

            EventTelemetry[] events = channel.Items.OfType<EventTelemetry>().ToArray();
            Assert.AreEqual(2, events.Length);
            Assert.AreEqual("Right.Error", events[0].Properties[Schema.Task.FailureType]);
            Assert.AreEqual("right", events[0].Properties[Schema.Task.RetryGroupId]);
            Assert.AreEqual("Left.Error", events[1].Properties[Schema.Task.FailureType]);
            Assert.AreEqual("left", events[1].Properties[Schema.Task.RetryGroupId]);
        }

        internal static TelemetryConfiguration CreateConfiguration(CapturingTelemetryChannel channel)
        {
            var configuration = TelemetryConfiguration.CreateDefault();
            configuration.ConnectionString = "InstrumentationKey=00000000-0000-0000-0000-000000000001";
            configuration.TelemetryChannel = channel;
            return configuration;
        }
    }

    internal sealed class CapturingTelemetryChannel : ITelemetryChannel
    {
        internal ConcurrentQueue<ITelemetry> Items { get; } = new ConcurrentQueue<ITelemetry>();

        public bool? DeveloperMode { get; set; }

        public string EndpointAddress { get; set; } = string.Empty;

        public void Send(ITelemetry item) => this.Items.Enqueue(item);

        public void Flush() { }

        public void Dispose() { }
    }
}
