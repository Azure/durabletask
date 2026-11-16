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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using System.Xml;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Serializing;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json.Linq;

    [TestClass]
    public sealed class ActivityStartedTimeTests
    {
        static readonly DateTime ScheduledTime = new DateTime(2000, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        [TestMethod]
        [DataRow(EventType.TaskCompleted, false)]
        [DataRow(EventType.TaskFailed, false)]
        [DataRow(EventType.TaskFailed, true)]
        public async Task Dispatcher_RecordsStartForCompletedAndFailedInvocations(EventType expectedType, bool unexpectedFailure)
        {
            using var service = new RecordingService();
            var activity = new RecordingActivity((_, input) =>
            {
                if (expectedType == EventType.TaskFailed)
                {
                    throw unexpectedFailure ? new InvalidOperationException("unexpected") : new TaskFailureException("normal");
                }

                return Task.FromResult(input);
            });
            TaskActivityDispatcher dispatcher = CreateDispatcher(service, activity, new DispatchMiddlewarePipeline());
            TaskActivityWorkItem workItem = CreateWorkItem(7, 42);

            await DispatchAsync(dispatcher, workItem);

            TaskMessage response = service.Responses.Single();
            Assert.AreEqual(expectedType, response.Event.EventType);
            Assert.AreSame(workItem.TaskMessage.OrchestrationInstance, response.OrchestrationInstance);
            Assert.AreEqual(7, GetScheduledId(response.Event));
            AssertKnownStart(response.Event);
            Assert.AreEqual(ScheduledTime, workItem.TaskMessage.Event.Timestamp);
            Assert.AreEqual(42L, activity.Invocations.Single().DeliveryAttempt);
            Assert.AreEqual(7, activity.Invocations.Single().TaskId);
            if (response.Event is TaskCompletedEvent completed)
            {
                Assert.AreEqual("input", completed.Result);
            }
            else
            {
                Assert.AreEqual(unexpectedFailure ? "unexpected" : "normal", ((TaskFailedEvent)response.Event).Reason);
            }
        }

        [TestMethod]
        [DataRow("default")]
        [DataRow("completed")]
        [DataRow("failed")]
        [DataRow("exception")]
        public async Task Dispatcher_LeavesStartUnknownWhenMiddlewareSkipsInvocation(string outcome)
        {
            using var service = new RecordingService();
            var activity = new RecordingActivity((_, input) => Task.FromResult(input));
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add((context, _) =>
            {
                if (outcome == "exception")
                {
                    throw new TaskFailureException("middleware");
                }

                if (outcome != "default")
                {
                    context.SetProperty(new ActivityExecutionResult
                    {
                        ResponseEvent = CreateResponse(outcome == "failed", 7, ScheduledTime),
                    });
                }

                return Task.CompletedTask;
            });
            TaskActivityDispatcher dispatcher = CreateDispatcher(service, activity, pipeline);

            await DispatchAsync(dispatcher, CreateWorkItem(7, 1));

            HistoryEvent response = service.Responses.Single().Event;
            Assert.AreEqual(outcome == "failed" || outcome == "exception" ? EventType.TaskFailed : EventType.TaskCompleted, response.EventType);
            Assert.IsNull(GetStartedTime(response));
            Assert.AreEqual(0, activity.Invocations.Count);
        }

        [TestMethod]
        [DataRow(false, true)]
        [DataRow(true, true)]
        [DataRow(false, false)]
        [DataRow(true, false)]
        public async Task Dispatcher_CorrelatesStartWithMiddlewareReplacement(bool failed, bool matchingTask)
        {
            using var service = new RecordingService();
            var activity = new RecordingActivity((_, input) =>
                failed ? throw new TaskFailureException("activity") : Task.FromResult(input));
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add(async (context, next) =>
            {
                try
                {
                    await next();
                }
                catch (TaskFailureException)
                {
                    Assert.IsTrue(failed);
                }

                context.SetProperty(new ActivityExecutionResult
                {
                    ResponseEvent = CreateResponse(failed, matchingTask ? 7 : 99, ScheduledTime),
                });
            });
            TaskActivityDispatcher dispatcher = CreateDispatcher(service, activity, pipeline);

            await DispatchAsync(dispatcher, CreateWorkItem(7, 1));

            HistoryEvent response = service.Responses.Single().Event;
            Assert.AreEqual(failed ? EventType.TaskFailed : EventType.TaskCompleted, response.EventType);
            Assert.AreEqual(matchingTask ? 7 : 99, GetScheduledId(response));
            Assert.AreEqual(1, activity.Invocations.Count);
            if (matchingTask)
            {
                AssertKnownStart(response);
            }
            else
            {
                Assert.IsNull(GetStartedTime(response));
            }
        }

        [TestMethod]
        public async Task Dispatcher_DoesNotCarryStartAcrossAbandonmentRedeliveryOrNewScheduledTasks()
        {
            using var service = new RecordingService();
            bool abort = true;
            bool skipInvocation = false;
            var activity = new RecordingActivity((_, input) =>
            {
                if (abort)
                {
                    abort = false;
                    throw new SessionAbortedException("redeliver");
                }

                return Task.FromResult(input);
            });
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add((_, next) => skipInvocation ? throw new TaskFailureException("middleware") : next());
            TaskActivityDispatcher dispatcher = CreateDispatcher(service, activity, pipeline);
            TaskActivityWorkItem workItem = CreateWorkItem(7, 1);

            await DispatchAsync(dispatcher, workItem);
            Assert.AreEqual(1, service.AbandonCount);
            Assert.AreEqual(0, service.Responses.Count);

            skipInvocation = true;
            workItem.DeliveryAttempt = 2;
            await DispatchAsync(dispatcher, workItem);
            Assert.IsNull(GetStartedTime(service.Responses.Single().Event));

            skipInvocation = false;
            workItem.DeliveryAttempt = 3;
            await DispatchAsync(dispatcher, workItem);
            AssertKnownStart(service.Responses[1].Event);
            Assert.AreEqual(7, GetScheduledId(service.Responses[1].Event));

            await DispatchAsync(dispatcher, CreateWorkItem(9, 1));
            AssertKnownStart(service.Responses[2].Event);
            Assert.AreEqual(9, GetScheduledId(service.Responses[2].Event));
            CollectionAssert.AreEqual(new long?[] { 1, 3, 1 }, activity.Invocations.Select(c => c.DeliveryAttempt).ToArray());
            CollectionAssert.AreEqual(new[] { 7, 7, 9 }, activity.Invocations.Select(c => c.TaskId).ToArray());
        }

        [TestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public void HistorySerialization_PreservesStartAndLeavesLegacyHistoryUnknown(bool failed, bool hasStart)
        {
            DateTime? startedTime = hasStart ? ScheduledTime.AddMinutes(12) : null;
            HistoryEvent original = CreateResponse(failed, 7, startedTime);
            original.Timestamp = ScheduledTime.AddMinutes(15);

            string json = JsonDataConverter.Default.Serialize(original);
            Assert.AreEqual(hasStart, JObject.Parse(json).Property("StartedTime") != null);
            AssertRoundTrip(JsonDataConverter.Default.Deserialize<HistoryEvent>(json));

            var serializer = new DataContractSerializer(typeof(HistoryEvent));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, original);
            stream.Position = 0;
            using (var reader = XmlReader.Create(stream, new XmlReaderSettings { CloseInput = false }))
            {
                AssertRoundTrip((HistoryEvent)serializer.ReadObject(reader)!);
            }

            void AssertRoundTrip(HistoryEvent actual)
            {
                Assert.AreEqual(original.EventType, actual.EventType);
                Assert.AreEqual(original.EventId, actual.EventId);
                Assert.AreEqual(original.Timestamp, actual.Timestamp);
                Assert.AreEqual(7, GetScheduledId(actual));
                Assert.AreEqual(startedTime, GetStartedTime(actual));
                if (hasStart)
                {
                    Assert.AreEqual(DateTimeKind.Utc, GetStartedTime(actual)!.Value.Kind);
                }
            }
        }

        static TaskActivityDispatcher CreateDispatcher(RecordingService service, TaskActivity activity, DispatchMiddlewarePipeline pipeline)
        {
            var manager = new NameVersionObjectManager<TaskActivity>();
            manager.Add(new TestObjectCreator<TaskActivity>("activity", "v1", () => activity));
            return new TaskActivityDispatcher(service, manager, pipeline, new LogHelper(null), ErrorPropagationMode.UseFailureDetails, null);
        }

        static TaskActivityWorkItem CreateWorkItem(int scheduledId, long deliveryAttempt) => new TaskActivityWorkItem
        {
            Id = "work-item",
            LockedUntilUtc = DateTime.MaxValue,
            DeliveryAttempt = deliveryAttempt,
            TaskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "instance", ExecutionId = "execution" },
                Event = new TaskScheduledEvent(scheduledId, "activity", "v1") { Input = "input", Timestamp = ScheduledTime },
            },
        };

        static Task DispatchAsync(TaskActivityDispatcher dispatcher, TaskActivityWorkItem workItem)
        {
            // Exercise the real dispatcher without background polling or changing production visibility.
            MethodInfo? process = typeof(TaskActivityDispatcher).GetMethod("OnProcessWorkItemAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(process);
            return (Task)process.Invoke(dispatcher, new object[] { workItem })!;
        }

        static HistoryEvent CreateResponse(bool failed, int scheduledId, DateTime? startedTime) => failed
            ? new TaskFailedEvent(-1, scheduledId, "failure", "details") { StartedTime = startedTime }
            : new TaskCompletedEvent(-1, scheduledId, "result") { StartedTime = startedTime };

        static DateTime? GetStartedTime(HistoryEvent e) => e is TaskCompletedEvent completed
            ? completed.StartedTime
            : ((TaskFailedEvent)e).StartedTime;

        static int GetScheduledId(HistoryEvent e) => e is TaskCompletedEvent completed
            ? completed.TaskScheduledId
            : ((TaskFailedEvent)e).TaskScheduledId;

        static void AssertKnownStart(HistoryEvent response)
        {
            DateTime? startedTime = GetStartedTime(response);
            Assert.IsNotNull(startedTime);
            Assert.AreEqual(DateTimeKind.Utc, startedTime.Value.Kind);
            Assert.AreNotEqual(ScheduledTime, startedTime.Value);
        }

        sealed class RecordingActivity : TaskActivity
        {
            readonly Func<TaskContext, string, Task<string>> run;

            public RecordingActivity(Func<TaskContext, string, Task<string>> run) => this.run = run;

            public List<TaskContext> Invocations { get; } = new List<TaskContext>();

            public override string Run(TaskContext context, string input) => throw new NotSupportedException();

            public override Task<string> RunAsync(TaskContext context, string input)
            {
                this.Invocations.Add(context);
                return this.run(context, input);
            }
        }

        sealed class RecordingService : LocalOrchestrationService, IOrchestrationService
        {
            public List<TaskMessage> Responses { get; } = new List<TaskMessage>();

            public int AbandonCount { get; private set; }

            public new Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
            {
                this.Responses.Add(responseMessage);
                return Task.CompletedTask;
            }

            public new Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
            {
                this.AbandonCount++;
                return Task.CompletedTask;
            }
        }
    }
}
