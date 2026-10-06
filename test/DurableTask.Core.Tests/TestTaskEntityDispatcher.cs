using DurableTask.Core.Entities;
using DurableTask.Core.Entities.OperationFormat;
using DurableTask.Core.History;
using DurableTask.Core.Logging;
using DurableTask.Core.Middleware;
using DurableTask.Core.Settings;
using DurableTask.Core.Tracing;
using DurableTask.Emulator;
using DurableTask.Test.Orchestrations;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
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
        private TaskEntityDispatcher GetTaskEntityDispatcher(IOrchestrationService service)
        {
            // TODO: these should probably be injectable parameters to this method,
            // initialized with sensible defaults if not provided
            ILoggerFactory loggerFactory = null;
            var entityManager = new NameVersionObjectManager<TaskEntity>();
            var entityMiddleware = new DispatchMiddlewarePipeline();
            var logger = new LogHelper(loggerFactory?.CreateLogger("DurableTask.Core"));

            TaskEntityDispatcher dispatcher = new TaskEntityDispatcher(
                service, entityManager, entityMiddleware, logger, ErrorPropagationMode.UseFailureDetails, null);
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
            using var service = new LocalOrchestrationService();
            TaskEntityDispatcher dispatcher = GetTaskEntityDispatcher(service);

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

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void StartNewOrchestrationOperationAction_Tags_SurviveSerialization(bool useSystemTextJson)
        {
            string json = JsonConvert.SerializeObject(new
            {
                OperationActionType = OperationActionType.StartNewOrchestration,
                Tags = new Dictionary<string, string> { { "custom", "value" } },
            });

            StartNewOrchestrationOperationAction action = useSystemTextJson
                ? System.Text.Json.JsonSerializer.Deserialize<StartNewOrchestrationOperationAction>(json)
                : JsonConvert.DeserializeObject<StartNewOrchestrationOperationAction>(json);
            string roundTripped = useSystemTextJson
                ? System.Text.Json.JsonSerializer.Serialize(action)
                : JsonConvert.SerializeObject(action);

            Assert.AreEqual("value", JObject.Parse(roundTripped)["Tags"]?["custom"]?.Value<string>());
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void StartNewOrchestrationOperationAction_MissingTags_DefaultToNull(bool useSystemTextJson)
        {
            string json = JsonConvert.SerializeObject(new
            {
                OperationActionType = OperationActionType.StartNewOrchestration,
            });

            StartNewOrchestrationOperationAction action = useSystemTextJson
                ? System.Text.Json.JsonSerializer.Deserialize<StartNewOrchestrationOperationAction>(json)
                : JsonConvert.DeserializeObject<StartNewOrchestrationOperationAction>(json);

            Assert.IsNull(new StartNewOrchestrationOperationAction().Tags);
            Assert.IsNull(action.Tags);
        }

        [TestMethod]
        public void ProcessSendStartMessage_SuppliedTags_AreIncluded()
        {
            var action = new StartNewOrchestrationOperationAction
            {
                Tags = new Dictionary<string, string> { { "custom", "value" } },
            };

            var (startEvent, _) = SendStartMessage(CreateEntityRuntimeState(), action);

            CollectionAssert.AreEquivalent(
                new Dictionary<string, string>
                {
                    { "custom", "value" },
                    { OrchestrationTags.FireAndForget, "" },
                }.ToArray(),
                startEvent.Tags.ToArray());
        }

        [TestMethod]
        public void ProcessSendStartMessage_SuppliedTags_OverrideInheritedTags()
        {
            var runtimeState = CreateEntityRuntimeState(new Dictionary<string, string>
            {
                { "shared", "entity-value" },
                { "inherited", "entity-only" },
                { OrchestrationTags.TraceState, "inherited-trace-state" },
            });
            var action = new StartNewOrchestrationOperationAction
            {
                Tags = new Dictionary<string, string>
                {
                    { "shared", "child-value" },
                    { "custom", "caller-value" },
                },
            };

            var (startEvent, _) = SendStartMessage(runtimeState, action);

            CollectionAssert.AreEquivalent(
                new Dictionary<string, string>
                {
                    { "shared", "child-value" },
                    { "inherited", "entity-only" },
                    { "custom", "caller-value" },
                    { OrchestrationTags.TraceState, "inherited-trace-state" },
                    { OrchestrationTags.FireAndForget, "" },
                }.ToArray(),
                startEvent.Tags.ToArray());
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public void ProcessSendStartMessage_NullOrEmptyTags_PreserveDefaults(bool hasInheritedTags, bool useEmptyTags)
        {
            IDictionary<string, string> inheritedTags = hasInheritedTags
                ? new Dictionary<string, string>
                {
                    { "inherited", "entity-value" },
                    { OrchestrationTags.FireAndForget, "parent-value" },
                }
                : null;
            var runtimeState = CreateEntityRuntimeState(inheritedTags);
            var action = new StartNewOrchestrationOperationAction
            {
                Tags = useEmptyTags ? new Dictionary<string, string>() : null,
            };
            var expectedTags = new Dictionary<string, string> { { OrchestrationTags.FireAndForget, "" } };
            if (hasInheritedTags)
            {
                expectedTags.Add("inherited", "entity-value");
            }

            var (startEvent, _) = SendStartMessage(runtimeState, action);

            CollectionAssert.AreEquivalent(expectedTags.ToArray(), startEvent.Tags.ToArray());
            Assert.AreSame(inheritedTags, runtimeState.Tags);
            if (hasInheritedTags)
            {
                Assert.AreEqual("parent-value", inheritedTags[OrchestrationTags.FireAndForget]);
                Assert.AreEqual(2, inheritedTags.Count);
            }
            if (useEmptyTags)
            {
                Assert.AreEqual(0, action.Tags.Count);
            }
        }

        [TestMethod]
        public void ProcessSendStartMessage_FireAndForgetTag_CannotBeOverridden()
        {
            var runtimeState = CreateEntityRuntimeState(new Dictionary<string, string>
            {
                { OrchestrationTags.FireAndForget, "parent-value" },
                { "inherited", "entity-value" },
            });
            var action = new StartNewOrchestrationOperationAction
            {
                Tags = new Dictionary<string, string>
                {
                    { OrchestrationTags.FireAndForget, "false" },
                    { "custom", "caller-value" },
                },
            };

            var (startEvent, _) = SendStartMessage(runtimeState, action);

            Assert.AreEqual("", startEvent.Tags[OrchestrationTags.FireAndForget]);
            Assert.IsTrue(startEvent.Tags.ContainsKey("custom"));
            Assert.AreEqual("caller-value", startEvent.Tags["custom"]);
            Assert.AreEqual("entity-value", startEvent.Tags["inherited"]);
            Assert.AreEqual("false", action.Tags[OrchestrationTags.FireAndForget]);
            Assert.AreEqual("parent-value", runtimeState.Tags[OrchestrationTags.FireAndForget]);
        }

        [TestMethod]
        public void ProcessSendStartMessage_DoesNotMutateSourceTags()
        {
            var inheritedTags = new Dictionary<string, string>
            {
                { "inherited", "entity-value" },
                { "shared", "entity-value" },
            };
            var suppliedTags = new Dictionary<string, string>
            {
                { "custom", "caller-value" },
                { "shared", "caller-value" },
            };
            var inheritedSnapshot = inheritedTags.ToArray();
            var suppliedSnapshot = suppliedTags.ToArray();
            var runtimeState = CreateEntityRuntimeState(inheritedTags);
            var action = new StartNewOrchestrationOperationAction { Tags = suppliedTags };

            var (startEvent, _) = SendStartMessage(runtimeState, action);

            Assert.AreSame(inheritedTags, runtimeState.Tags);
            Assert.AreSame(suppliedTags, action.Tags);
            Assert.AreNotSame(inheritedTags, startEvent.Tags);
            Assert.AreNotSame(suppliedTags, startEvent.Tags);
            CollectionAssert.AreEquivalent(inheritedSnapshot, inheritedTags.ToArray());
            CollectionAssert.AreEquivalent(suppliedSnapshot, suppliedTags.ToArray());

            startEvent.Tags["shared"] = "changed-child-value";
            startEvent.Tags["child-only"] = "value";
            CollectionAssert.AreEquivalent(inheritedSnapshot, inheritedTags.ToArray());
            CollectionAssert.AreEquivalent(suppliedSnapshot, suppliedTags.ToArray());
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public void ProcessSendStartMessage_SchedulingAndTracing_ArePreserved(bool useTags, bool scheduled)
        {
            var requestTime = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var parentTraceContext = new DistributedTraceContext(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "vendor=value");
            var action = new StartNewOrchestrationOperationAction
            {
                InstanceId = "child-instance",
                Name = "child-orchestration",
                Version = "2.0",
                Input = "{\"value\":42}",
                Tags = useTags ? new Dictionary<string, string> { { "custom", "value" } } : null,
                ScheduledStartTime = scheduled ? requestTime.UtcDateTime.AddMinutes(5) : (DateTime?)null,
                RequestTime = requestTime,
                ParentTraceContext = parentTraceContext,
            };
            var runtimeState = CreateEntityRuntimeState();
            var originalEvents = runtimeState.Events.ToArray();
            Activity schedulingActivity = null;
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "DurableTask.Core",
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => schedulingActivity = activity,
            };
            ActivitySource.AddActivityListener(listener);

            var (startEvent, effects) = SendStartMessage(runtimeState, action, taskIdCounter: 42);

            Assert.AreEqual(action.InstanceId, startEvent.OrchestrationInstance.InstanceId);
            Assert.IsTrue(Guid.TryParseExact(startEvent.OrchestrationInstance.ExecutionId, "N", out _));
            Assert.AreSame(startEvent.OrchestrationInstance, effects.InstanceMessages[0].OrchestrationInstance);
            Assert.AreEqual(action.Name, startEvent.Name);
            Assert.AreEqual(action.Version, startEvent.Version);
            Assert.AreEqual(action.Input, startEvent.Input);
            Assert.AreEqual(action.ScheduledStartTime, startEvent.ScheduledStartTime);
            Assert.AreSame(runtimeState.OrchestrationInstance, startEvent.ParentInstance.OrchestrationInstance);
            Assert.AreEqual(runtimeState.Name, startEvent.ParentInstance.Name);
            Assert.AreEqual(runtimeState.Version, startEvent.ParentInstance.Version);
            Assert.AreEqual(42, startEvent.ParentInstance.TaskScheduleId);
            Assert.AreEqual(43, effects.taskIdCounter);
            CollectionAssert.AreEqual(originalEvents, runtimeState.Events.ToArray());
            Assert.AreEqual(0, effects.ActivityMessages.Count);
            Assert.AreEqual(0, effects.TimerMessages.Count);

            Assert.IsNotNull(schedulingActivity);
            Assert.IsTrue(ActivityContext.TryParse(
                parentTraceContext.TraceParent, parentTraceContext.TraceState, out ActivityContext parentContext));
            Assert.AreEqual(parentContext.TraceId, schedulingActivity.TraceId);
            Assert.AreEqual(parentContext.SpanId, schedulingActivity.ParentSpanId);
            Assert.AreEqual(requestTime.UtcDateTime, schedulingActivity.StartTimeUtc);
            Assert.AreEqual(schedulingActivity.Id, startEvent.ParentTraceContext.TraceParent);
            Assert.AreEqual(parentTraceContext.TraceState, startEvent.ParentTraceContext.TraceState);
            Assert.AreEqual(action.ScheduledStartTime?.ToString(), schedulingActivity.GetTagItem(Schema.Task.ScheduledTime));
            Assert.AreSame(parentTraceContext, action.ParentTraceContext);
            Assert.AreEqual(requestTime, action.RequestTime);
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public async Task ProcessSendStartMessage_ChildCompletion_DoesNotNotifyEntity(bool useTags, bool fail)
        {
            var runtimeState = CreateEntityRuntimeState(new Dictionary<string, string> { { "inherited", "entity-value" } });
            var action = new StartNewOrchestrationOperationAction
            {
                InstanceId = "child-instance",
                Name = nameof(CompletingOrchestration),
                Version = "",
                Tags = useTags
                    ? new Dictionary<string, string>
                    {
                        { "custom", "caller-value" },
                        { OrchestrationTags.FireAndForget, "false" },
                    }
                    : null,
            };
            var (startEvent, effects) = SendStartMessage(runtimeState, action);
            using var service = new CapturingOrchestrationService();
            var dispatcher = new CompletingOrchestrationDispatcher(service, new CompletingOrchestration(fail));
            var workItem = new TaskOrchestrationWorkItem
            {
                InstanceId = startEvent.OrchestrationInstance.InstanceId,
                OrchestrationRuntimeState = new OrchestrationRuntimeState(),
                LockedUntilUtc = DateTime.MaxValue,
                NewMessages = effects.InstanceMessages,
            };

            bool completed = await dispatcher.ProcessAsync(workItem);

            Assert.IsTrue(completed);
            Assert.AreEqual(fail ? OrchestrationStatus.Failed : OrchestrationStatus.Completed, service.State.OrchestrationStatus);
            Assert.AreEqual(0, service.Messages.Count, "A fire-and-forget child must not send completion or failure to the entity.");
            Assert.IsNull(service.ContinuedAsNewMessage);
            Assert.AreEqual("", service.State.Tags[OrchestrationTags.FireAndForget]);
            Assert.AreEqual("entity-value", service.State.Tags["inherited"]);
            Assert.IsFalse(runtimeState.Tags.ContainsKey(OrchestrationTags.FireAndForget));
            Assert.AreEqual(1, runtimeState.Tags.Count);
            if (useTags)
            {
                Assert.IsTrue(service.State.Tags.ContainsKey("custom"));
                Assert.AreEqual("caller-value", service.State.Tags["custom"]);
            }
        }

        static OrchestrationRuntimeState CreateEntityRuntimeState(IDictionary<string, string> tags = null)
        {
            return new OrchestrationRuntimeState(new HistoryEvent[]
            {
                new ExecutionStartedEvent(-1, "entity-state")
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = "@testentity@test-key",
                        ExecutionId = "entity-execution",
                    },
                    Name = "testentity",
                    Version = "1.0",
                    Tags = tags,
                },
            });
        }

        (ExecutionStartedEvent StartEvent, WorkItemEffects Effects) SendStartMessage(
            OrchestrationRuntimeState runtimeState, StartNewOrchestrationOperationAction action, int taskIdCounter = 0)
        {
            var effects = new WorkItemEffects
            {
                InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                RuntimeState = runtimeState,
                taskIdCounter = taskIdCounter,
                InstanceMessages = new List<TaskMessage>(),
                ActivityMessages = new List<TaskMessage>(),
                TimerMessages = new List<TaskMessage>(),
            };

            using var service = new LocalOrchestrationService();
            GetTaskEntityDispatcher(service).ProcessSendStartMessage(effects, runtimeState, action);

            Assert.AreEqual(1, effects.InstanceMessages.Count);
            Assert.IsInstanceOfType(effects.InstanceMessages[0].Event, typeof(ExecutionStartedEvent));
            return ((ExecutionStartedEvent)effects.InstanceMessages[0].Event, effects);
        }

        sealed class CompletingOrchestration : TaskOrchestration<string, string>
        {
            readonly bool fail;

            public CompletingOrchestration(bool fail) => this.fail = fail;

            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                if (this.fail)
                {
                    throw new InvalidOperationException("Child orchestration failed.");
                }
                return Task.FromResult("completed");
            }
        }

        sealed class CompletingOrchestrationDispatcher : TaskOrchestrationDispatcher
        {
            public CompletingOrchestrationDispatcher(IOrchestrationService service, TaskOrchestration orchestration)
                : base(service, CreateObjectManager(orchestration), new DispatchMiddlewarePipeline(),
                    new LogHelper(null), ErrorPropagationMode.UseFailureDetails, new VersioningSettings(), null)
            {
            }

            public Task<bool> ProcessAsync(TaskOrchestrationWorkItem workItem) => this.OnProcessWorkItemAsync(workItem);

            static NameVersionObjectManager<TaskOrchestration> CreateObjectManager(TaskOrchestration orchestration)
            {
                var manager = new NameVersionObjectManager<TaskOrchestration>();
                manager.Add(new TestObjectCreator<TaskOrchestration>(nameof(CompletingOrchestration), "", () => orchestration));
                return manager;
            }
        }

        sealed class CapturingOrchestrationService : LocalOrchestrationService, IOrchestrationService
        {
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
                this.State = state;
                this.Messages = outboundMessages.Concat(orchestratorMessages).Concat(timerMessages).ToList();
                this.ContinuedAsNewMessage = continuedAsNewMessage;
                return Task.CompletedTask;
            }
        }
    }
}
