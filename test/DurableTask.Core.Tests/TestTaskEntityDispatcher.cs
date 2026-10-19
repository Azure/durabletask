using DurableTask.Core.Entities;
using DurableTask.Core.Entities.EventFormat;
using DurableTask.Core.Entities.OperationFormat;
using DurableTask.Core.History;
using DurableTask.Core.Logging;
using DurableTask.Core.Middleware;
using DurableTask.Core.Settings;
using DurableTask.Core.Tracing;
using DurableTask.Emulator;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
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
            Assert.IsTrue(startEvent.Tags.TryGetValue("custom", out string customTagValue));
            Assert.AreEqual("caller-value", customTagValue);
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
                Assert.IsTrue(service.State.Tags.TryGetValue("custom", out string customTagValue));
                Assert.AreEqual("caller-value", customTagValue);
            }
        }

        [TestMethod]
        public async Task ExtendedSession_NoWorkerBatches_PreserveStateAndQueueChanges()
        {
            const string firstState = "{\"count\":42,\"values\":[\"a\",\"b\"]}";
            const string secondState = "{\"count\":43,\"values\":[\"a\",\"b\",\"c\"]}";
            int workerCalls = 0;

            IReadOnlyList<OrchestrationState> checkpoints = await RunEntitySessionAsync(
                "initial-state",
                (request, metadata) =>
                {
                    Assert.AreEqual(1, request.Operations.Count);
                    Assert.IsTrue(metadata.IsExtendedSession);
                    if (workerCalls++ == 0)
                    {
                        Assert.AreEqual("set", request.Operations[0].Operation);
                        Assert.IsTrue(metadata.IncludeState);
                        Assert.AreEqual("initial-state", request.EntityState);
                        return firstState;
                    }

                    Assert.AreEqual("queued", request.Operations[0].Operation);
                    Assert.IsFalse(metadata.IncludeState);
                    Assert.IsNull(request.EntityState, "The dispatcher should still clear its state between worker batches.");
                    return secondState;
                },
                // 1. Update the entity state.
                new[] { EntityOperation("set") },
                // 2. Lock without invoking the worker.
                new[] { EntityLock() },
                // 3. Queue another caller's operation.
                new[] { EntityOperation("queued", "other-caller") },
                // 4. Unlock and execute the queued operation.
                new[] { EntityUnlock() },
                // 5. Lock and unlock without invoking the worker.
                new[] { EntityLock() },
                new[] { EntityUnlock() });

            // 6. Verify state, locks, and queues at each checkpoint.
            Assert.AreEqual(2, workerCalls, "Lock-only and blocked-operation batches must not invoke the worker.");
            Assert.AreEqual(6, checkpoints.Count);
            string[] expectedStates = { firstState, firstState, firstState, secondState, secondState, secondState };
            string[] expectedOwners = { null, "owner", "owner", null, "owner", null };
            int[] expectedQueueSizes = { 0, 0, 1, 0, 0, 0 };
            for (int i = 0; i < checkpoints.Count; i++)
            {
                JObject checkpoint = JObject.Parse(checkpoints[i].Input);
                Assert.AreEqual(expectedStates[i], (string)checkpoint["state"], $"Entity state at checkpoint {i}.");
                Assert.AreEqual(expectedOwners[i], (string)checkpoint["lockedBy"], $"Lock owner at checkpoint {i}.");
                Assert.AreEqual(expectedQueueSizes[i], checkpoint["queue"]?.Count() ?? 0, $"Queue at checkpoint {i}.");
                EntityStatus status = JsonConvert.DeserializeObject<EntityStatus>(checkpoints[i].Status);
                Assert.IsTrue(status.EntityExists);
                Assert.AreEqual(expectedOwners[i], status.LockedBy);
                Assert.AreEqual(expectedQueueSizes[i], status.BacklogQueueSize);
            }
        }

        [TestMethod]
        public async Task ExtendedSession_OnlyLockAndUnlock_PreserveInitialState()
        {
            IReadOnlyList<OrchestrationState> checkpoints = await RunEntitySessionAsync(
                "existing-state",
                (request, metadata) => throw new AssertFailedException("A lock-only session must not invoke the worker."),
                // 1. Lock the existing entity.
                new[] { EntityLock() },
                // 2. Unlock without invoking the worker.
                new[] { EntityUnlock() });

            // 3. Verify both checkpoints preserve the state.
            Assert.AreEqual(2, checkpoints.Count);
            Assert.AreEqual("existing-state", (string)JObject.Parse(checkpoints[0].Input)["state"]);
            Assert.AreEqual("owner", (string)JObject.Parse(checkpoints[0].Input)["lockedBy"]);
            Assert.AreEqual("existing-state", (string)JObject.Parse(checkpoints[1].Input)["state"]);
            Assert.IsNull((string)JObject.Parse(checkpoints[1].Input)["lockedBy"]);
            Assert.IsTrue(JsonConvert.DeserializeObject<EntityStatus>(checkpoints[1].Status).EntityExists);
        }

        [TestMethod]
        public async Task ExtendedSession_WorkerDeletion_IsNotRestoredByUnlock()
        {
            int workerCalls = 0;
            IReadOnlyList<OrchestrationState> checkpoints = await RunEntitySessionAsync(
                "initial-state",
                (request, metadata) =>
                {
                    workerCalls++;
                    Assert.AreEqual(1, request.Operations.Count);
                    if (request.Operations[0].Operation == "set")
                    {
                        return "updated-state";
                    }

                    Assert.AreEqual("delete", request.Operations[0].Operation);
                    Assert.IsFalse(metadata.IncludeState);
                    return null;
                },
                // 1. Update the entity state.
                new[] { EntityOperation("set") },
                // 2. Acquire the lock.
                new[] { EntityLock() },
                // 3. Delete the state while holding the lock.
                new[] { EntityOperation("delete", "owner") },
                // 4. Unlock without invoking the worker.
                new[] { EntityUnlock() });

            Assert.AreEqual(2, workerCalls);
            Assert.AreEqual(4, checkpoints.Count);
            // 5. Verify deletion preserves the lock.
            JObject deleted = JObject.Parse(checkpoints[2].Input);
            Assert.IsNull((string)deleted["state"]);
            Assert.AreEqual("owner", (string)deleted["lockedBy"]);
            Assert.IsFalse(JsonConvert.DeserializeObject<EntityStatus>(checkpoints[2].Status).EntityExists);
            // 6. Verify unlocking does not restore deleted state.
            Assert.IsNull(checkpoints[3].Input, "Unlock must delete the empty scheduler, not resurrect old entity state.");
            EntityStatus finalStatus = JsonConvert.DeserializeObject<EntityStatus>(checkpoints[3].Status);
            Assert.IsFalse(finalStatus.EntityExists);
            Assert.IsNull(finalStatus.LockedBy);
        }

        static async Task<IReadOnlyList<OrchestrationState>> RunEntitySessionAsync(
            string initialState,
            Func<EntityBatchRequest, WorkItemMetadata, string> runWorker,
            params TaskMessage[][] batches)
        {
            using var service = new CapturingOrchestrationService();
            var session = new EntityTestSession(batches.Skip(1));
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add((context, next) =>
            {
                EntityBatchRequest request = context.GetProperty<EntityBatchRequest>();
                string state = runWorker(request, context.GetProperty<WorkItemMetadata>());
                context.SetProperty(new EntityBatchResult
                {
                    EntityState = state,
                    Results = request.Operations.Select(_ => new OperationResult()).ToList(),
                    Actions = new List<OperationAction>(),
                });
                return Task.CompletedTask;
            });
            var dispatcher = new TaskEntityDispatcher(
                service, new NameVersionObjectManager<TaskEntity>(), pipeline,
                new LogHelper(null), ErrorPropagationMode.UseFailureDetails, null);
            dispatcher.EntityBackendProperties.SupportsImplicitEntityDeletion = true;
            dispatcher.EntityBackendProperties.EntityMessageReorderWindow = TimeSpan.Zero;
            OrchestrationRuntimeState runtimeState = CreateEntityRuntimeState();
            runtimeState.ExecutionStartedEvent.Input = JsonConvert.SerializeObject(new { state = initialState });
            var workItem = new TaskOrchestrationWorkItem
            {
                InstanceId = runtimeState.OrchestrationInstance.InstanceId,
                OrchestrationRuntimeState = runtimeState,
                LockedUntilUtc = DateTime.MaxValue,
                NewMessages = batches[0],
                Session = session,
            };

            // Exercise the real session loop without starting background dispatchers or changing production visibility.
            MethodInfo processSession = typeof(TaskEntityDispatcher).GetMethod(
                "OnProcessWorkItemSessionAsync", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(processSession);
            await (Task)processSession.Invoke(dispatcher, new object[] { workItem });
            Assert.IsTrue(session.Ended);
            return service.Checkpoints;
        }

        static TaskMessage EntityOperation(string operation, string parent = null) => EntityMessage(new RequestMessage
        {
            Id = Guid.NewGuid(), Operation = operation, IsSignal = true, ParentInstanceId = parent,
        });

        static TaskMessage EntityLock() => EntityMessage(new RequestMessage
        {
            Id = Guid.NewGuid(), ParentInstanceId = "owner", LockSet = new[] { new EntityId("testentity", "test-key") },
        });

        static TaskMessage EntityUnlock() => EntityMessage(new ReleaseMessage { ParentInstanceId = "owner" });

        static TaskMessage EntityMessage(EntityMessage message) => new TaskMessage
        {
            OrchestrationInstance = new OrchestrationInstance { InstanceId = "@testentity@test-key" },
            Event = new EventRaisedEvent(-1, JsonConvert.SerializeObject(message))
            {
                Name = message is ReleaseMessage
                    ? EntityMessageEventNames.ReleaseMessageEventName
                    : EntityMessageEventNames.RequestMessageEventName,
            },
        };

        sealed class EntityTestSession : IOrchestrationSession
        {
            readonly Queue<TaskMessage[]> batches;

            public EntityTestSession(IEnumerable<TaskMessage[]> batches) => this.batches = new Queue<TaskMessage[]>(batches);

            public bool Ended { get; private set; }

            public Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(TaskOrchestrationWorkItem workItem)
                => Task.FromResult<IList<TaskMessage>>(this.batches.Count > 0 ? this.batches.Dequeue() : null);

            public Task EndSessionAsync()
            {
                this.Ended = true;
                return Task.CompletedTask;
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
            public List<OrchestrationState> Checkpoints { get; } = new List<OrchestrationState>();
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
                this.Checkpoints.Add(state);
                this.Messages = outboundMessages.Concat(orchestratorMessages).Concat(timerMessages).ToList();
                this.ContinuedAsNewMessage = continuedAsNewMessage;
                return Task.CompletedTask;
            }
        }
    }
}
