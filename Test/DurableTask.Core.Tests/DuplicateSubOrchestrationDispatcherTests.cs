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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DuplicateSubOrchestrationDispatcherTests
    {
        [DataTestMethod]
        [DataRow(ErrorPropagationMode.SerializeExceptions, false)]
        [DataRow(ErrorPropagationMode.UseFailureDetails, false)]
        [DataRow(ErrorPropagationMode.UseFailureDetails, true)]
        public async Task RawDuplicateActionsFailBeforeSendingAnyMessages(ErrorPropagationMode errorMode, bool splitMessages)
        {
            using var service = new RecordingService { MaxMessages = splitMessages ? 1 : (int?)null };
            using var worker = new TaskHubWorker(service)
            {
                FailOnDuplicateSubOrchestrationInstanceIds = true,
                ErrorPropagationMode = errorMode,
            };
            worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                var state = context.GetProperty<OrchestrationRuntimeState>();
                context.SetProperty(new OrchestratorExecutionResult
                {
                    Actions = state.Name == "parent"
                        ? new OrchestratorAction[]
                        {
                            new ScheduleTaskOrchestratorAction { Id = 0, Name = "activity" },
                            new CreateTimerOrchestratorAction { Id = 1, FireAt = DateTime.UtcNow.AddHours(1) },
                            new SendEventOrchestratorAction
                            {
                                Id = 2,
                                Instance = new OrchestrationInstance { InstanceId = "recipient" },
                                EventName = "signal",
                            },
                            Child(3, "same-child"),
                            Child(4, "same-child"),
                        }
                        : Array.Empty<OrchestratorAction>(),
                });
                return Task.CompletedTask;
            });

            await worker.StartAsync();
            try
            {
                Assert.IsTrue(worker.TaskOrchestrationDispatcher.FailOnDuplicateSubOrchestrationInstanceIds);
                worker.FailOnDuplicateSubOrchestrationInstanceIds = false;
                Assert.IsTrue(worker.TaskOrchestrationDispatcher.FailOnDuplicateSubOrchestrationInstanceIds, "The option is captured at startup.");
                var client = new TaskHubClient(service);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync("parent", "", null);
                Checkpoint checkpoint = await service.FirstCheckpointAsync(instance.InstanceId);
                Assert.AreEqual(OrchestrationStatus.Failed, checkpoint.Status);
                Assert.AreEqual(0, checkpoint.Messages.Length);
                Assert.AreEqual(0, checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
                Assert.AreEqual(0, checkpoint.Events.OfType<TaskScheduledEvent>().Count());
                Assert.AreEqual(0, checkpoint.Events.OfType<TimerCreatedEvent>().Count());
                Assert.AreEqual(0, checkpoint.Events.OfType<EventSentEvent>().Count());
                ExecutionCompletedEvent completed = checkpoint.Events.OfType<ExecutionCompletedEvent>().Single();
                Assert.AreEqual(4, completed.EventId);
                AssertFailure(completed.FailureDetails);
                StringAssert.Contains(completed.Result, instance.InstanceId);
                StringAssert.Contains(completed.Result, "same-child");
                OrchestrationState persisted = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
                Assert.IsNotNull(persisted);
                Assert.AreEqual(OrchestrationStatus.Failed, persisted.OrchestrationStatus);
                AssertFailure(persisted.FailureDetails);
            }
            finally
            {
                await worker.StopAsync(true);
            }
        }

        [TestMethod]
        public async Task DefaultOffPreservesDuplicateStarts()
        {
            using var service = new RecordingService();
            using var worker = new TaskHubWorker(service);
            Assert.IsFalse(worker.FailOnDuplicateSubOrchestrationInstanceIds);
            worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                context.SetProperty(new OrchestratorExecutionResult
                {
                    Actions = context.GetProperty<OrchestrationRuntimeState>().Name == "parent"
                        ? new[] { Child(0, "same-child"), Child(1, "same-child") }
                        : Array.Empty<OrchestratorAction>(),
                });
                return Task.CompletedTask;
            });
            await worker.StartAsync();
            try
            {
                Assert.IsFalse(worker.TaskOrchestrationDispatcher.FailOnDuplicateSubOrchestrationInstanceIds);
                var client = new TaskHubClient(service);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync("parent", "", null);
                Checkpoint checkpoint = await service.FirstCheckpointAsync(instance.InstanceId);
                Assert.AreEqual(OrchestrationStatus.Running, checkpoint.Status);
                Assert.AreEqual(2, checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
                var starts = checkpoint.Messages.Select(m => m.Event).OfType<ExecutionStartedEvent>().ToArray();
                Assert.AreEqual(2, starts.Length);
                Assert.AreEqual(starts[0].OrchestrationInstance.InstanceId, starts[1].OrchestrationInstance.InstanceId);
                Assert.AreNotEqual(starts[0].OrchestrationInstance.ExecutionId, starts[1].OrchestrationInstance.ExecutionId);
                Assert.AreEqual(0, checkpoint.Events.OfType<ExecutionCompletedEvent>().Count());
            }
            finally
            {
                await worker.StopAsync(true);
            }
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public async Task PendingChildIsCheckedOnReplayAndResume(bool resume, bool completeFirstChild)
        {
            using var service = new RecordingService { ForwardCompletions = false };
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add((context, next) =>
            {
                bool alreadyStarted = context.GetProperty<OrchestrationRuntimeState>()
                    .Events.OfType<SubOrchestrationInstanceCreatedEvent>().Any();
                context.SetProperty(new OrchestratorExecutionResult
                {
                    Actions = new[] { Child(alreadyStarted ? 1 : 0, "same-child") },
                });
                return Task.CompletedTask;
            });
            var dispatcher = new TestDispatcher(service, pipeline);
            TaskOrchestrationWorkItem workItem = NewWorkItem();
            Assert.IsFalse(await dispatcher.ProcessAsync(workItem));
            Assert.IsNotNull(workItem.Cursor);

            Advance(workItem, resume, completeFirstChild
                ? (HistoryEvent)new SubOrchestrationInstanceCompletedEvent(-1, 0, "done")
                : new EventRaisedEvent(-1, null) { Name = "probe" });
            Assert.AreEqual(!completeFirstChild, await dispatcher.ProcessAsync(workItem));
            Checkpoint checkpoint = service.Checkpoints.Last();
            if (completeFirstChild)
            {
                Assert.AreEqual(OrchestrationStatus.Running, checkpoint.Status);
                Assert.AreEqual(1, checkpoint.Messages.Length);
                Assert.AreEqual(2, checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
            }
            else
            {
                Assert.AreEqual(OrchestrationStatus.Failed, checkpoint.Status);
                Assert.AreEqual(0, checkpoint.Messages.Length);
                Assert.AreEqual(1, checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
                AssertFailure(checkpoint.Events.OfType<ExecutionCompletedEvent>().Single().FailureDetails);
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task ContinueAsNewUsesOnlyNewGenerationHistory(bool duplicateInNewGeneration)
        {
            using var service = new RecordingService { ForwardCompletions = false };
            var pipeline = new DispatchMiddlewarePipeline();
            pipeline.Add((context, next) =>
            {
                OrchestrationRuntimeState state = context.GetProperty<OrchestrationRuntimeState>();
                context.SetProperty(new OrchestratorExecutionResult
                {
                    Actions = state.Input == "next"
                        ? (duplicateInNewGeneration
                            ? new[] { Child(0, "same-child"), Child(1, "same-child") }
                            : new[] { Child(0, "same-child") })
                        : new OrchestratorAction[]
                        {
                            new OrchestrationCompleteOrchestratorAction
                            {
                                Id = 1, OrchestrationStatus = OrchestrationStatus.ContinuedAsNew, Result = "next",
                            },
                        },
                });
                return Task.CompletedTask;
            });
            var dispatcher = new TestDispatcher(service, pipeline);
            TaskOrchestrationWorkItem workItem = NewWorkItem();
            workItem.OrchestrationRuntimeState = new OrchestrationRuntimeState(new[]
            {
                workItem.NewMessages[0].Event,
                new SubOrchestrationInstanceCreatedEvent(0) { InstanceId = "same-child" },
            });
            workItem.NewMessages = new[] { Message(workItem, new EventRaisedEvent(-1, null) { Name = "continue" }) };
            string previousExecutionId = workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId;

            Assert.AreEqual(duplicateInNewGeneration, await dispatcher.ProcessAsync(workItem));
            Checkpoint checkpoint = service.Checkpoints.Single();
            Assert.AreNotEqual(previousExecutionId, workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(duplicateInNewGeneration ? OrchestrationStatus.Failed : OrchestrationStatus.Running, checkpoint.Status);
            Assert.AreEqual(duplicateInNewGeneration ? 0 : 1, checkpoint.Messages.Length);
            Assert.AreEqual(duplicateInNewGeneration ? 0 : 1, checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task SuspendedOrchestrationRejectsConflictWhenResumed(bool extendedSession)
        {
            using var service = new RecordingService { ForwardCompletions = false };
            var manager = new NameVersionObjectManager<TaskOrchestration>();
            manager.Add(new DefaultObjectCreator<TaskOrchestration>(typeof(EventDrivenParent)));
            var dispatcher = new TestDispatcher(service, new DispatchMiddlewarePipeline(), manager);
            TaskOrchestrationWorkItem workItem = NewWorkItem(NameVersionHelper.GetDefaultName(typeof(EventDrivenParent)));
            Assert.IsFalse(await dispatcher.ProcessAsync(workItem));
            Advance(workItem, extendedSession, new ExecutionSuspendedEvent(-1, "pause"));
            Assert.IsFalse(await dispatcher.ProcessAsync(workItem));
            Advance(workItem, extendedSession, new EventRaisedEvent(-1, null) { Name = "start-another" });
            Assert.IsFalse(await dispatcher.ProcessAsync(workItem));
            Checkpoint suspended = service.Checkpoints.Last();
            Assert.AreEqual(OrchestrationStatus.Suspended, suspended.Status);
            Assert.AreEqual(0, suspended.Messages.Length);

            Advance(workItem, extendedSession, new ExecutionResumedEvent(-1, "resume"));
            Assert.IsTrue(await dispatcher.ProcessAsync(workItem));
            Checkpoint failed = service.Checkpoints.Last();
            Assert.AreEqual(OrchestrationStatus.Failed, failed.Status);
            Assert.AreEqual(0, failed.Messages.Length);
            Assert.AreEqual(1, failed.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
        }

        [TestMethod]
        public async Task FailureNotifiesParentAndDoesNotRetryGuardedOrchestration()
        {
            using var service = new RecordingService();
            using var worker = new TaskHubWorker(service)
            {
                FailOnDuplicateSubOrchestrationInstanceIds = true,
                ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails,
            };
            worker.AddTaskOrchestrations(typeof(RetryingParent));
            worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                string name = context.GetProperty<OrchestrationRuntimeState>().Name;
                if (name == "guarded-child" || name == "child")
                {
                    context.SetProperty(new OrchestratorExecutionResult
                    {
                        Actions = name == "guarded-child"
                            ? new[] { Child(0, "grandchild"), Child(1, "grandchild") }
                            : Array.Empty<OrchestratorAction>(),
                    });
                    return Task.CompletedTask;
                }

                return next();
            });
            await worker.StartAsync();
            try
            {
                var client = new TaskHubClient(service);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(RetryingParent), null);
                OrchestrationState result = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
                Assert.IsNotNull(result);
                Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
                Assert.AreEqual("\"caught non-retriable failure\"", result.Output);
                Checkpoint guarded = await service.FirstCheckpointAsync("guarded-child-id");
                Assert.AreEqual(OrchestrationStatus.Failed, guarded.Status);
                Assert.AreEqual(1, guarded.Messages.Length);
                var notification = guarded.Messages[0].Event as SubOrchestrationInstanceFailedEvent;
                Assert.IsNotNull(notification);
                Assert.AreEqual(0, notification.TaskScheduledId);
                Assert.AreEqual(instance.InstanceId, guarded.Messages[0].OrchestrationInstance.InstanceId);
                AssertFailure(notification.FailureDetails);
                Assert.AreEqual(0, guarded.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
                Checkpoint parent = service.Checkpoints.Last(c => c.InstanceId == instance.InstanceId);
                Assert.AreEqual(1, parent.Events.OfType<SubOrchestrationInstanceCreatedEvent>().Count());
                Assert.AreEqual(0, parent.Events.OfType<TimerCreatedEvent>().Count(), "A non-retriable failure must not schedule a retry.");
            }
            finally
            {
                await worker.StopAsync(true);
            }
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task SequentialAndRetryCallsCanReuseAnInstanceId(bool retryAfterFailure)
        {
            using var service = new RecordingService();
            using var worker = new TaskHubWorker(service)
            {
                FailOnDuplicateSubOrchestrationInstanceIds = true,
                ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails,
            };
            int attempts = 0;
            worker.AddTaskOrchestrations(typeof(ReuseParent));
            worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                if (context.GetProperty<OrchestrationRuntimeState>().Name != "reusable-child")
                {
                    return next();
                }

                bool fail = Interlocked.Increment(ref attempts) == 1 && retryAfterFailure;
                context.SetProperty(new OrchestratorExecutionResult
                {
                    Actions = new[]
                    {
                        new OrchestrationCompleteOrchestratorAction
                        {
                            Id = 0,
                            OrchestrationStatus = fail ? OrchestrationStatus.Failed : OrchestrationStatus.Completed,
                            Result = fail ? "transient failure" : "\"ok\"",
                            FailureDetails = fail ? new FailureDetails("Transient", "transient failure", null, null, false) : null,
                        },
                    },
                });
                return Task.CompletedTask;
            });
            await worker.StartAsync();
            try
            {
                var client = new TaskHubClient(service);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(ReuseParent), retryAfterFailure);
                OrchestrationState result = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(10));
                Assert.IsNotNull(result);
                Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
                Assert.AreEqual("\"ok\"", result.Output);
                Assert.AreEqual(2, attempts);
                Checkpoint checkpoint = service.Checkpoints.Last(c => c.InstanceId == instance.InstanceId);
                var starts = checkpoint.Events.OfType<SubOrchestrationInstanceCreatedEvent>().ToArray();
                Assert.AreEqual(2, starts.Length);
                Assert.AreEqual(starts[0].InstanceId, starts[1].InstanceId);
                Assert.AreNotEqual(starts[0].EventId, starts[1].EventId);
                Assert.AreEqual(retryAfterFailure ? 1 : 0, checkpoint.Events.OfType<SubOrchestrationInstanceFailedEvent>().Count());
            }
            finally
            {
                await worker.StopAsync(true);
            }
        }

        static void AssertFailure(FailureDetails failure)
        {
            Assert.IsNotNull(failure);
            Assert.AreEqual("DuplicateSubOrchestrationInstanceId", failure.ErrorType);
            Assert.IsTrue(failure.IsNonRetriable);
        }

        static TaskOrchestrationWorkItem NewWorkItem(string name = "parent")
        {
            var instance = new OrchestrationInstance { InstanceId = "parent-id", ExecutionId = Guid.NewGuid().ToString("N") };
            return new TaskOrchestrationWorkItem
            {
                InstanceId = instance.InstanceId,
                LockedUntilUtc = DateTime.MaxValue,
                OrchestrationRuntimeState = new OrchestrationRuntimeState(),
                NewMessages = new[]
                {
                    new TaskMessage
                    {
                        OrchestrationInstance = instance,
                        Event = new ExecutionStartedEvent(-1, null) { Name = name, Version = "", OrchestrationInstance = instance },
                    },
                },
            };
        }

        static TaskMessage Message(TaskOrchestrationWorkItem workItem, HistoryEvent historyEvent)
            => new TaskMessage { OrchestrationInstance = workItem.OrchestrationRuntimeState.OrchestrationInstance, Event = historyEvent };

        static void Advance(TaskOrchestrationWorkItem workItem, bool resume, HistoryEvent historyEvent)
        {
            if (resume)
            {
                workItem.OrchestrationRuntimeState.NewEvents.Clear();
            }
            else
            {
                workItem.OrchestrationRuntimeState = new OrchestrationRuntimeState(workItem.OrchestrationRuntimeState.Events);
                workItem.Cursor = null;
            }

            workItem.NewMessages = new[] { Message(workItem, historyEvent) };
        }

        static CreateSubOrchestrationAction Child(int taskId, string instanceId)
            => new CreateSubOrchestrationAction { Id = taskId, InstanceId = instanceId, Name = "child", Version = "" };

        public class EventDrivenParent : TaskOrchestration<string, string>
        {
            readonly TaskCompletionSource<bool> signal = new TaskCompletionSource<bool>();

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                Task<string> first = context.CreateSubOrchestrationInstance<string>("child", "", "same-child", null);
                await this.signal.Task;
                Task<string> second = context.CreateSubOrchestrationInstance<string>("child", "", "same-child", null);
                await Task.WhenAll(first, second);
                return "unreachable";
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
                => this.signal.TrySetResult(true);
        }

        public class RetryingParent : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                try
                {
                    return await context.CreateSubOrchestrationInstanceWithRetry<string>(
                        "guarded-child", "", "guarded-child-id", new RetryOptions(TimeSpan.FromSeconds(1), 3), null);
                }
                catch (SubOrchestrationFailedException exception) when (exception.FailureDetails?.IsNonRetriable == true)
                {
                    return "caught non-retriable failure";
                }
            }
        }

        public class ReuseParent : TaskOrchestration<string, bool>
        {
            public override async Task<string> RunTask(OrchestrationContext context, bool retryAfterFailure)
            {
                if (retryAfterFailure)
                {
                    return await context.CreateSubOrchestrationInstanceWithRetry<string>(
                        "reusable-child", "", "reused-id", new RetryOptions(TimeSpan.FromMilliseconds(1), 3), null);
                }

                await context.CreateSubOrchestrationInstance<string>("reusable-child", "", "reused-id", null);
                return await context.CreateSubOrchestrationInstance<string>("reusable-child", "", "reused-id", null);
            }
        }

        sealed class TestDispatcher : TaskOrchestrationDispatcher
        {
            public TestDispatcher(
                RecordingService service,
                DispatchMiddlewarePipeline pipeline,
                INameVersionObjectManager<TaskOrchestration> manager = null)
                : base(service, manager ?? new NameVersionObjectManager<TaskOrchestration>(), pipeline,
                    new LogHelper(null), ErrorPropagationMode.UseFailureDetails, null, null)
            {
                this.FailOnDuplicateSubOrchestrationInstanceIds = true;
            }

            public Task<bool> ProcessAsync(TaskOrchestrationWorkItem workItem) => this.OnProcessWorkItemAsync(workItem);
        }

        sealed class Checkpoint
        {
            public string InstanceId { get; set; }

            public OrchestrationStatus Status { get; set; }

            public HistoryEvent[] Events { get; set; }

            public TaskMessage[] Messages { get; set; }
        }

        sealed class RecordingService : LocalOrchestrationService, IOrchestrationService
        {
            readonly ConcurrentDictionary<string, TaskCompletionSource<Checkpoint>> firstCheckpoints =
                new ConcurrentDictionary<string, TaskCompletionSource<Checkpoint>>();

            public ConcurrentQueue<Checkpoint> Checkpoints { get; } = new ConcurrentQueue<Checkpoint>();

            public bool ForwardCompletions { get; set; } = true;

            public int? MaxMessages { get; set; }

            public new bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
                => this.MaxMessages.HasValue
                    ? currentMessageCount >= this.MaxMessages.Value
                    : base.IsMaxMessageCountExceeded(currentMessageCount, runtimeState);

            public async Task<Checkpoint> FirstCheckpointAsync(string instanceId)
            {
                Task<Checkpoint> task = this.GetCheckpointSource(instanceId).Task;
                Assert.AreSame(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))), "No checkpoint was committed.");
                return await task;
            }

            public new async Task CompleteTaskOrchestrationWorkItemAsync(
                TaskOrchestrationWorkItem workItem,
                OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage> outboundMessages,
                IList<TaskMessage> orchestratorMessages,
                IList<TaskMessage> timerMessages,
                TaskMessage continuedAsNewMessage,
                OrchestrationState state)
            {
                var checkpoint = new Checkpoint
                {
                    InstanceId = workItem.InstanceId,
                    Status = newOrchestrationRuntimeState.OrchestrationStatus,
                    Events = newOrchestrationRuntimeState.Events.ToArray(),
                    Messages = outboundMessages.Concat(orchestratorMessages).Concat(timerMessages).ToArray(),
                };
                this.Checkpoints.Enqueue(checkpoint);
                if (this.ForwardCompletions)
                {
                    await base.CompleteTaskOrchestrationWorkItemAsync(
                        workItem, newOrchestrationRuntimeState, outboundMessages, orchestratorMessages, timerMessages, continuedAsNewMessage, state);
                }
                this.GetCheckpointSource(workItem.InstanceId).TrySetResult(checkpoint);
            }

            TaskCompletionSource<Checkpoint> GetCheckpointSource(string instanceId)
                => this.firstCheckpoints.GetOrAdd(
                    instanceId, _ => new TaskCompletionSource<Checkpoint>(TaskCreationOptions.RunContinuationsAsynchronously));
        }
    }
}
