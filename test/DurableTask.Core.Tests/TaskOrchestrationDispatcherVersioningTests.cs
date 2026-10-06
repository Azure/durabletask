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
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Settings;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using static DurableTask.Core.Settings.VersioningSettings;

    [TestClass]
    public class TaskOrchestrationDispatcherVersioningTests
    {
        const string InternalName = "Internal.Orchestration";

        [TestMethod]
        [DataRow(VersionMatchStrategy.Strict, null, false)]
        [DataRow(VersionMatchStrategy.Strict, "", false)]
        [DataRow(VersionMatchStrategy.Strict, "1.0", true)]
        [DataRow(VersionMatchStrategy.Strict, "1.0.0", false)]
        [DataRow(VersionMatchStrategy.Strict, "2.0", false)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, null, true)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "", true)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "0.9", true)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "1.0", true)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "2.0", false)]
        [DataRow(VersionMatchStrategy.None, "", true)]
        [DataRow(VersionMatchStrategy.None, "2.0", true)]
        public async Task BusinessOrchestrationsRetainVersionPolicies(
            VersionMatchStrategy matchStrategy, string version, bool shouldExecute)
        {
            foreach (VersionFailureStrategy failureStrategy in Enum.GetValues(typeof(VersionFailureStrategy)))
            {
                foreach (bool configureExclusion in new[] { false, true })
                {
                    var settings = new VersioningSettings
                    {
                        Version = "1.0",
                        MatchStrategy = matchStrategy,
                        FailureStrategy = failureStrategy,
                    };
                    if (configureExclusion)
                    {
                        settings.ExcludedOrchestrationNames.Add(InternalName);
                    }

                    await AssertVersionPolicyAsync(settings, "Business.Orchestration", version, shouldExecute);
                }
            }
        }

        [TestMethod]
        [DataRow(InternalName, true)]
        [DataRow("internal.orchestration", false)]
        [DataRow("Internal.Orchestration.Child", false)]
        [DataRow("Internal", false)]
        [DataRow("Business.Orchestration", false)]
        public async Task OnlyExactExcludedUnversionedNamesExecute(string name, bool shouldExecute)
        {
            foreach (VersionFailureStrategy failureStrategy in Enum.GetValues(typeof(VersionFailureStrategy)))
            {
                foreach (string version in new[] { null, string.Empty })
                {
                    var settings = CreateSettings(failureStrategy);
                    await AssertVersionPolicyAsync(settings, name, version, shouldExecute);
                }
            }
        }

        [TestMethod]
        [DataRow(VersionMatchStrategy.Strict, "1.0", true)]
        [DataRow(VersionMatchStrategy.Strict, "2.0", false)]
        [DataRow(VersionMatchStrategy.Strict, " ", false)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "0.9", true)]
        [DataRow(VersionMatchStrategy.CurrentOrOlder, "2.0", false)]
        public async Task ExcludedNamesWithNonemptyVersionsRetainVersionPolicies(
            VersionMatchStrategy matchStrategy, string version, bool shouldExecute)
        {
            foreach (VersionFailureStrategy failureStrategy in Enum.GetValues(typeof(VersionFailureStrategy)))
            {
                var settings = CreateSettings(failureStrategy);
                settings.MatchStrategy = matchStrategy;
                await AssertVersionPolicyAsync(settings, InternalName, version, shouldExecute);
            }
        }

        [TestMethod]
        public async Task NullAndDefaultSettingsKeepVersioningDisabled()
        {
            foreach (VersioningSettings settings in new[] { null, new VersioningSettings() })
            {
                await AssertVersionPolicyAsync(settings, InternalName, string.Empty, shouldExecute: true);
                await AssertVersionPolicyAsync(settings, "Business.Orchestration", "2.0", shouldExecute: true);
            }
        }

        [TestMethod]
        public async Task ExclusionDoesNotBypassOrchestrationResolution()
        {
            using var service = new RecordingOrchestrationService();
            var dispatcher = new TestDispatcher(
                service, new NameVersionObjectManager<TaskOrchestration>(),
                new DispatchMiddlewarePipeline(), CreateSettings(VersionFailureStrategy.Fail));
            TaskOrchestrationWorkItem workItem = CreateWorkItem(InternalName, string.Empty);

            await Assert.ThrowsExceptionAsync<TypeMissingException>(() => dispatcher.ProcessAsync(workItem));

            Assert.AreEqual(0, service.AbandonCount);
            Assert.IsNull(service.CompletedState);
            Assert.AreEqual(string.Empty, workItem.OrchestrationRuntimeState.Version);
        }

        [TestMethod]
        [DataRow(null)]
        [DataRow("")]
        public async Task ExclusionAppliesToReplayAndContinueAsNewWithoutChangingVersion(string version)
        {
            using var service = new RecordingOrchestrationService();
            var orchestration = new TimerOrchestration();
            var manager = new NameVersionObjectManager<TaskOrchestration>();
            manager.Add(new NameValueObjectCreator<TaskOrchestration>(InternalName, version, orchestration));
            var pipeline = new DispatchMiddlewarePipeline();
            int middlewareCalls = 0;
            pipeline.Add((context, next) =>
            {
                OrchestrationRuntimeState state = context.GetProperty<OrchestrationRuntimeState>();
                Assert.AreEqual(InternalName, state.Name);
                Assert.AreEqual(version, state.Version);
                middlewareCalls++;
                return next();
            });
            var dispatcher = new TestDispatcher(service, manager, pipeline, CreateSettings(VersionFailureStrategy.Fail));
            TaskOrchestrationWorkItem workItem = CreateWorkItem(InternalName, version);

            await dispatcher.ProcessAsync(workItem);
            for (int generation = 0; generation < 3; generation++)
            {
                Assert.AreEqual(OrchestrationStatus.Running, service.CompletedState.OrchestrationStatus);
                string executionId = service.CompletedState.OrchestrationInstance.ExecutionId;
                TaskMessage timerMessage = service.TimerMessages.Single();
                Assert.IsInstanceOfType(timerMessage.Event, typeof(TimerFiredEvent));
                workItem = new TaskOrchestrationWorkItem
                {
                    InstanceId = workItem.InstanceId,
                    LockedUntilUtc = DateTime.MaxValue,
                    OrchestrationRuntimeState = new OrchestrationRuntimeState(service.CompletedRuntimeState.Events),
                    NewMessages = new[] { timerMessage },
                };

                await dispatcher.ProcessAsync(workItem);

                Assert.AreEqual(0, service.AbandonCount);
                Assert.AreEqual(version, service.CompletedState.Version);
                if (generation < 2)
                {
                    Assert.AreNotEqual(executionId, service.CompletedState.OrchestrationInstance.ExecutionId);
                }
            }

            Assert.AreEqual(OrchestrationStatus.Completed, service.CompletedState.OrchestrationStatus);
            Assert.AreEqual("2", service.CompletedState.Output);
            Assert.AreEqual(6, orchestration.ExecutionCount);
            Assert.AreEqual(6, middlewareCalls);
        }

        [TestMethod]
        [DataRow(VersionFailureStrategy.Reject)]
        [DataRow(VersionFailureStrategy.Fail)]
        public async Task ContinueAsNewToNonemptyVersionReappliesVersionPolicy(VersionFailureStrategy failureStrategy)
        {
            using var service = new RecordingOrchestrationService();
            var orchestration = new VersionChangingOrchestration();
            var manager = new NameVersionObjectManager<TaskOrchestration>();
            manager.Add(new NameValueObjectCreator<TaskOrchestration>(InternalName, string.Empty, orchestration));
            manager.Add(new NameValueObjectCreator<TaskOrchestration>(InternalName, "2.0", orchestration));
            var dispatcher = new TestDispatcher(service, manager, new DispatchMiddlewarePipeline(), CreateSettings(failureStrategy));
            TaskOrchestrationWorkItem workItem = CreateWorkItem(InternalName, string.Empty);

            await dispatcher.ProcessAsync(workItem);

            Assert.AreEqual(1, orchestration.ExecutionCount);
            Assert.AreEqual("2.0", workItem.OrchestrationRuntimeState.Version);
            AssertVersionFailure(service, failureStrategy);
        }

        static VersioningSettings CreateSettings(VersionFailureStrategy failureStrategy)
        {
            var settings = new VersioningSettings
            {
                Version = "1.0",
                MatchStrategy = VersionMatchStrategy.Strict,
                FailureStrategy = failureStrategy,
            };
            settings.ExcludedOrchestrationNames.Add(InternalName);
            return settings;
        }

        static async Task AssertVersionPolicyAsync(VersioningSettings settings, string name, string version, bool shouldExecute)
        {
            using var service = new RecordingOrchestrationService();
            var orchestration = new CompletingOrchestration();
            var manager = new NameVersionObjectManager<TaskOrchestration>();
            manager.Add(new NameValueObjectCreator<TaskOrchestration>(name, version, orchestration));
            var pipeline = new DispatchMiddlewarePipeline();
            int middlewareCalls = 0;
            pipeline.Add((context, next) =>
            {
                Assert.AreSame(orchestration, context.GetProperty<TaskOrchestration>());
                Assert.AreEqual(version, context.GetProperty<OrchestrationRuntimeState>().Version);
                middlewareCalls++;
                return next();
            });
            var dispatcher = new TestDispatcher(service, manager, pipeline, settings);
            TaskOrchestrationWorkItem workItem = CreateWorkItem(name, version);

            await dispatcher.ProcessAsync(workItem);

            Assert.AreEqual(shouldExecute ? 1 : 0, orchestration.ExecutionCount);
            Assert.AreEqual(shouldExecute ? 1 : 0, middlewareCalls);
            Assert.AreEqual(version, workItem.OrchestrationRuntimeState.Version);
            if (shouldExecute)
            {
                Assert.AreEqual(0, service.AbandonCount);
                Assert.AreEqual(OrchestrationStatus.Completed, service.CompletedState.OrchestrationStatus);
                Assert.AreEqual("0", service.CompletedState.Output);
            }
            else
            {
                AssertVersionFailure(service, settings.FailureStrategy);
            }
        }

        static void AssertVersionFailure(RecordingOrchestrationService service, VersionFailureStrategy failureStrategy)
        {
            if (failureStrategy == VersionFailureStrategy.Reject)
            {
                Assert.AreEqual(1, service.AbandonCount);
                Assert.IsNull(service.CompletedState);
            }
            else
            {
                Assert.AreEqual(0, service.AbandonCount);
                Assert.AreEqual(OrchestrationStatus.Failed, service.CompletedState.OrchestrationStatus);
                Assert.AreEqual("VersionMismatch", service.CompletedState.FailureDetails.ErrorType);
            }
        }

        static TaskOrchestrationWorkItem CreateWorkItem(string name, string version)
        {
            var instance = new OrchestrationInstance
            {
                InstanceId = Guid.NewGuid().ToString(),
                ExecutionId = Guid.NewGuid().ToString(),
            };
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
                        Event = new ExecutionStartedEvent(-1, "0")
                        {
                            Name = name,
                            Version = version,
                            OrchestrationInstance = instance,
                        },
                    },
                },
            };
        }

        sealed class TestDispatcher : TaskOrchestrationDispatcher
        {
            public TestDispatcher(
                IOrchestrationService service,
                INameVersionObjectManager<TaskOrchestration> manager,
                DispatchMiddlewarePipeline pipeline,
                VersioningSettings settings)
                : base(service, manager, pipeline, new LogHelper(null), ErrorPropagationMode.UseFailureDetails, settings, null)
            {
            }

            public Task<bool> ProcessAsync(TaskOrchestrationWorkItem workItem) => this.OnProcessWorkItemAsync(workItem);
        }

        sealed class RecordingOrchestrationService : LocalOrchestrationService, IOrchestrationService
        {
            public int AbandonCount { get; private set; }
            public OrchestrationState CompletedState { get; private set; }
            public OrchestrationRuntimeState CompletedRuntimeState { get; private set; }
            public IList<TaskMessage> TimerMessages { get; private set; }

            public new Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
            {
                this.AbandonCount++;
                return Task.CompletedTask;
            }

            public new Task CompleteTaskOrchestrationWorkItemAsync(
                TaskOrchestrationWorkItem workItem, OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage> outboundMessages, IList<TaskMessage> orchestratorMessages,
                IList<TaskMessage> timerMessages, TaskMessage continuedAsNewMessage, OrchestrationState state)
            {
                this.CompletedState = state;
                this.CompletedRuntimeState = newOrchestrationRuntimeState;
                this.TimerMessages = timerMessages;
                return Task.CompletedTask;
            }
        }

        sealed class CompletingOrchestration : TaskOrchestration<int, int>
        {
            public int ExecutionCount { get; private set; }

            public override Task<int> RunTask(OrchestrationContext context, int input)
            {
                this.ExecutionCount++;
                return Task.FromResult(input);
            }
        }

        sealed class TimerOrchestration : TaskOrchestration<int, int>
        {
            public int ExecutionCount { get; private set; }

            public override async Task<int> RunTask(OrchestrationContext context, int input)
            {
                this.ExecutionCount++;
                await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(1), input);
                if (input < 2)
                {
                    context.ContinueAsNew(input + 1);
                }

                return input;
            }
        }

        sealed class VersionChangingOrchestration : TaskOrchestration<int, int>
        {
            public int ExecutionCount { get; private set; }

            public override Task<int> RunTask(OrchestrationContext context, int input)
            {
                this.ExecutionCount++;
                context.ContinueAsNew("2.0", input);
                return Task.FromResult(input);
            }
        }
    }
}
