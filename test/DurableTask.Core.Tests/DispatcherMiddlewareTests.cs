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
#if !NET462 // for some reasons these tests are not discoverable on 1ES, leading to the test getting aborted. TODO: Needs investigation
#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;
    using System.Xml;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Console;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DispatcherMiddlewareTests
    {
        TaskHubWorker worker = null!;
        TaskHubClient client = null!;

        [TestInitialize]
        public void InitializeTests()
        {
            // configure logging so traces are emitted during tests.
            // This facilitates debugging when tests fail.
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole().SetMinimumLevel(LogLevel.Trace);
            });
            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service, loggerFactory);

            // We use `GetAwaiter().GetResult()` because otherwise this method will fail with:
            // "X has wrong signature. The method must be non-static, public, does not return a value and should not take any parameter."
            this.worker
                .AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration), typeof(ParentWorkflow), typeof(ChildWorkflow))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync().GetAwaiter().GetResult();

            this.client = new TaskHubClient(service);
        }

        [TestCleanup]
        public void CleanupTests()
        {
            // We use `GetAwaiter().GetResult()` because otherwise this method will fail with:
            // "X has wrong signature. The method must be non-static, public, does not return a value and should not take any parameter."
            this.worker!.StopAsync(true).GetAwaiter().GetResult();
        }

        [TestMethod]
        public async Task DispatchMiddlewareContextBuiltInProperties()
        {
            TaskOrchestration? orchestration = null;
            OrchestrationRuntimeState? state = null;
            OrchestrationInstance? instance1 = null;
            OrchestrationExecutionContext? executionContext1 = null;

            TaskActivity? activity = null;
            TaskScheduledEvent? taskScheduledEvent = null;
            OrchestrationInstance? instance2 = null;
            OrchestrationExecutionContext? executionContext2 = null;

            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                orchestration = context.GetProperty<TaskOrchestration>();
                state = context.GetProperty<OrchestrationRuntimeState>();
                instance1 = context.GetProperty<OrchestrationInstance>();
                executionContext1 = context.GetProperty<OrchestrationExecutionContext>();

                return next();
            });

            this.worker.AddActivityDispatcherMiddleware((context, next) =>
            {
                activity = context.GetProperty<TaskActivity>();
                taskScheduledEvent = context.GetProperty<TaskScheduledEvent>();
                instance2 = context.GetProperty<OrchestrationInstance>();
                executionContext2 = context.GetProperty<OrchestrationExecutionContext>();

                return next();
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.IsNotNull(orchestration);
            Assert.IsNotNull(state);
            Assert.IsNotNull(instance1);
            Assert.IsNotNull(executionContext1);

            Assert.IsNotNull(activity);
            Assert.IsNotNull(taskScheduledEvent);
            Assert.IsNotNull(instance2);
            Assert.IsNotNull(executionContext2);

            Assert.AreNotSame(instance1, instance2);
            Assert.AreEqual(instance1!.InstanceId, instance2!.InstanceId);
        }

        [TestMethod]
        public async Task OrchestrationDispatcherMiddlewareContextFlow()
        {
            StringBuilder? output = null;

            for (var i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
                {
                    output = context.GetProperty<StringBuilder>("output");
                    if (output == null)
                    {
                        output = new StringBuilder();
                        context.SetProperty("output", output);
                    }

                    // This is an async method and the output is out of the scope, output is used in closure
                    Debug.Assert(output != null);

                    output!.Append(value);
                    await next();
                    output.Append(value);
                });
            }

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each reply gets a new context, so the output should stay the same regardless of how
            // many replays an orchestration goes through.
            Assert.IsNotNull(output);
            Assert.AreEqual("01234567899876543210", output?.ToString());
        }

        [TestMethod]
        public async Task ActivityDispatcherMiddlewareContextFlow()
        {
            StringBuilder? output = null;

            for (var i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddActivityDispatcherMiddleware(async (context, next) =>
                {
                    output = context.GetProperty<StringBuilder>("output");
                    if (output == null)
                    {
                        output = new StringBuilder();
                        context.SetProperty("output", output);
                    }

                    // This is an async method and the output is out of the scope, output is used in closure
                    Debug.Assert(output != null);

                    output!.Append(value);
                    await next();
                    output.Append(value);
                });
            }

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each activity gets a new context, so the output should stay the same regardless of how
            // many activities an orchestration schedules (as long as there is at least one).
            Assert.IsNotNull(output);
            Assert.AreEqual("01234567899876543210", output?.ToString());
        }

        [TestMethod]
        public async Task EnsureOrchestrationDispatcherMiddlewareHasAccessToRuntimeState()
        {
            OrchestrationExecutionContext? executionContext = null;

            for (var i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
                {
                    executionContext = context.GetProperty<OrchestrationExecutionContext>();
                    await next();
                });
            }

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration)),
                NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration)),
                "testInstanceId",
                null,
                new Dictionary<string, string>
                {
                    { "Test", "Value" }
                });

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each activity gets a new context, so the output should stay the same regardless of how
            // many activities an orchestration schedules (as long as there is at least one).
            Assert.IsNotNull(executionContext);
            Assert.AreEqual("Value", executionContext?.OrchestrationTags?["Test"]);
        }

        /// <summary>
        /// Test to ensure <see cref="OrchestrationExecutionContext"/> supports DataContract serialization.
        /// </summary>
        [TestMethod]
        public void EnsureOrchestrationExecutionContextSupportsDataContractSerialization()
        {
            OrchestrationExecutionContext orchestrationExecutionContext = new OrchestrationExecutionContext
            {
                OrchestrationTags = new Dictionary<string, string> { { "Key", "Value" } }
            };

            DataContractSerializer dataContractSerializer = new DataContractSerializer(typeof(OrchestrationExecutionContext));

            StringBuilder stringBuilder = new StringBuilder();
            XmlWriter xmlWriter = XmlWriter.Create(stringBuilder);
            dataContractSerializer.WriteObject(xmlWriter, orchestrationExecutionContext);
            xmlWriter.Close();

            string writtenString = stringBuilder.ToString();

            Assert.AreEqual(
                "<?xml version=\"1.0\" encoding=\"utf-16\"?><OrchestrationExecutionContext xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://schemas.datacontract.org/2004/07/DurableTask.Core\"><OrchestrationTags xmlns:d2p1=\"http://schemas.microsoft.com/2003/10/Serialization/Arrays\"><d2p1:KeyValueOfstringstring><d2p1:Key>Key</d2p1:Key><d2p1:Value>Value</d2p1:Value></d2p1:KeyValueOfstringstring></OrchestrationTags></OrchestrationExecutionContext>",
                writtenString);

            StringReader stringReader = new StringReader(writtenString);
            XmlReader xmlReader = XmlReader.Create(stringReader);

            OrchestrationExecutionContext deserializedContext = (OrchestrationExecutionContext)dataContractSerializer.ReadObject(xmlReader)!;

            Assert.AreEqual("Value", deserializedContext.OrchestrationTags["Key"]);
        }

        [TestMethod]
        public async Task EnsureSubOrchestrationDispatcherMiddlewareHasAccessToRuntimeState()
        {
            ConcurrentBag<OrchestrationExecutionContext> capturedContexts = new ConcurrentBag<OrchestrationExecutionContext>(); 

            for (var i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
                {
                    capturedContexts.Add(context.GetProperty<OrchestrationExecutionContext>());
                    await next();
                });
            }

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                NameVersionHelper.GetDefaultName(typeof(ParentWorkflow)),
                NameVersionHelper.GetDefaultVersion(typeof(ParentWorkflow)),
                "testInstanceId",
                false,
                new Dictionary<string, string>
                {
                    { "Test", "Value" }
                });

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            foreach (OrchestrationExecutionContext context in capturedContexts)
            {
                Assert.IsNotNull(context);
                Assert.AreEqual("Value", context?.OrchestrationTags?["Test"]);
            }
        }

        [TestMethod]
        public async Task EnsureActivityDispatcherMiddlewareHasAccessToRuntimeState()
        {
            OrchestrationExecutionContext? executionContext = null;

            for (var i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddActivityDispatcherMiddleware(async (context, next) =>
                {
                    executionContext = context.GetProperty<OrchestrationExecutionContext>();
                    await next();
                });
            }

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                NameVersionHelper.GetDefaultName(typeof(SimplestGreetingsOrchestration)),
                NameVersionHelper.GetDefaultVersion(typeof(SimplestGreetingsOrchestration)),
                "testInstanceId",
                null,
                new Dictionary<string, string>
                {
                    { "Test", "Value" }
                });

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each activity gets a new context, so the output should stay the same regardless of how
            // many activities an orchestration schedules (as long as there is at least one).
            Assert.IsNotNull(executionContext);
            Assert.AreEqual("Value", executionContext?.OrchestrationTags?["Test"]);
        }

        [DataTestMethod]
        [DataRow(OrchestrationStatus.Completed)]
        [DataRow(OrchestrationStatus.Failed)]
        [DataRow(OrchestrationStatus.Terminated)]
        public async Task MockOrchestrationCompletion(OrchestrationStatus forcedStatus)
        {
            TaskOrchestration? orchestration = null;

            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                // Expecting a null here since "FakeName"/"FakeVersion" doesn't exist
                orchestration = context.GetProperty<TaskOrchestration>();

                context.SetProperty(new OrchestratorExecutionResult
                {
                    CustomStatus = "custom",
                    Actions = new[]
                    {
                        new OrchestrationCompleteOrchestratorAction
                        {
                            OrchestrationStatus = forcedStatus,
                        },
                    },
                });

                // don't call next() - we're short-circuiting the actual orchestration executor logic
                return Task.FromResult(0);
            });

            // We can safely use non-existing orchestrator names because the orchestration executor gets short-circuited.
            // This allows middleware the flexibility to invent their own dynamic orchestration logic.
            // A practical application of this is out-of-process orchestrator execution.
            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                name: "FakeName",
                version: "FakeVersion",
                input: null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 5);
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.AreEqual(forcedStatus, state.OrchestrationStatus);
            Assert.AreEqual("custom", state.Status);
            Assert.IsNull(orchestration, "Expected a null orchestration object in the middleware");
        }

        [TestMethod]
        public async Task MockActivityOrchestration()
        {
            // Orchestrator middleware mocks an orchestration that calls one activity
            // and returns the activity result as its own result
            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                OrchestrationRuntimeState state = context.GetProperty<OrchestrationRuntimeState>();

                if (state.NewEvents.OfType<ExecutionStartedEvent>().Any())
                {
                    // Manually schedule an activity execution
                    context.SetProperty(new OrchestratorExecutionResult
                    {
                        Actions = new[]
                        {
                            new ScheduleTaskOrchestratorAction
                            {
                                Name = "FakeActivity",
                                Version = "FakeActivityVersion",
                                Input = "SomeInput",
                            }
                        }
                    });
                }
                else
                {
                    // If we get here, it's because the activity completed
                    TaskCompletedEvent taskCompletedEvent = state.NewEvents.OfType<TaskCompletedEvent>().Single();

                    // We know the activity completed at this point
                    context.SetProperty(new OrchestratorExecutionResult
                    {
                        Actions = new[]
                        {
                            new OrchestrationCompleteOrchestratorAction
                            {
                                OrchestrationStatus = OrchestrationStatus.Completed,
                                Result = taskCompletedEvent?.Result,
                            },
                        },
                    });
                }


                // don't call next() - we're short-circuiting the actual orchestration executor logic
                return Task.FromResult(0);
            });

            // Activity middleware returns a result immediately
            this.worker.AddActivityDispatcherMiddleware((context, next) =>
            {
                TaskScheduledEvent taskScheduledEvent = context.GetProperty<TaskScheduledEvent>();

                // Use the activity parameters as the activity output
                string output = $"{taskScheduledEvent.Name},{taskScheduledEvent.Version},{taskScheduledEvent.Input}";

                context.SetProperty(new ActivityExecutionResult
                {
                    ResponseEvent = new TaskCompletedEvent(-1, taskScheduledEvent.EventId, output),
                });

                // don't call next() - we're short-circuiting the actual activity executor logic
                return Task.FromResult(0);
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                name: "FakeName",
                version: "FakeVersion",
                input: null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 5);
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual("FakeActivity,FakeActivityVersion,SomeInput", state.Output);
        }
    }
}
#endif