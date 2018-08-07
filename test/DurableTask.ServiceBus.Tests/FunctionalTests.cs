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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.ServiceBus.Settings;
    using DurableTask.Core.Tests;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class FunctionalTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;
        TaskHubWorker taskHubNoCompression;

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                this.client = TestHelpers.CreateTaskHubClient();
                this.taskHub = TestHelpers.CreateTaskHub();
                this.taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
                this.taskHub.orchestrationService.CreateAsync(true).Wait();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                this.taskHub.StopAsync(true).Wait();
                this.taskHubNoCompression.StopAsync(true).Wait();
                this.taskHub.orchestrationService.DeleteAsync(true).Wait();
            }
        }

        #region Generation Basic Test

        [TestMethod]
        public async Task GenerationBasicTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await this.taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task SessionStateDeleteTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await this.taskHub.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");

            Assert.AreEqual(0, await TestHelpers.GetOrchestratorQueueMessageCount());
        }

        [TestMethod]
        public async Task GenerationBasicNoCompressionTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationBasicOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class GenerationBasicOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await context.ScheduleTask<int>(typeof (GenerationBasicTask));
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count;
                return count;
            }
        }

        public sealed class GenerationBasicTask : TaskActivity<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int GenerationCount;

            protected override int Execute(TaskContext context, string input)
            {
                GenerationCount++;
                return GenerationCount;
            }
        }

        #endregion

        #region Generation with sub orchestration test 

        [TestMethod]
        public async Task GenerationSubTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            await this.taskHub.AddTaskOrchestrations(typeof (GenerationParentOrchestration), typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(4, GenerationParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(4, GenerationSubTask.GenerationCount, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task GenerationSubNoCompressionTest()
        {
            GenerationParentOrchestration.Result = 0;
            GenerationSubTask.GenerationCount = 0;

            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (GenerationParentOrchestration),
                typeof (GenerationChildOrchestration))
                .AddTaskActivities(new GenerationSubTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationParentOrchestration), 4);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(4, GenerationParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(4, GenerationSubTask.GenerationCount, "Orchestration Result is wrong!!!");
        }

        public class GenerationChildOrchestration : TaskOrchestration<int, int>
        {
            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await context.ScheduleTask<int>(typeof (GenerationSubTask));
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                return count;
            }
        }

        public class GenerationParentOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count =
                    await
                        context.CreateSubOrchestrationInstance<int>(typeof (GenerationChildOrchestration),
                            numberOfGenerations);

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count;
                return count;
            }
        }

        public sealed class GenerationSubTask : TaskActivity<string, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int GenerationCount;

            protected override int Execute(TaskContext context, string input)
            {
                GenerationCount++;
                return GenerationCount;
            }
        }

        #endregion

        #region Generation with SubOrchestrationInstance Failure Test

        [TestMethod]
        public async Task GenerationSubFailedTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (GenerationSubFailedParentOrchestration),
                typeof (GenerationSubFailedChildOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();
            this.taskHub.TaskOrchestrationDispatcher.IncludeDetails = true;

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            OrchestrationInstance id =
                await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationSubFailedParentOrchestration), true);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(1, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");

            GenerationSubFailedChildOrchestration.Count = 0;
            GenerationSubFailedParentOrchestration.Result = null;
            id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationSubFailedParentOrchestration), false);

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("Test", GenerationSubFailedParentOrchestration.Result, "Orchestration Result is wrong!!!");
            Assert.AreEqual(5, GenerationSubFailedChildOrchestration.Count, "Child Workflow Count invalid.");
        }

        public class GenerationSubFailedChildOrchestration : TaskOrchestration<string, int>
        {
            public static int Count;

            public override Task<string> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                if (numberOfGenerations == 1)
                {
                    throw new InvalidOperationException("Test");
                }

                Interlocked.Increment(ref Count);

                return Task.FromResult("done");
            }
        }

        public class GenerationSubFailedParentOrchestration : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                var results = new Task<string>[5];
                var numberOfChildGenerations = 3;
                try
                {
                    for (var i = 0; i < 5; i++)
                    {
                        Task<string> r =
                            context.CreateSubOrchestrationInstance<string>(
                                typeof (GenerationSubFailedChildOrchestration), numberOfChildGenerations);
                        if (waitForCompletion)
                        {
                            await r;
                        }

                        results[i] = r;
                    }

                    string[] data = await Task.WhenAll(results);
                    Result = string.Concat(data);
                }
                catch (SubOrchestrationFailedException e)
                {
                    Assert.IsInstanceOfType(e.InnerException, typeof (InvalidOperationException),
                        "Actual exception is not Invalid Operation Exception.");
                    Result = e.Message;
                }

                return Result;
            }
        }

        #endregion

        #region Generation Signal Test

        [TestMethod]
        public async Task GenerationSignalOrchestrationTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance {InstanceId = id.InstanceId};

            await Task.Delay(2*1000);
            await this.client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2*1000);
            GenerationSignalOrchestration.Signal.Reset();
            await this.client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2*1000);
            await this.client.RaiseEventAsync(signalId, "Count", "3"); // will be received by next generation
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2*1000);
            GenerationSignalOrchestration.Signal.Reset();
            await this.client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2*1000);
            await this.client.RaiseEventAsync(signalId, "Count", "5"); // will be received by next generation
            await this.client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await this.client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.Signal.Set();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, signalId, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("5", GenerationSignalOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class GenerationSignalOrchestration : TaskOrchestration<int, int>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            public static ManualResetEvent Signal = new ManualResetEvent(false);

            TaskCompletionSource<string> resumeHandle;

            public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
            {
                int count = await WaitForSignal();
                Signal.WaitOne();
                numberOfGenerations--;
                if (numberOfGenerations > 0)
                {
                    context.ContinueAsNew(numberOfGenerations);
                }

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = count.ToString();
                return count;
            }

            async Task<int> WaitForSignal()
            {
                this.resumeHandle = new TaskCompletionSource<string>();
                string data = await this.resumeHandle.Task;
                this.resumeHandle = null;
                return int.Parse(data);
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                Assert.AreEqual("Count", name, "Unknown signal received...");
                this.resumeHandle?.SetResult(input);
            }
        }

        #endregion

        #region Generation New Version Test

        [TestMethod]
        public async Task GenerationVersionTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V1", typeof (GenerationV1Orchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V2", typeof (GenerationV2Orchestration));

            var c3 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V3", typeof (GenerationV3Orchestration));

            await this.taskHub.AddTaskOrchestrations(c1, c2, c3)
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync("GenerationOrchestration", "V1", null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.IsTrue(GenerationV1Orchestration.WasRun);
            Assert.IsTrue(GenerationV2Orchestration.WasRun);
            Assert.IsTrue(GenerationV3Orchestration.WasRun);
        }

        public class GenerationV1Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V2", null);
                return Task.FromResult<object>(null);
            }
        }

        public class GenerationV2Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                context.ContinueAsNew("V3", null);
                return Task.FromResult<object>(null);
            }
        }

        public class GenerationV3Orchestration : TaskOrchestration<object, object>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static bool WasRun;

            public override Task<object> RunTask(OrchestrationContext context, object input)
            {
                WasRun = true;
                return Task.FromResult<object>(null);
            }
        }

        #endregion

        #region Tags Tests

        [TestMethod]
        public async Task TagsOrchestrationTest()
        {
            GenerationV1Orchestration.WasRun = false;
            GenerationV2Orchestration.WasRun = false;
            GenerationV3Orchestration.WasRun = false;

            var c1 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V1", typeof(GenerationV1Orchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V2", typeof(GenerationV2Orchestration));

            var c3 = new NameValueObjectCreator<TaskOrchestration>("GenerationOrchestration",
                "V3", typeof(GenerationV3Orchestration));

            await this.taskHub.AddTaskOrchestrations(c1, c2, c3)
                .StartAsync();

            // ReSharper disable once StringLiteralTypo
            const string TagName = "versiontag";
            const string TagValue = "sample_value";

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                "GenerationOrchestration",
                "V1",
                "TestInstance",
                null,
                new Dictionary<string, string>(1) { { TagName, TagValue } });

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1), CancellationToken.None);

            bool isCompleted = (state?.OrchestrationStatus == OrchestrationStatus.Completed);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, instance, 60));
            Assert.IsTrue(GenerationV1Orchestration.WasRun);
            Assert.IsTrue(GenerationV2Orchestration.WasRun);
            Assert.IsTrue(GenerationV3Orchestration.WasRun);
            IDictionary<string, string> returnedTags = state.Tags;

            Assert.IsTrue(returnedTags.TryGetValue(TagName, out string returnedValue));
            Assert.AreEqual(TagValue, returnedValue);
        }

        [TestMethod]
        public async Task TagsSubOrchestrationTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("ParentWorkflow",
                "V1", typeof(ParentWorkflow));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("ChildWorkflow",
                "V1", typeof(ChildWorkflow));

            await this.taskHub.AddTaskOrchestrations(c1, c2)
                .StartAsync();

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                "ParentWorkflow",
                "V1",
                "TestInstance",
                true,
                new Dictionary<string, string>(2) {
                    { ParentWorkflow.ParentTagName, ParentWorkflow.ParentTagValue },
                    { ParentWorkflow.SharedTagName, ParentWorkflow.ParentTagValue }
                });

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1), CancellationToken.None);

            bool isCompleted = (state?.OrchestrationStatus == OrchestrationStatus.Completed);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, instance, 60));
            Assert.AreEqual("Child completed.", ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            IDictionary<string, string> returnedTags = state.Tags;

            // Check for parent tag untouched
            Assert.IsTrue(returnedTags.TryGetValue(ParentWorkflow.ParentTagName, out string returnedValue));
            Assert.AreEqual(ParentWorkflow.ParentTagValue, returnedValue);
            // Check for shared tag on parent with parent value
            Assert.IsTrue(returnedTags.TryGetValue(ParentWorkflow.SharedTagName, out returnedValue));
            Assert.AreEqual(ParentWorkflow.ParentTagValue, returnedValue);
            
            // Get child state and check completion
            OrchestrationState childState = await this.client.GetOrchestrationStateAsync(ParentWorkflow.ChildWorkflowId);
            isCompleted = (childState?.OrchestrationStatus == OrchestrationStatus.Completed);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, instance, 60));

            returnedTags = childState.Tags;
            // Check for parent tag untouched
            Assert.IsTrue(returnedTags.TryGetValue(ParentWorkflow.ParentTagName, out returnedValue));
            Assert.AreEqual(ParentWorkflow.ParentTagValue, returnedValue);
            // Check for shared tag on with child value
            Assert.IsTrue(returnedTags.TryGetValue(ParentWorkflow.SharedTagName, out returnedValue));
            Assert.AreEqual(ParentWorkflow.ChildTagValue, returnedValue);
            // Check for child tag
            Assert.IsTrue(returnedTags.TryGetValue(ParentWorkflow.ChildTagName, out returnedValue));
            Assert.AreEqual(ParentWorkflow.ChildTagValue, returnedValue);
        }

        class ChildWorkflow : TaskOrchestration<string, int>
        {
            public override Task<string> RunTask(OrchestrationContext context, int input)
            {
                return Task.FromResult("Child completed.");
            }
        }

        class ParentWorkflow : TaskOrchestration<string, bool>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            public const string ParentTagName = "parent_tag";
            public const string SharedTagName = "shared_tag";
            public const string ChildTagName = "child_tag";
            public const string ParentTagValue = "parent's value";
            public const string ChildTagValue = "child's value";
            public const string ChildWorkflowId = "childtest";

            public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
            {
                Result = await context.CreateSubOrchestrationInstance<string>(
                    "ChildWorkflow",
                    "V1",
                    ChildWorkflowId,
                    1,
                    new Dictionary<string, string>(2)
                    {
                        { SharedTagName, ChildTagValue },
                        { ChildTagName, ChildTagValue }
                    });

                return Result;
            }
        }

        #endregion

        #region Concurrent Nodes Tests

        [TestMethod]
        public async Task MultipleConcurrentRoleStartsTestNoInitialHub()
        {
            // Make sure we cleanup we start from scratch
            await this.taskHub.StopAsync(true);
            await this.taskHub.orchestrationService.DeleteAsync();

            const int ConcurrentClientsAndHubs = 4;
            var rnd = new Random();
             
            var clients = new List<TaskHubClient>(ConcurrentClientsAndHubs);
            var workers = new List<TaskHubWorker>(ConcurrentClientsAndHubs);
            IList<Task> tasks = new List<Task>();
            for (var i = 0; i < ConcurrentClientsAndHubs; i++)
            {
                clients.Add(TestHelpers.CreateTaskHubClient());
                workers.Add(TestHelpers.CreateTaskHub(new ServiceBusOrchestrationServiceSettings
                {
                    TaskOrchestrationDispatcherSettings = { DispatcherCount = 4 },
                    TrackingDispatcherSettings = { DispatcherCount = 4 },
                    TaskActivityDispatcherSettings = { DispatcherCount = 4 }
                }));
                tasks.Add(workers[i].orchestrationService.CreateIfNotExistsAsync());
            }

            await Task.WhenAll(tasks);

            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            // ReSharper disable once UnusedVariable
            TaskHubWorker selectedHub = workers[(rnd.Next(ConcurrentClientsAndHubs))];
            TaskHubClient selectedClient = clients[(rnd.Next(ConcurrentClientsAndHubs))];

            tasks.Clear();
            for (var i = 0; i < ConcurrentClientsAndHubs; i++)
            {
                tasks.Add(workers[i].AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                    .AddTaskActivities(new GenerationBasicTask())
                    .StartAsync());
            }

            await Task.WhenAll(tasks);

            OrchestrationInstance instance = await selectedClient.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            OrchestrationState state = await selectedClient.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60), CancellationToken.None);
            Assert.IsNotNull(state);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus, TestHelpers.GetInstanceNotCompletedMessage(this.client, instance, 60));
            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
            await Task.WhenAll(workers.Select(worker => worker.StopAsync(true)));
        }

        #endregion

        #region Concurrent suborchestrations test

        [TestMethod]
        public async Task ConcurrentSubOrchestrationsTest()
        {
            var c1 = new NameValueObjectCreator<TaskOrchestration>("UberOrchestration",
                "V1", typeof(UberOrchestration));

            var c2 = new NameValueObjectCreator<TaskOrchestration>("SleeperSubOrchestration",
                "V1", typeof(SleeperSubOrchestration));

            await this.taskHub.AddTaskOrchestrations(c1, c2)
                .StartAsync();

            var numSubOrchestrations = 60;

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                "UberOrchestration",
                "V1",
                "TestInstance",
                new TestOrchestrationInput { Iterations = numSubOrchestrations, Payload = TestUtils.GenerateRandomString(90 * 1024) });

            // Waiting for 60 seconds guarantees that to pass the orchestrations must run in parallel
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, instance, 60));
            Assert.AreEqual(numSubOrchestrations, UberOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task ConcurrentSubOrchestrationsAsTopLevelOrchestrationsTest()
        {
            var c2 = new NameValueObjectCreator<TaskOrchestration>("SleeperSubOrchestration",
                "V1", typeof(SleeperSubOrchestration));

            await this.taskHub.AddTaskOrchestrations(c2)
                .StartAsync();

            var numSubOrchestrations = 60;

            var orchestrations = new List<Task<OrchestrationInstance>>();
            for (var i = 0; i < numSubOrchestrations; i++)
            {
                orchestrations.Add(this.client.CreateOrchestrationInstanceAsync(
                "SleeperSubOrchestration",
                "V1",
                $"{UberOrchestration.ChildWorkflowIdBase}_{i}",
                new TestOrchestrationInput { Iterations = 1, Payload = TestUtils.GenerateRandomString(8 * 1024) }));
            }

            IList<OrchestrationInstance> orchestrationInstances = (await Task.WhenAll(orchestrations)).ToList();

            IEnumerable<Task<OrchestrationState>> orchestrationResults = orchestrationInstances.Select(async instance =>
            {
                OrchestrationState result = await this.client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));
                return result;
            });

            OrchestrationState[] finalResults = await Task.WhenAll(orchestrationResults);
            Assert.AreEqual(numSubOrchestrations, finalResults.Count(status => status.OrchestrationStatus == OrchestrationStatus.Completed));
        }

        class TestOrchestrationInput
        {
            public int Iterations { get; set; }

            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            public string Payload { get; set; }
        }

        class SleeperSubOrchestration : TaskOrchestration<int, TestOrchestrationInput>
        {
            public override async Task<int> RunTask(OrchestrationContext context, TestOrchestrationInput input)
            {
                // ReSharper disable once UnusedVariable
                string retState = await context.CreateTimer(context.CurrentUtcDateTime + TimeSpan.FromSeconds(30), $"state: {input.Iterations}");

                return 1;
            }
        }

        class UberOrchestration : TaskOrchestration<int, TestOrchestrationInput>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static int Result;
            public const string ChildWorkflowIdBase = "childtest";

            public override async Task<int> RunTask(OrchestrationContext context, TestOrchestrationInput input)
            {
                var tasks = new List<Task<int>>();
                for (var i = 0; i < input.Iterations; i++)
                {
                    tasks.Add(context.CreateSubOrchestrationInstance<int>(
                        "SleeperSubOrchestration",
                        "V1",
                        $"{ChildWorkflowIdBase}_{i}",
                        new TestOrchestrationInput { Iterations = 1, Payload = TestUtils.GenerateRandomString(8 * 1024) }));
                }

                int[] data = await Task.WhenAll(tasks);

                Result = data.Sum();
                return Result;
            }
        }

        #endregion
    }
}