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

namespace DurableTask.Emulator.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the core dtfx via the emulator local orchestration service and client provider
    /// </summary>
    [TestClass]
    public class EmulatorFunctionalTests
    {
        public TestContext TestContext { get; set; }

        // Disabled while investigating CI timeout issues
        ////[TestMethod]
        public async Task MockOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result, "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        // Disabled while investigating CI timeout issues
        ////[TestMethod]
        public async Task MockRecreateOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            await Assert.ThrowsExceptionAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null));

            await Assert.ThrowsExceptionAsync<OrchestrationAlreadyExistsException>(() => client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new[] { OrchestrationStatus.Completed }));

            SimplestGreetingsOrchestration.Result = String.Empty;

            OrchestrationInstance id2 = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), id.InstanceId, null, new OrchestrationStatus[] { });
            result = await client.WaitForOrchestrationAsync(id2, TimeSpan.FromSeconds(30), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result on Re-Create is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockTimerTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "6");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.IsTrue(sw.Elapsed.Seconds > 6);

            Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockRepeatTimerTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GreetingsRepeatWaitOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GreetingsRepeatWaitOrchestration), "1");

            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), new CancellationToken());
            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);

            Assert.IsTrue(sw.Elapsed.Seconds > 3);

            Assert.AreEqual("Greeting send to Gabbar", GreetingsRepeatWaitOrchestration.Result,
                "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        // Disabled while investigating CI timeout issues
        ////[TestMethod]
        public async Task MockGenerationTest()
        {
            GenerationBasicOrchestration.Result = 0;
            GenerationBasicTask.GenerationCount = 0;

            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GenerationBasicOrchestration))
                .AddTaskActivities(new GenerationBasicTask())
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(GenerationBasicOrchestration), 4);

            // strip out the eid so we wait for the latest one always
            var masterId = new OrchestrationInstance { InstanceId = id.InstanceId };

            OrchestrationState result1 = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), CancellationToken.None);

            OrchestrationState result2 = await client.WaitForOrchestrationAsync(masterId, TimeSpan.FromSeconds(20), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.ContinuedAsNew, result1.OrchestrationStatus);
            Assert.AreEqual(OrchestrationStatus.Completed, result2.OrchestrationStatus);

            Assert.AreEqual(4, GenerationBasicOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        // Disabled while investigating CI timeout issues
        ////[TestMethod]
        public async Task MockSubOrchestrationTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(ParentWorkflow), typeof(ChildWorkflow))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), true);

            OrchestrationState result = await client.WaitForOrchestrationAsync(id,
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            ParentWorkflow.Result = string.Empty;

            id = await client.CreateOrchestrationInstanceAsync(typeof(ParentWorkflow), false);

            result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual(
                "Child '0' completed.Child '1' completed.Child '2' completed.Child '3' completed.Child '4' completed.",
                ParentWorkflow.Result, "Orchestration Result is wrong!!!");

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task MockRaiseEventTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            var client = new TaskHubClient(orchestrationService);

            await worker.AddTaskOrchestrations(typeof(GenerationSignalOrchestration))
                .StartAsync();

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(
                typeof(GenerationSignalOrchestration), 5);

            var signalId = new OrchestrationInstance { InstanceId = id.InstanceId };

            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "1");
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "2");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "3"); // will be received by next generation
            GenerationSignalOrchestration.Signal.Set();

            await Task.Delay(2 * 500);
            GenerationSignalOrchestration.Signal.Reset();
            await client.RaiseEventAsync(signalId, "Count", "4");
            await Task.Delay(2 * 500);
            await client.RaiseEventAsync(signalId, "Count", "5"); // will be received by next generation
            await client.RaiseEventAsync(signalId, "Count", "6"); // lost
            await client.RaiseEventAsync(signalId, "Count", "7"); // lost
            GenerationSignalOrchestration.Signal.Set();

            OrchestrationState result = await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = id.InstanceId },
                TimeSpan.FromSeconds(40), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
            Assert.AreEqual("5", GenerationSignalOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        ////[TestMethod]
        ////public async Task TerminateOrchestrationTest()
        ////{
        ////    var orchestrationService = new LocalOrchestrationService();

        ////    await orchestrationService.StartAsync();

        ////    TaskHubWorker worker = new TaskHubWorker(orchestrationService, "test", new TaskHubWorkerSettings());

        ////    worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
        ////        .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
        ////        .Start();

        ////    TaskHubClient client = new TaskHubClient(orchestrationService, "test", new TaskHubClientSettings());

        ////    OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), "60");

        ////    OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(10), new CancellationToken());
        ////    Assert.AreEqual("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result,
        ////        "Orchestration Result is wrong!!!");

        ////    await orchestrationService.StopAsync();
        ////}

        [TestMethod]
        public async Task MockOrchestrationTagsTest()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);

            var orchestrationType = typeof(SimplestGreetingsOrchestration);
            await worker.AddTaskOrchestrations(orchestrationType)
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);

            var instanceId = Guid.NewGuid().ToString("N");
            var tags = new Dictionary<string, string>
            {
                { "tag1", "value1" },
                { "tag2", "value2" },
            };

            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), instanceId, input: null, tags);
            Assert.AreEqual(instanceId, id.InstanceId);

            OrchestrationState state = await client.GetOrchestrationStateAsync(id);
            AssertTagsEqual(tags, state.Tags);

            state = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), CancellationToken.None);
            AssertTagsEqual(tags, state.Tags);

            await worker.StopAsync(true);
        }

        [TestMethod]
        public async Task RegisterOrchestrationTasksFromInterface_InterfaceUsingInheritanceGenericsMethodOverloading_OrchestrationSuccess()
        {
            var orchestrationService = new LocalOrchestrationService();

            var worker = new TaskHubWorker(orchestrationService);
            await worker.AddTaskOrchestrations(typeof(TestInheritedTasksOrchestration))
                .AddTaskActivitiesFromInterfaceV2<IInheritedTestOrchestrationTasksA>(new InheritedTestOrchestrationTasksA())
                .AddTaskActivitiesFromInterfaceV2(typeof(IInheritedTestOrchestrationTasksB<int, string>), new InheritedTestOrchestrationTasksB())
                .StartAsync();

            var client = new TaskHubClient(orchestrationService);
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(TestInheritedTasksOrchestration),
                null);

            var state = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);

            Assert.AreEqual(InheritedTestOrchestrationTasksA.BumbleResult, TestInheritedTasksOrchestration.BumbleResultA);
            Assert.AreEqual(InheritedTestOrchestrationTasksA.WobbleResult, TestInheritedTasksOrchestration.WobbleResultA);
            Assert.AreEqual(InheritedTestOrchestrationTasksA.DerivedTaskResult, TestInheritedTasksOrchestration.DerivedTaskResultA);
            Assert.AreEqual(InheritedTestOrchestrationTasksA.JuggleResult, TestInheritedTasksOrchestration.JuggleResultA);

            Assert.AreEqual(InheritedTestOrchestrationTasksB.BumbleResult, TestInheritedTasksOrchestration.BumbleResultB);
            Assert.AreEqual(InheritedTestOrchestrationTasksB.WobbleResult, TestInheritedTasksOrchestration.WobbleResultB);
            Assert.AreEqual(InheritedTestOrchestrationTasksB.OverloadedWobbleResult1, TestInheritedTasksOrchestration.OverloadedWobbleResult1B);
            Assert.AreEqual(InheritedTestOrchestrationTasksB.OverloadedWobbleResult2, TestInheritedTasksOrchestration.OverloadedWobbleResult2B);
            Assert.AreEqual(InheritedTestOrchestrationTasksB.JuggleResult, TestInheritedTasksOrchestration.JuggleResultB);
        }

        private static void AssertTagsEqual(IDictionary<string, string> expectedTags, IDictionary<string, string> actualTags)
        {
            Assert.IsNotNull(actualTags);
            Assert.AreEqual(expectedTags.Count, actualTags.Count);
            Assert.IsTrue(expectedTags.All(tag => actualTags.TryGetValue(tag.Key, out var value) && value == tag.Value));
        }

        // base interface without generic type parameters
        public interface IBaseTestOrchestrationTasks
        {
            Task<int> Juggle(int toss, bool withFlair);
        }

        // generic type base interface inheriting non-generic type with same name
        public interface IBaseTestOrchestrationTasks<TIn, TOut> : IBaseTestOrchestrationTasks
        {
            Task<TOut> Bumble(TIn fumble, bool likeAKlutz);
            Task<TOut> Wobble(TIn jiggle, bool withGusto);
        }

        // interface with derived task
        public interface IInheritedTestOrchestrationTasksA : IBaseTestOrchestrationTasks<string, string>
        {
            Task<string> DerivedTask(int i);
        }

        // interface with overloaded method
        public interface IInheritedTestOrchestrationTasksB<TIn, TOut> : IBaseTestOrchestrationTasks<TIn, TOut>
        {
            // this method overloads methods from both inherited interface and this interface
            Task<TOut> Wobble(TIn name);
            Task<string> Wobble(string id, TIn subId);
        }

        public class InheritedTestOrchestrationTasksA : IInheritedTestOrchestrationTasksA
        {
            public const string BumbleResult = nameof(Bumble) + "-A";
            public const string WobbleResult = nameof(Wobble) + "-A";
            public const string DerivedTaskResult = nameof(DerivedTask) + "-A";
            public const int JuggleResult = 419;

            public Task<string> Bumble(string fumble, bool likeAKlutz)
            {
                return Task.FromResult(BumbleResult);
            }

            public Task<string> Wobble(string jiggle, bool withGusto)
            {
                return Task.FromResult(WobbleResult);
            }

            public Task<string> DerivedTask(int i)
            {
                return Task.FromResult(DerivedTaskResult);
            }

            public Task<int> Juggle(int toss, bool withFlair)
            {
                return Task.FromResult(JuggleResult);
            }
        }

        public class InheritedTestOrchestrationTasksB : IInheritedTestOrchestrationTasksB<int, string>
        {
            public const string BumbleResult = nameof(Bumble) + "-B";
            public const string WobbleResult = nameof(Wobble) + "-B";
            public const string OverloadedWobbleResult1 = nameof(Wobble) + "-B-overloaded-1";
            public const string OverloadedWobbleResult2 = nameof(Wobble) + "-B-overloaded-2";
            public const int JuggleResult = 420;

            public Task<string> Bumble(int fumble, bool likeAKlutz)
            {
                return Task.FromResult(BumbleResult);
            }

            public Task<string> Wobble(int jiggle, bool withGusto)
            {
                return Task.FromResult(WobbleResult);
            }

            public Task<string> Wobble(string id, int subId)
            {
                return Task.FromResult(OverloadedWobbleResult1);
            }

            public Task<string> Wobble(int id)
            {
                return Task.FromResult(OverloadedWobbleResult2);
            }

            public Task<int> Juggle(int toss, bool withFlair)
            {
                return Task.FromResult(JuggleResult);
            }
        }

        public class TestInheritedTasksOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string BumbleResultA;
            public static string WobbleResultA;
            public static string DerivedTaskResultA;
            public static int JuggleResultA;
            public static string BumbleResultB;
            public static string WobbleResultB;
            public static string OverloadedWobbleResult1B;
            public static string OverloadedWobbleResult2B;
            public static int JuggleResultB;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var tasksA = context.CreateClientV2<IInheritedTestOrchestrationTasksA>();
                var tasksB = context.CreateClientV2<IInheritedTestOrchestrationTasksB<int, string>>();

                BumbleResultA = await tasksA.Bumble(string.Empty, false);
                WobbleResultA = await tasksA.Wobble(string.Empty, false);
                DerivedTaskResultA = await tasksA.DerivedTask(0);
                JuggleResultA = await tasksA.Juggle(1, true);

                BumbleResultB = await tasksB.Bumble(0, false);
                WobbleResultB = await tasksB.Wobble(-1, false);
                OverloadedWobbleResult1B = await tasksB.Wobble("a", 2);
                OverloadedWobbleResult2B = await tasksB.Wobble(1);
                JuggleResultB = await tasksB.Juggle(1, true);

                return string.Empty;
            }
        }
    }
}