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
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Settings;
    using DurableTask.ServiceBus.Settings;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DispatcherTests
    {
        TaskHubClient client;
        TaskHubClient clientNoCompression;
        TaskHubWorker fakeTaskHub;
        TaskHubWorker taskHub;
        TaskHubWorker taskHubAlwaysCompression;
        TaskHubWorker taskHubLegacyCompression;
        TaskHubWorker taskHubNoCompression;

        public TestContext TestContext { get; set; }

        [TestInitialize]
        public void TestInitialize()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                this.client = TestHelpers.CreateTaskHubClient();

                this.taskHub = TestHelpers.CreateTaskHub();
                this.fakeTaskHub = TestHelpers.CreateTaskHub();

                this.taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
                this.taskHubLegacyCompression = TestHelpers.CreateTaskHubLegacyCompression();
                this.taskHubAlwaysCompression = TestHelpers.CreateTaskHubAlwaysCompression();
                this.clientNoCompression = TestHelpers.CreateTaskHubClientNoCompression();

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
                this.taskHubAlwaysCompression.StopAsync(true).Wait();
                this.taskHubLegacyCompression.StopAsync(true).Wait();
                this.fakeTaskHub.StopAsync(true).Wait();
                this.taskHub.orchestrationService.DeleteAsync(true).Wait();
            }
        }

        [TestMethod]
        public async Task NoCompressionToCompressionCompatTest()
        {
            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (CompressionCompatTest))
                .AddTaskActivities(typeof (SimpleTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (CompressionCompatTest), null);

            await TestHelpers.WaitForInstanceAsync(this.client, id, 60, false);
            await Task.Delay(5000);

            await this.taskHubNoCompression.StopAsync(true);

            await this.taskHub.AddTaskOrchestrations(typeof (CompressionCompatTest))
                .AddTaskActivities(typeof (SimpleTask))
                .StartAsync();

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(60), CancellationToken.None);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
        }

#if NETCOREAPP
        [TestCategory("DisabledInCI")]
#endif
        [TestMethod]
        public async Task MessageCompressionToNoCompressionTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            OrchestrationInstance id =
                await this.clientNoCompression.CreateOrchestrationInstanceAsync(typeof (MessageCompressionCompatTest), null);

            await Task.Delay(2000);

            await this.taskHub.StopAsync(true);

            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        [TestMethod]
        public async Task MessageNoCompressionToCompressionTest()
        {
            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (MessageCompressionCompatTest), null);

            await Task.Delay(2000);

            await this.taskHubNoCompression.StopAsync(true);

            await this.taskHub.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        // TODO: Identify why this test is failing and re-enable it: https://github.com/Azure/durabletask/issues/813
        ////[TestMethod]
        public async Task MessageLegacyCompressToAlwaysCompressTest()
        {
            await this.taskHubLegacyCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (MessageCompressionCompatTest), null);

            await Task.Delay(5000);

            await this.taskHubLegacyCompression.StopAsync(true);

            await this.taskHubAlwaysCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        // TODO: Identify why this test is failing and re-enable it: https://github.com/Azure/durabletask/issues/813
        ////[TestMethod]
        public async Task MessageAlwaysCompressToLegacyCompressTest()
        {
            await this.taskHubAlwaysCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (MessageCompressionCompatTest), null);

            await Task.Delay(5000);

            await this.taskHubAlwaysCompression.StopAsync(true);

            await this.taskHubLegacyCompression.AddTaskOrchestrations(typeof (MessageCompressionCompatTest))
                .AddTaskActivities(typeof (AlternatingPayloadTask))
                .StartAsync();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        #region Non Deterministic Orchestration Test

        [TestMethod]
        public async Task NonDeterministicOrchestrationTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (NonDeterministicOrchestration))
                .AddTaskActivities(typeof (FirstTask))
                .StartAsync();
            this.taskHub.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof (NonDeterministicOrchestration), "FAILTIMER");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("timer task"));
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));

            instance = await this.client.CreateOrchestrationInstanceAsync(typeof (NonDeterministicOrchestration), "FAILTASK");

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("activity task"));
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));


            instance = await this.client.CreateOrchestrationInstanceAsync(typeof (NonDeterministicOrchestration), "FAILSUBORCH");

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("sub-orchestration task"));
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));


            instance = await this.client.CreateOrchestrationInstanceAsync(typeof (NonDeterministicOrchestration), "PARENTORCH");

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));
        }

        [TestMethod]
        public async Task NonDeterminisActivitySubOrchestrationTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(NonDeterministicOrchestration), typeof(NonDeterministicActivityAndSubOrchestration))
                .AddTaskActivities(typeof(FirstTask), typeof(SecondTask))
                .StartAsync();
            this.taskHub.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(NonDeterministicActivityAndSubOrchestration), "FAILTASK");

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("activity task"));
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));


            instance = await this.client.CreateOrchestrationInstanceAsync(typeof(NonDeterministicOrchestration), "FAILSUBORCH");

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, instance, 60);
            Utils.UnusedParameter(isCompleted);

            state = await this.client.GetOrchestrationStateAsync(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("sub-orchestration task"));
            Assert.IsTrue(state.Output.Contains("Was a change made to the orchestrator code after this instance had already started running?"));
        }

        public sealed class FirstTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return input;
            }
        }

        public sealed class SecondTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return input;
            }
        }

        public class NonDeterministicOrchestration : TaskOrchestration<object, string>
        {
            public override async Task<object> RunTask(OrchestrationContext context, string input)
            {
                await context.ScheduleTask<string>(typeof (FirstTask), string.Empty);
                if (input == string.Empty)
                {
                    return null;
                }

                if (input == "PARENTORCH")
                {
                    try
                    {
                        await
                            context.CreateSubOrchestrationInstance<string>(typeof (NonDeterministicOrchestration),
                                "FAILTASK");
                    }
                    catch (Exception exception)
                    {
                        return exception.ToString();
                    }
                }

                if (!context.IsReplaying)
                {
                    if (input == "FAILTIMER")
                    {
                        await
                            context.CreateTimer(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(10)), string.Empty);
                    }
                    else if (input == "FAILTASK")
                    {
                        await context.ScheduleTask<string>(typeof (FirstTask), string.Empty);
                    }
                    else if (input == "FAILSUBORCH")
                    {
                        await
                            context.CreateSubOrchestrationInstance<string>(typeof (NonDeterministicOrchestration),
                                string.Empty);
                    }
                }

                return null;
            }
        }


        public class NonDeterministicActivityAndSubOrchestration : TaskOrchestration<object, string>
        {
            public override async Task<object> RunTask(OrchestrationContext context, string input)
            {
                await context.ScheduleTask<string>(typeof(FirstTask), string.Empty);
                if (input == string.Empty)
                {
                    return null;
                }

                if (input == "FAILTASK")
                {
                    await context.ScheduleTask<string>(context.IsReplaying ? typeof(FirstTask) : typeof(SecondTask), string.Empty);
                }
                else if (input == "FAILSUBORCH")
                {
                    await
                        context.CreateSubOrchestrationInstance<string>(context.IsReplaying ? typeof(NonDeterministicOrchestration) : typeof(NonDeterministicActivityAndSubOrchestration),
                            string.Empty);
                }

                return null;
            }
        }

        #endregion

        #region TypeMissingException Test

        [TestMethod]
        public async Task TypeMissingTest()
        {
            await this.fakeTaskHub.AddTaskOrchestrations(typeof (TypeMissingOrchestration))
                .AddTaskActivities(typeof (ComputeSumTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (TypeMissingOrchestration), "test");
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 10);
            Assert.IsFalse(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 10));
            
            // Bring up the correct orchestration worker
            await this.taskHub.AddTaskOrchestrations(typeof (TypeMissingOrchestration))
                .AddTaskActivities(typeof (TypeMissingTask))
                .StartAsync();

            isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 20);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("done", TypeMissingOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public sealed class ComputeSumTask : TaskActivity<int[], int>
        {
            protected override int Execute(TaskContext context, int[] chunk)
            {
                if (chunk == null || chunk.Length != 2)
                {
                    throw new ArgumentException("chunk");
                }

                Console.WriteLine("Compute Sum for " + chunk[0] + "," + chunk[1]);

                var sum = 0;
                int start = chunk[0];
                int end = chunk[1];
                for (int i = start; i <= end; i++)
                {
                    sum += i;
                }

                Console.WriteLine("Total Sum for Chunk '" + chunk[0] + "," + chunk[1] + "' is " + sum);

                return sum;
            }
        }

        public class TypeMissingOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string r = await context.ScheduleTask<string>(typeof (TypeMissingTask), input);

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = r;
                return Result;
            }
        }

        public sealed class TypeMissingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "done";
            }
        }

        #endregion

        #region Max messages in a single transaction test

        [TestMethod]
        public async Task MaxMessagesLimitTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (MaxMessagesLimitOrchestration))
                .AddTaskActivities(new MaxMessagesLimitTask())
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (MaxMessagesLimitOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 120));

            Assert.AreEqual(19900, MaxMessagesLimitOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class MaxMessagesLimitOrchestration : TaskOrchestration<long, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static long Result;
            public static long Sum;

            public override async Task<long> RunTask(OrchestrationContext context, string input)
            {
                Sum = 0;
                var results = new List<Task<long>>();

                for (var i = 0; i < 200; i++)
                {
                    Sum += Sum;
                    results.Add(context.ScheduleTask<long>(typeof (MaxMessagesLimitTask), i));
                }

                long[] arr = await Task.WhenAll(results.ToArray());
                long result = arr.Sum();

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = result;

                return result;
            }
        }

        public sealed class MaxMessagesLimitTask : TaskActivity<long, long>
        {
            protected override long Execute(TaskContext context, long num)
            {
                return num;
            }
        }

        #endregion

        #region Simple Async Activity Test

        [TestMethod]
        public async Task AsyncGreetingsTest()
        {
            AsyncGreetingsOrchestration.Result = null;

            await this.taskHub.AddTaskOrchestrations(typeof (AsyncGreetingsOrchestration))
                .AddTaskActivities(typeof (AsyncGetUserTask), typeof (AsyncSendGreetingTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (AsyncGreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", AsyncGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        public sealed class AsyncGetUserTask : AsyncTaskActivity<string, string>
        {
            protected override async Task<string> ExecuteAsync(TaskContext context, string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Gabbar";
                });
            }
        }

        public class AsyncGreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string user = await context.ScheduleTask<string>(typeof (AsyncGetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof (AsyncSendGreetingTask), user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        public sealed class AsyncSendGreetingTask : AsyncTaskActivity<string, string>
        {
            protected override async Task<string> ExecuteAsync(TaskContext context, string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Greeting send to " + input;
                });
            }
        }

        #endregion

        #region Simple Async Activity With Dynamic Proxy Test

        [TestMethod]
        public async Task AsyncDynamicProxyGreetingsTest()
        {
            AsyncDynamicGreetingsOrchestration.Result = null;
            AsyncDynamicGreetingsOrchestration.Result2 = null;

            await this.taskHub.AddTaskOrchestrations(typeof (AsyncDynamicGreetingsOrchestration))
                .AddTaskActivitiesFromInterface<IGreetings>(new GreetingsManager(), true)
                .AddTaskActivitiesFromInterface(typeof(IGreetings2), new GreetingsManager2(), true)
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (AsyncDynamicGreetingsOrchestration),
                null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", AsyncDynamicGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting NOT sent to Samba", AsyncDynamicGreetingsOrchestration.Result2,
                "Orchestration Result is wrong!!!");
        }

        public class AsyncDynamicGreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            public static string Result2;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var client = context.CreateClient<IGreetings>(true);
                var client2 = context.CreateClient<IGreetings2>(true);

                string user = await client.GetUserAsync(input);
                string greeting = await client.SendGreetingAsync(user);

                string user2 = await client2.GetUserAsync(input);
                string greeting2 = await client2.SendGreetingAsync(user2);

                await client2.SendGreetingsAgainAsync(user2);

                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;
                Result2 = greeting2;

                return greeting;
            }
        }

        public class GreetingsManager : IGreetings
        {
            public async Task<string> GetUserAsync(string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Gabbar";
                });
            }

            public async Task<string> SendGreetingAsync(string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Greeting send to " + input;
                });
            }
        }

        public class GreetingsManager2 : IGreetings2
        {
            public async Task<string> GetUserAsync(string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Samba";
                });
            }

            public async Task<string> SendGreetingAsync(string input)
            {
                return await Task.Factory.StartNew(() =>
                {
                    Thread.Sleep(4000);
                    return "Greeting NOT sent to " + input;
                });
            }

            public async Task SendGreetingsAgainAsync(string input)
            {
                await Task.Factory.StartNew(() => Thread.Sleep(4000));
            }
        }

        public interface IGreetings
        {
            Task<string> GetUserAsync(string input);

            Task<string> SendGreetingAsync(string input);
        }

        public interface IGreetings2
        {
            Task<string> GetUserAsync(string input);

            Task<string> SendGreetingAsync(string input);

            Task SendGreetingsAgainAsync(string input);
        }

        #endregion

        #region Session Size Exceeded Test

        [TestMethod]
        public async Task SessionExceededLimitTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (LargeSessionOrchestration))
                .AddTaskActivities(typeof (LargeSessionTaskActivity))
                .StartAsync();

            await SessionExceededLimitSubTestWithInputSize(100 * 1024);
            await SessionExceededLimitSubTestWithInputSize(200 * 1024);
            await SessionExceededLimitSubTestWithInputSize(300 * 1024);
            await SessionExceededLimitSubTestWithInputSize(500 * 1024);
            await SessionExceededLimitSubTestWithInputSize(1000 * 1024);
        }

        async Task SessionExceededLimitSubTestWithInputSize(int inputSize)
        {
            string input = TestHelpers.GenerateRandomString(inputSize);
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(LargeSessionOrchestration), new Tuple<string, int>(input, 2));

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Utils.UnusedParameter(isCompleted);
            await Task.Delay(20000);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual($"0:{input}-1:{input}-", LargeSessionOrchestration.Result);
        }

        [TestMethod]
        public async Task SessionNotExceededLimitTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (LargeSessionOrchestration))
                .AddTaskActivities(typeof (LargeSessionTaskActivity))
                .StartAsync();

            var input = "abc";

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (LargeSessionOrchestration), new Tuple<string, int>(input, 2));

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Utils.UnusedParameter(isCompleted);
            await Task.Delay(20000);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual($"0:{input}-1:{input}-", LargeSessionOrchestration.Result);
        }

        [TestMethod]
        public async Task SessionExceededLimitNoCompressionTest()
        {
            string input = TestHelpers.GenerateRandomString(150 * 1024);

            var serviceBusOrchestrationService = this.taskHub.orchestrationService as ServiceBusOrchestrationService;

            Debug.Assert(serviceBusOrchestrationService != null);

            serviceBusOrchestrationService.Settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState = false;

            await this.taskHub.AddTaskOrchestrations(typeof (LargeSessionOrchestration))
                .AddTaskActivities(typeof (LargeSessionTaskActivity))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (LargeSessionOrchestration), new Tuple<string, int>(input, 2));

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Utils.UnusedParameter(isCompleted);
            await Task.Delay(20000);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual($"0:{input}-1:{input}-", LargeSessionOrchestration.Result);
        }

        [TestMethod]
        public async Task MessageExceededLimitNoCompressionTest()
        {
            string input = TestHelpers.GenerateRandomString(150 * 1024);

            var serviceBusOrchestrationService = this.client.ServiceClient as ServiceBusOrchestrationService;

            Debug.Assert(serviceBusOrchestrationService != null);

            serviceBusOrchestrationService.Settings.MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Never,
                ThresholdInBytes = 0
            };

            await this.taskHub.AddTaskOrchestrations(typeof(LargeSessionOrchestration))
                .AddTaskActivities(typeof(LargeSessionTaskActivity))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(LargeSessionOrchestration), new Tuple<string, int>(input, 2));

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Utils.UnusedParameter(isCompleted);
            await Task.Delay(20000);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.AreEqual($"0:{input}-1:{input}-", LargeSessionOrchestration.Result);
        }

        [TestMethod]
        public async Task SessionExceededTerminationLimitTest()
        {
            string input = TestHelpers.GenerateRandomString(200 * 1024);

            await this.taskHub.AddTaskOrchestrations(typeof(LargeSessionOrchestration))
                .AddTaskActivities(typeof(LargeSessionTaskActivity))
                .StartAsync();

            var serviceBusOrchestrationService = this.taskHub.orchestrationService as ServiceBusOrchestrationService;

            Debug.Assert(serviceBusOrchestrationService != null);

            serviceBusOrchestrationService.Settings.SessionSettings = new ServiceBusSessionSettings(230 * 1024, 1024 * 1024);

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(LargeSessionOrchestration), new Tuple<string, int>(input, 10));
            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Utils.UnusedParameter(isCompleted);
            await Task.Delay(20000);

            OrchestrationState state = await this.client.GetOrchestrationStateAsync(id);
            Assert.AreEqual(OrchestrationStatus.Terminated, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("exceeded"));
        }

        public class LargeSessionOrchestration : TaskOrchestration<string, Tuple<string, int>>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, Tuple<string, int> input)
            {
                var sb = new StringBuilder();
                for (var i = 0; i < input.Item2; i++)
                {
                    string outputI = await context.ScheduleTask<string>(typeof(LargeSessionTaskActivity), $"{i}:{input.Item1}");
                    sb.Append($"{outputI}-");
                }

                Result = sb.ToString();
                return Result;
            }
        }

        public sealed class LargeSessionTaskActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return input;
            }
        }

        #endregion

        #region Compression Tests

#if NETCOREAPP
        [TestCategory("DisabledInCI")]
#endif
        [TestMethod]
        public async Task CompressionToNoCompressionCompatTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof (CompressionCompatTest))
                .AddTaskActivities(typeof (SimpleTask))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof (CompressionCompatTest), null);

            await TestHelpers.WaitForInstanceAsync(this.client, id, 60, false);
            await Task.Delay(5000);

            await this.taskHub.StopAsync(true);

            await this.taskHubNoCompression.AddTaskOrchestrations(typeof (CompressionCompatTest))
                .AddTaskActivities(typeof (SimpleTask))
                .StartAsync();

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
        }

        public sealed class AlternatingPayloadTask : TaskActivity<bool, byte[]>
        {
            protected override byte[] Execute(TaskContext context, bool largeOutput)
            {
                byte[] arr = {1, 2, 3, 4};

                if (largeOutput)
                {
                    arr = new byte[1600];

                    for (var i = 0; i < 100; i++)
                    {
                        Guid.NewGuid().ToByteArray().CopyTo(arr, i*16);
                    }
                }

                return arr;
            }
        }

        public class CompressionCompatTest : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                // ReSharper disable once RedundantAssignment
                string output = await context.ScheduleTask<string>(typeof (SimpleTask), "test");

                await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(10), "TimedOut");

                output = await context.ScheduleTask<string>(typeof (SimpleTask), "test");

                await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(10), "TimedOut");

                return output;
            }
        }

        public class MessageCompressionCompatTest : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                for (var i = 0; i < 10; i++)
                {
                    await context.ScheduleTask<byte[]>(typeof (AlternatingPayloadTask), (i%2 == 0));
                }

                return string.Empty;
            }
        }

        public sealed class SimpleTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "compressed";
            }
        }

        #endregion
    }
}