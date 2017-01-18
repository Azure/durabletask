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


namespace FrameworkUnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Test;
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
                client = TestHelpers.CreateTaskHubClient();

                taskHub = TestHelpers.CreateTaskHub();
                fakeTaskHub = TestHelpers.CreateTaskHub();

                taskHubNoCompression = TestHelpers.CreateTaskHubNoCompression();
                taskHubLegacyCompression = TestHelpers.CreateTaskHubLegacyCompression();
                taskHubAlwaysCompression = TestHelpers.CreateTaskHubAlwaysCompression();
                clientNoCompression = TestHelpers.CreateTaskHubClientNoCompression();

                taskHub.DeleteHub();
                taskHub.CreateHubIfNotExists();
            }
        }

        [TestCleanup]
        public void TestCleanup()
        {
            if (!TestContext.TestName.Contains("TestHost"))
            {
                taskHub.Stop(true);
                taskHubNoCompression.Stop();
                taskHubAlwaysCompression.Stop();
                taskHubLegacyCompression.Stop();
                fakeTaskHub.Stop(true);
                taskHub.DeleteHub();
            }
        }

        [TestMethod]
        public void ResultTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof(ResultTestOrchestration))
                .AddTaskActivities(typeof(SimpleTask2))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(ResultTestOrchestration), null);

            TestHelpers.WaitForInstance(client, id, 60, true);

            var state = client.GetOrchestrationState(id);

            taskHubNoCompression.Stop(true);

            // Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
        }


        [TestMethod]
        public void NoCompressionToCompressionCompatTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof(CompressionCompatTest))
                .AddTaskActivities(typeof(SimpleTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(CompressionCompatTest), null);

            TestHelpers.WaitForInstance(client, id, 60, false);
            Thread.Sleep(5000);

            taskHubNoCompression.Stop(true);

            taskHub.AddTaskOrchestrations(typeof(CompressionCompatTest))
                .AddTaskActivities(typeof(SimpleTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
        }

        [TestMethod]
        public void MessageCompressionToNoCompressionTest()
        {
            taskHub.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            OrchestrationInstance id =
                clientNoCompression.CreateOrchestrationInstance(typeof(MessageCompressionCompatTest), null);

            Thread.Sleep(2000);

            taskHub.Stop(true);

            taskHubNoCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        [TestMethod]
        public void MessageNoCompressionToCompressionTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(MessageCompressionCompatTest), null);

            Thread.Sleep(2000);

            taskHubNoCompression.Stop(true);

            taskHub.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        [TestMethod]
        public void MessageLegacyCompressToAlwaysCompressTest()
        {
            taskHubLegacyCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(MessageCompressionCompatTest), null);

            Thread.Sleep(5000);

            taskHubLegacyCompression.Stop(true);

            taskHubAlwaysCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        [TestMethod]
        public void MessageAlwaysCompressToLegacyCompressTest()
        {
            taskHubAlwaysCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(MessageCompressionCompatTest), null);

            Thread.Sleep(5000);

            taskHubAlwaysCompression.Stop(true);

            taskHubLegacyCompression.AddTaskOrchestrations(typeof(MessageCompressionCompatTest))
                .AddTaskActivities(typeof(AlternatingPayloadTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        #region Non Deterministic Orchestration Test

        [TestMethod]
        public void NonDeterministicOrchestrationTest()
        {
            taskHub.AddTaskOrchestrations(typeof(NonDeterministicOrchestration))
                .AddTaskActivities(typeof(FirstTask))
                .Start();
            taskHub.TaskActivityDispatcher.IncludeDetails = true;

            OrchestrationInstance instance = client.CreateOrchestrationInstance(typeof(NonDeterministicOrchestration),
                "FAILTIMER");

            bool isCompleted = TestHelpers.WaitForInstance(client, instance, 60);
            OrchestrationState state = client.GetOrchestrationState(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("TimerCreatedEvent"));

            instance = client.CreateOrchestrationInstance(typeof(NonDeterministicOrchestration), "FAILTASK");

            isCompleted = TestHelpers.WaitForInstance(client, instance, 60);
            state = client.GetOrchestrationState(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("TaskScheduledEvent"));

            instance = client.CreateOrchestrationInstance(typeof(NonDeterministicOrchestration), "FAILSUBORCH");

            isCompleted = TestHelpers.WaitForInstance(client, instance, 60);
            state = client.GetOrchestrationState(instance);
            Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("SubOrchestrationInstanceCreatedEvent"));

            instance = client.CreateOrchestrationInstance(typeof(NonDeterministicOrchestration), "PARENTORCH");

            isCompleted = TestHelpers.WaitForInstance(client, instance, 60);
            state = client.GetOrchestrationState(instance);
            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("Non-Deterministic workflow detected"));
        }

        public sealed class FirstTask : TaskActivity<string, string>
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
                await context.ScheduleTask<string>(typeof(FirstTask), string.Empty);
                if (input == string.Empty)
                {
                    return null;
                }
                if (input == "PARENTORCH")
                {
                    try
                    {
                        await
                            context.CreateSubOrchestrationInstance<string>(typeof(NonDeterministicOrchestration),
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
                        await context.ScheduleTask<string>(typeof(FirstTask), string.Empty);
                    }
                    else if (input == "FAILSUBORCH")
                    {
                        await
                            context.CreateSubOrchestrationInstance<string>(typeof(NonDeterministicOrchestration),
                                string.Empty);
                    }
                }
                return null;
            }
        }

        #endregion

        #region TypeMissingException Test

        [TestMethod]
        public void TypeMissingTest()
        {
            fakeTaskHub.AddTaskOrchestrations(typeof(TypeMissingOrchestration))
                .AddTaskActivities(typeof(ComputeSumTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(TypeMissingOrchestration), "test");
            bool isCompleted = TestHelpers.WaitForInstance(client, id, 10);
            Assert.IsFalse(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 10));

            // Bring up the correct orchestration worker
            taskHub.AddTaskOrchestrations(typeof(TypeMissingOrchestration))
                .AddTaskActivities(typeof(TypeMissingTask))
                .Start();

            isCompleted = TestHelpers.WaitForInstance(client, id, 20);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
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
                int sum = 0;
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
                string r = await context.ScheduleTask<string>(typeof(TypeMissingTask), input);

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
        public void MaxMessagesLimitTest()
        {
            taskHub.AddTaskOrchestrations(typeof(MaxMessagesLimitOrchestration))
                .AddTaskActivities(new MaxMessagesLimitTask())
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(MaxMessagesLimitOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 120));

            Assert.AreEqual(19900, MaxMessagesLimitOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public class MaxMessagesLimitOrchestration : TaskOrchestration<long, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static long Result;
            public static long Sum;

            public override async Task<long> RunTask(OrchestrationContext context, string input)
            {
                long result = 0;
                Sum = 0;
                var results = new List<Task<long>>();
                for (int i = 0; i < 200; i++)
                {
                    Sum += Sum;
                    results.Add(context.ScheduleTask<long>(typeof(MaxMessagesLimitTask), i));
                }

                long[] arr = await Task.WhenAll(results.ToArray());
                foreach (long r in arr)
                {
                    result += r;
                }

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
        public void AsyncGreetingsTest()
        {
            AsyncGreetingsOrchestration.Result = null;

            taskHub.AddTaskOrchestrations(typeof(AsyncGreetingsOrchestration))
                .AddTaskActivities(typeof(AsyncGetUserTask), typeof(AsyncSendGreetingTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(AsyncGreetingsOrchestration), null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", AsyncGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task AsyncGreetingsTestHostTest()
        {
            AsyncGreetingsOrchestration.Result = null;

            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof(AsyncGreetingsOrchestration))
                .AddTaskActivities(new AsyncGetUserTask(), new AsyncSendGreetingTask());

            string result = await testHost.RunOrchestration<string>(typeof(AsyncGreetingsOrchestration), null);

            Assert.AreEqual("Greeting send to Gabbar", AsyncGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting send to Gabbar", result, "Orchestration Result is wrong!!!");
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
                string user = await context.ScheduleTask<string>(typeof(AsyncGetUserTask));
                string greeting = await context.ScheduleTask<string>(typeof(AsyncSendGreetingTask), user);
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
        public void AsyncDynamicProxyGreetingsTest()
        {
            AsyncDynamicGreetingsOrchestration.Result = null;
            AsyncDynamicGreetingsOrchestration.Result2 = null;

            taskHub.AddTaskOrchestrations(typeof(AsyncDynamicGreetingsOrchestration))
                .AddTaskActivitiesFromInterface<IGreetings>(new GreetingsManager(), true)
                .AddTaskActivitiesFromInterface<IGreetings2>(new GreetingsManager2(), true)
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(AsyncDynamicGreetingsOrchestration),
                null);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", AsyncDynamicGreetingsOrchestration.Result,
                "Orchestration Result is wrong!!!");
            Assert.AreEqual("Greeting NOT sent to Samba", AsyncDynamicGreetingsOrchestration.Result2,
                "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task AsyncDynamicProxyGreetingsTestHostTest()
        {
            AsyncDynamicGreetingsOrchestration.Result = null;
            AsyncDynamicGreetingsOrchestration.Result2 = null;

            var testHost = new OrchestrationTestHost();
            testHost.AddTaskOrchestrations(typeof(AsyncDynamicGreetingsOrchestration))
                .AddTaskActivitiesFromInterface<IGreetings2>(new GreetingsManager2(), true)
                .AddTaskActivitiesFromInterface<IGreetings>(new GreetingsManager(), true);

            string result = await testHost.RunOrchestration<string>(typeof(AsyncDynamicGreetingsOrchestration), null);
            Assert.AreEqual("Greeting send to Gabbar", result, "Orchestration Result is wrong!!!");
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
        public void SessionExceededLimitTest()
        {
            taskHub.AddTaskOrchestrations(typeof(LargeSessionOrchestration))
                .AddTaskActivities(typeof(LargeSessionTaskActivity))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(LargeSessionOrchestration), 50);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60, true);

            Thread.Sleep(20000);

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Terminated, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("exceeded"));
        }

        [TestMethod]
        public void SessionNotExceededLimitTest()
        {
            taskHub.AddTaskOrchestrations(typeof(LargeSessionOrchestration))
                .AddTaskActivities(typeof(LargeSessionTaskActivity))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(LargeSessionOrchestration), 15);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 90, true);

            Thread.Sleep(20000);

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
        }

        [TestMethod]
        public void SessionExceededLimitNoCompressionTest()
        {
            taskHubNoCompression.AddTaskOrchestrations(typeof(LargeSessionOrchestration))
                .AddTaskActivities(typeof(LargeSessionTaskActivity))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(LargeSessionOrchestration), 15);

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60, true);

            Thread.Sleep(20000);

            OrchestrationState state = client.GetOrchestrationState(id);

            Assert.AreEqual(OrchestrationStatus.Terminated, state.OrchestrationStatus);
            Assert.IsTrue(state.Output.Contains("exceeded"));
        }

        public class LargeSessionOrchestration : TaskOrchestration<string, int>
        {
            public override async Task<string> RunTask(OrchestrationContext context, int input)
            {
                for (int i = 0; i < input; i++)
                {
                    await context.ScheduleTask<byte[]>(typeof(LargeSessionTaskActivity));
                }

                return string.Empty;
            }
        }

        public sealed class LargeSessionTaskActivity : TaskActivity<string, byte[]>
        {
            protected override byte[] Execute(TaskContext context, string input)
            {
                var arr = new byte[16000];

                for (int i = 0; i < 1000; i++)
                {
                    Guid.NewGuid().ToByteArray().CopyTo(arr, i * 16);
                }

                return arr;
            }
        }

        #endregion

        #region Compression Tests

        [TestMethod]
        public void CompressionToNoCompressionCompatTest()
        {
            taskHub.AddTaskOrchestrations(typeof(CompressionCompatTest))
                .AddTaskActivities(typeof(SimpleTask))
                .Start();

            OrchestrationInstance id = client.CreateOrchestrationInstance(typeof(CompressionCompatTest), null);

            TestHelpers.WaitForInstance(client, id, 60, false);
            Thread.Sleep(5000);

            taskHub.Stop(true);

            taskHubNoCompression.AddTaskOrchestrations(typeof(CompressionCompatTest))
                .AddTaskActivities(typeof(SimpleTask))
                .Start();

            bool isCompleted = TestHelpers.WaitForInstance(client, id, 60);

            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));
        }

        public sealed class AlternatingPayloadTask : TaskActivity<bool, byte[]>
        {
            protected override byte[] Execute(TaskContext context, bool largeOutput)
            {
                byte[] arr = { 1, 2, 3, 4 };

                if (largeOutput)
                {
                    arr = new byte[1600];

                    for (int i = 0; i < 100; i++)
                    {
                        Guid.NewGuid().ToByteArray().CopyTo(arr, i * 16);
                    }
                }

                return arr;
            }
        }

        public class CompressionCompatTest : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string output = await context.ScheduleTask<string>(typeof(SimpleTask), "test");

                await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(10), "TimedOut");

                output = await context.ScheduleTask<string>(typeof(SimpleTask), "test");

                await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(10), "TimedOut");

                return output;
            }
        }

        public class MessageCompressionCompatTest : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                for (int i = 0; i < 10; i++)
                {
                    await context.ScheduleTask<byte[]>(typeof(AlternatingPayloadTask), (i % 2 == 0));
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


        public class ResultTestOrchestration : TaskOrchestration<Result, string>
        {
            public override async Task<Result> RunTask(OrchestrationContext context, string input)
            {
                var result = await context.ScheduleTask<Result>(typeof(SimpleTask2), input);

                return result;
            }
        }

        public class Result
        {
            public int Prop1 { get; set; }
            public string Prop2 { get; set; }
        }

        public sealed class SimpleTask2 : TaskActivity<string, Result>
        {
            protected override Result Execute(TaskContext context, string input)
            {
                return new Result() { Prop1 = 77, Prop2 = "Hello" };
            }
        }

        #endregion
    }
}