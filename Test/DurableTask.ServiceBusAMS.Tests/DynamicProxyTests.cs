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
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DynamicProxyTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            this.client = TestHelpers.CreateTaskHubClient();

            this.taskHub = TestHelpers.CreateTaskHub();
            this.taskHub.orchestrationService.CreateAsync(true).Wait();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            this.taskHub.StopAsync(true).Wait();
            this.taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        #region Common TaskActivities

        static readonly string SendGreetingName = "SendGreeting";
        static readonly string SendGreetingVersion = string.Empty;

        sealed class SendGreetingTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string user)
            {
                return "Greeting send to " + user;
            }
        }

        #endregion

        #region Greetings Test

        static readonly string GetUserName = "GetUser";
        static readonly string GetUserVersion = string.Empty;

        [TestMethod]
        public async Task GreetingsDynamicProxyTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(GreetingsOrchestration))
                .AddTaskActivities(
                    new NameValueObjectCreator<TaskActivity>(GetUserName, GetUserVersion, new GetUserTask()),
                    new NameValueObjectCreator<TaskActivity>(SendGreetingName, SendGreetingVersion,
                        new SendGreetingTask()))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration), null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual("Greeting send to Gabbar", GreetingsOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        sealed class GetUserTask : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Gabbar";
            }
        }

        public interface IGreetingsClient
        {
            Task<string> GetUser();

            Task<string> SendGreeting(string user);
        }

        class GreetingsOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var client = context.CreateClient<IGreetingsClient>();
                string user = await client.GetUser();
                string greeting = await client.SendGreeting(user);
                // This is a HACK to get unit test up and running.  Should never be done in actual code.
                Result = greeting;

                return greeting;
            }
        }

        #endregion

        #region AverageCalculator Test

        static readonly string ComputeSumName = "ComputeSum";
        static readonly string ComputeSumVersion = string.Empty;

        [TestMethod]
        public async Task AverageCalculatorDynamicProxyTest()
        {
            await this.taskHub.AddTaskOrchestrations(typeof(AverageCalculatorOrchestration))
                .AddTaskActivities(new NameValueObjectCreator<TaskActivity>(ComputeSumName, ComputeSumVersion,
                    new ComputeSumTask()))
                .StartAsync();

            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(typeof(AverageCalculatorOrchestration),
                new[] { 1, 50, 10 });

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 60));
            Assert.AreEqual(25.5, AverageCalculatorOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public interface IAverageCalculatorClient
        {
            Task<int> ComputeSum(int[] chunk);
        }

        class AverageCalculatorOrchestration : TaskOrchestration<double, int[]>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static double Result;

            public override async Task<double> RunTask(OrchestrationContext context, int[] input)
            {
                if (input == null || input.Length != 3)
                {
                    throw new ArgumentException("input");
                }

                int start = input[0];
                int end = input[1];
                int step = input[2];
                int total = end - start + 1;

                var chunks = new List<Task<int>>();
                while (start < end)
                {
                    int current = start + step - 1;
                    if (current > end)
                    {
                        current = end;
                    }

                    var client = context.CreateClient<IAverageCalculatorClient>();
                    Task<int> chunk = client.ComputeSum(new[] { start, current });
                    chunks.Add(chunk);

                    start = current + 1;
                }

                var sum = 0;
                int[] allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    sum += result;
                }

                double r = sum / (double) total;
                Result = r;
                return r;
            }
        }

        sealed class ComputeSumTask : TaskActivity<int[], int>
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

        #endregion

        #region Retry Proxy Tests

        static readonly string RETRY_NAME = "RetryOrchestration";
        static readonly string RETRY_VERSION = "1.0";

        [TestMethod]
        public async Task RetryProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 4);
            var retryTask = new RetryTask(3);

            await this.taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 120));
            Assert.AreEqual("Attempts: 3", RetryOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        [TestMethod]
        public async Task RetryFailProxyTest()
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(3), 3);
            var retryTask = new RetryTask(3);
            await this.taskHub.AddTaskOrchestrations(new TestObjectCreator<TaskOrchestration>(RETRY_NAME, RETRY_VERSION,
                () => new RetryOrchestration(retryOptions)))
                .AddTaskActivitiesFromInterface<IRetryTask>(retryTask)
                .StartAsync();

            RetryOrchestration.Result = null;
            OrchestrationInstance id = await this.client.CreateOrchestrationInstanceAsync(RETRY_NAME, RETRY_VERSION, null);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(this.client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(this.client, id, 120));
            Assert.AreEqual("RetryCount is: 3", RetryOrchestration.Result, "Orchestration Result is wrong!!!");
        }

        public interface IRetryTask
        {
            string DoWork();
        }

        public interface IRetryTaskClient
        {
            Task<string> DoWork();
        }

        sealed class RetryOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;
            readonly RetryOptions retryPolicy;

            public RetryOrchestration(RetryOptions retryOptions)
            {
                this.retryPolicy = retryOptions;
            }

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result;
                try
                {
                    var client = context.CreateRetryableClient<IRetryTaskClient>(this.retryPolicy);
                    result = await client.DoWork();
                }
                catch (TaskFailedException e)
                {
                    result = e.Message;
                }

                Result = result;
                return result;
            }
        }

        sealed class RetryTask : IRetryTask
        {
            public RetryTask(int failAttempts)
            {
                RetryCount = 0;
                FailAttempts = failAttempts;
            }

            // ReSharper disable once MemberCanBePrivate.Local
            public int RetryCount { get; set; }

            // ReSharper disable once MemberCanBePrivate.Local
            public int FailAttempts { get; set; }

            public string DoWork()
            {
                if (RetryCount < FailAttempts)
                {
                    RetryCount++;
                    throw new InvalidOperationException("RetryCount is: " + RetryCount);
                }

                return "Attempts: " + RetryCount;
            }
        }

        #endregion
    }
}