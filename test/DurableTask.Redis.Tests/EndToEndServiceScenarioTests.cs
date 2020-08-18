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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Test.Orchestrations;
using StackExchange.Redis;
using Xunit;

namespace DurableTask.Redis.Tests
{
    public class EndToEndServiceScenarioTests
    {
        [Fact]
        public async Task DeleteTaskHub_DeletesAllKeysInRelevantNamespace()
        {
            string otherTaskHub = "othertaskhub";
            ConnectionMultiplexer redisConnection = await TestHelpers.GetRedisConnection();
            string taskHub = nameof(DeleteTaskHub_DeletesAllKeysInRelevantNamespace);
            IDatabase database = redisConnection.GetDatabase();
            try
            {
                await Task.WhenAll(
                    database.StringSetAsync($"{taskHub}.string", "string"),
                    database.ListLeftPushAsync($"{taskHub}.list", "value1"),
                    database.HashSetAsync($"{taskHub}.hash", "key", "value"),
                    database.StringSetAsync($"{otherTaskHub}.string", "string")
                );

                RedisOrchestrationService service = TestHelpers.GetTestOrchestrationService(taskHub);
                await service.DeleteAsync();

                // Assert all task hub values were deleted
                string taskHubStringValue = await database.StringGetAsync($"{taskHub}.string");
                Assert.Null(taskHubStringValue);
                string taskHubHashValue = await database.HashGetAsync($"{taskHub}.hash", "key");
                Assert.Null(taskHubHashValue);
                RedisValue[] taskHubListValue = await database.ListRangeAsync($"{taskHub}.list", 0);
                Assert.Empty(taskHubListValue);

                // Assert non-task hub values were not deleted
                string otherTaskHubStringValue = await database.StringGetAsync($"{otherTaskHub}.string");
                Assert.Equal("string", otherTaskHubStringValue);
            }
            finally
            {
                database.KeyDelete($"{taskHub}.string");
                database.KeyDelete($"{taskHub}.list");
                database.KeyDelete($"{taskHub}.hash");
                database.KeyDelete($"{otherTaskHub}.string");
            }
        }

        [Fact]
        public async Task StopAsync_IsIdempotent()
        {
            int numStops = 3;
            RedisOrchestrationService service = TestHelpers.GetTestOrchestrationService();
            for (int i =0; i < numStops; i++)
            {
                await service.StopAsync();
            }
        }

        [Fact]
        public async Task UnstartedService_CanBeSafelyStopped()
        {
            RedisOrchestrationService service = TestHelpers.GetTestOrchestrationService();
            await service.StopAsync();
        }

        [Fact]
        public async Task SimpleGreetingOrchestration()
        {
            var orchestrationService = TestHelpers.GetTestOrchestrationService(nameof(SimpleGreetingOrchestration));

            var worker = new TaskHubWorker(orchestrationService);

            try
            {
                await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                    .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                    .StartAsync();

                var client = new TaskHubClient(orchestrationService);

                OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

                OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(Debugger.IsAttached ? 300 : 20), new CancellationToken());
                Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

                Assert.Equal("Greeting send to Gabbar", SimplestGreetingsOrchestration.Result);
            }
            finally
            {
                await worker.StopAsync(true);
                await orchestrationService.DeleteAsync();
            }
        }

        [Fact]
        public async Task SimpleFanOutOrchestration()
        {
            // Using 1 more than the maximum concurrent count.
            int numToIterateTo = 101;
            var orchestrationService = TestHelpers.GetTestOrchestrationService(nameof(SimpleFanOutOrchestration));

            var worker = new TaskHubWorker(orchestrationService);

            try
            {
                await worker.AddTaskOrchestrations(typeof(FanOutOrchestration))
                    .AddTaskActivities(typeof(SquareIntTask), typeof(SumIntTask))
                    .StartAsync();

                var client = new TaskHubClient(orchestrationService);

                int[] numsToSum = new int[numToIterateTo];
                for(int i = 0; i < numToIterateTo; i++)
                {
                    numsToSum[i] = i + 1;
                }
                OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(FanOutOrchestration), numsToSum);

                OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(Debugger.IsAttached ? 20 : 20), new CancellationToken());
                Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

                // Sum of square numbers 1 to n = n * (n+1) * (2n+1) / 6
                int expectedResult = (numToIterateTo * (numToIterateTo + 1) * (2 * numToIterateTo + 1)) / 6;
                Assert.Equal(expectedResult, FanOutOrchestration.Result);
            }
            finally
            {
                await worker.StopAsync(true);
                await orchestrationService.DeleteAsync();
            }
        }

        [Fact]
        public async Task SimpleFanOutOrchestration_DurabilityTest()
        {
            int numToIterateTo = 500;
            var orchestrationService = TestHelpers.GetTestOrchestrationService(nameof(SimpleFanOutOrchestration_DurabilityTest));
            var worker = new TaskHubWorker(orchestrationService);

            try
            {
                await worker.AddTaskOrchestrations(typeof(FanOutOrchestration))
                    .AddTaskActivities(typeof(SquareIntTask), typeof(SumIntTask))
                    .StartAsync();

                var client = new TaskHubClient(orchestrationService);

                int[] numsToSum = new int[numToIterateTo];
                for (int i = 0; i < numToIterateTo; i++)
                {
                    numsToSum[i] = i + 1;
                }
                OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof(FanOutOrchestration), numsToSum);

                try
                {
                    await client.WaitForOrchestrationAsync(id, TimeSpan.FromMilliseconds(500), new CancellationToken());
                }
                catch
                {
                    //Timeout is expected in this case. 500 activities can't finish that fast.
                    await worker.StopAsync(true);
                }

                // Resume orchestration on "new" client
                orchestrationService = TestHelpers.GetTestOrchestrationService(nameof(SimpleFanOutOrchestration_DurabilityTest));
                worker = new TaskHubWorker(orchestrationService);
                await worker.AddTaskOrchestrations(typeof(FanOutOrchestration))
                    .AddTaskActivities(typeof(SquareIntTask), typeof(SumIntTask))
                    .StartAsync();
                client = new TaskHubClient(orchestrationService);

                OrchestrationState result = await client.WaitForOrchestrationAsync(id, TimeSpan.FromSeconds(Debugger.IsAttached ? 20 : 20), new CancellationToken());
                Assert.Equal(OrchestrationStatus.Completed, result.OrchestrationStatus);

                // Sum of square numbers 1 to n = n * (n+1) * (2n+1) / 6
                int expectedResult = (numToIterateTo * (numToIterateTo + 1) * (2 * numToIterateTo + 1)) / 6;
                Assert.Equal(expectedResult, FanOutOrchestration.Result);
            }
            finally
            {
                await worker.StopAsync(true);
                await orchestrationService.DeleteAsync();
            }
        }


        /// <summary>
        /// Validates scheduled starts, ensuring that invalid operation exception is raised since the feature is not supported
        /// </summary>
        [Fact]
        public async Task ScheduledStart_NotSupported()
        {
            var orchestrationService = TestHelpers.GetTestOrchestrationService(nameof(ScheduledStart_NotSupported));
            var worker = new TaskHubWorker(orchestrationService);

            try
            {
                await worker.AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                   .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                   .StartAsync();

                var client = new TaskHubClient(orchestrationService);
                var expectedStartTime = DateTime.UtcNow.AddSeconds(30);
                await Assert.ThrowsAsync<NotSupportedException>(() => client.CreateScheduledOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null, expectedStartTime));
            }
            finally
            {
                await worker.StopAsync(true);
                await orchestrationService.DeleteAsync();
            }
        }
    }
}
