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

using System.Threading.Tasks;
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
            string taskHub = TestHelpers.GetTaskHubName();
            IDatabase database = redisConnection.GetDatabase();
            try
            {
                await Task.WhenAll(
                    database.StringSetAsync($"{taskHub}.string", "string"),
                    database.ListLeftPushAsync($"{taskHub}.list", "value1"),
                    database.HashSetAsync($"{taskHub}.hash", "key", "value"),
                    database.StringSetAsync($"{otherTaskHub}.string", "string")
                );

                RedisOrchestrationService service = await TestHelpers.GetTestOrchestrationServiceAsync();
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
            RedisOrchestrationService service = await TestHelpers.GetTestOrchestrationServiceAsync();
            for (int i =0; i < numStops; i++)
            {
                await service.StopAsync();
            }
        }

        [Fact]
        public async Task UnstartedService_CanBeSafelyStopped()
        {
            RedisOrchestrationService service = await TestHelpers.GetTestOrchestrationServiceAsync();
            await service.StopAsync();
        }
    }
}
