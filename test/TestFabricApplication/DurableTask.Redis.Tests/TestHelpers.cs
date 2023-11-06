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

namespace DurableTask.Redis.Tests
{
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;
    using StackExchange.Redis;

    public static class TestHelpers
    {
        static RedisTestConfig config;

        public static RedisOrchestrationService GetTestOrchestrationService(string taskHub = null)
        {
            RedisTestConfig testConfig = GetRedisTestConfig();
            var settings = new RedisOrchestrationServiceSettings
            {
                RedisConnectionString = testConfig.RedisConnectionString,
                TaskHubName = taskHub ?? "TaskHub"
            };

            return new RedisOrchestrationService(settings);
        }

        public static async Task<ConnectionMultiplexer> GetRedisConnection()
        {
            RedisTestConfig testConfig = GetRedisTestConfig();
            return await ConnectionMultiplexer.ConnectAsync(testConfig.RedisConnectionString);
        }

        private static RedisTestConfig GetRedisTestConfig()
        {
            if (config == null)
            {
                config = new RedisTestConfig();
                IConfigurationRoot root = new ConfigurationBuilder()
                    .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true)
                    .Build();
                root.Bind(config);
            }
            return config;
        }

        public class RedisTestConfig
        {
            public string RedisConnectionString { get; set; }
        }
    }
}
