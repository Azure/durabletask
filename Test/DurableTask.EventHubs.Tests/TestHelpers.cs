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

namespace DurableTask.EventHubs.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;

    internal static class TestHelpers
    {
        static EventHubsTestConfig config;

        public static EventHubsOrchestrationService GetTestOrchestrationService(string taskHub = null)
        {
            EventHubsTestConfig testConfig = GetEventHubsTestConfig();
            var settings = new EventHubsOrchestrationServiceSettings
            {
                EventHubName = eventHubName,
                NumberPartitions = 32,
                TaskHubName = taskHub,
            };

            return new EventHubsOrchestrationService(settings);
        }

        private static string eventHubName = null;

        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30)
        {

            var settings = new EventHubsOrchestrationServiceSettings
            {
                EventHubName = eventHubName,
                NumberPartitions = 32,
            };

            return new TestOrchestrationHost(settings);
        }


        private static EventHubsTestConfig GetEventHubsTestConfig()
        {
            if (config == null)
            {
                config = new EventHubsTestConfig();
                IConfigurationRoot root = new ConfigurationBuilder()
                    .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true)
                    .Build();
                root.Bind(config);
            }
            return config;
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException("A Storage connection string must be defined in either an environment variable or in configuration.");
            }

            return storageConnectionString;
        }

        static string GetTestSetting(string name)
        {
            return Environment.GetEnvironmentVariable("DurableTaskTest" + name);
        }

        public class EventHubsTestConfig
        {
            public string RedisConnectionString { get; set; }
        }
    }
}
