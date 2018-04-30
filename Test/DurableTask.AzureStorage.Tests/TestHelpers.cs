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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Configuration;

    static class TestHelpers
    {
        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30)
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var service = new AzureStorageOrchestrationService(
                new AzureStorageOrchestrationServiceSettings
                {
                    StorageConnectionString = storageConnectionString,
                    TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName"),
                    ExtendedSessionsEnabled = enableExtendedSessions,
                    ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(extendedSessionTimeoutInSeconds),
                });

            service.CreateAsync().GetAwaiter().GetResult();

            return new TestOrchestrationHost(service);
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
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }

            return value;
        }
    }
}
