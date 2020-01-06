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
    using System.Diagnostics;
    using System.Threading.Tasks;

    static class TestHelpers
    {
        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30,
            bool fetchLargeMessages = true)
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageConnectionString = storageConnectionString,
                TaskHubName = GetTestTaskHubName(),
                ExtendedSessionsEnabled = enableExtendedSessions,
                ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(extendedSessionTimeoutInSeconds),
                FetchLargeMessageDataEnabled = fetchLargeMessages,
            };

            return new TestOrchestrationHost(settings);
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

        public static string GetTestTaskHubName()
        {
            Configuration appConfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            return appConfig.AppSettings.Settings["TaskHubName"].Value;
        }

        static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).AppSettings.Settings[name].Value;
            }

            return value;
        }

        public static async Task WaitFor(Func<bool> condition, TimeSpan timeout)
        {
            Stopwatch timer = Stopwatch.StartNew();
            do
            {
                bool result = condition();
                if (result)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100));

            } while (timer.Elapsed < timeout);

            throw new TimeoutException("Timed out waiting for condition to be true.");
        }
    }
}
