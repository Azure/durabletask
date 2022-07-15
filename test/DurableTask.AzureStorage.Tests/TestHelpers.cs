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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;
    using System.Threading.Tasks;
    using DurableTask.Core.Logging;
    using Microsoft.Extensions.Logging;

    static class TestHelpers
    {

        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30,
            bool fetchLargeMessages = true,
            Action<AzureStorageOrchestrationServiceSettings>? modifySettingsAction = null)
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageConnectionString = storageConnectionString,
                TaskHubName = GetTestTaskHubName(),
                ExtendedSessionsEnabled = enableExtendedSessions,
                ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(extendedSessionTimeoutInSeconds),
                FetchLargeMessageDataEnabled = fetchLargeMessages,

                // Setting up a logger factory to enable the new DurableTask.Core logs
                // TODO: Add a logger provider so we can collect these logs in memory.
                LoggerFactory = new LoggerFactory(),
            };

            // Give the caller a chance to make test-specific changes to the settings
            modifySettingsAction?.Invoke(settings);

            return new TestOrchestrationHost(settings);
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string? storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                storageConnectionString = "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:10002/";
            }

            return storageConnectionString!;
        }

        public static string GetTestTaskHubName()
        {
            string? taskHubName = GetTestSetting("TaskHubName");
            return taskHubName ?? "test";
        }

        static string? GetTestSetting(string name)
        {
            return Environment.GetEnvironmentVariable("DurableTaskTest" + name);
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

        public static void Await(this Task task) => task.GetAwaiter().GetResult();

        public static T Await<T>(this Task<T> task) => task.GetAwaiter().GetResult();
    }
}
