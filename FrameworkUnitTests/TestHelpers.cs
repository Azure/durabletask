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
    using System.Configuration;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Common;
    using DurableTask.Settings;
    using DurableTask.Tracking;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public static class TestHelpers
    {
        static string ServiceBusConnectionString;
        static string StorageConnectionString;
        static string TaskHubName;

        static TestHelpers()
        {
            ServiceBusConnectionString = GetTestSetting("ServiceBusConnectionString");
            if (string.IsNullOrEmpty(ServiceBusConnectionString))
            {
                throw new Exception("A ServiceBus connection string must be defined in either an environment variable or in configuration.");
            }

            StorageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(StorageConnectionString))
            {
                throw new Exception("A Storage connection string must be defined in either an environment variable or in configuration.");
            }

            TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName");
        }

        public static ServiceBusOrchestrationServiceSettings CreateTestWorkerSettings(CompressionStyle style = CompressionStyle.Threshold)
        {
            var settings = new ServiceBusOrchestrationServiceSettings
            {
                TaskOrchestrationDispatcherSettings = { CompressOrchestrationState = true },
                MessageCompressionSettings = new CompressionSettings { Style = style, ThresholdInBytes = 1024 }
            };

            return settings;
        }

        public static ServiceBusOrchestrationServiceSettings CreateTestClientSettings()
        {
            var settings = new ServiceBusOrchestrationServiceSettings
            {
                MessageCompressionSettings = new CompressionSettings
                {
                    Style = CompressionStyle.Threshold,
                    ThresholdInBytes = 1024
                }
            };

            return settings;
        }

        static IOrchestrationService CreateOrchestrationServiceWorker(
            ServiceBusOrchestrationServiceSettings settings)
        {
            var service = new ServiceBusOrchestrationService(
                ServiceBusConnectionString,
                TaskHubName,
                new AzureTableInstanceStore(TaskHubName, StorageConnectionString),
                settings);
            return service;
        }

        static IOrchestrationServiceClient CreateOrchestrationServiceClient(
            ServiceBusOrchestrationServiceSettings settings)
        {
            var service = new ServiceBusOrchestrationService(
                ServiceBusConnectionString,
                TaskHubName,
                new AzureTableInstanceStore(TaskHubName, StorageConnectionString),
                settings);
            return service;
        }

        public static TaskHubClient CreateTaskHubClientNoCompression()
        {
            return new TaskHubClient(CreateOrchestrationServiceClient(null));
        }

        public static TaskHubClient CreateTaskHubClient()
        {
            return new TaskHubClient(CreateOrchestrationServiceClient(CreateTestClientSettings()));
        }

        public static TaskHubWorker CreateTaskHubNoCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(null));
        }

        public static TaskHubWorker CreateTaskHubLegacyCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Legacy)));
        }

        public static TaskHubWorker CreateTaskHubAlwaysCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Always)));
        }

        public static TaskHubWorker CreateTaskHub()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings()));
        }

        public static TaskHubWorker CreateTaskHub(ServiceBusOrchestrationServiceSettings settings)
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(settings));
        }

        public static long GetOrchestratorQueueSizeInBytes()
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(ServiceBusConnectionString);
            QueueDescription queueDesc = nsManager.GetQueue(TaskHubName + "/orchestrator");

            return queueDesc.SizeInBytes;
        }

        public static async Task<bool> WaitForInstanceAsync(TaskHubClient taskHubClient, OrchestrationInstance instance,
            int timeoutSeconds,
            bool waitForCompletion = true)
        {
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            int sleepForSeconds = 2;

            while (timeoutSeconds > 0)
            {
                OrchestrationState state = await taskHubClient.GetOrchestrationStateAsync(instance.InstanceId);
                if (state == null || (waitForCompletion && state.OrchestrationStatus == OrchestrationStatus.Running))
                {
                    await Task.Delay(sleepForSeconds * 1000);
                    timeoutSeconds -= sleepForSeconds;
                }
                else
                {
                    // Session state deleted after completion
                    return true;
                }
            }

            return false;
        }

        public static string PrintHistory(TaskHubClient taskHubClient, OrchestrationInstance instance)
        {
            return taskHubClient.GetOrchestrationHistoryAsync(instance).Result;
        }

        public static string GetInstanceNotCompletedMessage(
            TaskHubClient taskHubClient,
            OrchestrationInstance instance,
            int timeWaited)
        {
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            string history = PrintHistory(taskHubClient, instance);
            string message = $"Instance '{instance}' not completed within {timeWaited} seconds.\n History: {history}";

            return message;
        }

        public static string GetTestSetting(string name)
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