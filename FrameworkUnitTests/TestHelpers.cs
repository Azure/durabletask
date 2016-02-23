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
    using System.Threading;
    using DurableTask;
    using DurableTask.Common;
    using DurableTask.Exceptions;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public static class TestHelpers
    {
        private static string ServiceBusConnectionString;
        private static string StorageConnectionString;
        private static string TaskHubName;

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

        public static TaskHubWorkerSettings CreateTestWorkerSettings(CompressionStyle style = CompressionStyle.Threshold)
        {
            var settings = new TaskHubWorkerSettings();
            settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState = true;
            settings.MessageCompressionSettings = new CompressionSettings {Style = style, ThresholdInBytes = 1024};
            return settings;
        }

        public static TaskHubClientSettings CreateTestClientSettings()
        {
            var settings = new TaskHubClientSettings();
            settings.MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Threshold,
                ThresholdInBytes = 1024
            };
            return settings;
        }

        public static TaskHubClient CreateTaskHubClientNoCompression(bool createInstanceStore = true)
        {
            if (createInstanceStore)
            {
                return new TaskHubClient(TaskHubName, ServiceBusConnectionString, StorageConnectionString);
            }
            return new TaskHubClient(TaskHubName, ServiceBusConnectionString);
        }

        public static TaskHubClient CreateTaskHubClient(bool createInstanceStore = true)
        {
            TaskHubClientSettings clientSettings = CreateTestClientSettings();

            if (createInstanceStore)
            {
                return new TaskHubClient(TaskHubName, ServiceBusConnectionString, StorageConnectionString, clientSettings);
            }
            return new TaskHubClient(TaskHubName, ServiceBusConnectionString, clientSettings);
        }

        public static TaskHubWorker CreateTaskHubNoCompression(bool createInstanceStore = true)
        {
            if (createInstanceStore)
            {
                return new TaskHubWorker(TaskHubName, ServiceBusConnectionString, StorageConnectionString);
            }

            return new TaskHubWorker(TaskHubName, ServiceBusConnectionString);
        }

        public static TaskHubWorker CreateTaskHubLegacyCompression(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings(CompressionStyle.Legacy);

            if (createInstanceStore)
            {
                return new TaskHubWorker(TaskHubName, ServiceBusConnectionString, StorageConnectionString, workerSettings);
            }

            return new TaskHubWorker(TaskHubName, ServiceBusConnectionString);
        }

        public static TaskHubWorker CreateTaskHubAlwaysCompression(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings(CompressionStyle.Always);

            if (createInstanceStore)
            {
                return new TaskHubWorker(TaskHubName, ServiceBusConnectionString, StorageConnectionString, workerSettings);
            }

            return new TaskHubWorker(TaskHubName, ServiceBusConnectionString);
        }


        public static TaskHubWorker CreateTaskHub(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings();

            if (createInstanceStore)
            {
                return new TaskHubWorker(TaskHubName, ServiceBusConnectionString, StorageConnectionString, workerSettings);
            }

            return new TaskHubWorker(TaskHubName, ServiceBusConnectionString, workerSettings);
        }

        public static long GetOrchestratorQueueSizeInBytes()
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(ServiceBusConnectionString);
            QueueDescription queueDesc = nsManager.GetQueue(TaskHubName + "/orchestrator");

            return queueDesc.SizeInBytes;
        }

        public static bool WaitForInstance(TaskHubClient taskHubClient, OrchestrationInstance instance,
            int timeoutSeconds,
            bool waitForCompletion = true)
        {
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            int sleepForSeconds = 2;

            while (timeoutSeconds > 0)
            {
                OrchestrationState state = taskHubClient.GetOrchestrationState(instance.InstanceId);
                if (state == null || (waitForCompletion && state.OrchestrationStatus == OrchestrationStatus.Running))
                {
                    Thread.Sleep(sleepForSeconds*1000);
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
            return taskHubClient.GetOrchestrationHistory(instance);
        }

        public static string GetInstanceNotCompletedMessage(TaskHubClient taskHubClient, OrchestrationInstance instance,
            int timeWaited)
        {
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            string history = PrintHistory(taskHubClient, instance);
            string message = string.Format("Instance '{0}' not completed within {1} seconds.\n History: {2}", instance,
                timeWaited, history);

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