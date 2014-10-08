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
    using System.Threading;
    using DurableTask;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public static class TestHelpers
    {
        // TODO : move this to app.config
        static string connectionString = "<TODO: PLUGIN YOUR CONNECTION STRING HERE>";

        static string tableConnectionString =
            "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1:10002/";

        static string taskHubName = "test";

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
                return new TaskHubClient(taskHubName, connectionString, tableConnectionString);
            }
            return new TaskHubClient(taskHubName, connectionString);
        }

        public static TaskHubClient CreateTaskHubClient(bool createInstanceStore = true)
        {
            TaskHubClientSettings clientSettings = CreateTestClientSettings();

            if (createInstanceStore)
            {
                return new TaskHubClient(taskHubName, connectionString, tableConnectionString, clientSettings);
            }
            return new TaskHubClient(taskHubName, connectionString, clientSettings);
        }

        public static TaskHubWorker CreateTaskHubNoCompression(bool createInstanceStore = true)
        {
            if (createInstanceStore)
            {
                return new TaskHubWorker(taskHubName, connectionString, tableConnectionString);
            }

            return new TaskHubWorker(taskHubName, connectionString);
        }

        public static TaskHubWorker CreateTaskHubLegacyCompression(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings(CompressionStyle.Legacy);

            if (createInstanceStore)
            {
                return new TaskHubWorker(taskHubName, connectionString, tableConnectionString, workerSettings);
            }

            return new TaskHubWorker(taskHubName, connectionString);
        }

        public static TaskHubWorker CreateTaskHubAlwaysCompression(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings(CompressionStyle.Always);

            if (createInstanceStore)
            {
                return new TaskHubWorker(taskHubName, connectionString, tableConnectionString, workerSettings);
            }

            return new TaskHubWorker(taskHubName, connectionString);
        }


        public static TaskHubWorker CreateTaskHub(bool createInstanceStore = true)
        {
            TaskHubWorkerSettings workerSettings = CreateTestWorkerSettings();

            if (createInstanceStore)
            {
                return new TaskHubWorker(taskHubName, connectionString, tableConnectionString, workerSettings);
            }

            return new TaskHubWorker(taskHubName, connectionString, workerSettings);
        }

        public static long GetOrchestratorQueueSizeInBytes()
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(connectionString);
            QueueDescription queueDesc = nsManager.GetQueue(taskHubName + "/orchestrator");

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
    }
}