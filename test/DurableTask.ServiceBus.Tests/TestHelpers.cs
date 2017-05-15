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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Configuration;
    using System.Diagnostics.Tracing;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus;
    using DurableTask.ServiceBus.Settings;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

    public static class TestHelpers
    {
        static string ServiceBusConnectionString;
        static string StorageConnectionString;
        static string TaskHubName;
        static ObservableEventListener eventListener;

        static TestHelpers()
        {
            ServiceBusConnectionString = GetTestSetting("ServiceBusConnectionString");
            if (string.IsNullOrEmpty(ServiceBusConnectionString))
            {
                throw new ArgumentNullException("A ServiceBus connection string must be defined in either an environment variable or in configuration.");
            }

            StorageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(StorageConnectionString))
            {
                throw new ArgumentNullException("A Storage connection string must be defined in either an environment variable or in configuration.");
            }

            TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName");

            eventListener = new ObservableEventListener();
            eventListener.LogToConsole();
            eventListener.EnableEvents(DefaultEventSource.Log, EventLevel.LogAlways);
        }

        public static ServiceBusOrchestrationServiceSettings CreateTestWorkerSettings(CompressionStyle style = CompressionStyle.Threshold)
        {
            var settings = new ServiceBusOrchestrationServiceSettings
            {
                TaskOrchestrationDispatcherSettings = { CompressOrchestrationState = true },
                MessageCompressionSettings = new CompressionSettings { Style = style, ThresholdInBytes = 1024 },
                JumpStartSettings = { JumpStartEnabled = true, IgnoreWindow = TimeSpan.FromSeconds(10) }
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
            ServiceBusOrchestrationServiceSettings settings,
            TimeSpan jumpStartAttemptInterval)
        {
            var service = new ServiceBusOrchestrationService(
                ServiceBusConnectionString,
                TaskHubName,
                new AzureTableInstanceStore(TaskHubName, StorageConnectionString),
                new AzureStorageBlobStore(TaskHubName, StorageConnectionString),
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
                new AzureStorageBlobStore(TaskHubName, StorageConnectionString),
                settings);
            return service;
        }

        public static AzureTableInstanceStore CreateAzureTableInstanceStore()
        {
            return new AzureTableInstanceStore(TaskHubName, StorageConnectionString);
        }

        public static AzureStorageBlobStore CreateAzureStorageBlobStore()
        {
            return new AzureStorageBlobStore(TaskHubName, StorageConnectionString);
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
            return new TaskHubWorker(CreateOrchestrationServiceWorker(null, TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHubLegacyCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Legacy), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHubAlwaysCompression()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(CompressionStyle.Always), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHub()
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(), TimeSpan.FromMinutes(1)));
        }

        public static TaskHubWorker CreateTaskHub(TimeSpan jumpStartAttemptInterval)
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(CreateTestWorkerSettings(), jumpStartAttemptInterval));
        }

        public static TaskHubWorker CreateTaskHub(ServiceBusOrchestrationServiceSettings settings)
        {
            return new TaskHubWorker(CreateOrchestrationServiceWorker(settings, TimeSpan.FromMinutes(10)));
        }

        public static long GetOrchestratorQueueSizeInBytes()
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(ServiceBusConnectionString);
            QueueDescription queueDesc = nsManager.GetQueue(TaskHubName + "/orchestrator");

            return queueDesc.SizeInBytes;
        }

        public static long GetOrchestratorQueueMessageCount()
        {
            NamespaceManager nsManager = NamespaceManager.CreateFromConnectionString(ServiceBusConnectionString);
            QueueDescription queueDesc = nsManager.GetQueue(TaskHubName + "/orchestrator");

            return queueDesc.MessageCount;
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
                if (state == null)
                {
                    throw new ArgumentException("OrchestrationState is expected but NULL value returned");
                }

                if (waitForCompletion &&
                    (state.OrchestrationStatus == OrchestrationStatus.Running ||
                     state.OrchestrationStatus == OrchestrationStatus.Pending))
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


        public static async Task<OrchestrationInstance> CreateOrchestrationInstanceAsync(
            ServiceBusOrchestrationService sboService,
            string name,
            string version,
            string instanceId,
            string executionId,
            bool jumpStartOnly,
            bool serviceBusOnly)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                instanceId = Guid.NewGuid().ToString("N");
            }

            if (string.IsNullOrWhiteSpace(executionId))
            {
                executionId = Guid.NewGuid().ToString("N");
            }

            var orchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            };

            var startedEvent = new ExecutionStartedEvent(-1, null)
            {
                Tags = null,
                Name = name,
                Version = version,
                OrchestrationInstance = orchestrationInstance
            };

            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = startedEvent
            };

            if (jumpStartOnly)
            {
                await sboService.UpdateJumpStartStoreAsync(taskMessage);
            }
            else if (serviceBusOnly)
            {
                await sboService.SendTaskOrchestrationMessageAsync(taskMessage);
            }
            else
            {
                await sboService.CreateTaskOrchestrationAsync(taskMessage);
            }

            return orchestrationInstance;
        }

        public async static Task<TException> ThrowsAsync<TException>(Func<Task> action, string errorMessage = null) where TException : Exception
        {
            errorMessage = errorMessage ?? "Failed";
            try
            {
                await action();
            }
            catch (TException ex)
            {
                return ex;
            }
            catch (Exception ex)
            {
                throw new AssertFailedException(
                    string.Format("{0}. Expected:<{1}> Actual<{2}>", errorMessage, typeof(TException).ToString(), ex.GetType().ToString()), ex);
            }

            throw new AssertFailedException(string.Format("{0}. Expected {1} exception but no exception is thrown", errorMessage, typeof(TException).ToString()));
        }
    }
}