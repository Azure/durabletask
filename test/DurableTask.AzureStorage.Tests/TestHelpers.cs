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
    using Azure;
    using Azure.Storage.Queues;
    using Azure.Storage.Queues.Models;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Settings;
    using Microsoft.Extensions.Logging;
    using Moq;
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;
    using System.Threading;
    using System.Threading.Tasks;

    static class TestHelpers
    {
        public static AzureStorageOrchestrationServiceSettings GetMockQueueSettings(out Mock<QueueClient> queueClient)
        {
            const string ConnectionString = "UseDevelopmentStorage=true";
            const string TaskHubName = "BufferFailureTests";
            string queueName = AzureStorageOrchestrationService.GetControlQueueName(TaskHubName, 0);

            queueClient = new Mock<QueueClient>(MockBehavior.Strict, ConnectionString, queueName);
            queueClient.SetupGet(client => client.Name).Returns(queueName);
            queueClient.SetupGet(client => client.Uri).Returns(new Uri($"http://127.0.0.1:10001/devstoreaccount1/{queueName}"));
            queueClient.Setup(client => client.UpdateMessageAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync(Response.FromValue(
                    QueuesModelFactory.UpdateReceipt("retry-receipt", DateTimeOffset.UtcNow.AddMinutes(1)),
                    Mock.Of<Response>()));

            var serviceClient = new Mock<QueueServiceClient>(MockBehavior.Strict, ConnectionString);
            serviceClient.SetupGet(client => client.AccountName).Returns("devstoreaccount1");
            serviceClient.Setup(client => client.GetQueueClient(It.IsAny<string>())).Returns(queueClient.Object);

            var queueProvider = new Mock<IStorageServiceClientProvider<QueueServiceClient, QueueClientOptions>>();
            queueProvider.Setup(provider => provider.CreateOptions()).Returns(() => new QueueClientOptions());
            queueProvider.Setup(provider => provider.CreateClient(It.IsAny<QueueClientOptions>())).Returns(serviceClient.Object);

            return new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = TaskHubName,
                PartitionCount = 1,
                StorageAccountClientProvider = new StorageAccountClientProvider(
                    StorageServiceClientProvider.ForBlob(ConnectionString),
                    queueProvider.Object,
                    StorageServiceClientProvider.ForTable(ConnectionString)),
            };
        }

        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30,
            bool fetchLargeMessages = true,
            bool allowReplayingTerminalInstances = false,
            VersioningSettings? versioningSettings = null,
            Action<AzureStorageOrchestrationServiceSettings>? modifySettingsAction = null)
        {
            AzureStorageOrchestrationServiceSettings settings = GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions,
                extendedSessionTimeoutInSeconds,
                fetchLargeMessages,
                allowReplayingTerminalInstances);
            // Give the caller a chance to make test-specific changes to the settings
            modifySettingsAction?.Invoke(settings);

            return new TestOrchestrationHost(settings, versioningSettings);
        }

        public static AzureStorageOrchestrationServiceSettings GetTestAzureStorageOrchestrationServiceSettings(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30,
            bool fetchLargeMessages = true,
            bool allowReplayingTerminalInstances = false)
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole().SetMinimumLevel(LogLevel.Warning);
            });

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(extendedSessionTimeoutInSeconds),
                ExtendedSessionsEnabled = enableExtendedSessions,
                FetchLargeMessageDataEnabled = fetchLargeMessages,
                StorageAccountClientProvider = new StorageAccountClientProvider(storageConnectionString),
                TaskHubName = GetTestTaskHubName(),
                AllowReplayingTerminalInstances = allowReplayingTerminalInstances,

                // Setting up a logger factory to enable the new DurableTask.Core logs
                LoggerFactory = loggerFactory,
            };

            return settings;
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string? storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                storageConnectionString = "UseDevelopmentStorage=true";
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
