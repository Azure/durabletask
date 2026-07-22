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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    /// <summary>
    /// Integration tests for poison message handling in <see cref="AzureStorageOrchestrationService"/>.
    /// These tests require the Azure Storage emulator (Azurite) to be running.
    /// </summary>
    /// <remarks>
    /// Because <see cref="AzureStorageOrchestrationService"/> is sealed, these tests use a
    /// <see cref="FaultInjectingOrchestrationService"/> decorator to force a transient failure the first time a work
    /// item is completed. That failure causes the underlying message to be abandoned and redelivered, which increments
    /// its dequeue count (and therefore its <see cref="HistoryEvent.DispatchCount"/>) so that it exceeds the configured
    /// maximum dispatch count and is treated as a poison message.
    /// </remarks>
    [TestClass]
    public class PoisonMessageHandlingTests
    {
        static readonly TimeSpan DefaultTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(60);

        [TestMethod]
        public async Task OrchestrationWithPoisonMessage_Failed_AndPoisonMessageStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient instanceContainerClient = blobServiceClient.GetBlobContainerClient(instanceContainerName);
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await instanceContainerClient.DeleteIfExistsAsync();
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            // Fail the first orchestration work item completion so the ExecutionStarted message is redelivered
            // with a dequeue count of 2, which exceeds the maximum dequeue count of 1.
            var service = new FaultInjectingOrchestrationService(inner)
            {
                OrchestrationCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                var tags = new Dictionary<string, string> { { "key", "value" } };
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    instanceId: Guid.NewGuid().ToString("N"),
                    input: "hello",
                    tags: tags);

                // The orchestration itself never completes because the ExecutionStartedEvent was discarded, so we wait
                // for the poison blob to appear rather than for orchestration completion.
                await TestHelpers.WaitFor(
                    () => instanceContainerClient.Exists().Value && ListBlobsAsync(instanceContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                Assert.IsFalse(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should not exist");

                List<BlobItem> blobs = await ListBlobsAsync(instanceContainerClient);
                Assert.AreEqual(1, blobs.Count);

                string expectedPrefix = $"{instance.InstanceId}~{instance.ExecutionId}";
                Assert.AreEqual(expectedPrefix, blobs[0].Name.Substring(0, expectedPrefix.Length));

                MessageData poisonMessage = await DownloadPoisonMessagesAsync(instanceContainerClient, blobs[0].Name);
                Assert.AreEqual(string.Empty, poisonMessage.Sender.InstanceId);
                Assert.AreEqual(string.Empty, poisonMessage.Sender.ExecutionId);
                Assert.AreEqual(instance.InstanceId, poisonMessage.TaskMessage.OrchestrationInstance.InstanceId);
                Assert.AreEqual(instance.ExecutionId, poisonMessage.TaskMessage.OrchestrationInstance.ExecutionId);

                Assert.IsInstanceOfType(poisonMessage.TaskMessage.Event, typeof(ExecutionStartedEvent));
                var executionStartedEvent = (ExecutionStartedEvent)poisonMessage.TaskMessage.Event;
                Assert.AreEqual(instance.InstanceId, executionStartedEvent.OrchestrationInstance.InstanceId);
                Assert.AreEqual(instance.ExecutionId, executionStartedEvent.OrchestrationInstance.ExecutionId);
                Assert.AreEqual(NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)), executionStartedEvent.Name);
                Assert.AreEqual("\"hello\"", executionStartedEvent.Input);
                Assert.IsNotNull(executionStartedEvent.Tags);
                Assert.AreEqual(1, executionStartedEvent.Tags.Count);
                Assert.IsTrue(executionStartedEvent.Tags.ContainsKey("key"));
                Assert.AreEqual("value", executionStartedEvent.Tags["key"]);

                await AssertQueuesAreEmptyAsync(settings);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await instanceContainerClient.DeleteIfExistsAsync();
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        static IEnumerable<object[]> InvalidInstanceIdCases()
        {
            // A single invalid control character (U+0080) is replaced with a dash.
            yield return new object[] { "bad\u0080id", "bad-id" };

            // Multiple invalid control characters (U+0080 and U+008E) are each replaced with a dash.
            yield return new object[] { "a\u0080b\u008Ec", "a-b-c" };

            // An instance ID that is too long for a blob name is truncated. The sanitized value is unchanged (all
            // characters are valid) but the composed blob name prefix exceeds the length limit and must be cut.
            yield return new object[] { new string('a', 1000), new string('a', 1000) };
        }

        [DataTestMethod]
        [DynamicData(nameof(InvalidInstanceIdCases), DynamicDataSourceType.Method)]
        public async Task OrchestrationWithPoisonMessage_InvalidInstanceId_IsSanitizedInBlobName(
            string instanceId,
            string expectedSanitizedInstanceId)
        {
            string prefix = CreateUniquePrefix();

            // MaxDequeueCount of 0 causes the message to be treated as poison on its very first dequeue, so we can
            // enqueue a control message directly and observe the poison behavior without running an orchestration.
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 0, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            BlobContainerClient instanceContainerClient = blobServiceClient.GetBlobContainerClient(instanceContainerName);
            await instanceContainerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            // Build and enqueue an ExecutionStarted control message with the (potentially invalid) instance ID
            // directly onto the single control queue. This intentionally bypasses the tracking table, which cannot
            // store arbitrarily long or exotic instance IDs, so we can focus on the poison-blob naming behavior.

            // Note that ExecutionStartedEvents can be enqueued before a table entry is created (for example for a suborchestration),
            // so this path is valid to exercise
            string executionId = Guid.NewGuid().ToString("N");
            var orchestrationInstance = new OrchestrationInstance { InstanceId = instanceId, ExecutionId = executionId };
            var executionStartedEvent = new ExecutionStartedEvent(-1, "\"hello\"")
            {
                Name = "SomeOrchestration",
                Version = string.Empty,
                OrchestrationInstance = orchestrationInstance,
            };
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = orchestrationInstance,
                Event = executionStartedEvent,
            };

            string controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, 0);
            MessageManager messageManager = CreateMessageManager(settings);
            var messageData = new MessageData(
                taskMessage,
                Guid.NewGuid(),
                controlQueueName,
                orchestrationEpisode: null,
                sender: new OrchestrationInstance { InstanceId = string.Empty, ExecutionId = string.Empty });
            string body = await messageManager.SerializeMessageDataAsync(messageData);

            var azureStorageClient = new DurableTask.AzureStorage.Storage.AzureStorageClient(settings);
            DurableTask.AzureStorage.Storage.Queue controlQueue = azureStorageClient.GetQueueReference(controlQueueName);
            await controlQueue.AddMessageAsync(body, visibilityDelay: null);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                await TestHelpers.WaitFor(
                    () => instanceContainerClient.Exists().Value && ListBlobsAsync(instanceContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(instanceContainerClient);
                Assert.AreEqual(1, blobs.Count);

                // The blob name uses the sanitized instance ID, and the composed "{instanceId}~{executionId}" prefix
                // is truncated to keep the total blob name within the 1024 character limit.
                const int maxPrefixLength = 1024 - 32 - 1;
                string expectedPrefix = $"{expectedSanitizedInstanceId}~{executionId}";
                if (expectedPrefix.Length > maxPrefixLength)
                {
                    expectedPrefix = expectedPrefix.Substring(0, maxPrefixLength);
                }

                Assert.AreEqual(expectedPrefix, blobs[0].Name.Substring(0, expectedPrefix.Length));
                Assert.IsTrue(blobs[0].Name.Length <= 1024, $"Blob name length {blobs[0].Name.Length} exceeds the 1024 character limit.");

                // Sanitization only affects the blob name. The stored poison message must preserve the original
                // (unsanitized) instance ID.
                MessageData poisonMessage = await DownloadPoisonMessagesAsync(instanceContainerClient, blobs[0].Name);
                Assert.AreEqual(instanceId, poisonMessage.TaskMessage.OrchestrationInstance.InstanceId);
                Assert.AreEqual(executionId, poisonMessage.TaskMessage.OrchestrationInstance.ExecutionId);
                Assert.IsInstanceOfType(poisonMessage.TaskMessage.Event, typeof(ExecutionStartedEvent));

                await AssertQueuesAreEmptyAsync(settings);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await instanceContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithMessageEqualToMaxDequeueCount_CompletesSuccessfully()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 2, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            // Fail the first completion to force the dispatch count to 2, which is equal to (not greater than) the
            // maximum dispatch count, so the message is not treated as poisoned.
            var service = new FaultInjectingOrchestrationService(inner)
            {
                OrchestrationCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    input: "hello");

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.IsFalse(await containerClient.ExistsAsync(), $"Blob container '{containerName}' should not exist");
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithDequeueExceedingMax_PoisonHandlingDisabled_CompletesSuccessfully_NoBlob()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix, poisonEnabled: false);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);
            var service = new FaultInjectingOrchestrationService(inner)
            {
                OrchestrationCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    input: "hello");

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.IsNull(state.FailureDetails);

                Assert.IsFalse(
                    await containerClient.ExistsAsync(),
                    $"Blob container '{containerName}' should not exist when poison handling is disabled");
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ActivityWithPoisonMessage_Failed_AndPoisonMessageStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient instanceContainerClient = blobServiceClient.GetBlobContainerClient(instanceContainerName);
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await instanceContainerClient.DeleteIfExistsAsync();
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            // Fail the first activity work item completion so the TaskScheduled message is redelivered with a dispatch
            // count of 2, which exceeds the maximum dispatch count of 1.
            var service = new FaultInjectingOrchestrationService(inner)
            {
                ActivityCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
            worker.AddTaskOrchestrations(typeof(ScheduleActivityOrchestration));
            worker.AddTaskActivities(typeof(EchoActivity));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(ScheduleActivityOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(ScheduleActivityOrchestration)),
                    input: "hello");

                // The activity message exceeds the maximum dequeue count and is moved to poison storage. The
                // orchestration itself never completes because the activity result is never produced, so we wait
                // for the poison blob to appear rather than for orchestration completion.
                await TestHelpers.WaitFor(
                    () => activityContainerClient.Exists().Value && ListBlobsAsync(activityContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                Assert.IsFalse(
                    await instanceContainerClient.ExistsAsync(),
                    $"Blob container '{instanceContainerName}' should not exist");

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                string expectedPrefix = $"{instance.InstanceId}~{instance.ExecutionId}";
                Assert.AreEqual(expectedPrefix, blobs[0].Name.Substring(0, expectedPrefix.Length));

                MessageData poisonMessage = await DownloadPoisonMessagesAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(instance.InstanceId, poisonMessage.Sender.InstanceId);
                Assert.AreEqual(instance.ExecutionId, poisonMessage.Sender.ExecutionId);
                Assert.AreEqual(instance.InstanceId, poisonMessage.TaskMessage.OrchestrationInstance.InstanceId);
                Assert.AreEqual(instance.ExecutionId, poisonMessage.TaskMessage.OrchestrationInstance.ExecutionId);

                string activityName = NameVersionHelper.GetDefaultName(typeof(EchoActivity));
                Assert.IsInstanceOfType(poisonMessage.TaskMessage.Event, typeof(TaskScheduledEvent));
                var taskScheduledEvent = (TaskScheduledEvent)poisonMessage.TaskMessage.Event;
                Assert.AreEqual(activityName, taskScheduledEvent.Name);
                Assert.AreEqual("[\"hello\"]", taskScheduledEvent.Input);

                await AssertQueuesAreEmptyAsync(settings);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await instanceContainerClient.DeleteIfExistsAsync();
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ActivityWithMessageEqualToMaxDequeueCount_CompletesSuccessfully()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 2, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            // Fail the first activity completion to force the dispatch count to 2, which is equal to (not greater than)
            // the maximum dispatch count, so the message is not treated as poisoned.
            var service = new FaultInjectingOrchestrationService(inner)
            {
                ActivityCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(ScheduleActivityOrchestration));
            worker.AddTaskActivities(typeof(EchoActivity));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(ScheduleActivityOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(ScheduleActivityOrchestration)),
                    input: "hello");

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.IsFalse(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should not exist");
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ActivityWithDispatchExceedingMax_PoisonHandlingDisabled_CompletesSuccessfully_NoBlob()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix, poisonEnabled: false);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);
            var service = new FaultInjectingOrchestrationService(inner)
            {
                ActivityCompletionFailuresRemaining = 1,
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(ScheduleActivityOrchestration));
            worker.AddTaskActivities(typeof(EchoActivity));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(ScheduleActivityOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(ScheduleActivityOrchestration)),
                    input: "hello");

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                Assert.IsNull(state.FailureDetails);

                Assert.IsFalse(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should not exist when poison handling is disabled");
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ControlQueueMessageWithBadJson_StoredAsPoison_AndDeleted()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient instanceContainerClient = blobServiceClient.GetBlobContainerClient(instanceContainerName);
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await instanceContainerClient.DeleteIfExistsAsync();
            await activityContainerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            // Insert a malformed message directly into the single control queue. It cannot be deserialized, so once
            // its dequeue count exceeds the maximum it must be moved to poison storage and deleted from the queue.
            var azureStorageClient = new DurableTask.AzureStorage.Storage.AzureStorageClient(settings);
            string controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, 0);
            DurableTask.AzureStorage.Storage.Queue controlQueue = azureStorageClient.GetQueueReference(controlQueueName);
            const string badMessage = "{ this is not valid json";
            await controlQueue.AddMessageAsync(badMessage, visibilityDelay: null);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            await worker.StartAsync();

            try
            {
                await TestHelpers.WaitFor(
                    () => instanceContainerClient.Exists().Value && ListBlobsAsync(instanceContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                Assert.IsFalse(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should not exist");

                List<BlobItem> blobs = await ListBlobsAsync(instanceContainerClient);
                Assert.AreEqual(1, blobs.Count);

                // The stored poison message must match the original (undeserializable) queue message body.
                string blobContent = await DownloadBlobTextAsync(instanceContainerClient, blobs[0].Name);
                Assert.AreEqual(badMessage, blobContent);

                // The malformed message must have been deleted from the control queue.
                await AssertQueuesAreEmptyAsync(settings);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await instanceContainerClient.DeleteIfExistsAsync();
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task WorkItemQueueMessageWithBadJson_StoredAsPoison_AndDeleted()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(MaxDequeueCount: 1, prefix: prefix);

            // The work item queue does not abandon undeserializable messages, so redelivery only happens after the
            // visibility timeout expires. Shorten it so the message is quickly redelivered (bumping its dequeue
            // count past the maximum) and moved to poison storage within the test's wait window.
            settings.WorkItemQueueVisibilityTimeout = TimeSpan.FromSeconds(3);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{settings.TaskHubName}-{prefix}-instance-messages";
            string activityContainerName = $"{settings.TaskHubName}-{prefix}-activity-messages";
            BlobContainerClient instanceContainerClient = blobServiceClient.GetBlobContainerClient(instanceContainerName);
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await instanceContainerClient.DeleteIfExistsAsync();
            await activityContainerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            // Insert a malformed message directly into the work item queue. It cannot be deserialized, so once its
            // dequeue count exceeds the maximum it must be moved to poison storage and deleted from the queue.
            var azureStorageClient = new DurableTask.AzureStorage.Storage.AzureStorageClient(settings);
            string workItemQueueName = AzureStorageOrchestrationService.GetWorkItemQueueName(settings.TaskHubName);
            DurableTask.AzureStorage.Storage.Queue workItemQueue = azureStorageClient.GetQueueReference(workItemQueueName);
            const string badMessage = "{ this is not valid json";
            await workItemQueue.AddMessageAsync(badMessage, visibilityDelay: null);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            await worker.StartAsync();

            try
            {
                await TestHelpers.WaitFor(
                    () => activityContainerClient.Exists().Value && ListBlobsAsync(activityContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                Assert.IsFalse(
                    await instanceContainerClient.ExistsAsync(),
                    $"Blob container '{instanceContainerName}' should not exist");

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                // The stored poison message must match the original (undeserializable) queue message body.
                string blobContent = await DownloadBlobTextAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(badMessage, blobContent);

                // The malformed message must have been deleted from the work item queue.
                await AssertQueuesAreEmptyAsync(settings);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await instanceContainerClient.DeleteIfExistsAsync();
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [DataTestMethod]
        [DataRow("durable-task-poison")]
        [DataRow("abc")]
        [DataRow("a1")]
        [DataRow("a-b-c")]
        [DataRow("123")]
        [DataRow("a1-2b-3c")]
        public void PoisonMessageStorageContainerNamePrefix_ValidValue_IsAccepted(string value)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PoisonMessageStorageContainerNamePrefix = value,
            };

            Assert.AreEqual(value, settings.PoisonMessageStorageContainerNamePrefix);
        }

        [TestMethod]
        public void PoisonMessageStorageContainerNamePrefix_MaxLength_IsAccepted()
        {
            // The prefix is embedded in "{taskhubname}-{prefix}-instance-messages"; the validation reserves room for
            // the "-instance-messages" suffix plus a "-" and one char for the taskhub name within the 63-character limit.
            int maxPrefixLength = 63 - "-instance-messages".Length - 2;
            string maxLength = new string('a', maxPrefixLength);
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                PoisonMessageStorageContainerNamePrefix = maxLength,
            };

            Assert.AreEqual(maxLength, settings.PoisonMessageStorageContainerNamePrefix);
        }

        [DataTestMethod]
        [DataRow("")]                 // empty
        [DataRow("Abc")]              // uppercase
        [DataRow("ABC")]              // uppercase
        [DataRow("-abc")]             // leading hyphen
        [DataRow("abc-")]             // trailing hyphen
        [DataRow("a--b")]             // consecutive hyphens
        [DataRow("a_b")]              // underscore is not allowed
        [DataRow("a.b")]              // period is not allowed
        [DataRow("a b")]              // whitespace is not allowed
        public void PoisonMessageStorageContainerNamePrefix_InvalidValue_ThrowsArgumentException(string value)
        {
            var settings = new AzureStorageOrchestrationServiceSettings();

            Assert.ThrowsException<ArgumentException>(
                () => settings.PoisonMessageStorageContainerNamePrefix = value);
        }

        [TestMethod]
        public void PoisonMessageStorageContainerNamePrefix_TooLong_ThrowsArgumentException()
        {
            var settings = new AzureStorageOrchestrationServiceSettings();
            int maxPrefixLength = 63 - "-instance-messages".Length - 2;
            string tooLong = new string('a', maxPrefixLength + 1);

            Assert.ThrowsException<ArgumentException>(
                () => settings.PoisonMessageStorageContainerNamePrefix = tooLong);
        }

        [TestMethod]
        public void PoisonMessageStorageContainerNamePrefix_Null_ThrowsArgumentNullException()
        {
            var settings = new AzureStorageOrchestrationServiceSettings();

            Assert.ThrowsException<ArgumentNullException>(
                () => settings.PoisonMessageStorageContainerNamePrefix = null!);
        }


        static AzureStorageOrchestrationServiceSettings CreateSettings(
            int MaxDequeueCount,
            string prefix,
            bool poisonEnabled = true)
        {
            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);

            // Use a unique task hub per test to isolate queues/tables from other tests.
            settings.TaskHubName = "poison" + Guid.NewGuid().ToString("N").Substring(0, 10);
            settings.IsPoisonMessageStorageEnabled = poisonEnabled;
            settings.MaxDequeueCount = MaxDequeueCount;
            settings.PoisonMessageStorageContainerNamePrefix = prefix;

            // Use a single partition so tests have a single, deterministic control queue to inspect.
            settings.PartitionCount = 1;

            return settings;
        }

        // Verifies that every control queue and the work item queue for the given task hub is empty. This confirms
        // that processed and poisoned messages have been removed from their source queues.
        static async Task AssertQueuesAreEmptyAsync(AzureStorageOrchestrationServiceSettings settings)
        {
            var azureStorageClient = new DurableTask.AzureStorage.Storage.AzureStorageClient(settings);

            var queueNames = new List<string>();
            for (int i = 0; i < settings.PartitionCount; i++)
            {
                queueNames.Add(AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, i));
            }

            queueNames.Add(AzureStorageOrchestrationService.GetWorkItemQueueName(settings.TaskHubName));

            foreach (string queueName in queueNames)
            {
                DurableTask.AzureStorage.Storage.Queue queue = azureStorageClient.GetQueueReference(queueName);
                await TestHelpers.WaitFor(
                    () => queue.GetApproximateMessagesCountAsync().GetAwaiter().GetResult() == 0,
                    TimeSpan.FromSeconds(30));
            }
        }

        static BlobServiceClient CreateBlobServiceClient()
        {
            return new BlobServiceClient(TestHelpers.GetTestStorageAccountConnectionString());
        }

        // Container names must be lowercase and no longer than 63 characters. The longest suffix we append is
        // "-instance-messages" (18 characters), so keep the prefix short.
        static string CreateUniquePrefix()
        {
            return "pmtest" + Guid.NewGuid().ToString("N").Substring(0, 12);
        }

        static MessageManager CreateMessageManager(AzureStorageOrchestrationServiceSettings settings)
        {
            var azureStorageClient = new DurableTask.AzureStorage.Storage.AzureStorageClient(settings);
            return new MessageManager(settings, azureStorageClient, "$root");
        }

        // When an orchestration fails, its FailureDetails may be surfaced directly on OrchestrationState.FailureDetails,
        // or (in the current AzureStorage completion path) serialized into OrchestrationState.Output using the
        // FailureDetails.ToString() format "{ErrorType}: {ErrorMessage}". This helper returns the FailureDetails from
        // whichever the backend populated so tests can assert ErrorType/ErrorMessage uniformly.
        static FailureDetails GetFailureDetails(OrchestrationState state)
        {
            if (state.FailureDetails != null)
            {
                return state.FailureDetails;
            }

            string output = state.Output ?? string.Empty;
            int separatorIndex = output.IndexOf(": ", StringComparison.Ordinal);
            if (separatorIndex < 0)
            {
                return null;
            }

            string errorType = output.Substring(0, separatorIndex);
            string errorMessage = output.Substring(separatorIndex + 2);

            // The IsNonRetriable flag is not preserved in the flattened "{ErrorType}: {ErrorMessage}" output. The poison
            // failure types are always written as non-retriable, so infer the flag from the error type.
            bool isNonRetriable = errorType == "OrchestrationHistoryCorrupted" || errorType == "PoisonMessages";

            return new FailureDetails(errorType, errorMessage, stackTrace: null, innerFailure: null, isNonRetriable: isNonRetriable);
        }

        static async Task<List<BlobItem>> ListBlobsAsync(BlobContainerClient containerClient)
        {
            var blobs = new List<BlobItem>();
            await foreach (BlobItem blob in containerClient.GetBlobsAsync())
            {
                blobs.Add(blob);
            }

            return blobs;
        }

        static async Task<MessageData> DownloadPoisonMessagesAsync(BlobContainerClient containerClient, string blobName)
        {
            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();
            string blobContent = downloadResult.Content.ToString();

            Assert.IsFalse(string.IsNullOrEmpty(blobContent), "Blob content should not be empty");

            MessageData poisonMessage = JsonConvert.DeserializeObject<MessageData>(
                blobContent,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });

            Assert.IsNotNull(poisonMessage);
            return poisonMessage;
        }

        static async Task<string> DownloadBlobTextAsync(BlobContainerClient containerClient, string blobName)
        {
            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();
            return downloadResult.Content.ToString();
        }

        sealed class EchoOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult(input);
            }
        }

        [KnownType(typeof(EchoActivity))]
        sealed class ScheduleActivityOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(EchoActivity), input);
            }
        }

        sealed class EchoActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return input;
            }
        }

        [KnownType(typeof(ThrowingActivity))]
        sealed class ScheduleThrowingActivityOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(ThrowingActivity), input);
            }
        }

        sealed class ThrowingActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new InvalidOperationException("boom from activity");
            }
        }

        /// <summary>
        /// A decorator around <see cref="AzureStorageOrchestrationService"/> that forwards all calls to the inner
        /// service but can be configured to throw a transient failure the first N times a work item is completed.
        /// This is used to force work items to be abandoned and redelivered (incrementing their dequeue/dispatch
        /// counts) so that poison message handling can be exercised.
        /// </summary>
        sealed class FaultInjectingOrchestrationService : IEntityOrchestrationService, IOrchestrationServiceClient
        {
            readonly AzureStorageOrchestrationService inner;

            int orchestrationCompletionFailuresRemaining;
            int activityCompletionFailuresRemaining;

            public FaultInjectingOrchestrationService(AzureStorageOrchestrationService inner)
            {
                this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public int OrchestrationCompletionFailuresRemaining
            {
                get => this.orchestrationCompletionFailuresRemaining;
                set => this.orchestrationCompletionFailuresRemaining = value;
            }

            public int ActivityCompletionFailuresRemaining
            {
                get => this.activityCompletionFailuresRemaining;
                set => this.activityCompletionFailuresRemaining = value;
            }

            /// <summary>
            /// Optional hook invoked on each activity work item after it is locked (but before it is dispatched),
            /// allowing a test to corrupt the work item to exercise the invalid-work-item poison path.
            /// </summary>
            public Action<TaskActivityWorkItem> CorruptActivityWorkItem { get; set; }

            /// <summary>
            /// Optional hook invoked on each orchestration work item after it is locked (but before it is dispatched),
            /// allowing a test to corrupt the work item to exercise the invalid-work-item poison path.
            /// </summary>
            public Action<TaskOrchestrationWorkItem> CorruptOrchestrationWorkItem { get; set; }

            IOrchestrationService InnerService => this.inner;

            IOrchestrationServiceClient InnerClient => this.inner;

            IEntityOrchestrationService InnerEntityService => this.inner;

            // ---- IOrchestrationService: management/lifecycle ----

            public int TaskOrchestrationDispatcherCount => this.InnerService.TaskOrchestrationDispatcherCount;

            public int MaxConcurrentTaskOrchestrationWorkItems => this.InnerService.MaxConcurrentTaskOrchestrationWorkItems;

            public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => this.InnerService.EventBehaviourForContinueAsNew;

            public int TaskActivityDispatcherCount => this.InnerService.TaskActivityDispatcherCount;

            public int MaxConcurrentTaskActivityWorkItems => this.InnerService.MaxConcurrentTaskActivityWorkItems;

            public Task StartAsync() => this.InnerService.StartAsync();

            public Task StopAsync() => this.InnerService.StopAsync();

            public Task StopAsync(bool isForced) => this.InnerService.StopAsync(isForced);

            public Task CreateAsync() => this.InnerService.CreateAsync();

            public Task CreateAsync(bool recreateInstanceStore) => this.InnerService.CreateAsync(recreateInstanceStore);

            public Task CreateIfNotExistsAsync() => this.InnerService.CreateIfNotExistsAsync();

            public Task DeleteAsync() => this.InnerService.DeleteAsync();

            public Task DeleteAsync(bool deleteInstanceStore) => this.InnerService.DeleteAsync(deleteInstanceStore);

            public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState) =>
                this.InnerService.IsMaxMessageCountExceeded(currentMessageCount, runtimeState);

            public int GetDelayInSecondsAfterOnProcessException(Exception exception) =>
                this.InnerService.GetDelayInSecondsAfterOnProcessException(exception);

            public int GetDelayInSecondsAfterOnFetchException(Exception exception) =>
                this.InnerService.GetDelayInSecondsAfterOnFetchException(exception);

            // ---- IOrchestrationService: orchestration dispatcher ----

            public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
            {
                TaskOrchestrationWorkItem workItem = await this.InnerService.LockNextTaskOrchestrationWorkItemAsync(receiveTimeout, cancellationToken);
                if (workItem != null)
                {
                    this.CorruptOrchestrationWorkItem?.Invoke(workItem);
                }

                return workItem;
            }

            public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem) =>
                this.InnerService.RenewTaskOrchestrationWorkItemLockAsync(workItem);

            public Task CompleteTaskOrchestrationWorkItemAsync(
                TaskOrchestrationWorkItem workItem,
                OrchestrationRuntimeState newOrchestrationRuntimeState,
                IList<TaskMessage> outboundMessages,
                IList<TaskMessage> orchestratorMessages,
                IList<TaskMessage> timerMessages,
                TaskMessage continuedAsNewMessage,
                OrchestrationState orchestrationState)
            {
                if (Interlocked.Decrement(ref this.orchestrationCompletionFailuresRemaining) >= 0)
                {
                    throw new Exception("Simulated transient failure");
                }

                return this.InnerService.CompleteTaskOrchestrationWorkItemAsync(
                    workItem,
                    newOrchestrationRuntimeState,
                    outboundMessages,
                    orchestratorMessages,
                    timerMessages,
                    continuedAsNewMessage,
                    orchestrationState);
            }

            public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem) =>
                this.InnerService.AbandonTaskOrchestrationWorkItemAsync(workItem);

            public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem) =>
                this.InnerService.ReleaseTaskOrchestrationWorkItemAsync(workItem);

            // ---- IOrchestrationService: activity dispatcher ----

            public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken) =>
                this.LockNextTaskActivityWorkItemInternalAsync(receiveTimeout, cancellationToken);

            async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItemInternalAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
            {
                TaskActivityWorkItem workItem = await this.InnerService.LockNextTaskActivityWorkItem(receiveTimeout, cancellationToken);
                if (workItem != null)
                {
                    this.CorruptActivityWorkItem?.Invoke(workItem);
                }

                return workItem;
            }

            public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem) =>
                this.InnerService.RenewTaskActivityWorkItemLockAsync(workItem);

            public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
            {
                if (Interlocked.Decrement(ref this.activityCompletionFailuresRemaining) >= 0)
                {
                    throw new Exception("Simulated transient failure");
                }

                return this.InnerService.CompleteTaskActivityWorkItemAsync(workItem, responseMessage);
            }

            public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem) =>
                this.InnerService.AbandonTaskActivityWorkItemAsync(workItem);

            // ---- IEntityOrchestrationService ----

            public EntityBackendProperties EntityBackendProperties => this.InnerEntityService.EntityBackendProperties;

            public EntityBackendQueries EntityBackendQueries => this.InnerEntityService.EntityBackendQueries;

            public async Task<TaskOrchestrationWorkItem> LockNextOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
            {
                TaskOrchestrationWorkItem workItem = await this.InnerEntityService.LockNextOrchestrationWorkItemAsync(receiveTimeout, cancellationToken);
                if (workItem != null)
                {
                    this.CorruptOrchestrationWorkItem?.Invoke(workItem);
                }

                return workItem;
            }

            public Task<TaskOrchestrationWorkItem> LockNextEntityWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken) =>
                this.InnerEntityService.LockNextEntityWorkItemAsync(receiveTimeout, cancellationToken);

            // ---- IOrchestrationServiceClient ----

            public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage) =>
                this.InnerClient.CreateTaskOrchestrationAsync(creationMessage);

            public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses) =>
                this.InnerClient.CreateTaskOrchestrationAsync(creationMessage, dedupeStatuses);

            public Task SendTaskOrchestrationMessageAsync(TaskMessage message) =>
                this.InnerClient.SendTaskOrchestrationMessageAsync(message);

            public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages) =>
                this.InnerClient.SendTaskOrchestrationMessageBatchAsync(messages);

            public Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken) =>
                this.InnerClient.WaitForOrchestrationAsync(instanceId, executionId, timeout, cancellationToken);

            public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason) =>
                this.InnerClient.ForceTerminateTaskOrchestrationAsync(instanceId, reason);

            public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions) =>
                this.InnerClient.GetOrchestrationStateAsync(instanceId, allExecutions);

            public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId) =>
                this.InnerClient.GetOrchestrationStateAsync(instanceId, executionId);

            public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId) =>
                this.InnerClient.GetOrchestrationHistoryAsync(instanceId, executionId);

            public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType) =>
                this.InnerClient.PurgeOrchestrationHistoryAsync(thresholdDateTimeUtc, timeRangeFilterType);
        }
    }
}
