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
    using System.ClientModel;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Queues.Models;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
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
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 1, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            // Fail the first orchestration work item completion so the ExecutionStarted message is redelivered
            // with a dispatch count of 2, which exceeds the maximum dispatch count of 1.
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

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);

                // The orchestration-level FailureDetails is serialized into the output as "{ErrorType}: {ErrorMessage}",
                // so reconstruct it from whichever the backend populated.
                FailureDetails failureDetails = GetFailureDetails(state);
                Assert.IsNotNull(failureDetails);
                Assert.AreEqual("PoisonMessages", failureDetails.ErrorType);
                StringAssert.Contains(failureDetails.ErrorMessage, EventType.ExecutionStarted.ToString());
                StringAssert.Contains(failureDetails.ErrorMessage, "maximum dispatch count of 1");
                StringAssert.Contains(failureDetails.ErrorMessage, "dispatch counts 2");
                Assert.IsTrue(failureDetails.IsNonRetriable);

                Assert.IsTrue(await containerClient.ExistsAsync(), $"Blob container '{containerName}' should exist");

                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);

                string expectedPrefix = $"{instance.InstanceId}~{instance.ExecutionId}";
                Assert.AreEqual(expectedPrefix, blobs[0].Name.Substring(0, expectedPrefix.Length));

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(ExecutionStartedEvent));
                var executionStartedEvent = (ExecutionStartedEvent)poisonMessages[0].Event;
                Assert.AreEqual(instance.InstanceId, executionStartedEvent.OrchestrationInstance.InstanceId);
                Assert.AreEqual(instance.ExecutionId, executionStartedEvent.OrchestrationInstance.ExecutionId);
                Assert.AreEqual(NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)), executionStartedEvent.Name);
                Assert.AreEqual("\"hello\"", executionStartedEvent.Input);
                Assert.IsNotNull(executionStartedEvent.Tags);
                Assert.AreEqual(1, executionStartedEvent.Tags.Count);
                Assert.IsTrue(executionStartedEvent.Tags.ContainsKey("key"));
                Assert.AreEqual("value", executionStartedEvent.Tags["key"]);
                Assert.AreEqual(2, executionStartedEvent.DispatchCount);
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithMessageEqualToMaxDispatchCount_CompletesSuccessfully()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 2, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
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
        public async Task OrchestrationWithDispatchExceedingMax_PoisonHandlingDisabled_CompletesSuccessfully_NoBlob()
        {
            // Even with MaxDispatchCount=1 and an injected transient failure that pushes DispatchCount above the limit,
            // when poison handling is disabled the message must NOT be treated as poisoned, the orchestration must NOT
            // fail, and no blob container should be created.
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 1, prefix: prefix, poisonEnabled: false);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
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
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 1, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string instanceContainerName = $"{prefix}-instance-messages";
            string activityContainerName = $"{prefix}-activity-messages";
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

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);

                // The orchestration fails because the activity exceeded the maximum dispatch count. The poison reason
                // now propagates back to the calling orchestration and is surfaced in the output.
                FailureDetails failureDetails = GetFailureDetails(state);
                Assert.IsNotNull(failureDetails);
                StringAssert.Contains(failureDetails.ErrorMessage, "maximum dispatch count of 1");
                StringAssert.Contains(failureDetails.ErrorMessage, "dispatch count 2");
                // Currently DT.Core does not propagate the nonretriable property from the Activity/entity that caused the failure
                //Assert.IsTrue(failureDetails.IsNonRetriable);

                Assert.IsFalse(
                    await instanceContainerClient.ExistsAsync(),
                    $"Blob container '{instanceContainerName}' should not exist");
                Assert.IsTrue(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should exist");

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                string activityName = NameVersionHelper.GetDefaultName(typeof(EchoActivity));
                string expectedPrefix = $"{activityName}~{instance.InstanceId}~{instance.ExecutionId}";
                Assert.AreEqual(expectedPrefix, blobs[0].Name.Substring(0, expectedPrefix.Length));

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(TaskScheduledEvent));
                var taskScheduledEvent = (TaskScheduledEvent)poisonMessages[0].Event;
                Assert.AreEqual(activityName, taskScheduledEvent.Name);
                Assert.AreEqual("[\"hello\"]", taskScheduledEvent.Input);
                Assert.AreEqual(2, taskScheduledEvent.DispatchCount);
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
        public async Task ActivityWithMessageEqualToMaxDispatchCount_CompletesSuccessfully()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 2, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{prefix}-activity-messages";
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
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 1, prefix: prefix, poisonEnabled: false);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{prefix}-activity-messages";
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
        public async Task ActivityWithInvalidWorkItem_MissingOrchestrationInstance_PoisonMessageStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{prefix}-activity-messages";
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                // Corrupt the first activity dispatch by removing its OrchestrationInstance, which routes the work
                // item through the activity dispatcher's "missing OrchestrationInstance" invalid-work-item path.
                CorruptActivityWorkItem = wi =>
                {
                    if (Interlocked.Increment(ref corruptionCount) == 1)
                    {
                        wi.TaskMessage.OrchestrationInstance = null;
                    }
                },
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(ScheduleActivityOrchestration));
            worker.AddTaskActivities(typeof(EchoActivity));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(ScheduleActivityOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(ScheduleActivityOrchestration)),
                    input: "hello");

                await TestHelpers.WaitFor(
                    () => activityContainerClient.Exists().Value && ListBlobsAsync(activityContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                string activityName = NameVersionHelper.GetDefaultName(typeof(EchoActivity));
                StringAssert.StartsWith(blobs[0].Name, $"{activityName}~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                // The poison message preserves the corruption that we injected.
                Assert.IsNull(poisonMessages[0].OrchestrationInstance);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(TaskScheduledEvent));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ActivityWithInvalidWorkItem_NonTaskScheduledEvent_PoisonMessageStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{prefix}-activity-messages";
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptActivityWorkItem = wi =>
                {
                    if (Interlocked.Increment(ref corruptionCount) == 1)
                    {
                        // Replace the TaskScheduled event with an unrelated event type to trigger the
                        // "unsupported event type" invalid-work-item path in the activity dispatcher.
                        wi.TaskMessage.Event = new EventRaisedEvent(-1, "garbage") { Name = "notAnActivity" };
                    }
                },
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.AddTaskOrchestrations(typeof(ScheduleActivityOrchestration));
            worker.AddTaskActivities(typeof(EchoActivity));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(ScheduleActivityOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(ScheduleActivityOrchestration)),
                    input: "hello");

                await TestHelpers.WaitFor(
                    () => activityContainerClient.Exists().Value && ListBlobsAsync(activityContainerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                // When the event type isn't TaskScheduled, the blob name uses an empty activity name prefix ("~").
                StringAssert.StartsWith(blobs[0].Name, "~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(EventRaisedEvent));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task ActivityWithInvalidWorkItem_MissingActivityName_CallingOrchestrationFails()
        {
            // When an activity work item's TaskScheduledEvent has no activity name, the activity dispatcher cannot
            // dispatch it and (with poison handling enabled) stores the poison message and responds to the calling
            // orchestration with a non-retriable TaskFailedEvent. This surfaces as a failed activity, which fails the
            // calling orchestration.
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string activityContainerName = $"{prefix}-activity-messages";
            BlobContainerClient activityContainerClient = blobServiceClient.GetBlobContainerClient(activityContainerName);
            await activityContainerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptActivityWorkItem = wi =>
                {
                    // Strip the activity name from every dispatch so the message can never be dispatched. The dispatcher
                    // detects the missing name and fails the activity back to the orchestration.
                    if (wi.TaskMessage.Event is TaskScheduledEvent scheduledEvent)
                    {
                        scheduledEvent.Name = null;
                    }
                },
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

                OrchestrationState state = await client.WaitForOrchestrationAsync(instance, DefaultTimeout);

                Assert.IsNotNull(state);
                // The activity could not be dispatched (no activity name), so the calling orchestration fails with the
                // poison reason propagated back to it.
                Assert.AreEqual(OrchestrationStatus.Failed, state.OrchestrationStatus);
                FailureDetails failureDetails = GetFailureDetails(state);
                Assert.IsNotNull(failureDetails);
                StringAssert.Contains(failureDetails.ErrorMessage, "does not specify an activity name");
                // Currently DT.Core does not propagate the nonretriable property from the Activity/entity that caused the failure
                //Assert.IsTrue(failureDetails.IsNonRetriable);

                // The poison message is also stored to the activity poison container before the failure is returned.
                Assert.IsTrue(
                    await activityContainerClient.ExistsAsync(),
                    $"Blob container '{activityContainerName}' should exist");

                List<BlobItem> blobs = await ListBlobsAsync(activityContainerClient);
                Assert.AreEqual(1, blobs.Count);

                // When the activity name is missing, the blob name uses an empty activity name prefix ("~").
                StringAssert.StartsWith(blobs[0].Name, "~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(activityContainerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(TaskScheduledEvent));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await activityContainerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithInvalidWorkItem_MissingOrchestrationInstance_PoisonMessagesStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptOrchestrationWorkItem = wi =>
                {
                    if (Interlocked.Increment(ref corruptionCount) == 1 && wi.NewMessages.Count > 0)
                    {
                        // Swap for a mutable list, then replace the first message's OrchestrationInstance with one that
                        // has an empty InstanceId. ReconcileMessagesWithState returns false with "Work item includes a
                        // message with no orchestration instance ID...", which routes through HandleInvalidWorkItemAsync
                        // and fails the orchestration with OrchestrationHistoryCorrupted.
                        wi.NewMessages = new List<TaskMessage>(wi.NewMessages);

                        OrchestrationInstance originalInstance = wi.NewMessages[0].OrchestrationInstance;
                        wi.NewMessages[0].OrchestrationInstance = new OrchestrationInstance
                        {
                            InstanceId = string.Empty,
                            ExecutionId = string.Empty,
                        };

                        // Inject an extra message so the poison handler must persist the full batch.
                        wi.NewMessages.Add(new TaskMessage
                        {
                            OrchestrationInstance = originalInstance,
                            Event = new EventRaisedEvent(-1, "extra payload") { Name = "extraMarker" },
                        });
                    }
                },
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    input: "hello");

                await TestHelpers.WaitFor(
                    () => containerClient.Exists().Value && ListBlobsAsync(containerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);
                // Empty instance/execution ids sanitize to "~~{guid}".
                StringAssert.StartsWith(blobs[0].Name, "~~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                // Both the OI-stripped ExecutionStarted message and the injected event must be persisted as poison.
                Assert.AreEqual(2, poisonMessages.Count);
                Assert.IsTrue(poisonMessages.Any(m => m.Event is ExecutionStartedEvent && string.IsNullOrEmpty(m.OrchestrationInstance?.InstanceId)));
                Assert.IsTrue(poisonMessages.Any(m => m.Event is EventRaisedEvent raised && raised.Name == "extraMarker"));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithInvalidWorkItem_NoExecutionStartedEvent_PoisonMessagesStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptOrchestrationWorkItem = wi =>
                {
                    if (Interlocked.Increment(ref corruptionCount) == 1 && wi.NewMessages.Count > 0)
                    {
                        // Replace the ExecutionStarted message with a non-ES message so the orchestration has no
                        // ExecutionStarted event either in history or in new messages. ReconcileMessagesWithState
                        // returns false with "Orchestration contains no ExecutionStarted event..." and fails the
                        // orchestration with OrchestrationHistoryCorrupted.
                        wi.NewMessages = new List<TaskMessage>(wi.NewMessages);

                        OrchestrationInstance instance = null;
                        for (int i = 0; i < wi.NewMessages.Count; i++)
                        {
                            if (wi.NewMessages[i].Event is ExecutionStartedEvent)
                            {
                                instance = wi.NewMessages[i].OrchestrationInstance;
                                wi.NewMessages[i] = new TaskMessage
                                {
                                    OrchestrationInstance = instance,
                                    Event = new EventRaisedEvent(-1, "fake input") { Name = "fakeEvent" },
                                };
                                break;
                            }
                        }

                        if (instance != null)
                        {
                            wi.NewMessages.Add(new TaskMessage
                            {
                                OrchestrationInstance = instance,
                                Event = new EventRaisedEvent(-1, "extra payload") { Name = "extraMarker" },
                            });
                        }
                    }
                },
            };
            
            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    input: "hello");

                await TestHelpers.WaitFor(
                    () => containerClient.Exists().Value && ListBlobsAsync(containerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);
                // Empty instance/execution ids sanitize to "~~{guid}".
                StringAssert.StartsWith(blobs[0].Name, "~~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                // Both the replaced ES message and the injected rider must be persisted as poison.
                Assert.AreEqual(2, poisonMessages.Count);
                Assert.IsTrue(poisonMessages.Any(m => m.Event is EventRaisedEvent raised && raised.Name == "fakeEvent"));
                Assert.IsTrue(poisonMessages.Any(m => m.Event is EventRaisedEvent raised && raised.Name == "extraMarker"));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithInvalidWorkItem_InvalidRuntimeState_PoisonMessagesStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptOrchestrationWorkItem = wi =>
                {
                    if (Interlocked.Increment(ref corruptionCount) == 1 && wi.NewMessages.Count > 0)
                    {
                        // Add a single EventRaisedEvent (neither an ExecutionStarted nor an OrchestratorStarted event)
                        // to the existing runtime state's history so OrchestrationRuntimeState.IsValid becomes false.
                        // We mutate the existing runtime state in place (rather than replacing it) so the work item and
                        // the session share the same runtime-state reference, which the completion path relies on.
                        // ReconcileMessagesWithState then returns false with an "Orchestration runtime state is
                        // invalid..." reason and fails the orchestration with OrchestrationHistoryCorrupted. New
                        // messages are left untouched so they are persisted as poison.
                        wi.OrchestrationRuntimeState.AddEvent(new EventRaisedEvent(-1, "fake input") { Name = "fakeEvent" });
                    }
                },
            };

            await service.CreateAsync(recreateInstanceStore: true);

            using var worker = new TaskHubWorker(service, loggerFactory: settings.LoggerFactory);
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
            worker.AddTaskOrchestrations(typeof(EchoOrchestration));
            await worker.StartAsync();

            try
            {
                var client = new TaskHubClient(service, loggerFactory: settings.LoggerFactory);
                await client.CreateOrchestrationInstanceAsync(
                    name: NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                    version: NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                    input: "hello");

                await TestHelpers.WaitFor(
                    () => containerClient.Exists().Value && ListBlobsAsync(containerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                // The untouched new messages (the original ExecutionStarted message) must be persisted as poison.
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(ExecutionStartedEvent));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task OrchestrationWithInvalidWorkItem_RewindWithOtherMessages_PoisonMessageStored()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var inner = new AzureStorageOrchestrationService(settings);

            int corruptionCount = 0;
            var service = new FaultInjectingOrchestrationService(inner)
            {
                CorruptOrchestrationWorkItem = wi =>
                {
                    // Only corrupt the very first dispatch. The poison handler abandons the work item, so it is
                    // redelivered; we let the redelivery run normally so the orchestration can complete.
                    if (Interlocked.Increment(ref corruptionCount) != 1 || wi.NewMessages.Count == 0)
                    {
                        return;
                    }

                    // Will contain the fresh ExecutionStartedEvent along with the ExecutionRewoundEvent below, which is illegal
                    // (the only new message should be the ExecutionRewoundEvent in this case)
                    wi.NewMessages = new List<TaskMessage>(wi.NewMessages);

                    OrchestrationInstance instance = wi.NewMessages[0].OrchestrationInstance;

                    // Pre-populate a synthetic runtime state in a terminal (Failed) status. ReconcileMessagesWithState's
                    // rewind poison branch requires OrchestrationStatus != Running, an ExecutionRewoundEvent in the
                    // batch, and NewMessages.Count > 1. The ParentTraceContext on the ExecutionStartedEvent is required
                    // so the dispatcher's rewind setup does not throw a NullReferenceException before the poison check.
                    var syntheticEs = new ExecutionStartedEvent(-1, null)
                    {
                        OrchestrationInstance = instance,
                        Name = NameVersionHelper.GetDefaultName(typeof(EchoOrchestration)),
                        Version = NameVersionHelper.GetDefaultVersion(typeof(EchoOrchestration)),
                        ParentTraceContext = new DistributedTraceContext("00-00000000000000000000000000000000-0000000000000000-00"),
                    };
                    var syntheticEc = new ExecutionCompletedEvent(-1, null, OrchestrationStatus.Failed);
                    wi.OrchestrationRuntimeState = new OrchestrationRuntimeState(new List<HistoryEvent> { syntheticEs, syntheticEc });

                    wi.NewMessages.Add(new TaskMessage
                    {
                        OrchestrationInstance = instance,
                        Event = new ExecutionRewoundEvent(-1, "fake rewind")
                        {
                            ParentTraceContext = new DistributedTraceContext("00-00000000000000000000000000000000-0000000000000000-00"),
                        },
                    });
                },
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

                await TestHelpers.WaitFor(
                    () => containerClient.Exists().Value && ListBlobsAsync(containerClient).GetAwaiter().GetResult().Count > 0,
                    TimeSpan.FromSeconds(30));

                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);
                StringAssert.StartsWith(blobs[0].Name, $"{instance.InstanceId}~");

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                // The poisoned batch must contain the injected rewind event alongside the original ExecutionStarted message.
                Assert.IsTrue(poisonMessages.Count > 1, "Expected the poisoned batch to contain the rewind event alongside other messages.");
                Assert.IsTrue(poisonMessages.Any(m => m.Event is ExecutionRewoundEvent));
                Assert.IsTrue(poisonMessages.Any(m => m.Event is ExecutionStartedEvent));
            }
            finally
            {
                await worker.StopAsync(isForced: true);
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task PoisonMessageHandlerApis_PoisonHandlingDisabled_ReturnFalse()
        {
            // Directly invokes each IPoisonMessageHandler API on a service constructed with poison handling disabled.
            // All handler APIs must return false, MaxDispatchCount must be int.MaxValue (so the dispatchers never treat
            // any message as poisoned), and no blob containers should be created by these calls.
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 1, prefix: prefix, poisonEnabled: false);

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string[] containerSuffixes = { "instance-messages", "activity-messages" };
            foreach (string suffix in containerSuffixes)
            {
                await blobServiceClient.GetBlobContainerClient($"{prefix}-{suffix}").DeleteIfExistsAsync();
            }

            var service = new AzureStorageOrchestrationService(settings);
            IPoisonMessageHandler handler = service;

            var orchInstance = new OrchestrationInstance
            {
                InstanceId = Guid.NewGuid().ToString("N"),
                ExecutionId = Guid.NewGuid().ToString("N"),
            };

            // 1. Poison detection is effectively off (unbounded dispatch count) when handling is disabled.
            Assert.AreEqual(int.MaxValue, handler.MaxDispatchCount);

            // 2. HandlePoisonEntityMessageAsync returns false.
            var entityRequest = new EventRaisedEvent(eventId: -1, input: "{}")
            {
                Name = "op",
                DispatchCount = 100,
            };
            Assert.IsFalse(await handler.HandlePoisonEntityMessageAsync(
                orchInstance, entityRequest, PoisonMessageReason.DeserializationError, "disabled-test"));

            // 3. HandleInvalidWorkItemAsync(TaskOrchestrationWorkItem) returns false.
            var orchestrationWorkItem = new TaskOrchestrationWorkItem
            {
                InstanceId = orchInstance.InstanceId,
                NewMessages = new List<TaskMessage>(),
                OrchestrationRuntimeState = new OrchestrationRuntimeState(),
                LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
            };
            Assert.IsFalse(await handler.HandleInvalidWorkItemAsync(
                orchestrationWorkItem, PoisonMessageReason.InvalidRuntimeState, "disabled-test", isEntity: false));

            // 4. HandleInvalidWorkItemAsync(TaskActivityWorkItem) returns false.
            var activityWorkItem = new TaskActivityWorkItem
            {
                Id = "act-1",
                LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
                TaskMessage = new TaskMessage
                {
                    Event = new TaskScheduledEvent(eventId: -1)
                    {
                        Name = "echoActivity",
                        Version = "1",
                        DispatchCount = 100,
                    },
                },
            };
            Assert.IsFalse(await handler.HandleInvalidWorkItemAsync(
                activityWorkItem, PoisonMessageReason.MissingActivityName, "disabled-test"));

            // 5. None of the poison blob containers should have been created.
            foreach (string containerName in containerSuffixes.Select(suffix => $"{prefix}-{suffix}"))
            {
                Assert.IsFalse(
                    await blobServiceClient.GetBlobContainerClient(containerName).ExistsAsync(),
                    $"Blob container '{containerName}' should not exist when poison handling is disabled");
            }
        }

        /*
         * Lower-level entity coverage: there is no in-repo way to make an orchestration call an entity,
         * so we exercise HandlePoisonEntityMessageAsync directly against a real service.
        */

        [TestMethod]
        public async Task EntityPoisonMessage_MalformedOpRequest_PoisonMessageStoredAndFailureResponseEnqueued()
        {
            // A malformed entity "op" request is poisoned; assert it is persisted to the (consolidated) instance
            // poison container and that a failure response is enqueued back to the calling orchestration.
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);
            // A single partition means a single control queue that we can inspect for the emitted failure response.
            settings.PartitionCount = 1;

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            string controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, 0);
            var controlQueueClient = new Azure.Storage.Queues.QueueClient(
                TestHelpers.GetTestStorageAccountConnectionString(), controlQueueName);

            try
            {
                IPoisonMessageHandler handler = service;

                var entityInstance = new OrchestrationInstance
                {
                    InstanceId = "@counter@myEntity",
                    ExecutionId = Guid.NewGuid().ToString("N"),
                };

                // The request id of the poisoned operation. The failure response event is named after this id so the
                // calling orchestration can correlate the response with its outstanding call.
                string requestId = Guid.NewGuid().ToString();

                // An "op" (entity operation call) request whose JSON preserves the "id" and "parent" (calling
                // orchestration) fields but is malformed afterwards. The strict decode fails (routing through poison
                // handling), but the lenient decode can still recover the caller so a failure response can be returned.
                var opEvent = new EventRaisedEvent(-1, $"{{\"op\":\"add\",\"id\":\"{requestId}\",\"parent\":\"@caller@1\",\"input\": \"not valid json")
                {
                    Name = "op",
                };

                bool handled = await handler.HandlePoisonEntityMessageAsync(
                    entityInstance, opEvent, PoisonMessageReason.DeserializationError, "malformed entity op request");

                Assert.IsTrue(handled);

                // The poison message is stored.
                Assert.IsTrue(await containerClient.ExistsAsync(), $"Blob container '{containerName}' should exist");
                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(EventRaisedEvent));
                Assert.AreEqual("op", ((EventRaisedEvent)poisonMessages[0].Event).Name);

                // A failure response is enqueued back to the calling orchestration ("@caller@1") so it does not hang
                // waiting for a response that will never come. Dequeue it and confirm it targets the caller and is
                // correlated with the original request id.
                QueueMessage queueMessage = null;
                await TestHelpers.WaitFor(
                    () =>
                    {
                        queueMessage = controlQueueClient.ReceiveMessage(TimeSpan.FromMinutes(1)).Value;
                        return queueMessage != null;
                    },
                    TimeSpan.FromSeconds(15));
                Assert.IsNotNull(queueMessage, "Expected a failure response to be enqueued to the caller's control queue.");

                MessageData messageData = CreateMessageManager(settings).DeserializeMessageData(queueMessage.MessageText);
                TaskMessage responseTaskMessage = messageData.TaskMessage;

                // The response is an EventRaisedEvent whose name is the request id, targeting the calling orchestration.
                Assert.IsInstanceOfType(responseTaskMessage.Event, typeof(EventRaisedEvent));
                var responseEvent = (EventRaisedEvent)responseTaskMessage.Event;
                Assert.AreEqual(requestId, responseEvent.Name);
                Assert.AreEqual("@caller@1", responseTaskMessage.OrchestrationInstance.InstanceId);

                // The response payload must carry a failure so the caller observes an error rather than a success.
                Assert.IsNotNull(responseEvent.Input);
                StringAssert.Contains(responseEvent.Input, "EntityRequestMessageDeserializationError");

                // No further messages should have been emitted.
                QueueMessage extraMessage = controlQueueClient.ReceiveMessage(TimeSpan.FromSeconds(1)).Value;
                Assert.IsNull(extraMessage, "Only a single failure response should have been emitted.");
            }
            finally
            {
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task EntityPoisonMessage_DispatchCountReason_ReturnsFalseAndNoMessagesStored()
        {
            // When an entity message is poisoned because it exceeded the maximum dispatch count (rather than because it
            // could not be deserialized), the message is still archived to the poison container, but
            // HandlePoisonEntityMessageAsync returns false so the dispatcher can continue processing, and it does NOT
            // emit any follow-up entity message (no failure response or unlock).
            // This is because this method is only invoked with this reason in the case of an unlock or self-continue message,
            // and we do not want to leave the entity stuck in that case so we return false to continue processing
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);
            settings.PartitionCount = 1;

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            try
            {
                IPoisonMessageHandler handler = service;

                var entityInstance = new OrchestrationInstance
                {
                    InstanceId = "@counter@myEntity",
                    ExecutionId = Guid.NewGuid().ToString("N"),
                };

                // A well-formed release message that is poisoned only because it exceeded the dispatch count.
                var releaseEvent = new EventRaisedEvent(-1, "{\"parent\":\"@caller@1\",\"id\":\"fix\"}")
                {
                    Name = "release",
                };

                bool handled = await handler.HandlePoisonEntityMessageAsync(
                    entityInstance, releaseEvent, PoisonMessageReason.DispatchCount, "dispatch count exceeded");

                // The dispatch-count reason must return false (let the dispatcher continue) ...
                Assert.IsFalse(handled);

                // ... and no poison message should be stored (will be stored when work item completes) ...
                Assert.IsFalse(await containerClient.ExistsAsync(), $"Blob container '{containerName}' should not exist");
            }
            finally
            {
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task EntityPoisonMessage_MalformedReleaseRequest_PoisonMessageStoredAndUnlockEmitted()
        {
            string prefix = CreateUniquePrefix();
            AzureStorageOrchestrationServiceSettings settings = CreateSettings(maxDispatchCount: 5, prefix: prefix);
            // A single partition means a single control queue that we can inspect for the emitted unlock message.
            settings.PartitionCount = 1;

            BlobServiceClient blobServiceClient = CreateBlobServiceClient();
            string containerName = $"{prefix}-instance-messages";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.DeleteIfExistsAsync();

            var service = new AzureStorageOrchestrationService(settings);
            await service.CreateAsync(recreateInstanceStore: true);

            string controlQueueName = AzureStorageOrchestrationService.GetControlQueueName(settings.TaskHubName, 0);
            var controlQueueClient = new Azure.Storage.Queues.QueueClient(
                TestHelpers.GetTestStorageAccountConnectionString(), controlQueueName);

            try
            {
                IPoisonMessageHandler handler = service;

                var entityInstance = new OrchestrationInstance
                {
                    InstanceId = "@counter@myEntity",
                    ExecutionId = Guid.NewGuid().ToString("N"),
                };

                // A "release" (lock release) message whose JSON preserves the "parent" (lock owner) field but is
                // malformed afterwards. The strict decode fails (routing through poison handling), but the lenient
                // decode can still recover the lock owner so a fresh unlock can be emitted to the entity.
                var releaseEvent = new EventRaisedEvent(-1, "{\"parent\":\"@caller@1\",\"id\":\"fix\",\"lockset\": \"not valid json")
                {
                    Name = "release",
                };

                bool handled = await handler.HandlePoisonEntityMessageAsync(
                    entityInstance, releaseEvent, PoisonMessageReason.DeserializationError, "malformed entity release");

                Assert.IsTrue(handled);

                // The poison message is stored.
                Assert.IsTrue(await containerClient.ExistsAsync(), $"Blob container '{containerName}' should exist");
                List<BlobItem> blobs = await ListBlobsAsync(containerClient);
                Assert.AreEqual(1, blobs.Count);

                List<TaskMessage> poisonMessages = await DownloadPoisonMessagesAsync(containerClient, blobs[0].Name);
                Assert.AreEqual(1, poisonMessages.Count);
                Assert.IsInstanceOfType(poisonMessages[0].Event, typeof(EventRaisedEvent));
                Assert.AreEqual("release", ((EventRaisedEvent)poisonMessages[0].Event).Name);

                // A fresh unlock (release) message is emitted to the entity's control queue so the entity does not
                // remain locked forever by the failed caller. Dequeue it and confirm it is a well-formed release that
                // targets the entity and recovers the original lock owner ("@caller@1").
                QueueMessage queueMessage = null;
                await TestHelpers.WaitFor(
                    () =>
                    {
                        queueMessage = controlQueueClient.ReceiveMessage(TimeSpan.FromMinutes(1)).Value;
                        return queueMessage != null;
                    },
                    TimeSpan.FromSeconds(15));
                Assert.IsNotNull(queueMessage, "Expected an unlock message to be emitted to the entity's control queue.");

                MessageData messageData = CreateMessageManager(settings).DeserializeMessageData(queueMessage.MessageText);
                TaskMessage unlockTaskMessage = messageData.TaskMessage;

                // The emitted message must be an entity "release" event targeting the entity instance.
                Assert.IsInstanceOfType(unlockTaskMessage.Event, typeof(EventRaisedEvent));
                var unlockEvent = (EventRaisedEvent)unlockTaskMessage.Event;
                Assert.AreEqual("release", unlockEvent.Name);
                Assert.AreEqual(entityInstance.InstanceId, unlockTaskMessage.OrchestrationInstance.InstanceId);

                // The recreated release must recover the original lock owner so the correct caller's lock is released.
                Assert.IsNotNull(unlockEvent.Input);
                StringAssert.Contains(unlockEvent.Input, "@caller@1");

                // No further messages should have been emitted.
                QueueMessage extraMessage = controlQueueClient.ReceiveMessage(TimeSpan.FromSeconds(1)).Value;
                Assert.IsNull(extraMessage, "Only a single unlock message should have been emitted.");
            }
            finally
            {
                await containerClient.DeleteIfExistsAsync();
                await service.DeleteAsync();
            }
        }

        static AzureStorageOrchestrationServiceSettings CreateSettings(
            int maxDispatchCount,
            string prefix,
            bool poisonEnabled = true)
        {
            AzureStorageOrchestrationServiceSettings settings = TestHelpers.GetTestAzureStorageOrchestrationServiceSettings(
                enableExtendedSessions: false);

            // Use a unique task hub per test to isolate queues/tables from other tests.
            settings.TaskHubName = "poison" + Guid.NewGuid().ToString("N").Substring(0, 10);
            settings.IsPoisonMessageStorageEnabled = poisonEnabled;
            settings.MaxDispatchCount = maxDispatchCount;
            settings.PoisonMessageStorageContainerNamePrefix = prefix;

            return settings;
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

        static async Task<List<TaskMessage>> DownloadPoisonMessagesAsync(BlobContainerClient containerClient, string blobName)
        {
            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            BlobDownloadResult downloadResult = await blobClient.DownloadContentAsync();
            string blobContent = downloadResult.Content.ToString();

            Assert.IsFalse(string.IsNullOrEmpty(blobContent), "Blob content should not be empty");

            List<TaskMessage> poisonMessages = JsonConvert.DeserializeObject<List<TaskMessage>>(
                blobContent,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });

            Assert.IsNotNull(poisonMessages);
            return poisonMessages;
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
        sealed class FaultInjectingOrchestrationService : IEntityOrchestrationService, IOrchestrationServiceClient, IPoisonMessageHandler
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

            IPoisonMessageHandler InnerPoisonHandler => this.inner;

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

            // ---- IPoisonMessageHandler ----

            public int MaxDispatchCount => this.InnerPoisonHandler.MaxDispatchCount;

            public Task<bool> HandlePoisonEntityMessageAsync(OrchestrationInstance entityInstance, HistoryEvent historyEvent, PoisonMessageReason reason, string details) =>
                this.InnerPoisonHandler.HandlePoisonEntityMessageAsync(entityInstance, historyEvent, reason, details);

            public Task<bool> HandleInvalidWorkItemAsync(TaskOrchestrationWorkItem workItem, PoisonMessageReason reason, string details, bool isEntity) =>
                this.InnerPoisonHandler.HandleInvalidWorkItemAsync(workItem, reason, details, isEntity);

            public Task<bool> HandleInvalidWorkItemAsync(TaskActivityWorkItem workItem, PoisonMessageReason reason, string details) =>
                this.InnerPoisonHandler.HandleInvalidWorkItemAsync(workItem, reason, details);
        }
    }
}
