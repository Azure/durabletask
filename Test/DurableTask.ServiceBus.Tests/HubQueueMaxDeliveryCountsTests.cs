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
    using System.Collections.Generic;
    using Azure.Messaging.ServiceBus;
    using Azure.Messaging.ServiceBus.Administration;
    using DurableTask.ServiceBus.Common.Abstraction;
    using DurableTask.ServiceBus.Settings;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Unit tests for <see cref="ServiceBusOrchestrationService.GetHubQueueMaxDeliveryCounts"/>.
    /// These use in-memory queue descriptions so they do not require a live Service Bus namespace.
    /// </summary>
    [TestClass]
    public class HubQueueMaxDeliveryCountsTests
    {
        const string HubName = "testhub";
        static ServiceBusOrchestrationService CreateService()
        {
            return new ServiceBusOrchestrationService(
                NoOpTokenCredential.NamespaceHostName,
                new NoOpTokenCredential(),
                HubName,
                null,
                null,
                new ServiceBusOrchestrationServiceSettings());
        }

        static QueueDescription CreateQueue(string name, int maxDeliveryCount)
        {
            QueueProperties properties = ServiceBusModelFactory.QueueProperties(
                name: name,
                lockDuration: TimeSpan.FromMinutes(1),
                maxSizeInMegabytes: 1024,
                requiresDuplicateDetection: false,
                requiresSession: false,
                defaultMessageTimeToLive: TimeSpan.MaxValue,
                autoDeleteOnIdle: TimeSpan.MaxValue,
                deadLetteringOnMessageExpiration: false,
                duplicateDetectionHistoryTimeWindow: TimeSpan.FromMinutes(10),
                maxDeliveryCount: maxDeliveryCount,
                enableBatchedOperations: true,
                status: EntityStatus.Active,
                forwardTo: string.Empty,
                forwardDeadLetteredMessagesTo: string.Empty,
                userMetadata: string.Empty,
                enablePartitioning: false);

            return new QueueDescription(properties);
        }

        [TestMethod]
        public void ReturnsMaxDeliveryCountForEachHubQueue()
        {
            ServiceBusOrchestrationService service = CreateService();

            Dictionary<string, int> counts = service.GetHubQueueMaxDeliveryCounts(new[]
            {
                CreateQueue($"{HubName}/orchestrator", 10),
                CreateQueue($"{HubName}/worker", 20),
                CreateQueue($"{HubName}/tracking", 30),
            });

            Assert.AreEqual(10, counts["TaskOrchestration"]);
            Assert.AreEqual(20, counts["TaskActivity"]);
            Assert.AreEqual(30, counts["Tracking"]);
        }

        [TestMethod]
        public void ReturnsMinusOneForMissingTrackingQueue()
        {
            // Without an instance store the tracking queue is never created.
            ServiceBusOrchestrationService service = CreateService();

            Dictionary<string, int> counts = service.GetHubQueueMaxDeliveryCounts(new[]
            {
                CreateQueue($"{HubName}/orchestrator", 10),
                CreateQueue($"{HubName}/worker", 20),
            });

            Assert.AreEqual(10, counts["TaskOrchestration"]);
            Assert.AreEqual(20, counts["TaskActivity"]);
            Assert.AreEqual(-1, counts["Tracking"]);
        }

        [TestMethod]
        public void ReturnsMinusOneForAllQueuesWhenHubDoesNotExist()
        {
            ServiceBusOrchestrationService service = CreateService();

            Dictionary<string, int> counts = service.GetHubQueueMaxDeliveryCounts(new[]
            {
                CreateQueue("otherhub/orchestrator", 10),
                CreateQueue("otherhub/worker", 20),
                CreateQueue("otherhub/tracking", 30),
            });

            Assert.AreEqual(-1, counts["TaskOrchestration"]);
            Assert.AreEqual(-1, counts["TaskActivity"]);
            Assert.AreEqual(-1, counts["Tracking"]);
        }
    }
}
