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

namespace DurableTask.AzureStorage.Logging
{
    using System;
    using DurableTask.Core;
    using DurableTask.Core.Logging;
    using Microsoft.Extensions.Logging;

    class LogHelper
    {
        readonly ILogger log;

        public LogHelper(ILogger log)
        {
            this.log = log;
        }

        internal void SendingMessage(
            Guid relatedActivityId,
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string instanceId,
            string executionId,
            long sizeInBytes,
            string partitionId,
            string targetInstanceId,
            string targetExecutionId,
            long sequenceNumber,
            int episode)
        {
            var logEvent = new LogEvents.SendingMessage(
                relatedActivityId,
                account,
                taskHub,
                eventType,
                taskEventId,
                instanceId,
                executionId,
                sizeInBytes,
                partitionId,
                targetInstanceId,
                targetExecutionId,
                sequenceNumber,
                episode);
            this.WriteStructuredLog(logEvent);
        }

        internal void ReceivedMessage(
            Guid relatedActivityId,
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string instanceId,
            string executionId,
            string messageId,
            int age,
            long dequeueCount,
            string nextVisibleTime,
            long sizeInBytes,
            string partitionId,
            long sequenceNumber,
            string popReceipt,
            int episode)
        {
            var logEvent = new LogEvents.ReceivedMessage(
                relatedActivityId,
                account,
                taskHub,
                eventType,
                taskEventId,
                instanceId,
                executionId,
                messageId,
                age,
                dequeueCount,
                nextVisibleTime,
                sizeInBytes,
                partitionId,
                sequenceNumber,
                popReceipt,
                episode);
            this.WriteStructuredLog(logEvent);
        }

        internal void DeletingMessage(
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            long sequenceNumber,
            string popReceipt)
        {
            var logEvent = new LogEvents.DeletingMessage(
                account,
                taskHub,
                eventType,
                taskEventId,
                messageId,
                instanceId,
                executionId,
                partitionId,
                sequenceNumber,
                popReceipt);
            this.WriteStructuredLog(logEvent);
        }

        internal void AbandoningMessage(
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            long sequenceNumber,
            string popReceipt,
            int visibilityTimeoutSeconds)
        {
            var logEvent = new LogEvents.AbandoningMessage(
                account,
                taskHub,
                eventType,
                taskEventId,
                messageId,
                instanceId,
                executionId,
                partitionId,
                sequenceNumber,
                popReceipt,
                visibilityTimeoutSeconds);
            this.WriteStructuredLog(logEvent);
        }

        internal void AssertFailure(
            string account,
            string taskHub,
            string details)
        {
            var logEvent = new LogEvents.AssertFailure(
                account,
                taskHub,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void MessageGone(
            string account,
            string taskHub,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            string eventType,
            int taskEventId,
            string details,
            string popReceipt)
        {
            var logEvent = new LogEvents.MessageGone(
                account,
                taskHub,
                messageId,
                instanceId,
                executionId,
                partitionId,
                eventType,
                taskEventId,
                details,
                popReceipt);
            this.WriteStructuredLog(logEvent);
        }

        internal void GeneralError(
            string account,
            string taskHub,
            string details)
        {
            var logEvent = new LogEvents.GeneralError(
                account,
                taskHub,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void DuplicateMessageDetected(
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            long dequeueCount,
            string popReceipt)
        {
            var logEvent = new LogEvents.DuplicateMessageDetected(
                account,
                taskHub,
                eventType,
                taskEventId,
                messageId,
                instanceId,
                executionId,
                partitionId,
                dequeueCount,
                popReceipt);
            this.WriteStructuredLog(logEvent);
        }

        internal void PoisonMessageDetected(
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            long dequeueCount)
        {
            var logEvent = new LogEvents.PoisonMessageDetected(
                account,
                taskHub,
                eventType,
                taskEventId,
                messageId,
                instanceId,
                executionId,
                partitionId,
                dequeueCount);
            this.WriteStructuredLog(logEvent);
        }

        internal void FetchedInstanceHistory(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            int eventCount,
            int episode,
            int requestCount,
            long latencyMs,
            string eTag,
            DateTime lastCheckpointTime)
        {
            var logEvent = new LogEvents.FetchedInstanceHistory(
                account,
                taskHub,
                instanceId,
                executionId,
                eventCount,
                episode,
                requestCount,
                latencyMs,
                eTag,
                lastCheckpointTime);
            this.WriteStructuredLog(logEvent);
        }

        internal void AppendedInstanceHistory(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            int newEventCount,
            int totalEventCount,
            string newEvents,
            int episode,
            long latencyMs,
            int sizeInBytes,
            string eTag,
            bool isCheckpointComplete)
        {
            var logEvent = new LogEvents.AppendedInstanceHistory(
                account,
                taskHub,
                instanceId,
                executionId,
                newEventCount,
                totalEventCount,
                newEvents,
                episode,
                latencyMs,
                sizeInBytes,
                eTag,
                isCheckpointComplete);
            this.WriteStructuredLog(logEvent);
        }

        internal void OrchestrationServiceStats(
            string account,
            string taskHub,
            long storageRequests,
            long messagesSent,
            long messagesRead,
            long messagesUpdated,
            long tableEntitiesWritten,
            long tableEntitiesRead,
            long pendingOrchestrators,
            long pendingOrchestratorMessages,
            long activeOrchestrators,
            long activeActivities)
        {
            var logEvent = new LogEvents.OrchestrationServiceStats(
                account,
                taskHub,
                storageRequests,
                messagesSent,
                messagesRead,
                messagesUpdated,
                tableEntitiesWritten,
                tableEntitiesRead,
                pendingOrchestrators,
                pendingOrchestratorMessages,
                activeOrchestrators,
                activeActivities);
            this.WriteStructuredLog(logEvent);
        }

        internal void RenewingMessage(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            string partitionId,
            string eventType,
            int taskEventId,
            string messageId,
            string popReceipt,
            int visibilityTimeoutSeconds)
        {
            var logEvent = new LogEvents.RenewingMessage(
                account,
                taskHub,
                instanceId,
                executionId,
                partitionId,
                eventType,
                taskEventId,
                messageId,
                popReceipt,
                visibilityTimeoutSeconds);
            this.WriteStructuredLog(logEvent);
        }

        internal void MessageFailure(
            string account,
            string taskHub,
            string messageId,
            string instanceId,
            string executionId,
            string partitionId,
            string eventType,
            int taskEventId,
            string details)
        {
            var logEvent = new LogEvents.MessageFailure(
                account,
                taskHub,
                messageId,
                instanceId,
                executionId,
                partitionId,
                eventType,
                taskEventId,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void OrchestrationProcessingFailure(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            string details)
        {
            var logEvent = new LogEvents.OrchestrationProcessingFailure(
                account,
                taskHub,
                instanceId,
                executionId,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void PendingOrchestratorMessageLimitReached(
            string account,
            string taskHub,
            string partitionId,
            long pendingOrchestratorMessages)
        {
            var logEvent = new LogEvents.PendingOrchestratorMessageLimitReached(
                account,
                taskHub,
                partitionId,
                pendingOrchestratorMessages);
            this.WriteStructuredLog(logEvent);
        }

        internal void WaitingForMoreMessages(
            string account,
            string taskHub,
            string partitionId)
        {
            var logEvent = new LogEvents.WaitingForMoreMessages(
                account,
                taskHub,
                partitionId);
            this.WriteStructuredLog(logEvent);
        }

        internal void ReceivedOutOfOrderMessage(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            string partitionId,
            string eventType,
            int taskEventId,
            string messageId,
            int episode,
            DateTime lastCheckpointTime)
        {
            var logEvent = new LogEvents.ReceivedOutOfOrderMessage(
                account,
                taskHub,
                instanceId,
                executionId,
                partitionId,
                eventType,
                taskEventId,
                messageId,
                episode,
                lastCheckpointTime);
            this.WriteStructuredLog(logEvent);
        }

        internal void PartitionManagerInfo(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string details)
        {
            var logEvent = new LogEvents.PartitionManagerInfo(
                account,
                taskHub,
                workerName,
                partitionId,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void PartitionManagerWarning(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string details)
        {
            var logEvent = new LogEvents.PartitionManagerWarning(
                account,
                taskHub,
                workerName,
                partitionId,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void PartitionManagerError(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string details)
        {
            var logEvent = new LogEvents.PartitionManagerError(
                account,
                taskHub,
                workerName,
                partitionId,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void StartingLeaseRenewal(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string token,
            string leaseType)
        {
            var logEvent = new LogEvents.StartingLeaseRenewal(
                account,
                taskHub,
                workerName,
                partitionId,
                token,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseRenewalResult(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            bool success,
            string token,
            string leaseType,
            string details)
        {
            var logEvent = new LogEvents.LeaseRenewalResult(
                account,
                taskHub,
                workerName,
                partitionId,
                success,
                token,
                leaseType,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseRenewalFailed(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string token,
            string leaseType,
            string details)
        {
            var logEvent = new LogEvents.LeaseRenewalFailed(
                account,
                taskHub,
                workerName,
                partitionId,
                token,
                leaseType,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseAcquisitionStarted(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseAcquisitionStarted(
                account,
                taskHub,
                workerName,
                partitionId,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseAcquisitionSucceeded(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseAcquisitionSucceeded(
                account,
                taskHub,
                workerName,
                partitionId,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseAcquisitionFailed(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseAcquisitionFailed(
                account,
                taskHub,
                workerName,
                partitionId,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void AttemptingToStealLease(
            string account,
            string taskHub,
            string workerName,
            string fromWorkerName,
            string leaseType,
            string partitionId)
        {
            var logEvent = new LogEvents.AttemptingToStealLease(
                account,
                taskHub,
                workerName,
                fromWorkerName,
                leaseType,
                partitionId);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseStealingSucceeded(
            string account,
            string taskHub,
            string workerName,
            string fromWorkerName,
            string leaseType,
            string partitionId)
        {
            var logEvent = new LogEvents.LeaseStealingSucceeded(
                account,
                taskHub,
                workerName,
                fromWorkerName,
                leaseType,
                partitionId);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseStealingFailed(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseStealingFailed(
                account,
                taskHub,
                workerName,
                partitionId,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void PartitionRemoved(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string token)
        {
            var logEvent = new LogEvents.PartitionRemoved(
                account,
                taskHub,
                workerName,
                partitionId,
                token);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseRemoved(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string token,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseRemoved(
                account,
                taskHub,
                workerName,
                partitionId,
                token,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void LeaseRemovalFailed(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            string token,
            string leaseType)
        {
            var logEvent = new LogEvents.LeaseRemovalFailed(
                account,
                taskHub,
                workerName,
                partitionId,
                token,
                leaseType);
            this.WriteStructuredLog(logEvent);
        }

        internal void InstanceStatusUpdate(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            OrchestrationStatus runtimeStatus,
            int episode,
            long latencyMs)
        {
            var logEvent = new LogEvents.InstanceStatusUpdate(
                account,
                taskHub,
                instanceId,
                executionId,
                runtimeStatus,
                episode,
                latencyMs);
            this.WriteStructuredLog(logEvent);
        }

        internal void FetchedInstanceStatus(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            string runtimeStatus,
            long latencyMs)
        {
            var logEvent = new LogEvents.FetchedInstanceStatus(
                account,
                taskHub,
                instanceId,
                executionId,
                runtimeStatus,
                latencyMs);
            this.WriteStructuredLog(logEvent);
        }

        internal void GeneralWarning(
            string account,
            string taskHub,
            string details,
            string instanceId = null)
        {
            var logEvent = new LogEvents.GeneralWarning(
                account,
                taskHub,
                details,
                instanceId ?? string.Empty);
            this.WriteStructuredLog(logEvent);
        }

        internal void SplitBrainDetected(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            int newEventCount,
            int totalEventCount,
            string newEvents,
            long latencyMs,
            string eTag)
        {
            var logEvent = new LogEvents.SplitBrainDetected(
                account,
                taskHub,
                instanceId,
                executionId,
                newEventCount,
                totalEventCount,
                newEvents,
                latencyMs,
                eTag);
            this.WriteStructuredLog(logEvent);
        }

        internal void DiscardingWorkItem(
            string account,
            string taskHub,
            string instanceId,
            string executionId,
            int newEventCount,
            int totalEventCount,
            string newEvents,
            string details)
        {
            var logEvent = new LogEvents.DiscardingWorkItem(
                account,
                taskHub,
                instanceId,
                executionId,
                newEventCount,
                totalEventCount,
                newEvents,
                details);
            this.WriteStructuredLog(logEvent);
        }

        internal void ProcessingMessage(
            Guid relatedActivityId,
            string account,
            string taskHub,
            string eventType,
            int taskEventId,
            string instanceId,
            string executionId,
            string messageId,
            int age,
            string partitionId,
            long sequenceNumber,
            int episode,
            bool isExtendedSession)
        {
            var logEvent = new LogEvents.ProcessingMessage(
                relatedActivityId,
                account,
                taskHub,
                eventType,
                taskEventId,
                instanceId,
                executionId,
                messageId,
                age,
                partitionId,
                sequenceNumber,
                episode,
                isExtendedSession);
            this.WriteStructuredLog(logEvent);
        }

        internal void PurgeInstanceHistory(
            string account,
            string taskHub,
            string instanceId,
            string createdTimeFrom,
            string createdTimeTo,
            string runtimeStatus,
            int requestCount,
            int instanceCount,
            long latencyMs)
        {
            var logEvent = new LogEvents.PurgeInstanceHistory(
                account,
                taskHub,
                instanceId,
                createdTimeFrom,
                createdTimeTo,
                runtimeStatus,
                requestCount,
                instanceCount,
                latencyMs);
            this.WriteStructuredLog(logEvent);
        }

        void WriteStructuredLog(ILogEvent logEvent, Exception exception = null)
        {
            LoggingExtensions.LogDurableEvent(this.log, logEvent, exception);
        }
    }
}
