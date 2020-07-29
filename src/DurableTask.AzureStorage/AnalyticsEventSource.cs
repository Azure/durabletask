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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Diagnostics.Tracing;
    using System.Threading;
    using DurableTask.AzureStorage.Logging;

    /// <summary>
    /// ETW Event Provider for the DurableTask.AzureStorage provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {4c4ad4a2-f396-5e18-01b6-618c12a10433}.
    /// </remarks>
    [EventSource(Name = "DurableTask-AzureStorage")]
    class AnalyticsEventSource : EventSource
    {
        static readonly AsyncLocal<Guid> ActivityIdState = new AsyncLocal<Guid>();

        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly AnalyticsEventSource Log = new AnalyticsEventSource();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
            // We use AsyncLocal to preserve activity IDs across async/await boundaries.
            ActivityIdState.Value = activityId;
            SetCurrentThreadActivityId(activityId);
        }

        [Event(EventIds.SendingMessage, Level = EventLevel.Informational, Opcode = EventOpcode.Send, Task = Tasks.Enqueue, Version = 5)]
        public void SendingMessage(
            Guid relatedActivityId,
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string InstanceId,
            string ExecutionId,
            long SizeInBytes,
            string PartitionId,
            string TargetInstanceId,
            string TargetExecutionId,
            long SequenceNumber,
            int Episode,
            string ExtensionVersion)
        {
            this.WriteEventWithRelatedActivityId(
                EventIds.SendingMessage,
                relatedActivityId,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                InstanceId ?? string.Empty,
                ExecutionId ?? string.Empty,
                SizeInBytes,
                PartitionId,
                TargetInstanceId,
                TargetExecutionId ?? string.Empty,
                SequenceNumber,
                Episode,
                ExtensionVersion);
        }

        [Event(EventIds.ReceivedMessage, Level = EventLevel.Informational, Opcode = EventOpcode.Receive, Task = Tasks.Dequeue, Version = 5)]
        public void ReceivedMessage(
            Guid relatedActivityId,
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string InstanceId,
            string ExecutionId,
            string MessageId,
            int Age,
            int DequeueCount,
            string NextVisibleTime,
            long SizeInBytes,
            string PartitionId,
            long SequenceNumber,
            int Episode,
            string ExtensionVersion)
        {
            this.WriteEventWithRelatedActivityId(
                EventIds.ReceivedMessage,
                relatedActivityId,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                InstanceId,
                ExecutionId ?? string.Empty,
                MessageId,
                Age,
                DequeueCount,
                NextVisibleTime,
                SizeInBytes,
                PartitionId,
                SequenceNumber,
                Episode,
                ExtensionVersion);
        }

        [Event(EventIds.DeletingMessage, Level = EventLevel.Informational, Version = 4)]
        public void DeletingMessage(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            long SequenceNumber,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.DeletingMessage,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                SequenceNumber,
                ExtensionVersion);
        }

        [Event(EventIds.AbandoningMessage, Level = EventLevel.Warning, Version = 5)]
        public void AbandoningMessage(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            long SequenceNumber,
            int VisibilityTimeoutSeconds,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.AbandoningMessage,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                SequenceNumber,
                VisibilityTimeoutSeconds,
                ExtensionVersion);
        }

        [Event(EventIds.AssertFailure, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {2}")]
        public void AssertFailure(
            string Account,
            string TaskHub,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(EventIds.AssertFailure, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(EventIds.MessageGone, Level = EventLevel.Warning, Version = 4)]
        public void MessageGone(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.MessageGone,
                Account,
                TaskHub,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType,
                TaskEventId,
                Details,
                ExtensionVersion);
        }

        [Event(EventIds.GeneralError, Level = EventLevel.Error)]
        public void GeneralError(string Account, string TaskHub, string Details, string ExtensionVersion)
        {
            this.WriteEvent(EventIds.GeneralError, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(EventIds.DuplicateMessageDetected, Level = EventLevel.Warning, Version = 3, Message = "A duplicate message was detected. This can indicate a potential performance problem. Message ID = '{4}'. DequeueCount = {8}.")]
        public void DuplicateMessageDetected(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            int DequeueCount,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.DuplicateMessageDetected,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                DequeueCount,
                ExtensionVersion);
        }

        [Event(EventIds.PoisonMessageDetected, Level = EventLevel.Warning, Message = "A poison message was detected! Message ID = '{4}'. DequeueCount = {8}.")]
        public void PoisonMessageDetected(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            int DequeueCount,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PoisonMessageDetected,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                DequeueCount,
                ExtensionVersion);
        }

        [Event(EventIds.FetchedInstanceHistory, Level = EventLevel.Informational, Version = 3)]
        public void FetchedInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int EventCount,
            int Episode,
            int RequestCount,
            long LatencyMs,
            string ETag,
            DateTime LastCheckpointTime,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.FetchedInstanceHistory,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                EventCount,
                Episode,
                RequestCount,
                LatencyMs,
                ETag ?? string.Empty,
                LastCheckpointTime,
                ExtensionVersion);
        }

        [Event(EventIds.AppendedInstanceHistory, Level = EventLevel.Informational, Version = 4)]
        public void AppendedInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int NewEventCount,
            int TotalEventCount,
            string NewEvents,
            int Episode,
            long LatencyMs,
            int SizeInBytes,
            string ETag,
            bool IsCheckpointComplete,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.AppendedInstanceHistory,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                NewEventCount,
                TotalEventCount,
                NewEvents,
                Episode,
                LatencyMs,
                SizeInBytes,
                ETag ?? string.Empty,
                IsCheckpointComplete,
                ExtensionVersion);
        }

        [Event(EventIds.OrchestrationServiceStats, Level = EventLevel.Informational)]
        public void OrchestrationServiceStats(
            string Account,
            string TaskHub,
            long StorageRequests,
            long MessagesSent,
            long MessagesRead,
            long MessagesUpdated,
            long TableEntitiesWritten,
            long TableEntitiesRead,
            long PendingOrchestrators,
            long PendingOrchestratorMessages,
            long ActiveOrchestrators,
            long ActiveActivities,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.OrchestrationServiceStats,
                Account,
                TaskHub,
                StorageRequests,
                MessagesSent,
                MessagesRead,
                MessagesUpdated,
                TableEntitiesWritten,
                TableEntitiesRead,
                PendingOrchestrators,
                PendingOrchestratorMessages,
                ActiveOrchestrators,
                ActiveActivities,
                ExtensionVersion);
        }

        [Event(EventIds.RenewingMessage, Level = EventLevel.Informational, Version = 2)]
        public void RenewingMessage(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            string EventType,
            int TaskEventId,
            string MessageId,
            int VisibilityTimeoutSeconds,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.RenewingMessage,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType,
                TaskEventId,
                MessageId,
                VisibilityTimeoutSeconds,
                ExtensionVersion);
        }

        [Event(EventIds.MessageFailure, Level = EventLevel.Error, Version = 2)]
        public void MessageFailure(
            string Account,
            string TaskHub,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            string EventType,
            int TaskEventId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.MessageFailure,
                Account,
                TaskHub,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType,
                TaskEventId,
                Details,
                ExtensionVersion);
        }

        [Event(EventIds.OrchestrationProcessingFailure, Level = EventLevel.Error)]
        public void OrchestrationProcessingFailure(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.OrchestrationProcessingFailure,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                Details,
                ExtensionVersion);
        }

        [Event(EventIds.PendingOrchestratorMessageLimitReached, Level = EventLevel.Informational, Version = 2)]
        public void PendingOrchestratorMessageLimitReached(
            string Account,
            string TaskHub,
            string PartitionId,
            long PendingOrchestratorMessages,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PendingOrchestratorMessageLimitReached,
                Account,
                TaskHub,
                PartitionId,
                PendingOrchestratorMessages,
                ExtensionVersion);
        }

        [Event(EventIds.WaitingForMoreMessages, Level = EventLevel.Informational)]
        public void WaitingForMoreMessages(
            string Account,
            string TaskHub,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.WaitingForMoreMessages,
                Account,
                TaskHub,
                PartitionId,
                ExtensionVersion);
        }

        [Event(EventIds.ReceivedOutOfOrderMessage, Level = EventLevel.Warning, Version = 3)]
        public void ReceivedOutOfOrderMessage(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            string EventType,
            int TaskEventId,
            string MessageId,
            int Episode,
            DateTime LastCheckpointTime,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.ReceivedOutOfOrderMessage,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType,
                TaskEventId,
                MessageId,
                Episode,
                LastCheckpointTime,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerInfo, Level = EventLevel.Informational, Version = 2)]
        public void PartitionManagerInfo(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerInfo,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerWarning, Level = EventLevel.Warning, Version = 2)]
        public void PartitionManagerWarning(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerWarning,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [NonEvent]
        public void PartitionManagerError(
            string account,
            string taskHub,
            string workerName,
            string partitionId,
            Exception exception,
            string ExtensionVersion)
        {
            this.PartitionManagerError(account, taskHub, workerName, partitionId, exception.ToString(), ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerError, Level = EventLevel.Error, Version = 2)]
        public void PartitionManagerError(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerError,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.StartingLeaseRenewal, Level = EventLevel.Verbose, Message = "Host '{2}' renewing lease for PartitionId '{3}' with lease token '{4}'.")]
        public void StartingLeaseRenewal(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.StartingLeaseRenewal,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRenewalResult, Level = EventLevel.Verbose)]
        public void LeaseRenewalResult(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            bool Success,
            string Token,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRenewalResult,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Success,
                Token ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRenewalFailed, Level = EventLevel.Informational)]
        public void LeaseRenewalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRenewalFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionStarted, Level = EventLevel.Informational, Message = "Host '{2}' attempting to take lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionStarted(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionStarted,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionSucceeded, Level = EventLevel.Informational, Message = "Host '{2}' successfully acquired lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionSucceeded,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionFailed, Level = EventLevel.Informational, Message = "Host '{2}' failed to acquire lease for PartitionId '{3}' due to conflict.")]
        public void LeaseAcquisitionFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.AttemptingToStealLease, Level = EventLevel.Informational, Message = "Host '{2} is attempting to steal a lease from '{3}' for PartitionId '{4}'.")]
        public void AttemptingToStealLease(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.AttemptingToStealLease,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseStealingSucceeded, Level = EventLevel.Informational, Message = "Host '{2}' stole lease from '{3}' for PartitionId '{4}'.")]
        public void LeaseStealingSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseStealingSucceeded,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseStealingFailed, Level = EventLevel.Informational, Message = "Host '{2}' failed to steal lease for PartitionId '{3}' due to conflict.")]
        public void LeaseStealingFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseStealingFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionRemoved, Level = EventLevel.Informational, Message = "Host '{2}' successfully removed PartitionId '{3}' with lease token '{4}' from currently owned partitions.")]
        public void PartitionRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionRemoved,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRemoved, Level = EventLevel.Informational, Message = "Host '{2}' successfully released lease on PartitionId '{3}' with lease token '{4}'")]
        public void LeaseRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRemoved,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRemovalFailed, Level = EventLevel.Warning, Message = "Host '{2}' failed to release lease for PartitionId '{3}' with lease token '{4}' due to conflict.")]
        public void LeaseRemovalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRemovalFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.InstanceStatusUpdate, Level = EventLevel.Informational, Version = 2)]
        public void InstanceStatusUpdate(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string EventType,
            int Episode,
            long LatencyMs,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.InstanceStatusUpdate,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                EventType,
                Episode,
                LatencyMs,
                ExtensionVersion);
        }

        [Event(EventIds.FetchedInstanceStatus, Level = EventLevel.Informational)]
        public void FetchedInstanceStatus(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            long LatencyMs,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.FetchedInstanceStatus,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                LatencyMs,
                ExtensionVersion);
        }

        [Event(EventIds.GeneralWarning, Level = EventLevel.Warning)]
        public void GeneralWarning(string Account, string TaskHub, string Details, string ExtensionVersion)
        {
            this.WriteEvent(EventIds.GeneralWarning, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(EventIds.SplitBrainDetected, Level = EventLevel.Warning)]
        public void SplitBrainDetected(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int NewEventCount,
            int TotalEventCount,
            string NewEvents,
            long LatencyMs,
            string ETag,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.SplitBrainDetected,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                NewEventCount,
                TotalEventCount,
                NewEvents,
                LatencyMs,
                ETag ?? string.Empty,
                ExtensionVersion);
        }

        [Event(EventIds.DiscardingWorkItem, Level = EventLevel.Warning)]
        public void DiscardingWorkItem(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int NewEventCount,
            int TotalEventCount,
            string NewEvents,
            string Details,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.DiscardingWorkItem,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                NewEventCount,
                TotalEventCount,
                NewEvents,
                Details,
                ExtensionVersion);
        }

        [Event(EventIds.ProcessingMessage, Level = EventLevel.Informational, Task = Tasks.Processing, Opcode = EventOpcode.Receive, Version = 4)]
        public void ProcessingMessage(
            Guid relatedActivityId,
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string InstanceId,
            string ExecutionId,
            string MessageId,
            int Age,
            long SequenceNumber,
            int Episode,
            bool IsExtendedSession,
            string ExtensionVersion)
        {
            this.WriteEventWithRelatedActivityId(
                EventIds.ProcessingMessage,
                relatedActivityId,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                InstanceId,
                ExecutionId ?? string.Empty,
                MessageId,
                Age,
                SequenceNumber,
                Episode,
                IsExtendedSession,
                ExtensionVersion);
        }

        [Event(EventIds.PurgeInstanceHistory, Level = EventLevel.Informational, Version = 2)]
        public void PurgeInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string CreatedTimeFrom,
            string CreatedTimeTo,
            string RuntimeStatus,
            int RequestCount,
            long LatencyMs,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PurgeInstanceHistory,
                Account,
                TaskHub,
                InstanceId,
                CreatedTimeFrom,
                CreatedTimeTo,
                RuntimeStatus,
                RequestCount,
                LatencyMs,
                ExtensionVersion);
        }

        // Specifying tasks is necessary when using WriteEventWithRelatedActivityId
        // or else the "TaskName" property written to ETW is the name of the opcode instead
        // of the name of the trace method.
        static class Tasks
        {
            public const EventTask Enqueue = (EventTask)0x01;
            public const EventTask Dequeue = (EventTask)0x02;
            public const EventTask Processing = (EventTask)0x03;
        }
    }
}
