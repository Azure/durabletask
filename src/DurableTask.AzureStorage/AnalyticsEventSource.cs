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
    using System.Runtime.CompilerServices;
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
        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly AnalyticsEventSource Log = new AnalyticsEventSource();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
            try
            {
                SetCoreTraceActivityId(activityId);
            }
            catch (MissingMethodException)
            {
                // Best effort. This method is only available starting in DurableTask.Core v2.5.3.
                // We catch to maintain backwards compatibility with previous versions.
            }
        }

        // NoInlining ensures we can get a predictable MissingMethodException if the build of DurableTask.Core
        // we're using doesn't define the LoggingExtensions.SetLogicalTraceActivityId() method.
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void SetCoreTraceActivityId(Guid activityId)
        {
            DurableTask.Core.Logging.LoggingExtensions.SetLogicalTraceActivityId(activityId);
        }

        [Event(EventIds.SendingMessage, Level = EventLevel.Informational, Opcode = EventOpcode.Send, Task = Tasks.Enqueue, Version = 6)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.ReceivedMessage, Level = EventLevel.Informational, Opcode = EventOpcode.Receive, Task = Tasks.Dequeue, Version = 7)]
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
            long DequeueCount,
            string NextVisibleTime,
            long SizeInBytes,
            string PartitionId,
            long SequenceNumber,
            string PopReceipt,
            int Episode,
            string AppName,
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
                PopReceipt ?? string.Empty,
                Episode,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.DeletingMessage, Level = EventLevel.Informational, Version = 6)]
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
            string PopReceipt,
            string AppName,
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
                PopReceipt ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.AbandoningMessage, Level = EventLevel.Warning, Version = 7)]
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
            string PopReceipt,
            int VisibilityTimeoutSeconds,
            string AppName,
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
                PopReceipt ?? string.Empty,
                VisibilityTimeoutSeconds,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.AssertFailure, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {2}", Version = 2)]
        public void AssertFailure(
            string Account,
            string TaskHub,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(EventIds.AssertFailure, Account, TaskHub, Details, AppName, ExtensionVersion);
        }

        [Event(EventIds.MessageGone, Level = EventLevel.Warning, Version = 5)]
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
            string PopReceipt,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.MessageGone,
                Account,
                TaskHub,
                EventType,
                TaskEventId,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                Details,
                PopReceipt ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.GeneralError, Level = EventLevel.Error, Version = 2)]
        public void GeneralError(string Account, string TaskHub, string Details, string AppName, string ExtensionVersion)
        {
            this.WriteEvent(EventIds.GeneralError, Account, TaskHub, Details, AppName, ExtensionVersion);
        }

        [Event(EventIds.DuplicateMessageDetected, Level = EventLevel.Warning, Version = 4)]
        public void DuplicateMessageDetected(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            long DequeueCount,
            string PopReceipt,
            string AppName,
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
                PopReceipt ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PoisonMessageDetected, Level = EventLevel.Warning, Version = 3)]
        public void PoisonMessageDetected(
            string Account,
            string TaskHub,
            string EventType,
            int TaskEventId,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            long DequeueCount,
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.FetchedInstanceHistory, Level = EventLevel.Informational, Version = 4)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.AppendedInstanceHistory, Level = EventLevel.Informational, Version = 5)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.OrchestrationServiceStats, Level = EventLevel.Informational, Version = 3)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.RenewingMessage, Level = EventLevel.Informational, Version = 4)]
        public void RenewingMessage(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            string EventType,
            int TaskEventId,
            string MessageId,
            string PopReceipt,
            int VisibilityTimeoutSeconds,
            string AppName,
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
                PopReceipt ?? string.Empty,
                VisibilityTimeoutSeconds,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.MessageFailure, Level = EventLevel.Error, Version = 3)]
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
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.MessageFailure,
                Account,
                TaskHub,
                MessageId ?? string.Empty,
                InstanceId ?? string.Empty,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType ?? string.Empty,
                TaskEventId,
                Details,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.OrchestrationProcessingFailure, Level = EventLevel.Error, Version = 2)]
        public void OrchestrationProcessingFailure(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.OrchestrationProcessingFailure,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                Details,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PendingOrchestratorMessageLimitReached, Level = EventLevel.Informational, Version = 3)]
        public void PendingOrchestratorMessageLimitReached(
            string Account,
            string TaskHub,
            string PartitionId,
            long PendingOrchestratorMessages,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PendingOrchestratorMessageLimitReached,
                Account,
                TaskHub,
                PartitionId,
                PendingOrchestratorMessages,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.WaitingForMoreMessages, Level = EventLevel.Informational, Version = 2)]
        public void WaitingForMoreMessages(
            string Account,
            string TaskHub,
            string PartitionId,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.WaitingForMoreMessages,
                Account,
                TaskHub,
                PartitionId,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.ReceivedOutOfOrderMessage, Level = EventLevel.Warning, Version = 4)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerInfo, Level = EventLevel.Informational, Version = 3)]
        public void PartitionManagerInfo(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerInfo,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerWarning, Level = EventLevel.Warning, Version = 3)]
        public void PartitionManagerWarning(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerWarning,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details ?? string.Empty,
                AppName,
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
            this.PartitionManagerError(
                account,
                taskHub,
                workerName,
                partitionId,
                exception.ToString(),
                Utils.AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionManagerError, Level = EventLevel.Error, Version = 3)]
        public void PartitionManagerError(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionManagerError,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Details ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.StartingLeaseRenewal, Level = EventLevel.Verbose, Version = 3)]
        public void StartingLeaseRenewal(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.StartingLeaseRenewal,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRenewalResult, Level = EventLevel.Verbose, Version = 3)]
        public void LeaseRenewalResult(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            bool Success,
            string Token,
            string LeaseType,
            string Details,
            string AppName,
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
                LeaseType,
                Details ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRenewalFailed, Level = EventLevel.Informational, Version = 3)]
        public void LeaseRenewalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string LeaseType,
            string Details,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRenewalFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                LeaseType,
                Details ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionStarted, Level = EventLevel.Informational, Version = 3)]
        public void LeaseAcquisitionStarted(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionStarted,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionSucceeded, Level = EventLevel.Informational, Version = 3)]
        public void LeaseAcquisitionSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionSucceeded,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseAcquisitionFailed, Level = EventLevel.Informational, Version = 3)]
        public void LeaseAcquisitionFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseAcquisitionFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.AttemptingToStealLease, Level = EventLevel.Informational, Version = 3)]
        public void AttemptingToStealLease(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string LeaseType,
            string PartitionId,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.AttemptingToStealLease,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                LeaseType ?? string.Empty,
                PartitionId ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseStealingSucceeded, Level = EventLevel.Informational, Version = 3)]
        public void LeaseStealingSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string LeaseType,
            string PartitionId,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseStealingSucceeded,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                LeaseType ?? string.Empty,
                PartitionId ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseStealingFailed, Level = EventLevel.Informational, Version = 3)]
        public void LeaseStealingFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseStealingFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PartitionRemoved, Level = EventLevel.Informational, Version = 2)]
        public void PartitionRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PartitionRemoved,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRemoved, Level = EventLevel.Informational, Version = 3)]
        public void LeaseRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRemoved,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.LeaseRemovalFailed, Level = EventLevel.Warning, Version = 3)]
        public void LeaseRemovalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string LeaseType,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.LeaseRemovalFailed,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                LeaseType,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.InstanceStatusUpdate, Level = EventLevel.Informational, Version = 4)]
        public void InstanceStatusUpdate(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string RuntimeStatus,
            int Episode,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.InstanceStatusUpdate,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                RuntimeStatus ?? string.Empty,
                Episode,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.FetchedInstanceStatus, Level = EventLevel.Informational, Version = 3)]
        public void FetchedInstanceStatus(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string RuntimeStatus,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.FetchedInstanceStatus,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                RuntimeStatus ?? string.Empty,
                LatencyMs,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.GeneralWarning, Level = EventLevel.Warning, Version = 2)]
        public void GeneralWarning(string Account, string TaskHub, string Details, string AppName, string ExtensionVersion, string InstanceId)
        {
            this.WriteEvent(EventIds.GeneralWarning, Account, TaskHub, Details, AppName, ExtensionVersion, InstanceId ?? string.Empty);
        }

        [Event(EventIds.SplitBrainDetected, Level = EventLevel.Warning, Version = 2)]
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
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.DiscardingWorkItem, Level = EventLevel.Warning, Version = 2)]
        public void DiscardingWorkItem(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int NewEventCount,
            int TotalEventCount,
            string NewEvents,
            string Details,
            string AppName,
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
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.ProcessingMessage, Level = EventLevel.Informational, Task = Tasks.Processing, Opcode = EventOpcode.Receive, Version = 6)]
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
            string PartitionId,
            long SequenceNumber,
            int Episode,
            bool IsExtendedSession,
            string AppName,
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
                PartitionId,
                SequenceNumber,
                Episode,
                IsExtendedSession,
                AppName,
                ExtensionVersion);
        }

        [Event(EventIds.PurgeInstanceHistory, Level = EventLevel.Informational, Version = 4)]
        public void PurgeInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string CreatedTimeFrom,
            string CreatedTimeTo,
            string RuntimeStatus,
            int RequestCount,
            int InstanceCount,
            long LatencyMs,
            string AppName,
            string ExtensionVersion)
        {
            this.WriteEvent(
                EventIds.PurgeInstanceHistory,
                Account,
                TaskHub,
                InstanceId ?? string.Empty,
                CreatedTimeFrom,
                CreatedTimeTo,
                RuntimeStatus ?? string.Empty,
                RequestCount,
                InstanceCount,
                LatencyMs,
                AppName,
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
