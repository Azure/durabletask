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
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Threading;
    using DurableTask.Core;

    /// <summary>
    /// ETW Event Provider for the DurableTask.AzureStorage provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {4c4ad4a2-f396-5e18-01b6-618c12a10433}.
    /// </remarks>
    [EventSource(Name = "DurableTask-AzureStorage")]
    class AnalyticsEventSource : EventSource
    {
#if NETSTANDARD2_0
        static readonly AsyncLocal<Guid> ActivityIdState = new AsyncLocal<Guid>();
#else
        const string TraceActivityIdSlot = "TraceActivityId";
#endif

        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly AnalyticsEventSource Log = new AnalyticsEventSource();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
#if NETSTANDARD2_0
            // We use AsyncLocal to preserve activity IDs across async/await boundaries.
            ActivityIdState.Value = activityId;
#else
            // We use LogicalSetData to preserve activity IDs across async/await boundaries.
            System.Runtime.Remoting.Messaging.CallContext.LogicalSetData(TraceActivityIdSlot, activityId);
#endif
            SetCurrentThreadActivityId(activityId);
        }

        [NonEvent]
        private static void EnsureLogicalTraceActivityId()
        {
#if NETSTANDARD2_0
            Guid currentActivityId = ActivityIdState.Value;
            if (currentActivityId != CurrentThreadActivityId)
            {
                SetCurrentThreadActivityId(currentActivityId);
            }
#else
            object data = System.Runtime.Remoting.Messaging.CallContext.LogicalGetData(TraceActivityIdSlot);
            if (data != null)
            {
                Guid currentActivityId = (Guid)data;
                if (currentActivityId != CurrentThreadActivityId)
                {
                    SetCurrentThreadActivityId(currentActivityId);
                }
            }
#endif
        }

        [Event(101, Level = EventLevel.Informational, Opcode = EventOpcode.Send, Task = Tasks.Enqueue, Version = 5)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(
                101,
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

        [Event(102, Level = EventLevel.Informational, Opcode = EventOpcode.Receive, Task = Tasks.Dequeue, Version = 5)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(
                102,
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

        [Event(103, Level = EventLevel.Informational, Version = 4)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                103,
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

        [Event(104, Level = EventLevel.Warning, Version = 5)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                104,
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

        [Event(105, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {2}")]
        public void AssertFailure(
            string Account,
            string TaskHub,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(105, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(106, Level = EventLevel.Warning, Version = 3)]
        public void MessageGone(
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                106,
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

        [Event(107, Level = EventLevel.Error)]
        public void GeneralError(string Account, string TaskHub, string Details, string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(107, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(108, Level = EventLevel.Warning, Version = 2, Message = "A duplicate message was detected. This can indicate a potential performance problem. Message ID = '{2}'. DequeueCount = {3}.")]
        public void DuplicateMessageDetected(
            string Account,
            string TaskHub,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            int DequeueCount,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                108,
                Account,
                TaskHub,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                DequeueCount,
                ExtensionVersion);
        }

        [Event(109, Level = EventLevel.Warning, Message = "A poison message was detected! Message ID = '{2}'. DequeueCount = {6}.")]
        public void PoisonMessageDetected(
            string Account,
            string TaskHub,
            string MessageId,
            string InstanceId,
            string ExecutionId,
            string PartitionId,
            int DequeueCount,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                109,
                Account,
                TaskHub,
                MessageId,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                DequeueCount,
                ExtensionVersion);
        }

        [Event(110, Level = EventLevel.Informational, Version = 2)]
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
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                110,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                EventCount,
                Episode,
                RequestCount,
                LatencyMs,
                ETag ?? string.Empty,
                ExtensionVersion);
        }

        [Event(111, Level = EventLevel.Informational, Version = 3)]
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
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                111,
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
                ExtensionVersion);
        }

        [Event(112, Level = EventLevel.Informational)]
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
                112,
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

        [Event(113, Level = EventLevel.Informational, Version = 2)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                113,
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

        [Event(114, Level = EventLevel.Error, Version = 2)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                114,
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

        [Event(115, Level = EventLevel.Error)]
        public void OrchestrationProcessingFailure(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                115,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                Details,
                ExtensionVersion);
        }

        [Event(116, Level = EventLevel.Informational)]
        public void PendingOrchestratorMessageLimitReached(
            string Account,
            string TaskHub,
            long PendingOrchestratorMessages,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                116,
                Account,
                TaskHub,
                PendingOrchestratorMessages,
                ExtensionVersion);
        }

        [Event(117, Level = EventLevel.Informational)]
        public void WaitingForMoreMessages(
            string Account,
            string TaskHub,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                117,
                Account,
                TaskHub,
                PartitionId,
                ExtensionVersion);
        }

        [Event(118, Level = EventLevel.Warning, Version = 2)]
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
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                118,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                PartitionId,
                EventType,
                TaskEventId,
                MessageId,
                Episode,
                ExtensionVersion);
        }

        [Event(120, Level = EventLevel.Informational, Version = 2)]
        public void PartitionManagerInfo(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(120, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, Details, ExtensionVersion);
        }

        [Event(121, Level = EventLevel.Warning, Version = 2)]
        public void PartitionManagerWarning(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(121, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, Details ?? string.Empty, ExtensionVersion);
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

        [Event(122, Level = EventLevel.Error, Version = 2)]
        public void PartitionManagerError(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(122, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, Details ?? string.Empty, ExtensionVersion);
        }

        [Event(123, Level = EventLevel.Verbose, Message = "Host '{2}' renewing lease for PartitionId '{3}' with lease token '{4}'.")]
        public void StartingLeaseRenewal(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                123,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(124, Level = EventLevel.Verbose)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                124,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Success,
                Token ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [Event(125, Level = EventLevel.Informational)]
        public void LeaseRenewalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string Details,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                125,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                Details ?? string.Empty,
                ExtensionVersion);
        }

        [Event(126, Level = EventLevel.Informational, Message = "Host '{2}' attempting to take lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionStarted(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(126, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, ExtensionVersion);
        }

        [Event(127, Level = EventLevel.Informational, Message = "Host '{2}' successfully acquired lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(127, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, ExtensionVersion);
        }

        [Event(128, Level = EventLevel.Informational, Message = "Host '{2}' failed to acquire lease for PartitionId '{3}' due to conflict.")]
        public void LeaseAcquisitionFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(128, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, ExtensionVersion);
        }

        [Event(129, Level = EventLevel.Informational, Message = "Host '{2} is attempting to steal a lease from '{3}' for PartitionId '{4}'.")]
        public void AttemptingToStealLease(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                129,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(130, Level = EventLevel.Informational, Message = "Host '{2}' stole lease from '{3}' for PartitionId '{4}'.")]
        public void LeaseStealingSucceeded(
            string Account,
            string TaskHub,
            string WorkerName,
            string FromWorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                130,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                FromWorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                ExtensionVersion);
        }

        [Event(131, Level = EventLevel.Informational, Message = "Host '{2}' failed to steal lease for PartitionId '{3}' due to conflict.")]
        public void LeaseStealingFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(131, Account, TaskHub, WorkerName ?? string.Empty, PartitionId ?? string.Empty, ExtensionVersion);
        }

        [Event(132, Level = EventLevel.Informational, Message = "Host '{2}' successfully removed PartitionId '{3}' with lease token '{4}' from currently owned partitions.")]
        public void PartitionRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                132,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(133, Level = EventLevel.Informational, Message = "Host '{2}' successfully released lease on PartitionId '{3}' with lease token '{4}'")]
        public void LeaseRemoved(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                133,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(134, Level = EventLevel.Warning, Message = "Host '{2}' failed to release lease for PartitionId '{3}' with lease token '{4}' due to conflict.")]
        public void LeaseRemovalFailed(
            string Account,
            string TaskHub,
            string WorkerName,
            string PartitionId,
            string Token,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                134,
                Account,
                TaskHub,
                WorkerName ?? string.Empty,
                PartitionId ?? string.Empty,
                Token ?? string.Empty,
                ExtensionVersion);
        }

        [Event(135, Level = EventLevel.Informational, Version = 2)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                135,
                Account,
                TaskHub,
                InstanceId,
                ExecutionId ?? string.Empty,
                EventType,
                Episode,
                LatencyMs,
                ExtensionVersion);
        }

        [Event(136, Level = EventLevel.Informational)]
        public void FetchedInstanceStatus(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            long LatencyMs,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(136, Account, TaskHub, InstanceId, ExecutionId ?? string.Empty, LatencyMs, ExtensionVersion);
        }

        [Event(137, Level = EventLevel.Warning)]
        public void GeneralWarning(string Account, string TaskHub, string Details, string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(137, Account, TaskHub, Details, ExtensionVersion);
        }

        [Event(138, Level = EventLevel.Warning)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                138,
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

        [Event(139, Level = EventLevel.Warning)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                139,
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

        [Event(140, Level = EventLevel.Informational, Task = Tasks.Processing, Opcode = EventOpcode.Receive, Version = 4)]
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
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(
                140,
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

        [Event(141, Level = EventLevel.Informational)]
        public void PurgeInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string createdTimeFrom,
            string createdTimeTo,
            string runtimeStatus,
            int RequestCount,
            long LatencyMs,
            string ExtensionVersion)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(
                141,
                Account,
                TaskHub,
                InstanceId,
                createdTimeFrom,
                createdTimeTo,
                runtimeStatus,
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
