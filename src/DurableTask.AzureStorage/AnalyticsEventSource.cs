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

        [Event(101, Level = EventLevel.Informational, Opcode = EventOpcode.Send)]
        public void SendingMessage(
            Guid relatedActivityId,
            string Account,
            string TaskHub,
            string EventType,
            string InstanceId,
            string ExecutionId,
            long SizeInBytes,
            string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(101, relatedActivityId, Account, TaskHub, EventType, InstanceId, ExecutionId, SizeInBytes, PartitionId);
        }

        [Event(102, Level = EventLevel.Informational, Opcode = EventOpcode.Receive)]
        public void ReceivedMessage(
            Guid relatedActivityId,
            string Account,
            string TaskHub,
            string EventType,
            string InstanceId,
            string ExecutionId,
            string MessageId,
            int Age,
            int DequeueCount,
            long SizeInBytes,
            string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(102, relatedActivityId, Account, TaskHub, EventType, InstanceId, ExecutionId, MessageId, Age, DequeueCount, SizeInBytes, PartitionId);
        }

        [Event(103, Level = EventLevel.Informational)]
        public void DeletingMessage(string Account, string TaskHub, string EventType, string MessageId, string InstanceId, string ExecutionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(103, Account, TaskHub, EventType, MessageId, InstanceId, ExecutionId);
        }

        [Event(104, Level = EventLevel.Warning, Message = "Abandoning message of type {0} with ID = {1}. Orchestration ID = {2}.")]
        public void AbandoningMessage(string Account, string TaskHub, string EventType, string MessageId, string InstanceId, string ExecutionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(104, Account, TaskHub, EventType, MessageId, InstanceId, ExecutionId);
        }

        [Event(105, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {0}")]
        public void AssertFailure(string Account, string TaskHub, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(105, Account, TaskHub, Details);
        }

        [Event(106, Level = EventLevel.Warning)]
        public void MessageGone(string Account, string TaskHub, string MessageId, string InstanceId, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(106, Account, TaskHub, MessageId, InstanceId, Details);
        }

        [Event(107, Level = EventLevel.Error)]
        public void GeneralError(string Account, string TaskHub, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(107, Account, TaskHub, Details);
        }

        [Event(108, Level = EventLevel.Warning, Message = "A duplicate message was detected. This can indicate a potential performance problem. Message ID = '{2}'. DequeueCount = {3}.")]
        public void DuplicateMessageDetected(string Account, string TaskHub, string MessageId, int DequeueCount)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(108, Account, TaskHub, MessageId, DequeueCount);
        }

        [Event(110, Level = EventLevel.Informational)]
        public void FetchedInstanceState(string Account, string TaskHub, string InstanceId, string ExecutionId, int EventCount, int RequestCount, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(110, Account, TaskHub, InstanceId, ExecutionId, EventCount, RequestCount, LatencyMs);
        }

        [Event(111, Level = EventLevel.Informational)]
        public void AppendedInstanceState(string Account, string TaskHub, string InstanceId, string ExecutionId, int NewEventCount, int TotalEventCount, string NewEvents, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(111, Account, TaskHub, InstanceId, ExecutionId, NewEventCount, TotalEventCount, NewEvents, LatencyMs);
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
            long ActiveActivities)
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
                ActiveActivities);
        }

        [Event(120, Level = EventLevel.Informational)]
        public void PartitionManagerInfo(string Account, string TaskHub, string WorkerName, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(120, Account, TaskHub, WorkerName, Details);
        }

        [Event(121, Level = EventLevel.Warning)]
        public void PartitionManagerWarning(string Account, string TaskHub, string WorkerName, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(121, Account, TaskHub, WorkerName, Details);
        }

        [NonEvent]
        public void PartitionManagerError(string account, string taskHub, string workerName, Exception exception)
        {
            this.PartitionManagerError(account, taskHub, workerName, exception.ToString());
        }

        [Event(122, Level = EventLevel.Error)]
        public void PartitionManagerError(string Account, string TaskHub, string WorkerName, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(122, Account, TaskHub, WorkerName, Details);
        }

        [Event(123, Level = EventLevel.Verbose, Message = "Host '{2}' renewing lease for PartitionId '{3}' with lease token '{4}'.")]
        public void StartingLeaseRenewal(string Account, string TaskHub, string WorkerName, string PartitionId, string Token)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(123, Account, TaskHub, WorkerName, PartitionId, Token);
        }

        [Event(124, Level = EventLevel.Verbose)]
        public void LeaseRenewalResult(string Account, string TaskHub, string WorkerName, string PartitionId, bool Success, string Token, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(124, Account, TaskHub, WorkerName, PartitionId, Success, Token, Details);
        }

        [Event(125, Level = EventLevel.Informational)]
        public void LeaseRenewalFailed(string Account, string TaskHub, string WorkerName, string PartitionId, string Token, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(125, Account, TaskHub, WorkerName, PartitionId, Token, Details);
        }

        [Event(126, Level = EventLevel.Informational, Message = "Host '{2}' attempting to take lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionStarted(string Account, string TaskHub, string WorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(126, Account, TaskHub, WorkerName, PartitionId);
        }

        [Event(127, Level = EventLevel.Informational, Message = "Host '{2}' successfully acquired lease for PartitionId '{3}'.")]
        public void LeaseAcquisitionSucceeded(string Account, string TaskHub, string WorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(127, Account, TaskHub, WorkerName, PartitionId);
        }

        [Event(128, Level = EventLevel.Informational, Message = "Host '{2}' failed to acquire lease for PartitionId '{3}' due to conflict.")]
        public void LeaseAcquisitionFailed(string Account, string TaskHub, string WorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(128, Account, TaskHub, WorkerName, PartitionId);
        }

        [Event(129, Level = EventLevel.Informational, Message = "Host '{2} is attempting to steal a lease from '{3}' for PartitionId '{4}'.")]
        public void AttemptingToStealLease(string Account, string TaskHub, string WorkerName, string FromWorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(129, Account, TaskHub, WorkerName, FromWorkerName, PartitionId);
        }

        [Event(130, Level = EventLevel.Informational, Message = "Host '{2}' stole lease from '{3}' for PartitionId '{4}'.")]
        public void LeaseStealingSucceeded(string Account, string TaskHub, string WorkerName, string FromWorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(130, Account, TaskHub, WorkerName, FromWorkerName, PartitionId);
        }

        [Event(131, Level = EventLevel.Informational, Message = "Host '{2}' failed to steal lease for PartitionId '{3}' due to conflict.")]
        public void LeaseStealingFailed(string Account, string TaskHub, string WorkerName, string PartitionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(131, Account, TaskHub, WorkerName, PartitionId);
        }

        [Event(132, Level = EventLevel.Informational, Message = "Host '{2}' successfully removed PartitionId '{3}' with lease token '{4}' from currently owned partitions.")]
        public void PartitionRemoved(string Account, string TaskHub, string WorkerName, string PartitionId, string Token)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(132, Account, TaskHub, WorkerName, PartitionId, Token);
        }

        [Event(133, Level = EventLevel.Informational, Message = "Host '{2}' successfully released lease on PartitionId '{3}' with lease token '{4}'")]
        public void LeaseRemoved(string Account, string TaskHub, string WorkerName, string PartitionId, string Token)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(133, Account, TaskHub, WorkerName, PartitionId, Token);
        }

        [Event(134, Level = EventLevel.Warning, Message = "Host '{2}' failed to release lease for PartitionId '{3}' with lease token '{4}' due to conflict.")]
        public void LeaseRemovalFailed(string Account, string TaskHub, string WorkerName, string PartitionId, string Token)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(134, Account, TaskHub, WorkerName, PartitionId, Token);
        }

        [Event(135, Level = EventLevel.Informational, Message = "Host '{2}' successfully updated Instances table with '{5}' event")]
        public void InstanceStatusUpdate(string Account, string TaskHub, string InstanceId, string ExecutionId, string EventType ,long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(135, Account, TaskHub, InstanceId, ExecutionId, EventType, LatencyMs);
        }

        [Event(136, Level = EventLevel.Informational)]
        public void FetchedInstanceStatus(string Account, string TaskHub, string InstanceId, string ExecutionId, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(136, Account, TaskHub, InstanceId, ExecutionId, LatencyMs);
        }
    }
}
