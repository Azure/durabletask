//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Diagnostics.Tracing;
    using System.Runtime.Remoting.Messaging;

    /// <summary>
    /// ETW Event Provider for the DurableTask.AzureStorage provider extension.
    /// </summary>
    [EventSource(Name = "DurableTask.AzureStorage")]
    class AnalyticsEventSource : EventSource
    {
        const string TraceActivityIdSlot = "TraceActivityId";

        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly AnalyticsEventSource Log = new AnalyticsEventSource();

        [NonEvent]
        public static void SetLogicalTraceActivityId(Guid activityId)
        {
            // We use LogicalSetData to preserve activity IDs across async/await boundaries.
            CallContext.LogicalSetData(TraceActivityIdSlot, activityId);
            SetCurrentThreadActivityId(activityId);
        }

        [NonEvent]
        private static void EnsureLogicalTraceActivityId()
        {
            object data = CallContext.LogicalGetData(TraceActivityIdSlot);
            if (data != null)
            {
                Guid currentActivityId = (Guid)data;
                if (currentActivityId != CurrentThreadActivityId)
                {
                    SetCurrentThreadActivityId(currentActivityId);
                }
            }
        }

        [Event(1, Level = EventLevel.Informational, Opcode = EventOpcode.Send)]
        public void SendingMessage(Guid relatedActivityId, string EventType, string InstanceId, string ExecutionId, long SizeInBytes)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(1, relatedActivityId, EventType, InstanceId, ExecutionId, SizeInBytes);
        }

        [Event(2, Level = EventLevel.Informational, Opcode = EventOpcode.Receive)]
        public void ReceivedMessage(
            Guid relatedActivityId,
            string EventType,
            string InstanceId,
            string ExecutionId,
            string MessageId,
            int Age,
            int DequeueCount,
            long SizeInBytes)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(2, relatedActivityId, EventType, InstanceId, ExecutionId, MessageId, Age, DequeueCount, SizeInBytes);
        }

        [Event(3, Level = EventLevel.Informational)]
        public void DeletingMessage(string EventType, string MessageId, string InstanceId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(3, EventType, MessageId, InstanceId);
        }

        [Event(4, Level = EventLevel.Warning, Message = "Abandoning message of type {0} with ID = {1}. Orchestration ID = {2}.")]
        public void AbandoningMessage(string EventType, string MessageId, string InstanceId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(4, EventType, MessageId, InstanceId);
        }

        [Event(5, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {0}")]
        public void AssertFailure(string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(5, Details);
        }

        [Event(10, Level = EventLevel.Informational)]
        public void FetchedInstanceState(string InstanceId, int EventCount, int RequestCount, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(10, InstanceId, EventCount, RequestCount, LatencyMs);
        }

        [Event(11, Level = EventLevel.Informational)]
        public void AppendedInstanceState(string InstanceId, int NewEventCount, int TotalEventCount, string NewEvents, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(11, InstanceId, NewEventCount, TotalEventCount, NewEvents, LatencyMs);
        }
    }
}
