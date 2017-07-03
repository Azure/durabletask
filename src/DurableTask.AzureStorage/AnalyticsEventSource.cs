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
    using System.Runtime.Remoting.Messaging;

    /// <summary>
    /// ETW Event Provider for the DurableTask.AzureStorage provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {4c4ad4a2-f396-5e18-01b6-618c12a10433}.
    /// </remarks>
    [EventSource(Name = "DurableTask-AzureStorage")]
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

        [Event(101, Level = EventLevel.Informational, Opcode = EventOpcode.Send)]
        public void SendingMessage(Guid relatedActivityId, string EventType, string InstanceId, string ExecutionId, long SizeInBytes)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEventWithRelatedActivityId(101, relatedActivityId, EventType, InstanceId, ExecutionId, SizeInBytes);
        }

        [Event(102, Level = EventLevel.Informational, Opcode = EventOpcode.Receive)]
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
            this.WriteEventWithRelatedActivityId(102, relatedActivityId, EventType, InstanceId, ExecutionId, MessageId, Age, DequeueCount, SizeInBytes);
        }

        [Event(103, Level = EventLevel.Informational)]
        public void DeletingMessage(string EventType, string MessageId, string InstanceId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(103, EventType, MessageId, InstanceId);
        }

        [Event(104, Level = EventLevel.Warning, Message = "Abandoning message of type {0} with ID = {1}. Orchestration ID = {2}.")]
        public void AbandoningMessage(string EventType, string MessageId, string InstanceId, string ExecutionId)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(104, EventType, MessageId, InstanceId, ExecutionId);
        }

        [Event(105, Level = EventLevel.Warning, Message = "An unexpected condition was detected: {0}")]
        public void AssertFailure(string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(105, Details);
        }

        [Event(106, Level = EventLevel.Warning)]
        public void MessageGone(string MessageId, string InstanceId, string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(106, MessageId, InstanceId, Details);
        }

        [Event(110, Level = EventLevel.Informational)]
        public void FetchedInstanceState(string InstanceId, string ExecutionId, int EventCount, int RequestCount, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(110, InstanceId, ExecutionId, EventCount, RequestCount, LatencyMs);
        }

        [Event(111, Level = EventLevel.Informational)]
        public void AppendedInstanceState(string InstanceId, string ExecutionId, int NewEventCount, int TotalEventCount, string NewEvents, long LatencyMs)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(111, InstanceId, ExecutionId, NewEventCount, TotalEventCount, NewEvents, LatencyMs);
        }

        [Event(120, Level = EventLevel.Informational)]
        public void LogPartitionInfo(string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(120, Details);
        }

        [Event(121, Level = EventLevel.Error)]
        public void LogPartitionWarning(string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(121, Details);
        }

        [NonEvent]
        public void LogPartitionException(Exception e)
        {
            this.LogPartitionError(e.ToString());
        }

        [Event(122, Level = EventLevel.Error)]
        public void LogPartitionError(string Details)
        {
            EnsureLogicalTraceActivityId();
            this.WriteEvent(122, Details);
        }
    }
}
