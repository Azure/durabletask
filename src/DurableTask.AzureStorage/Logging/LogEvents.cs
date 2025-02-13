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

    static class LogEvents
    {
        internal class SendingMessage : StructuredLogEvent, IEventSourceEvent
        {
            public SendingMessage(
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
                this.RelatedActivityId = relatedActivityId;
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.SizeInBytes = sizeInBytes;
                this.PartitionId = partitionId;
                this.TargetInstanceId = targetInstanceId;
                this.TargetExecutionId = targetExecutionId;
                this.SequenceNumber = sequenceNumber;
                this.Episode = episode;
            }

            public Guid RelatedActivityId { get; }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public long SizeInBytes { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string TargetInstanceId { get; }

            [StructuredLogField]
            public string TargetExecutionId { get; }

            [StructuredLogField]
            public long SequenceNumber { get; }

            [StructuredLogField]
            public int Episode { get; }

            public override EventId EventId => new EventId(
                EventIds.SendingMessage,
                nameof(EventIds.SendingMessage));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => string.Format(
                "{0}Sending {1} message to {2} for instance '{3}'",
                string.IsNullOrEmpty(this.InstanceId) ? string.Empty : this.InstanceId + ": ",
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId,
                this.TargetInstanceId);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.SendingMessage(
                this.RelatedActivityId,
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.InstanceId,
                this.ExecutionId,
                this.SizeInBytes,
                this.PartitionId,
                this.TargetInstanceId,
                this.TargetExecutionId,
                this.SequenceNumber,
                this.Episode,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class ReceivedMessage : StructuredLogEvent, IEventSourceEvent
        {
            public ReceivedMessage(
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
                this.RelatedActivityId = relatedActivityId;
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.MessageId = messageId;
                this.Age = age;
                this.DequeueCount = dequeueCount;
                this.NextVisibleTime = nextVisibleTime;
                this.SizeInBytes = sizeInBytes;
                this.PartitionId = partitionId;
                this.SequenceNumber = sequenceNumber;
                this.PopReceipt = popReceipt;
                this.Episode = episode;
            }

            public Guid RelatedActivityId { get; }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public int Age { get; }

            [StructuredLogField]
            public long DequeueCount { get; }

            [StructuredLogField]
            public string NextVisibleTime { get; }

            [StructuredLogField]
            public long SizeInBytes { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long SequenceNumber { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            [StructuredLogField]
            public int Episode { get; }

            public override EventId EventId => new EventId(
                EventIds.ReceivedMessage,
                nameof(EventIds.ReceivedMessage));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Fetched {1} message from {2} (delay = {3}ms)",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId,
                this.Age);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.ReceivedMessage(
                this.RelatedActivityId,
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.InstanceId,
                this.ExecutionId,
                this.MessageId,
                this.Age,
                this.DequeueCount,
                this.NextVisibleTime,
                this.SizeInBytes,
                this.PartitionId,
                this.SequenceNumber,
                this.PopReceipt,
                this.Episode,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class DeletingMessage : StructuredLogEvent, IEventSourceEvent
        {
            public DeletingMessage(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.SequenceNumber = sequenceNumber;
                this.PopReceipt = popReceipt;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long SequenceNumber { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            public override EventId EventId => new EventId(
                EventIds.DeletingMessage,
                nameof(EventIds.DeletingMessage));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Deleting {1} message from {2}",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.DeletingMessage(
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.SequenceNumber,
                this.PopReceipt,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class AbandoningMessage : StructuredLogEvent, IEventSourceEvent
        {
            public AbandoningMessage(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.SequenceNumber = sequenceNumber;
                this.PopReceipt = popReceipt;
                this.VisibilityTimeoutSeconds = visibilityTimeoutSeconds;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long SequenceNumber { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            [StructuredLogField]
            public int VisibilityTimeoutSeconds { get; }

            public override EventId EventId => new EventId(
                EventIds.AbandoningMessage,
                nameof(EventIds.AbandoningMessage));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Abandoning {1} message back to {2} and setting a visibility delay of {3}ms",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId,
                this.VisibilityTimeoutSeconds);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.AbandoningMessage(
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.SequenceNumber,
                this.PopReceipt,
                this.VisibilityTimeoutSeconds,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class AssertFailure : StructuredLogEvent, IEventSourceEvent
        {
            public AssertFailure(
                string account,
                string taskHub,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.AssertFailure,
                nameof(EventIds.AssertFailure));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => "Internal assert failure: " + this.Details;

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.AssertFailure(
                this.Account,
                this.TaskHub,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class MessageGone : StructuredLogEvent, IEventSourceEvent
        {
            public MessageGone(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.Details = details;
                this.PopReceipt = popReceipt;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            public override EventId EventId => new EventId(
                EventIds.MessageGone,
                nameof(EventIds.MessageGone));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Failed to update or delete message {1} with ID = {2} because it no longer exists",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.MessageId);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.MessageGone(
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.Details,
                this.PopReceipt,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class GeneralError : StructuredLogEvent, IEventSourceEvent
        {
            public GeneralError(
                string account,
                string taskHub,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.GeneralError,
                nameof(EventIds.GeneralError));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() => "General error: " + this.Details;

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.GeneralError(
                this.Account,
                this.TaskHub,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class DuplicateMessageDetected : StructuredLogEvent, IEventSourceEvent
        {
            public DuplicateMessageDetected(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.DequeueCount = dequeueCount;
                this.PopReceipt = popReceipt;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long DequeueCount { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            public override EventId EventId => new EventId(
                EventIds.DuplicateMessageDetected,
                nameof(EventIds.DuplicateMessageDetected));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Received a duplicate message {1} from {2} with ID = {3} and dequeue count = {4}",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId,
                this.MessageId,
                this.DequeueCount);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.DuplicateMessageDetected(
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.DequeueCount,
                this.PopReceipt,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PoisonMessageDetected : StructuredLogEvent, IEventSourceEvent
        {
            public PoisonMessageDetected(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.DequeueCount = dequeueCount;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long DequeueCount { get; }

            public override EventId EventId => new EventId(
                EventIds.PoisonMessageDetected,
                nameof(EventIds.PoisonMessageDetected));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Message {1} with ID {2} has been dequeued {3} times and is now considered poison",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.MessageId,
                this.DequeueCount);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PoisonMessageDetected(
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.DequeueCount,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class FetchedInstanceHistory : StructuredLogEvent, IEventSourceEvent
        {
            public FetchedInstanceHistory(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.EventCount = eventCount;
                this.Episode = episode;
                this.RequestCount = requestCount;
                this.LatencyMs = latencyMs;
                this.ETag = eTag;
                this.LastCheckpointTime = lastCheckpointTime;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int EventCount { get; }

            [StructuredLogField]
            public int Episode { get; }

            [StructuredLogField]
            public int RequestCount { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public string ETag { get; }

            [StructuredLogField]
            public DateTime LastCheckpointTime { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchedInstanceHistory,
                nameof(EventIds.FetchedInstanceHistory));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                this.EventCount > 0 ?
                    $"{this.InstanceId}: Fetched {this.EventCount} history events from the history table in {this.LatencyMs}ms" :
                    $"{this.InstanceId}: No history events were found";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.FetchedInstanceHistory(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.EventCount,
                this.Episode,
                this.RequestCount,
                this.LatencyMs,
                this.ETag,
                this.LastCheckpointTime,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class AppendedInstanceHistory : StructuredLogEvent, IEventSourceEvent
        {
            public AppendedInstanceHistory(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.NewEventCount = newEventCount;
                this.TotalEventCount = totalEventCount;
                this.NewEvents = newEvents;
                this.Episode = episode;
                this.LatencyMs = latencyMs;
                this.SizeInBytes = sizeInBytes;
                this.ETag = eTag;
                this.IsCheckpointComplete = isCheckpointComplete;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int NewEventCount { get; }

            [StructuredLogField]
            public int TotalEventCount { get; }

            [StructuredLogField]
            public string NewEvents { get; }

            [StructuredLogField]
            public int Episode { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public int SizeInBytes { get; }

            [StructuredLogField]
            public string ETag { get; }

            [StructuredLogField]
            public bool IsCheckpointComplete { get; }

            public override EventId EventId => new EventId(
                EventIds.AppendedInstanceHistory,
                nameof(EventIds.AppendedInstanceHistory));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => 
                $"{this.InstanceId}: Appended {this.NewEventCount} new events to the history table in {this.LatencyMs}ms";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.AppendedInstanceHistory(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.NewEventCount,
                this.TotalEventCount,
                this.NewEvents,
                this.Episode,
                this.LatencyMs,
                this.SizeInBytes,
                this.ETag,
                this.IsCheckpointComplete,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class OrchestrationServiceStats : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationServiceStats(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.StorageRequests = storageRequests;
                this.MessagesSent = messagesSent;
                this.MessagesRead = messagesRead;
                this.MessagesUpdated = messagesUpdated;
                this.TableEntitiesWritten = tableEntitiesWritten;
                this.TableEntitiesRead = tableEntitiesRead;
                this.PendingOrchestrators = pendingOrchestrators;
                this.PendingOrchestratorMessages = pendingOrchestratorMessages;
                this.ActiveOrchestrators = activeOrchestrators;
                this.ActiveActivities = activeActivities;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public long StorageRequests { get; }

            [StructuredLogField]
            public long MessagesSent { get; }

            [StructuredLogField]
            public long MessagesRead { get; }

            [StructuredLogField]
            public long MessagesUpdated { get; }

            [StructuredLogField]
            public long TableEntitiesWritten { get; }

            [StructuredLogField]
            public long TableEntitiesRead { get; }

            [StructuredLogField]
            public long PendingOrchestrators { get; }

            [StructuredLogField]
            public long PendingOrchestratorMessages { get; }

            [StructuredLogField]
            public long ActiveOrchestrators { get; }

            [StructuredLogField]
            public long ActiveActivities { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationServiceStats,
                nameof(EventIds.OrchestrationServiceStats));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() => 
                $"There are currently {this.ActiveOrchestrators + this.PendingOrchestrators} orchestration(s) and {this.ActiveActivities} activities loaded into memory on this worker";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.OrchestrationServiceStats(
                this.Account,
                this.TaskHub,
                this.StorageRequests,
                this.MessagesSent,
                this.MessagesRead,
                this.MessagesUpdated,
                this.TableEntitiesWritten,
                this.TableEntitiesRead,
                this.PendingOrchestrators,
                this.PendingOrchestratorMessages,
                this.ActiveOrchestrators,
                this.ActiveActivities,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class RenewingMessage : StructuredLogEvent, IEventSourceEvent
        {
            public RenewingMessage(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.PopReceipt = popReceipt;
                this.VisibilityTimeoutSeconds = visibilityTimeoutSeconds;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string PopReceipt { get; }

            [StructuredLogField]
            public int VisibilityTimeoutSeconds { get; }

            public override EventId EventId => new EventId(
                EventIds.RenewingMessage,
                nameof(EventIds.RenewingMessage));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Renewing {1} message on {2} for an additional {3} seconds",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId,
                this.VisibilityTimeoutSeconds);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.RenewingMessage(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.PopReceipt,
                this.VisibilityTimeoutSeconds,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class MessageFailure : StructuredLogEvent, IEventSourceEvent
        {
            public MessageFailure(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.MessageId = messageId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.MessageFailure,
                nameof(EventIds.MessageFailure));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage()
            {
                if (!string.IsNullOrEmpty(this.InstanceId))
                {
                    return $"{this.InstanceId}: An error occurred while processing message {GetEventDescription(this.EventType, this.TaskEventId)}: {this.Details}";
                }
                else
                {
                    return $"An error occurred while processing messages on {this.PartitionId}: {this.Details}";
                }
            }

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.MessageFailure(
                this.Account,
                this.TaskHub,
                this.MessageId,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.EventType,
                this.TaskEventId,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class OrchestrationProcessingFailure : StructuredLogEvent, IEventSourceEvent
        {
            public OrchestrationProcessingFailure(
                string account,
                string taskHub,
                string instanceId,
                string executionId,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.OrchestrationProcessingFailure,
                nameof(EventIds.OrchestrationProcessingFailure));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() => 
                $"An unexpected failure occurred while processing instance '{this.InstanceId}': {this.Details}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.OrchestrationProcessingFailure(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PendingOrchestratorMessageLimitReached : StructuredLogEvent, IEventSourceEvent
        {
            public PendingOrchestratorMessageLimitReached(
                string account,
                string taskHub,
                string partitionId,
                long pendingOrchestratorMessages)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.PartitionId = partitionId;
                this.PendingOrchestratorMessages = pendingOrchestratorMessages;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long PendingOrchestratorMessages { get; }

            public override EventId EventId => new EventId(
                EventIds.PendingOrchestratorMessageLimitReached,
                nameof(EventIds.PendingOrchestratorMessageLimitReached));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => 
                $"The maximum orchestration message prefetch limit of {this.PendingOrchestratorMessages} has been reached; " +
                $"fetching from {this.PartitionId} will be temporarily suspended until existing messages are consumed";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PendingOrchestratorMessageLimitReached(
                this.Account,
                this.TaskHub,
                this.PartitionId,
                this.PendingOrchestratorMessages,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class WaitingForMoreMessages : StructuredLogEvent, IEventSourceEvent
        {
            public WaitingForMoreMessages(
                string account,
                string taskHub,
                string partitionId)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.PartitionId = partitionId;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            public override EventId EventId => new EventId(
                EventIds.WaitingForMoreMessages,
                nameof(EventIds.WaitingForMoreMessages));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => 
                $"{this.PartitionId}: No new messages were found - backing off";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.WaitingForMoreMessages(
                this.Account,
                this.TaskHub,
                this.PartitionId,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class ReceivedOutOfOrderMessage : StructuredLogEvent, IEventSourceEvent
        {
            public ReceivedOutOfOrderMessage(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.PartitionId = partitionId;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.MessageId = messageId;
                this.Episode = episode;
                this.LastCheckpointTime = lastCheckpointTime;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public int Episode { get; }

            [StructuredLogField]
            public DateTime LastCheckpointTime { get; }

            public override EventId EventId => new EventId(
                EventIds.ReceivedOutOfOrderMessage,
                nameof(EventIds.ReceivedOutOfOrderMessage));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => string.Format(
                "{0}: Message {1} was received out of order and will be temporarily returned back to {2}",
                this.InstanceId,
                GetEventDescription(this.EventType, this.TaskEventId),
                this.PartitionId);

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.ReceivedOutOfOrderMessage(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.PartitionId,
                this.EventType,
                this.TaskEventId,
                this.MessageId,
                this.Episode,
                this.LastCheckpointTime,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PartitionManagerInfo : StructuredLogEvent, IEventSourceEvent
        {
            public PartitionManagerInfo(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.PartitionManagerInfo,
                nameof(EventIds.PartitionManagerInfo));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => 
                !string.IsNullOrEmpty(this.PartitionId) ? $"{this.PartitionId}: {this.Details}" : this.Details;

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PartitionManagerInfo(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PartitionManagerWarning : StructuredLogEvent, IEventSourceEvent
        {
            public PartitionManagerWarning(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.PartitionManagerWarning,
                nameof(EventIds.PartitionManagerWarning));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                !string.IsNullOrEmpty(this.PartitionId) ? $"{this.PartitionId}: {this.Details}" : this.Details;

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PartitionManagerWarning(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PartitionManagerError : StructuredLogEvent, IEventSourceEvent
        {
            public PartitionManagerError(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.PartitionManagerError,
                nameof(EventIds.PartitionManagerError));

            public override LogLevel Level => LogLevel.Error;

            protected override string CreateLogMessage() =>
                !string.IsNullOrEmpty(this.PartitionId) ? $"{this.PartitionId}: {this.Details}" : this.Details;

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PartitionManagerError(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class StartingLeaseRenewal : StructuredLogEvent, IEventSourceEvent
        {
            public StartingLeaseRenewal(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string token,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Token = token;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Token { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.StartingLeaseRenewal,
                nameof(EventIds.StartingLeaseRenewal));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: Starting {this.LeaseType} lease renewal with token {this.Token}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.StartingLeaseRenewal(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Token,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseRenewalResult : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseRenewalResult(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                bool success,
                string token,
                string leaseType,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Success = success;
                this.Token = token;
                this.LeaseType = leaseType;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public bool Success { get; }

            [StructuredLogField]
            public string Token { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseRenewalResult,
                nameof(EventIds.LeaseRenewalResult));

            public override LogLevel Level => LogLevel.Debug;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: {this.LeaseType} lease renewal with token {this.Token} {(this.Success ? "succeeded" : "failed")}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseRenewalResult(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Success,
                this.Token,
                this.LeaseType,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseRenewalFailed : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseRenewalFailed(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string token,
                string leaseType,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Token = token;
                this.LeaseType = leaseType;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Token { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseRenewalFailed,
                nameof(EventIds.LeaseRenewalFailed));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: {this.LeaseType} lease renewal with token {this.Token} failed: {this.Details}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseRenewalFailed(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Token,
                this.LeaseType,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseAcquisitionStarted : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseAcquisitionStarted(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseAcquisitionStarted,
                nameof(EventIds.LeaseAcquisitionStarted));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: Attempting to acquire {this.LeaseType} lease";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseAcquisitionStarted(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseAcquisitionSucceeded : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseAcquisitionSucceeded(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseAcquisitionSucceeded,
                nameof(EventIds.LeaseAcquisitionSucceeded));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: Successfully acquired {this.LeaseType} lease";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseAcquisitionSucceeded(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseAcquisitionFailed : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseAcquisitionFailed(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseAcquisitionFailed,
                nameof(EventIds.LeaseAcquisitionFailed));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: Failed to acquire {this.LeaseType} lease";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseAcquisitionFailed(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class AttemptingToStealLease : StructuredLogEvent, IEventSourceEvent
        {
            public AttemptingToStealLease(
                string account,
                string taskHub,
                string workerName,
                string fromWorkerName,
                string leaseType,
                string partitionId)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.FromWorkerName = fromWorkerName;
                this.LeaseType = leaseType;
                this.PartitionId = partitionId;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string FromWorkerName { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            public override EventId EventId => new EventId(
                EventIds.AttemptingToStealLease,
                nameof(EventIds.AttemptingToStealLease));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: Attempting to steal {this.LeaseType} lease from {this.FromWorkerName}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.AttemptingToStealLease(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.FromWorkerName,
                this.LeaseType,
                this.PartitionId,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseStealingSucceeded : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseStealingSucceeded(
                string account,
                string taskHub,
                string workerName,
                string fromWorkerName,
                string leaseType,
                string partitionId)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.FromWorkerName = fromWorkerName;
                this.LeaseType = leaseType;
                this.PartitionId = partitionId;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string FromWorkerName { get; }

            [StructuredLogField]
            public string LeaseType { get; }
            
            [StructuredLogField]
            public string PartitionId { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseStealingSucceeded,
                nameof(EventIds.LeaseStealingSucceeded));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: Successfully stole {this.LeaseType} lease from {this.FromWorkerName}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseStealingSucceeded(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.FromWorkerName,
                this.LeaseType,
                this.PartitionId,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseStealingFailed : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseStealingFailed(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseStealingFailed,
                nameof(EventIds.LeaseStealingFailed));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: Failed to steal {this.LeaseType} lease";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseStealingFailed(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PartitionRemoved : StructuredLogEvent, IEventSourceEvent
        {
            public PartitionRemoved(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string token)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Token = token;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Token { get; }

            public override EventId EventId => new EventId(
                EventIds.PartitionRemoved,
                nameof(EventIds.PartitionRemoved));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: {this.WorkerName} is no longer processing messages for this partition";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PartitionRemoved(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Token,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseRemoved : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseRemoved(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string token,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Token = token;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Token { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseRemoved,
                nameof(EventIds.LeaseRemoved));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"{this.PartitionId}: {this.WorkerName} has released its {this.LeaseType} lease";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseRemoved(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Token,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class LeaseRemovalFailed : StructuredLogEvent, IEventSourceEvent
        {
            public LeaseRemovalFailed(
                string account,
                string taskHub,
                string workerName,
                string partitionId,
                string token,
                string leaseType)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.WorkerName = workerName;
                this.PartitionId = partitionId;
                this.Token = token;
                this.LeaseType = leaseType;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string WorkerName { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public string Token { get; }

            [StructuredLogField]
            public string LeaseType { get; }

            public override EventId EventId => new EventId(
                EventIds.LeaseRemovalFailed,
                nameof(EventIds.LeaseRemovalFailed));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.PartitionId}: {this.WorkerName} failed to release its {this.LeaseType} lease due to a conflict";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.LeaseRemovalFailed(
                this.Account,
                this.TaskHub,
                this.WorkerName,
                this.PartitionId,
                this.Token,
                this.LeaseType,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class InstanceStatusUpdate : StructuredLogEvent, IEventSourceEvent
        {
            public InstanceStatusUpdate(
                string account,
                string taskHub,
                string instanceId,
                string executionId,
                OrchestrationStatus runtimeStatus,
                int episode,
                long latencyMs)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.RuntimeStatus = runtimeStatus.ToString();
                this.Episode = episode;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string RuntimeStatus { get; }

            [StructuredLogField]
            public int Episode { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.InstanceStatusUpdate,
                nameof(EventIds.InstanceStatusUpdate));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Updated Instances table and set the runtime status to '{this.RuntimeStatus}'";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.RuntimeStatus,
                this.Episode,
                this.LatencyMs,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class FetchedInstanceStatus : StructuredLogEvent, IEventSourceEvent
        {
            public FetchedInstanceStatus(
                string account,
                string taskHub,
                string instanceId,
                string executionId,
                string runtimeStatus,
                long latencyMs)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.RuntimeStatus = runtimeStatus;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string RuntimeStatus { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.FetchedInstanceStatus,
                nameof(EventIds.FetchedInstanceStatus));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() => $"Fetched instance status for {this.InstanceId}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.FetchedInstanceStatus(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.RuntimeStatus,
                this.LatencyMs,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class GeneralWarning : StructuredLogEvent, IEventSourceEvent
        {
            public GeneralWarning(
                string account,
                string taskHub,
                string details,
                string instanceId)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.Details = details;
                this.InstanceId = instanceId;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string Details { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            public override EventId EventId => new EventId(
                EventIds.GeneralWarning,
                nameof(EventIds.GeneralWarning));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => $"WARNING: {this.Details}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.GeneralWarning(
                this.Account,
                this.TaskHub,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion,
                this.InstanceId);
        }

        internal class SplitBrainDetected : StructuredLogEvent, IEventSourceEvent
        {
            public SplitBrainDetected(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.NewEventCount = newEventCount;
                this.TotalEventCount = totalEventCount;
                this.NewEvents = newEvents;
                this.LatencyMs = latencyMs;
                this.ETag = eTag;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int NewEventCount { get; }

            [StructuredLogField]
            public int TotalEventCount { get; }

            [StructuredLogField]
            public string NewEvents { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            [StructuredLogField]
            public string ETag { get; }

            public override EventId EventId => new EventId(
                EventIds.SplitBrainDetected,
                nameof(EventIds.SplitBrainDetected));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Split brain detected: Another worker already updated the history for this instance - the {this.NewEventCount} history event result(s) will be discarded";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.SplitBrainDetected(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.NewEventCount,
                this.TotalEventCount,
                this.NewEvents,
                this.LatencyMs,
                this.ETag,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class DiscardingWorkItem : StructuredLogEvent, IEventSourceEvent
        {
            public DiscardingWorkItem(
                string account,
                string taskHub,
                string instanceId,
                string executionId,
                int newEventCount,
                int totalEventCount,
                string newEvents,
                string details)
            {
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.NewEventCount = newEventCount;
                this.TotalEventCount = totalEventCount;
                this.NewEvents = newEvents;
                this.Details = details;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public int NewEventCount { get; }

            [StructuredLogField]
            public int TotalEventCount { get; }

            [StructuredLogField]
            public string NewEvents { get; }

            [StructuredLogField]
            public string Details { get; }

            public override EventId EventId => new EventId(
                EventIds.DiscardingWorkItem,
                nameof(EventIds.DiscardingWorkItem));

            public override LogLevel Level => LogLevel.Warning;

            protected override string CreateLogMessage() => 
                $"{this.InstanceId}: Discarding {this.NewEventCount} dequeued history event(s): {this.Details}";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.DiscardingWorkItem(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.ExecutionId,
                this.NewEventCount,
                this.TotalEventCount,
                this.NewEvents,
                this.Details,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class ProcessingMessage : StructuredLogEvent, IEventSourceEvent
        {
            public ProcessingMessage(
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
                this.RelatedActivityId = relatedActivityId;
                this.Account = account;
                this.TaskHub = taskHub;
                this.EventType = eventType;
                this.TaskEventId = taskEventId;
                this.InstanceId = instanceId;
                this.ExecutionId = executionId;
                this.MessageId = messageId;
                this.Age = age;
                this.PartitionId = partitionId;
                this.SequenceNumber = sequenceNumber;
                this.Episode = episode;
                this.IsExtendedSession = isExtendedSession;
            }

            public Guid RelatedActivityId { get; }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string EventType { get; }

            [StructuredLogField]
            public int TaskEventId { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string ExecutionId { get; }

            [StructuredLogField]
            public string MessageId { get; }

            [StructuredLogField]
            public int Age { get; }

            [StructuredLogField]
            public string PartitionId { get; }

            [StructuredLogField]
            public long SequenceNumber { get; }

            [StructuredLogField]
            public int Episode { get; }

            [StructuredLogField]
            public bool IsExtendedSession { get; }

            public override EventId EventId => new EventId(
                EventIds.ProcessingMessage,
                nameof(EventIds.ProcessingMessage));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                $"{this.InstanceId}: Processing {GetEventDescription(this.EventType, this.TaskEventId)} (total delay = {this.Age}ms)";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.ProcessingMessage(
                this.RelatedActivityId,
                this.Account,
                this.TaskHub,
                this.EventType,
                this.TaskEventId,
                this.InstanceId,
                this.ExecutionId,
                this.MessageId,
                this.Age,
                this.PartitionId,
                this.SequenceNumber,
                this.Episode,
                this.IsExtendedSession,
                Utils.AppName,
                Utils.ExtensionVersion);
        }

        internal class PurgeInstanceHistory : StructuredLogEvent, IEventSourceEvent
        {
            public PurgeInstanceHistory(
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
                this.Account = account;
                this.TaskHub = taskHub;
                this.InstanceId = instanceId;
                this.CreatedTimeFrom = createdTimeFrom;
                this.CreatedTimeTo = createdTimeTo;
                this.RuntimeStatus = runtimeStatus;
                this.RequestCount = requestCount;
                this.InstanceCount = instanceCount;
                this.LatencyMs = latencyMs;
            }

            [StructuredLogField]
            public string Account { get; }

            [StructuredLogField]
            public string TaskHub { get; }

            [StructuredLogField]
            public string InstanceId { get; }

            [StructuredLogField]
            public string CreatedTimeFrom { get; }

            [StructuredLogField]
            public string CreatedTimeTo { get; }

            [StructuredLogField]
            public string RuntimeStatus { get; }

            [StructuredLogField]
            public int RequestCount { get; }

            [StructuredLogField]
            public int InstanceCount { get; }

            [StructuredLogField]
            public long LatencyMs { get; }

            public override EventId EventId => new EventId(
                EventIds.PurgeInstanceHistory,
                nameof(EventIds.PurgeInstanceHistory));

            public override LogLevel Level => LogLevel.Information;

            protected override string CreateLogMessage() =>
                !string.IsNullOrEmpty(this.InstanceId) ?
                    $"Purged instance history data for instance '{this.InstanceId}' in {this.LatencyMs / 1000.0:F} seconds" :
                    $"Purged instance history data for {this.InstanceCount} instance(s) in {this.LatencyMs / 1000.0:F} seconds";

            void IEventSourceEvent.WriteEventSource() => AnalyticsEventSource.Log.PurgeInstanceHistory(
                this.Account,
                this.TaskHub,
                this.InstanceId,
                this.CreatedTimeFrom,
                this.CreatedTimeTo,
                this.RuntimeStatus,
                this.RequestCount,
                this.InstanceCount,
                this.LatencyMs,
                Utils.AppName,
                Utils.ExtensionVersion);
        }
    }
}
