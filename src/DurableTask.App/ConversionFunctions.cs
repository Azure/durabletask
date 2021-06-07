// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.Protobuf.Converters
{
    using System;
    using DurableTask.Core;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using Google.Protobuf.WellKnownTypes;

    public static class ConversionFunctions
    {
        public static Protobuf.HistoryEvent ToHistoryEventProto(HistoryEvent e)
        {
            var payload = new Protobuf.HistoryEvent()
            {
                EventId = e.EventId,
                Timestamp = Timestamp.FromDateTime(e.Timestamp),
            };

            switch (e.EventType)
            {
                case EventType.ContinueAsNew:
                    // TODO
                    break;
                case EventType.EventRaised:
                    // TODO
                    break;
                case EventType.EventSent:
                    // TODO
                    break;
                case EventType.ExecutionCompleted:
                    // No-op, apps aren't expected to handle this
                    payload.ExecutionCompleted = new Protobuf.ExecutionCompletedEvent();
                    break;
                case EventType.ExecutionFailed:
                    // No-op, apps aren't expected to handle this
                    payload.ExecutionFailed = new Protobuf.ExecutionFailedEvent();
                    break;
                case EventType.ExecutionStarted:
                    // Start of a new orchestration instance
                    var startedEvent = (ExecutionStartedEvent)e;
                    payload.ExecutionStarted = new Protobuf.ExecutionStartedEvent
                    {
                        Name = startedEvent.Name,
                        Version = startedEvent.Version,
                        Input = startedEvent.Input,
                        OrchestrationInstance = new Protobuf.OrchestrationInstance
                        {
                            InstanceId = startedEvent.OrchestrationInstance.InstanceId,
                            ExecutionId = startedEvent.OrchestrationInstance.ExecutionId,
                        },
                        ParentInstance = startedEvent.ParentInstance == null ? null : new Protobuf.OrchestrationInstance
                        {
                            InstanceId = startedEvent.ParentInstance.OrchestrationInstance.InstanceId,
                            ExecutionId = startedEvent.ParentInstance.OrchestrationInstance.ExecutionId,
                        },
                    };
                    break;
                case EventType.ExecutionTerminated:
                    // TODO
                    break;
                case EventType.TaskScheduled:
                    // TODO
                    break;
                case EventType.TaskCompleted:
                    // TODO
                    break;
                case EventType.TaskFailed:
                    // TODO
                    break;
                case EventType.SubOrchestrationInstanceCreated:
                    // TODO
                    break;
                case EventType.SubOrchestrationInstanceCompleted:
                    // TODO
                    break;
                case EventType.SubOrchestrationInstanceFailed:
                    // TODO
                    break;
                case EventType.TimerCreated:
                    // TODO
                    break;
                case EventType.TimerFired:
                    // TODO
                    break;
                case EventType.OrchestratorStarted:
                    // This event has no data
                    payload.OrchestratorStarted = new Protobuf.OrchestratorStartedEvent();
                    break;
                case EventType.OrchestratorCompleted:
                    // This event has no data
                    payload.OrchestratorCompleted = new Protobuf.OrchestratorCompletedEvent();
                    break;
                case EventType.GenericEvent:
                    // This event has no data
                    payload.GenericEvent = new Protobuf.GenericEvent();
                    break;
                case EventType.HistoryState:
                    // TODO
                    break;
                default:
                    throw new NotSupportedException($"Found unsupported history event '{e.EventType}'.");
            }

            return payload;
        }
    }
}
