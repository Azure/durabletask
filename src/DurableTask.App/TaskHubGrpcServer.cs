// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.App
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Command;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Protobuf.Converters;
    using Google.Protobuf.WellKnownTypes;
    using Grpc.Core;

    class TaskHubGrpcServer : Protobuf.TaskHubService.TaskHubServiceBase
    {
        readonly INameVersionObjectManager<TaskOrchestration> orchestrationManager;
        readonly INameVersionObjectManager<TaskActivity> activityManager;

        public TaskHubGrpcServer(
            INameVersionObjectManager<TaskOrchestration>? orchestrationManager,
            INameVersionObjectManager<TaskActivity>? activityManager)
        {
            this.orchestrationManager = orchestrationManager ?? throw new ArgumentNullException(nameof(orchestrationManager));
            this.activityManager = activityManager ?? throw new ArgumentNullException(nameof(activityManager));
        }

        public override async Task<Protobuf.OrchestratorResponse> ExecuteOrchestrator(
            Protobuf.OrchestratorRequest request,
            ServerCallContext context)
        {
            OrchestrationRuntimeState runtimeState = ConstructRuntimeState(request);

            // TODO: Error handling for orchestrations that aren't registered
            TaskOrchestration implementation = this.orchestrationManager.GetObject(
                runtimeState.Name,
                runtimeState.Version);

            // TODO: For extended sessions, we can cache the executor objects
            var executor = new TaskOrchestrationExecutor(runtimeState, implementation, BehaviorOnContinueAsNew.Carryover);
            IEnumerable<OrchestratorAction> nextActions = await executor.ExecuteAsync();

            Protobuf.OrchestratorResponse response = ConstructOrchestratorResponse(nextActions);
            return response;
        }

        public override async Task<Protobuf.ActivityResponse> ExecuteActivity(
            Protobuf.ActivityRequest request,
            ServerCallContext callContext)
        {
            // TODO: Error handling for activities that aren't registered
            TaskActivity implementation = this.activityManager.GetObject(request.Name, request.Version);
            var taskContext = new TaskContext(ConvertOrchestrationInstance(request.OrchestrationInstance));

            try
            {
                string result = await implementation.RunAsync(taskContext, request.Input);
                return new Protobuf.ActivityResponse { Result = result };
            }
            catch (TaskFailureException e)
            {
                string details = e.InnerException?.ToString() ?? e.Message;
                var status = new Status(StatusCode.Unknown, details);
                throw new RpcException(status, "TaskFailure");
            }
            catch (Exception e)
            {
                string details = e.InnerException?.ToString() ?? e.Message;
                var status = new Status(StatusCode.Internal, details);
                throw new RpcException(status, "SdkFailure");
            }
        }

        static OrchestrationRuntimeState ConstructRuntimeState(Protobuf.OrchestratorRequest request)
        {
            IEnumerable<HistoryEvent> pastEvents = request.PastEvents.Select(ConvertHistoryEvent);
            IEnumerable<HistoryEvent> newEvents = request.NewEvents.Select(ConvertHistoryEvent);

            // Reconstruct the orchestration state in a way that correctly distinguishes new events from past events
            var runtimeState = new OrchestrationRuntimeState(pastEvents.ToList());
            foreach (HistoryEvent e in newEvents)
            {
                // AddEvent() puts events into the NewEvents list.
                runtimeState.AddEvent(e);
            }

            if (runtimeState.ExecutionStartedEvent == null)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "The provided orchestration history was incomplete"));
            }

            return runtimeState;
        }

        static HistoryEvent ConvertHistoryEvent(Protobuf.HistoryEvent proto)
        {
            HistoryEvent historyEvent;
            switch (proto.EventTypeCase)
            {
                case Protobuf.HistoryEvent.EventTypeOneofCase.ContinueAsNew:
                    historyEvent = new ContinueAsNewEvent(proto.EventId, proto.ContinueAsNew.Input);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.ExecutionStarted:
                    historyEvent = new ExecutionStartedEvent(proto.EventId, proto.ExecutionStarted.Input)
                    {
                        Name = proto.ExecutionStarted.Name,
                        Version = proto.ExecutionStarted.Version,
                        OrchestrationInstance = ConvertOrchestrationInstance(proto.ExecutionStarted.OrchestrationInstance),
                        ParentInstance = null /* TODO: need to update protobuf schema */,
                        Correlation = null /* TODO */,
                        ScheduledStartTime = null /* TODO */,
                    };
                    break;
                ////case Protobuf.HistoryEvent.EventTypeOneofCase.ExecutionCompleted:
                ////    break;
                ////case Protobuf.HistoryEvent.EventTypeOneofCase.ExecutionFailed:
                ////    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.ExecutionTerminated:
                    historyEvent = new ExecutionTerminatedEvent(proto.EventId, proto.ExecutionTerminated.Input);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.TaskScheduled:
                    historyEvent = new TaskScheduledEvent(
                        proto.EventId,
                        proto.TaskScheduled.Name,
                        proto.TaskScheduled.Version,
                        proto.TaskScheduled.Input);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.TaskCompleted:
                    historyEvent = new TaskCompletedEvent(
                        proto.EventId,
                        proto.TaskCompleted.TaskScheduledId,
                        proto.TaskCompleted.Result);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.TaskFailed:
                    historyEvent = new TaskFailedEvent(
                        proto.EventId,
                        proto.TaskFailed.TaskScheduledId,
                        proto.TaskFailed.Reason,
                        proto.TaskFailed.Details);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceCreated:
                    historyEvent = new SubOrchestrationInstanceCreatedEvent(proto.EventId)
                    {
                        Input = proto.SubOrchestrationInstanceCreated.Input,
                        InstanceId = proto.SubOrchestrationInstanceCreated.InstanceId,
                        Name = proto.SubOrchestrationInstanceCreated.Name,
                        Version = proto.SubOrchestrationInstanceCreated.Version,
                    };
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceCompleted:
                    historyEvent = new SubOrchestrationInstanceCompletedEvent(
                        proto.EventId,
                        proto.SubOrchestrationInstanceCompleted.TaskScheduledId,
                        proto.SubOrchestrationInstanceCompleted.Result);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.SubOrchestrationInstanceFailed:
                    historyEvent = new SubOrchestrationInstanceFailedEvent(
                        proto.EventId,
                        proto.SubOrchestrationInstanceFailed.TaskScheduledId,
                        proto.SubOrchestrationInstanceFailed.Reason,
                        proto.SubOrchestrationInstanceFailed.Details);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.TimerCreated:
                    historyEvent = new TimerCreatedEvent(
                        proto.EventId,
                        ConvertTimestamp(proto.TimerCreated.FireAt));
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.TimerFired:
                    historyEvent = new TimerFiredEvent(
                        proto.TimerFired.TimerId,
                        ConvertTimestamp(proto.TimerFired.FireAt));
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.OrchestratorStarted:
                    historyEvent = new OrchestratorStartedEvent(proto.EventId);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.OrchestratorCompleted:
                    historyEvent = new OrchestratorCompletedEvent(proto.EventId);
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.EventSent:
                    historyEvent = new EventSentEvent(proto.EventId)
                    {
                        InstanceId = proto.EventSent.InstanceId,
                        Name = proto.EventSent.Name,
                        Input = proto.EventSent.Input,
                    };
                    break;
                case Protobuf.HistoryEvent.EventTypeOneofCase.EventRaised:
                    historyEvent = new EventRaisedEvent(proto.EventId, proto.EventRaised.Input)
                    {
                        Name = proto.EventRaised.Name,
                    };
                    break;
                ////case Protobuf.HistoryEvent.EventTypeOneofCase.GenericEvent:
                ////    break;
                ////case Protobuf.HistoryEvent.EventTypeOneofCase.HistoryState:
                ////    break;
                default:
                    throw new NotImplementedException($"Deserialization of {proto.EventTypeCase} is not implemented.");
            }

            historyEvent.Timestamp = ConvertTimestamp(proto.Timestamp);
            return historyEvent;
        }

        static DateTime ConvertTimestamp(Timestamp ts) => ts.ToDateTime();

        static Timestamp ConvertTimestamp(DateTime dateTime)
        {
            // The protobuf libraries require timestamps to be in UTC
            if (dateTime.Kind == DateTimeKind.Unspecified)
            {
                dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
            }
            else if (dateTime.Kind == DateTimeKind.Local)
            {
                dateTime = dateTime.ToUniversalTime();
            }

            return Timestamp.FromDateTime(dateTime);
        }

        static Protobuf.OrchestratorResponse ConstructOrchestratorResponse(IEnumerable<OrchestratorAction> actions)
        {
            var response = new Protobuf.OrchestratorResponse();

            foreach (OrchestratorAction action in actions)
            {
                var protoAction = new Protobuf.OrchestratorAction { Id = action.Id };

                switch (action.OrchestratorActionType)
                {
                    case OrchestratorActionType.ScheduleOrchestrator:
                        var scheduleTaskAction = (ScheduleTaskOrchestratorAction)action;
                        protoAction.ScheduleTask = new Protobuf.ScheduleTaskAction
                        {
                            Name = scheduleTaskAction.Name,
                            Version = scheduleTaskAction.Version,
                            Input = scheduleTaskAction.Input,
                        };
                        break;
                    case OrchestratorActionType.CreateSubOrchestration:
                        var subOrchestrationAction = (CreateSubOrchestrationAction)action;
                        protoAction.CreateSubOrchestration = new Protobuf.CreateSubOrchestrationAction
                        {
                            Input = subOrchestrationAction.Input,
                            InstanceId = subOrchestrationAction.InstanceId,
                            Name = subOrchestrationAction.Name,
                            Version = subOrchestrationAction.Version,
                        };
                        break;
                    case OrchestratorActionType.CreateTimer:
                        var createTimerAction = (CreateTimerOrchestratorAction)action;
                        protoAction.CreateTimer = new Protobuf.CreateTimerAction
                        {
                            FireAt = ConvertTimestamp(createTimerAction.FireAt),
                        };
                        break;
                    case OrchestratorActionType.SendEvent:
                        var sendEventAction = (SendEventOrchestratorAction)action;
                        protoAction.SendEvent = new Protobuf.SendEventAction
                        {
                            Instance = ConvertOrchestrationInstance(sendEventAction.Instance),
                            Name = sendEventAction.EventName,
                            Data = sendEventAction.EventData,
                        };
                        break;
                    case OrchestratorActionType.OrchestrationComplete:
                        var completeAction = (OrchestrationCompleteOrchestratorAction)action;
                        protoAction.CompleteOrchestration = new Protobuf.CompleteOrchestrationAction
                        {
                            CarryoverEvents =
                            {
                                completeAction.CarryoverEvents.Select(ConversionFunctions.ToHistoryEventProto)
                            },
                            Details = completeAction.Details,
                            NewVersion = completeAction.NewVersion,
                            OrchestrationStatus = (Protobuf.OrchestrationStatus)completeAction.OrchestrationStatus,
                            Result = completeAction.Result,
                        };
                        break;
                    default:
                        throw new NotImplementedException($"Unknown orchestrator action: {action.OrchestratorActionType}");
                }

                response.Actions.Add(protoAction);
            }

            return response;
        }

        [return: NotNullIfNotNull("instanceProto")]
        static OrchestrationInstance? ConvertOrchestrationInstance(Protobuf.OrchestrationInstance? instanceProto)
        {
            if (instanceProto == null)
            {
                return null;
            }

            return new OrchestrationInstance
            {
                InstanceId = instanceProto.InstanceId,
                ExecutionId = instanceProto.ExecutionId,
            };
        }

        static Protobuf.OrchestrationInstance ConvertOrchestrationInstance(OrchestrationInstance instance)
        {
            return new Protobuf.OrchestrationInstance
            {
                InstanceId = instance.InstanceId,
                ExecutionId = instance.ExecutionId,
            };
        }
    }
}
