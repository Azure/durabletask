﻿//  ----------------------------------------------------------------------------------
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
#nullable enable
namespace DurableTask.Core.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.ExceptionServices;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using DurableTask.Core.Entities.EventFormat;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.OperationFormat;
    using System.Linq;

    /// <summary>
    ///     Helper class for logging/tracing
    /// </summary>
    public class TraceHelper
    {
        const string Source = "DurableTask.Core";

        static readonly ActivitySource ActivityTraceSource = new ActivitySource(Source);

        /// <summary>
        /// Starts a new trace activity for scheduling an orchestration from the client.
        /// </summary>
        /// <param name="startEvent">The orchestration's execution started event.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartActivityForNewOrchestration(ExecutionStartedEvent startEvent)
        {
            startEvent.TryGetParentTraceContext(out ActivityContext activityContext);
            DateTimeOffset? startTime = null;
            if (startEvent.Tags != null && startEvent.Tags.ContainsKey(OrchestrationTags.RequestTime) &&
                DateTimeOffset.TryParse(startEvent.Tags[OrchestrationTags.RequestTime], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTimeOffset requestTime))
            {
                startTime = requestTime;
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.CreateOrchestration, startEvent.Name, startEvent.Version),
                kind: ActivityKind.Producer,
                parentContext: activityContext,
                startTime: startTime ?? DateTimeOffset.UtcNow);

            if (newActivity != null)
            {
                newActivity.SetTag(Schema.Task.Type, TraceActivityConstants.Orchestration);
                newActivity.SetTag(Schema.Task.Name, startEvent.Name);
                newActivity.SetTag(Schema.Task.InstanceId, startEvent.OrchestrationInstance.InstanceId);
                newActivity.SetTag(Schema.Task.ExecutionId, startEvent.OrchestrationInstance.ExecutionId);

                if (!string.IsNullOrEmpty(startEvent.Version))
                {
                    newActivity.SetTag(Schema.Task.Version, startEvent.Version);
                }


                startEvent.SetParentTraceContext(newActivity);
            }

            return newActivity;
        }

        /// <summary>
        /// Starts a new trace activity for orchestration execution.
        /// </summary>
        /// <param name="startEvent">The orchestration's execution started event.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForOrchestrationExecution(ExecutionStartedEvent? startEvent)
        {
            if (startEvent == null)
            {
                return null;
            }

            if (startEvent.Tags != null && startEvent.Tags.ContainsKey(OrchestrationTags.CreateTraceForNewOrchestration))
            {
                startEvent.Tags.Remove(OrchestrationTags.CreateTraceForNewOrchestration);
                // This immediately disposes of and ends the activity for the new orchestration. Note that in this case since the start time of activity is set to the time the 
                // the request for a new orchestration was made, but the Activity is only disposed of now, the duration of the Activity will be longer than if it was created and stopped immediately after the request creation.
                using var activityForNewOrchestration = StartActivityForNewOrchestration(startEvent);
            }

            if (!startEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return null;
            }

            string activityName = CreateSpanName(TraceActivityConstants.Orchestration, startEvent.Name, startEvent.Version);
            ActivityKind activityKind = ActivityKind.Server;
            DateTimeOffset startTime = startEvent.ParentTraceContext.ActivityStartTime ?? default;

            Activity? activity = ActivityTraceSource.StartActivity(
                activityName,
                kind: activityKind,
                parentContext: activityContext,
                startTime: startTime);

            if (activity == null)
            {
                return null;
            }

            activity.SetTag(Schema.Task.Type, TraceActivityConstants.Orchestration);
            activity.SetTag(Schema.Task.Name, startEvent.Name);
            activity.SetTag(Schema.Task.InstanceId, startEvent.OrchestrationInstance.InstanceId);

            if (!string.IsNullOrEmpty(startEvent.Version))
            {
                activity.SetTag(Schema.Task.Version, startEvent.Version);
            }

            if (startEvent.ParentTraceContext.Id != null && startEvent.ParentTraceContext.SpanId != null)
            {
                activity.SetId(startEvent.ParentTraceContext.Id!);
                activity.SetSpanId(startEvent.ParentTraceContext.SpanId!);
            }
            else
            {
                startEvent.ParentTraceContext.Id = activity.Id;
                startEvent.ParentTraceContext.SpanId = activity.SpanId.ToString();
                startEvent.ParentTraceContext.ActivityStartTime = activity.StartTimeUtc;
            }

            DistributedTraceActivity.Current = activity;
            return DistributedTraceActivity.Current;
        }


        /// <summary>
        /// Starts a new trace activity for (task) activity execution. 
        /// </summary>
        /// <param name="scheduledEvent">The associated <see cref="TaskScheduledEvent"/>.</param>
        /// <param name="instance">The associated orchestration instance metadata.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForTaskExecution(
            TaskScheduledEvent scheduledEvent,
            OrchestrationInstance instance)
        {
            if (!scheduledEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return null;
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.Activity, scheduledEvent.Name, scheduledEvent.Version),
                kind: ActivityKind.Server,
                parentContext: activityContext);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.SetTag(Schema.Task.Type, TraceActivityConstants.Activity);
            newActivity.SetTag(Schema.Task.Name, scheduledEvent.Name);
            newActivity.SetTag(Schema.Task.InstanceId, instance.InstanceId);
            newActivity.SetTag(Schema.Task.TaskId, scheduledEvent.EventId);

            if (!string.IsNullOrEmpty(scheduledEvent.Version))
            {
                newActivity.SetTag(Schema.Task.Version, scheduledEvent.Version);
            }

            return newActivity;
        }

        /// <summary>
        /// Starts a new trace activity for (task) activity that represents the time between when the task message
        /// is enqueued and when the response message is received.
        /// </summary>
        /// <param name="instance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="taskScheduledEvent">The associated <see cref="TaskScheduledEvent"/>.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForSchedulingTask(
            OrchestrationInstance? instance,
            TaskScheduledEvent taskScheduledEvent)
        {
            if (taskScheduledEvent == null)
            {
                return null;
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.Activity, taskScheduledEvent.Name, taskScheduledEvent.Version),
                kind: ActivityKind.Client,
                startTime: taskScheduledEvent.Timestamp,
                parentContext: Activity.Current?.Context ?? default);

            if (newActivity == null)
            {
                return null;
            }

            if (taskScheduledEvent.ParentTraceContext != null)
            {
                if (ActivityContext.TryParse(taskScheduledEvent.ParentTraceContext.TraceParent, taskScheduledEvent.ParentTraceContext?.TraceState, out ActivityContext parentContext))
                {
                    newActivity.SetSpanId(parentContext.SpanId.ToString());
                }
            }

            newActivity.AddTag(Schema.Task.Type, TraceActivityConstants.Activity);
            newActivity.AddTag(Schema.Task.Name, taskScheduledEvent.Name);
            newActivity.AddTag(Schema.Task.InstanceId, instance?.InstanceId);
            newActivity.AddTag(Schema.Task.TaskId, taskScheduledEvent.EventId);

            if (!string.IsNullOrEmpty(taskScheduledEvent.Version))
            {
                newActivity.AddTag(Schema.Task.Version, taskScheduledEvent.Version);
            }

            return newActivity;
        }

        /// <summary>
        /// Emits a new trace activity for a (task) activity that successfully completes.
        /// </summary>
        /// <param name="taskScheduledEvent">The associated <see cref="TaskScheduledEvent"/>.</param>
        /// <param name="orchestrationInstance">The associated <see cref="OrchestrationInstance"/>.</param>
        internal static void EmitTraceActivityForTaskCompleted(
            OrchestrationInstance? orchestrationInstance,
            TaskScheduledEvent taskScheduledEvent)
        {
            // The parent of this is the parent orchestration span ID. It should be the client span which started this
            Activity? activity = StartTraceActivityForSchedulingTask(orchestrationInstance, taskScheduledEvent);

            activity?.Dispose();
        }

        /// <summary>
        /// Emits a new trace activity for a (task) activity that fails.
        /// </summary>
        /// <param name="orchestrationInstance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="taskScheduledEvent">The associated <see cref="TaskScheduledEvent"/>.</param>
        /// <param name="failedEvent">The associated <see cref="TaskFailedEvent"/>.</param>
        /// <param name="errorPropagationMode">Specifies the method to propagate unhandled exceptions to parent orchestrations.</param>
        internal static void EmitTraceActivityForTaskFailed(
            OrchestrationInstance? orchestrationInstance,
            TaskScheduledEvent taskScheduledEvent,
            TaskFailedEvent? failedEvent,
            ErrorPropagationMode errorPropagationMode)
        {
            Activity? activity = StartTraceActivityForSchedulingTask(orchestrationInstance, taskScheduledEvent);

            if (activity is null)
            {
                return;
            }

            if (failedEvent != null)
            {
                string statusDescription = failedEvent.Reason ?? "Unspecified task activity failure";
                activity?.SetStatus(ActivityStatusCode.Error, statusDescription);
            }

            activity?.Dispose();
        }

        /// <summary>
        /// Starts a new trace activity for sub-orchestrations. Represents the time between enqueuing
        /// the sub-orchestration message and it completing.
        /// </summary>
        /// <param name="orchestrationInstance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="createdEvent">The associated <see cref="SubOrchestrationInstanceCreatedEvent"/>.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? CreateTraceActivityForSchedulingSubOrchestration(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent)
        {
            if (orchestrationInstance == null || createdEvent == null)
            {
                return null;
            }

            Activity? activity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.Orchestration, createdEvent.Name, createdEvent.Version),
                kind: ActivityKind.Client,
                startTime: createdEvent.Timestamp,
                parentContext: Activity.Current?.Context ?? default);

            if (activity == null)
            {
                return null;
            }

            activity.SetSpanId(createdEvent.ClientSpanId);

            activity.SetTag(Schema.Task.Type, TraceActivityConstants.Orchestration);
            activity.SetTag(Schema.Task.Name, createdEvent.Name);
            activity.SetTag(Schema.Task.InstanceId, orchestrationInstance?.InstanceId);

            if (!string.IsNullOrEmpty(createdEvent.Version))
            {
                activity.SetTag(Schema.Task.Version, createdEvent.Version);
            }

            return activity;
        }

        /// <summary>
        /// Emits a new trace activity for sub-orchestration execution when the sub-orchestration
        /// completes successfully.
        /// </summary>
        /// <param name="orchestrationInstance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="createdEvent">The associated <see cref="SubOrchestrationInstanceCreatedEvent"/>.</param>
        internal static void EmitTraceActivityForSubOrchestrationCompleted(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent)
        {
            // The parent of this is the parent orchestration span ID. It should be the client span which started this
            Activity? activity = CreateTraceActivityForSchedulingSubOrchestration(orchestrationInstance, createdEvent);

            activity?.Dispose();
        }

        /// <summary>
        /// Emits a new trace activity for sub-orchestration execution when the sub-orchestration fails.
        /// </summary>
        /// <param name="orchestrationInstance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="createdEvent">The associated <see cref="SubOrchestrationInstanceCreatedEvent"/>.</param>
        /// <param name="failedEvent">The associated <see cref="SubOrchestrationInstanceCreatedEvent"/>.</param>
        /// <param name="errorPropagationMode">Specifies the method to propagate unhandled exceptions to parent orchestrations.</param>
        internal static void EmitTraceActivityForSubOrchestrationFailed(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent,
            SubOrchestrationInstanceFailedEvent? failedEvent,
            ErrorPropagationMode errorPropagationMode)
        {
            Activity? activity = CreateTraceActivityForSchedulingSubOrchestration(orchestrationInstance, createdEvent);

            if (activity is null)
            {
                return;
            }

            if (failedEvent != null)
            {
                string statusDescription = failedEvent.Reason ?? "Unspecified sub-orchestration failure";
                activity?.SetStatus(ActivityStatusCode.Error, statusDescription);
            }

            activity?.Dispose();
        }

        /// <summary>
        /// Emits a new trace activity for events created from the worker.
        /// </summary>
        /// <param name="eventRaisedEvent">The associated <see cref="EventRaisedEvent"/>.</param>
        /// <param name="instance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="targetInstanceId">The instance id of the orchestration that will receive the event.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForEventRaisedFromWorker(
            EventRaisedEvent eventRaisedEvent,
            OrchestrationInstance? instance,
            string? targetInstanceId)
        {
            // There is a possibility that we mislabel the event as an entity event if entities are not enabled
            if (Entities.IsEntityInstance(targetInstanceId ?? string.Empty))
            {
                return TryParseEntityRequest(eventRaisedEvent, targetInstanceId!);
            }
            else if (Entities.IsEntityInstance(instance?.InstanceId ?? string.Empty))
            {
                return TryParseEntityResponse(eventRaisedEvent, instance?.InstanceId!);
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.OrchestrationEvent, eventRaisedEvent.Name, null),
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.AddTag(Schema.Task.Type, TraceActivityConstants.Event);
            newActivity.AddTag(Schema.Task.Name, eventRaisedEvent.Name);
            newActivity.AddTag(Schema.Task.InstanceId, instance?.InstanceId);
            newActivity.AddTag(Schema.Task.ExecutionId, instance?.ExecutionId);

            if (!string.IsNullOrEmpty(targetInstanceId))
            {
                newActivity.AddTag(Schema.Task.EventTargetInstanceId, targetInstanceId);
            }

            return newActivity;
        }

        /// <summary>
        /// Creates a new trace activity for events created from the client.
        /// </summary>
        /// <param name="eventRaised">The associated <see cref="EventRaisedEvent"/>.</param>
        /// <param name="instance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartActivityForNewEventRaisedFromClient(EventRaisedEvent eventRaised, OrchestrationInstance instance)
        {
            // There is a possibility that we mislabel the event as an entity event if entities are not enabled
            if (Entities.IsEntityInstance(instance.InstanceId))
            {
                return TryParseEntityRequest(eventRaised, instance.InstanceId);
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(TraceActivityConstants.OrchestrationEvent, eventRaised.Name, null),
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default,
                tags: new KeyValuePair<string, object?>[]
                {
                    new(Schema.Task.Type, TraceActivityConstants.Event),
                    new(Schema.Task.Name, eventRaised.Name),
                    new(Schema.Task.EventTargetInstanceId, instance.InstanceId),
                });

            return newActivity;
        }

        /// <summary>
        /// Emits a new trace activity for timers.
        /// </summary>
        /// <param name="instance">The associated <see cref="OrchestrationInstance"/>.</param>
        /// <param name="orchestrationName">The name of the orchestration invoking the timer.</param>
        /// <param name="startTime">The timer's start time.</param>
        /// <param name="timerFiredEvent">The associated <see cref="TimerFiredEvent"/>.</param>
        internal static void EmitTraceActivityForTimer(
            OrchestrationInstance? instance,
            string orchestrationName,
            DateTime startTime,
            TimerFiredEvent timerFiredEvent)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateTimerSpanName(orchestrationName),
                kind: ActivityKind.Internal,
                startTime: startTime,
                parentContext: Activity.Current?.Context ?? default);

            if (newActivity is not null)
            {
                newActivity.AddTag(Schema.Task.Type, TraceActivityConstants.Timer);
                newActivity.AddTag(Schema.Task.Name, orchestrationName);
                newActivity.AddTag(Schema.Task.InstanceId, instance?.InstanceId);
                newActivity.AddTag(Schema.Task.FireAt, timerFiredEvent.FireAt.ToString("o"));
                newActivity.AddTag(Schema.Task.TaskId, timerFiredEvent.TimerId);

                newActivity.Dispose();
            }
        }

        internal static Activity? StartActivityForCallingOrSignalingEntity(string targetEntityId, string entityName, string operationName, bool signalEntity, DateTime? scheduledTime, ActivityContext parentTraceContext, DateTimeOffset? startTime, string? entityId = null)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateEntitySpanName(entityName, operationName),
                kind: signalEntity ? ActivityKind.Producer : ActivityKind.Client,
                parentContext: parentTraceContext,
                startTime: startTime ?? DateTimeOffset.UtcNow);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.SetTag(Schema.Entity.Type, TraceActivityConstants.Entity);
            newActivity.SetTag(Schema.Entity.EntityOperation, signalEntity ? TraceActivityConstants.SignalEntity : TraceActivityConstants.CallEntity);
            newActivity.SetTag(Schema.Entity.TargetEntityId, targetEntityId);

            if (!string.IsNullOrEmpty(entityId))
            {
                newActivity.SetTag(Schema.Entity.EntityId, entityId);
            }

            if (scheduledTime != null)
            {
                newActivity.SetTag(Schema.Entity.ScheduledTime, scheduledTime.Value.ToString());
            }

            return newActivity;
        }

        internal static Activity? StartActivityForEntityStartingAnOrchestration(string entityId, string entityName, string targetInstanceId, ActivityContext parentTraceContext, DateTimeOffset? startTime, DateTime? scheduledTime = null)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateSpanName(entityName, TraceActivityConstants.CreateOrchestration, null),
                kind: ActivityKind.Producer,
                parentContext: parentTraceContext,
                startTime: startTime ?? default);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.SetTag(Schema.Entity.Type, TraceActivityConstants.Entity);
            newActivity.SetTag(Schema.Entity.TargetInstanceId, targetInstanceId);
            newActivity.SetTag(Schema.Entity.EntityId, entityId);

            if (scheduledTime != null)
            {
                newActivity.SetTag(Schema.Entity.ScheduledTime, scheduledTime.Value.ToString());
            }

            return newActivity;
        }

        internal static Activity? StartActivityForProcessingEntityInvocation(string entityId, string entityName, string operationName, bool signalEntity, ActivityContext? parentTraceContext)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                CreateEntitySpanName(entityName, operationName),
                kind: signalEntity ? ActivityKind.Consumer : ActivityKind.Server,
                parentContext: parentTraceContext ?? default);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.SetTag(Schema.Entity.Type, TraceActivityConstants.Entity);
            newActivity.SetTag(Schema.Entity.EntityOperation, signalEntity ? TraceActivityConstants.SignalEntity : TraceActivityConstants.CallEntity);
            newActivity.SetTag(Schema.Entity.EntityId, entityId);

            return newActivity;
        }

        internal static void EndActivitiesForProcessingEntityInvocation(List<Activity> traceActivities, List<OperationResult> results, FailureDetails? batchFailureDetails)
        {
            if (results.Count == traceActivities.Count)
            {
                foreach (var (activity, result) in traceActivities.Zip(results, (activity, result) => (activity, result)))
                {
                    if (activity != null)
                    {
                        if (result.ErrorMessage != null || result.FailureDetails != null)
                        {
                            activity.SetTag(Schema.Entity.ErrorMessage, result.ErrorMessage ?? result.FailureDetails!.ErrorMessage);
                        }
                        if (result.EndTime is DateTime endTime)
                        {
                            activity.SetEndTime(endTime);
                        }
                        activity.Dispose();
                    }
                }
            }
            // This can happen if some of the operations failed and have no corresponding OperationResult
            // There is no way to map the successful operation results to the corresponding operation requests or trace activities, so we will just "fail" the trace activities in this case and dispose them
            else
            {
                string errorMessage = "Unable to generate a trace activity for the entity invocation even though it may have succeeded.";
                if (batchFailureDetails is FailureDetails failureDetails)
                {
                    errorMessage += $" If it failed, it may be due to {failureDetails.ErrorMessage}";
                }
                foreach (var activity in traceActivities)
                {
                    if (activity != null)
                    {
                        activity.SetTag(Schema.Entity.ErrorMessage, errorMessage);
                        activity.Dispose();
                    }
                }
            }
        }

        internal static void SetRuntimeStatusTag(string runtimeStatus)
        {
            DistributedTraceActivity.Current?.SetTag(Schema.Task.Status, runtimeStatus);
        }

        internal static void AddErrorDetailsToSpan(Activity? activity, Exception e)
        {
            activity?.SetStatus(ActivityStatusCode.Error, e.Message.ToString());
        }

        static string CreateSpanName(string spanDescription, string? taskName, string? taskVersion)
        {
            if (!string.IsNullOrEmpty(taskVersion))
            {
                return $"{spanDescription}:{taskName}@({taskVersion})";
            }
            else
            {
                return $"{spanDescription}:{taskName}";
            }
        }

        static string CreateTimerSpanName(string orchestrationName)
        {
            return $"{TraceActivityConstants.Orchestration}:{orchestrationName}:{TraceActivityConstants.Timer}";
        }

        static string CreateEntitySpanName(string entityName, string operationName)
        {
            return $"{TraceActivityConstants.Entity}:{entityName}:{operationName}";
        }

        /// <summary>
        ///     Simple trace with no iid or eid
        /// </summary>
        public static void Trace(TraceEventType eventLevel, string eventType, Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, string.Empty, string.Empty, string.Empty, generateMessage(), eventType));
        }

        /// <summary>
        ///     Simple trace with no iid or eid
        /// </summary>
        public static void Trace(TraceEventType eventLevel, string eventType, string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, string.Empty, string.Empty, string.Empty, FormatString(format, args), eventType));
        }

        /// <summary>
        ///     Trace with iid but no eid
        /// </summary>
        public static void TraceSession(TraceEventType eventLevel, string eventType, string sessionId, Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, string.Empty, string.Empty, sessionId, generateMessage(), eventType));
        }

        /// <summary>
        ///     Trace with iid but no eid
        /// </summary>
        public static void TraceSession(TraceEventType eventLevel, string eventType, string sessionId, string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, string.Empty, string.Empty, sessionId, FormatString(format, args), eventType));
        }

        /// <summary>
        ///     Trace with iid and eid
        /// </summary>
        public static void TraceInstance(TraceEventType eventLevel, string eventType, OrchestrationInstance orchestrationInstance,
            string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(
                    eventLevel,
                    Source,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                    string.Empty,
                    FormatString(format, args),
                    eventType));
        }

        /// <summary>
        ///     Trace with iid and eid
        /// </summary>
        public static void TraceInstance(TraceEventType eventLevel, string eventType, OrchestrationInstance orchestrationInstance,
            Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(
                    eventLevel,
                    Source,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                    string.Empty,
                    generateMessage(),
                    eventType));
        }

        /// <summary>
        ///     Trace an exception
        /// </summary>
        public static Exception TraceException(TraceEventType eventLevel, string eventType, Exception exception)
        {
            return TraceException(eventLevel, eventType, exception, string.Empty);
        }

        /// <summary>
        ///     Trace an exception and message
        /// </summary>
        public static Exception TraceException(TraceEventType eventLevel, string eventType, Exception exception,
            Func<string> generateMessage)
        {
            return TraceExceptionCore(eventLevel, eventType, string.Empty, string.Empty, exception, generateMessage);
        }

        /// <summary>
        ///     Trace an exception and message
        /// </summary>
        public static Exception TraceException(TraceEventType eventLevel, string eventType, Exception exception, string format,
            params object[] args)
        {
            return TraceExceptionCore(eventLevel, eventType, string.Empty, string.Empty, ExceptionDispatchInfo.Capture(exception), format, args).SourceException;
        }

        /// <summary>
        ///     Trace an instance exception
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventLevel, string eventType,
            OrchestrationInstance orchestrationInstance, Exception exception)
        {
            return TraceExceptionCore(eventLevel, eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                ExceptionDispatchInfo.Capture(exception), string.Empty).SourceException;
        }

        /// <summary>
        ///     Trace an instance exception and message
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventLevel, string eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, Func<string> generateMessage)
        {
            return TraceExceptionCore(eventLevel, eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, generateMessage);
        }

        /// <summary>
        ///     Trace an instance exception and message
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventLevel, string eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, string format, params object[] args)
        {
            return TraceExceptionCore(eventLevel, eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                ExceptionDispatchInfo.Capture(exception), format, args).SourceException;
        }

        /// <summary>
        ///     Trace a session exception without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventLevel, string eventType, string sessionId, Exception exception)
        {
            return TraceExceptionCore(eventLevel, eventType, sessionId, string.Empty, ExceptionDispatchInfo.Capture(exception), string.Empty).SourceException;
        }

        /// <summary>
        ///     Trace a session exception without execution id
        /// </summary>
        public static ExceptionDispatchInfo TraceExceptionSession(TraceEventType eventLevel, string eventType, string sessionId, ExceptionDispatchInfo exceptionDispatchInfo)
        {
            return TraceExceptionCore(eventLevel, eventType, sessionId, string.Empty, exceptionDispatchInfo, string.Empty);
        }

        /// <summary>
        ///     Trace a session exception and message without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventLevel, string eventType, string sessionId, Exception exception,
            Func<string> generateMessage)
        {
            return TraceExceptionCore(eventLevel, eventType, sessionId, string.Empty, exception, generateMessage);
        }

        /// <summary>
        ///     Trace a session exception and message without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventLevel, string eventType, string sessionId, Exception exception,
            string format, params object[] args)
        {
            return TraceExceptionCore(eventLevel, eventType, sessionId, string.Empty, ExceptionDispatchInfo.Capture(exception), format, args).SourceException;
        }

        /// <summary>
        ///     Trace a session exception and message without execution id
        /// </summary>
        public static ExceptionDispatchInfo TraceExceptionSession(TraceEventType eventLevel, string eventType, string sessionId, ExceptionDispatchInfo exceptionDispatchInfo,
            string format, params object[] args)
        {
            return TraceExceptionCore(eventLevel, eventType, sessionId, string.Empty, exceptionDispatchInfo, format, args);
        }

        // helper methods
        static ExceptionDispatchInfo TraceExceptionCore(TraceEventType eventLevel, string eventType, string iid, string eid, ExceptionDispatchInfo exceptionDispatchInfo,
            string format, params object[] args)
        {
            Exception exception = exceptionDispatchInfo.SourceException;

            string newFormat = format + "\nException: " + exception.GetType() + " : " + exception.Message + "\n\t" +
                               exception.StackTrace + "\nInner Exception: " +
                               exception.InnerException?.ToString();

            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, iid, eid, string.Empty, FormatString(newFormat, args), eventType));

            return exceptionDispatchInfo;
        }

        static Exception TraceExceptionCore(TraceEventType eventLevel, string eventType, string iid, string eid, Exception exception,
            Func<string> generateMessage)
        {
            string newFormat = generateMessage() + "\nException: " + exception.GetType() + " : " + exception.Message +
                               "\n\t" + exception.StackTrace + "\nInner Exception: " +
                               exception.InnerException?.ToString();

            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventLevel, Source, iid, eid, string.Empty, newFormat, eventType));

            return exception;
        }

        static string FormatString(string formatted, params object[] args)
        {
            if (args == null || args.Length == 0)
            {
                return formatted;
            }

            try
            {
                return string.Format(CultureInfo.InvariantCulture, formatted, args);
            }
            catch (FormatException ex)
            {
                string message = string.Format(CultureInfo.InvariantCulture, "String FormatException for '{0}'. Args count: {1}. Exception: {2}", formatted, args.Length, ex);
                DefaultEventSource.Log.TraceEvent(TraceEventType.Error, Source, string.Empty, string.Empty, string.Empty, message, "LogFormattingFailed");

                return formatted;
            }
        }

        static void ExceptionHandlingWrapper(Action innerFunc)
        {
            try
            {
                innerFunc();
            }
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                try
                {
                    DefaultEventSource.Log.TraceEvent(TraceEventType.Error, Source, string.Empty, string.Empty, string.Empty, exception, "WriteEventFailed");
                }
                catch (Exception anotherException) when (!Utils.IsFatal(anotherException))
                {
                }
            }
        }

        private static Activity? TryParseEntityRequest(EventRaisedEvent raisedEvent, string targetInstanceId)
        {
            try
            {
                var requestMessage = JsonConvert.DeserializeObject<RequestMessage>(raisedEvent.Input, Serializer.InternalSerializerSettings);
                if (requestMessage == null || !requestMessage.CreateTrace)
                {
                    return null;
                }

                var parentTraceContext = Activity.Current?.Context;
                if (requestMessage.ParentTraceContext != null)
                {
                    if (ActivityContext.TryParse(requestMessage.ParentTraceContext.TraceParent, requestMessage.ParentTraceContext.TraceState, out ActivityContext activityContext))
                    {
                        parentTraceContext = activityContext;
                    }
                    // If a parent trace context was passed with the request message, this should be the parent of the current Activity, so if we cannot parse it we should not create the Activity
                    // or else it will be incorrectly linked.
                    else
                    {
                        parentTraceContext = null;
                    }
                }

                // We only want to create a trace activity for calling/signaling an entity in the case that we can successfully get the parent trace context of the request.
                // Otherwise, we will create an unlinked trace activity with no parent.
                if (parentTraceContext == null)
                {
                    return null;
                }

                Activity? newActivity = StartActivityForCallingOrSignalingEntity(
                    targetInstanceId!,
                    EntityId.FromString(targetInstanceId!).Name,
                    requestMessage.Operation!,
                    requestMessage.IsSignal,
                    requestMessage.ScheduledTime,
                    parentTraceContext.Value,
                    entityId: requestMessage.ParentInstanceId != null && Entities.IsEntityInstance(requestMessage.ParentInstanceId) ? requestMessage.ParentInstanceId : null,
                    startTime: requestMessage.RequestTime);

                if (!string.IsNullOrEmpty(newActivity?.Id))
                {
                    requestMessage.ParentTraceContext = new DistributedTraceContext(newActivity!.Id!, newActivity.TraceStateString);
                    raisedEvent.Input = JsonConvert.SerializeObject(requestMessage, Serializer.InternalSerializerSettings);
                }

                return newActivity;
            }
            catch (Exception)
            {
                return null;
            }
        }

        private static Activity? TryParseEntityResponse(EventRaisedEvent raisedEvent, string instanceId)
        {
            try
            {
                var responseMessage = JsonConvert.DeserializeObject<ResponseMessage>(raisedEvent.Input, Serializer.InternalSerializerSettings);
                if (responseMessage == null || responseMessage.RequestInfo == null)
                {
                    return null;
                }

                if (!ActivityContext.TryParse(responseMessage.RequestInfo.ParentTraceContext?.TraceParent, responseMessage.RequestInfo.ParentTraceContext?.TraceState, out ActivityContext parentTraceContext))
                {
                    return null;
                }

                Activity? newActivity = StartActivityForCallingOrSignalingEntity(
                    instanceId,
                    EntityId.FromString(instanceId).Name,
                    responseMessage.RequestInfo.Operation!,
                    signalEntity: false,
                    responseMessage.RequestInfo.ScheduledTime,
                    parentTraceContext,
                    startTime: responseMessage.RequestInfo.RequestTime);

                newActivity.SetSpanId(responseMessage.RequestInfo.ClientSpanId);

                return newActivity;
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}