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
#nullable enable
namespace DurableTask.Core.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.ExceptionServices;
    using System.Reflection;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;

    /// <summary>
    ///     Helper class for logging/tracing
    /// </summary>
    public class TraceHelper
    {
        const string Source = "DurableTask";

        static readonly ActivitySource ActivityTraceSource = new ActivitySource(Source);

        internal static Activity? CreateActivityForNewOrchestration(ExecutionStartedEvent startEvent)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: CreateActivityName("create_orchestration", startEvent.Name, startEvent.Version),
                kind: ActivityKind.Producer);

            if (newActivity != null)
            {
                newActivity.SetTag("durabletask.type", "orchestration");
                newActivity.SetTag("durabletask.task.name", startEvent.Name);
                newActivity.SetTag("durabletask.task.version", startEvent.Version);
                newActivity.SetTag("durabletask.task.instance_id", startEvent.OrchestrationInstance.InstanceId);
                newActivity.SetTag("durabletask.task.execution_id", startEvent.OrchestrationInstance.ExecutionId);
                
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
        internal static Activity? StartTraceActivityForExecution(ExecutionStartedEvent? startEvent)
        {
            if (startEvent == null)
            {
                return null;
            }

            if (!startEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return null;
            }

            string activityName = CreateActivityName("orchestration", startEvent.Name, startEvent.Version);
            ActivityKind activityKind = ActivityKind.Server;
            DateTimeOffset startTime = startEvent.ParentTraceContext.ActivityStartTime ?? default;

            Activity? activity = ActivityTraceSource.StartActivity(
                name: activityName,
                kind: activityKind,
                parentContext: activityContext,
                startTime: startTime);

            if (activity == null)
            {
                return null;
            }

            activity.SetTag("durabletask.type", "orchestration");
            activity.SetTag("durabletask.task.name", startEvent.Name);
            activity.SetTag("durabletask.task.version", startEvent.Version);
            activity.SetTag("durabletask.task.instance_id", startEvent.OrchestrationInstance.InstanceId);

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
        internal static Activity? StartTraceActivityForTask(
            TaskScheduledEvent scheduledEvent,
            OrchestrationInstance instance)
        {
            if (!scheduledEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return null;
            }

            return ActivityTraceSource.StartActivity(
                name: CreateActivityName("activity", scheduledEvent.Name, scheduledEvent.Version),
                kind: ActivityKind.Client,
                parentContext: activityContext,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("durabletask.type", "activity"),
                    new("durabletask.task.name", scheduledEvent.Name),
                    new("durabletask.task.version", scheduledEvent.Version),
                    new("durabletask.task.instance_id", instance.InstanceId),
                    new("durabletask.task.task_id", scheduledEvent.EventId),
                });
        }

        internal static Activity? CreateTraceActivityForSubOrchestration(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent)
        {
            if (orchestrationInstance == null || createdEvent == null)
            {
                return null;
            }

            Activity? activity = ActivityTraceSource.StartActivity(
                name: CreateActivityName("orchestration", createdEvent.Name, createdEvent.Version),
                kind: ActivityKind.Client,
                startTime: createdEvent.Timestamp,
                parentContext: Activity.Current?.Context ?? default);

            if (activity == null)
            {
                return null;
            }

            activity.SetTag("durabletask.type", "orchestration");
            activity.SetTag("durabletask.task.name", createdEvent.Name);
            activity.SetTag("durabletask.task.version", createdEvent.Version);
            activity.SetTag("durabletask.task.instance_id", orchestrationInstance?.InstanceId);

            return activity;
        }

        internal static void EmitTraceActivityForSubOrchestrationCompleted(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent)
        {
            Activity? activity = CreateTraceActivityForSubOrchestration(orchestrationInstance, createdEvent);

            activity?.Dispose();
        }

        internal static void EmitTraceActivityForSubOrchestrationFailed(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent,
            SubOrchestrationInstanceFailedEvent? failedEvent,
            ErrorPropagationMode errorPropagationMode)
        {
            Activity? activity = CreateTraceActivityForSubOrchestration(orchestrationInstance, createdEvent);

            if (activity is null)
            {
                return;
            }

            if (failedEvent != null)
            {
                string statusDescription = "";
                if (errorPropagationMode == ErrorPropagationMode.SerializeExceptions)
                {
                    statusDescription = JsonDataConverter.Default.Deserialize<Exception>(failedEvent.Details).Message;
                }
                else if (errorPropagationMode == ErrorPropagationMode.UseFailureDetails)
                {
                    FailureDetails? failureDetails = failedEvent.FailureDetails;
                    if (failureDetails != null)
                    {
                        statusDescription = failureDetails.ErrorMessage;
                    }
                }

                activity?.SetStatus(ActivityStatusCode.Error, statusDescription);
            }

            activity?.Dispose();
        }

        internal static Activity? CreateActivityForNewEventRaised(EventRaisedEvent eventRaised, OrchestrationInstance instance)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: eventRaised.Name,
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("durabletask.type", "event"),
                    new("durabletask.task.instance_id", instance.InstanceId),
                    new("durabletask.task.execution_id", instance.ExecutionId),
                });

            return newActivity;
        }

        internal static void EmitTraceActivityForTimer(OrchestrationInstance? instance, DateTime startTime, TimerFiredEvent timerFiredEvent, string version)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: "timer",
                kind: ActivityKind.Internal,
                startTime: startTime,
                parentContext: Activity.Current?.Context ?? default);

            if (newActivity is not null)
            {
                newActivity.AddTag("durabletask.type", "timer");
                newActivity.AddTag("durabletask.fire_at", timerFiredEvent.FireAt.ToString("o"));
                newActivity.AddTag("durabletask.task.version", version);
                newActivity.AddTag("durabletask.task.instance_id", instance?.InstanceId);
                newActivity.AddTag("durabletask.task.task_id", timerFiredEvent.EventId);

                newActivity.Dispose();
            }
        }

        internal static Activity? CreateTraceActivityForTask(
            OrchestrationInstance? instance,
            TaskScheduledEvent taskScheduledEvent)
        {
            if (taskScheduledEvent == null)
            {
                return null;
            }

            if (!taskScheduledEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return null;
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: CreateActivityName("activity", taskScheduledEvent.Name, taskScheduledEvent.Version),
                kind: ActivityKind.Server,
                startTime: taskScheduledEvent.Timestamp,
                parentContext: activityContext);

            if (newActivity == null)
            {
                return null;
            }

            newActivity.AddTag("durabletask.type", "activity");
            newActivity.AddTag("durabletask.task.name", taskScheduledEvent.Name);
            newActivity.AddTag("durabletask.task.version", taskScheduledEvent.Version);
            newActivity.AddTag("durabletask.task.instance_id", instance?.InstanceId);
            newActivity.AddTag("durabletask.task_id", taskScheduledEvent.EventId);

            return newActivity;
        }

        internal static void EmitTraceActivityForTaskCompleted(
            OrchestrationInstance? orchestrationInstance,
            TaskScheduledEvent taskScheduledEvent)
        {
            Activity? activity = CreateTraceActivityForTask(orchestrationInstance, taskScheduledEvent);

            activity?.Dispose();
        }

        internal static void EmitTraceActivityForTaskFailed(
            OrchestrationInstance? orchestrationInstance,
            TaskScheduledEvent taskScheduledEvent,
            TaskFailedEvent? taskFailedEvent,
            ErrorPropagationMode errorPropagationMode)
        {
            Activity? activity = CreateTraceActivityForTask(orchestrationInstance, taskScheduledEvent);

            if (activity is null)
            {
                return;
            }

            if (taskFailedEvent != null)
            {
                string statusDescription = "";

                if (errorPropagationMode == ErrorPropagationMode.SerializeExceptions)
                {
                    statusDescription = JsonDataConverter.Default.Deserialize<Exception>(taskFailedEvent.Details).Message;
                }
                else if (errorPropagationMode == ErrorPropagationMode.UseFailureDetails)
                {
                    FailureDetails? failureDetails = taskFailedEvent.FailureDetails;
                    if (failureDetails != null)
                    {
                        statusDescription = failureDetails.ErrorMessage;
                    }
                }

                activity?.SetStatus(ActivityStatusCode.Error, statusDescription);
            }

            activity?.Dispose();
        }

        /// <summary>
        /// Starts a new trace activity for (task) activity execution.
        /// </summary>
        /// <param name="eventRaisedEvent">The associated <see cref="EventRaisedEvent"/>.</param>
        /// <param name="instance">The associated orchestration instance metadata.</param>
        /// <param name="targetInstanceId">The instance id of the orchestration instance that will receive the event.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForEventRaised(
            EventRaisedEvent eventRaisedEvent,
            OrchestrationInstance? instance,
            string? targetInstanceId)
        {
            return ActivityTraceSource.StartActivity(
                name: CreateActivityName("orchestration_event", eventRaisedEvent.Name, null),
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("durabletask.type", "event"),
                    new("durabletask.event.name", eventRaisedEvent.Name),
                    new("durabletask.event.target_instance_id", targetInstanceId),
                    new("durabletask.task.instance_id", instance?.InstanceId),
                    new("durabletask.task.execution_id", instance?.ExecutionId)
                });
        }

        private static string CreateActivityName(string spanDescription, string? taskName, string? taskVersion)
        {
            string activityName = $"{spanDescription}:{taskName}";

            if (!string.IsNullOrEmpty(taskVersion))
            {
                activityName += $"@({taskVersion})";
            }

            return activityName;
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
    }
}
