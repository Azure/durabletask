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

        private static readonly Action<Activity, string> s_spanIdSet;
        private static readonly Action<Activity, string> s_idSet;

        static TraceHelper()
        {
            BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
            s_spanIdSet = typeof(Activity).GetField("_spanId", flags).CreateSetter<Activity, string>();
            s_idSet = typeof(Activity).GetField("_id", flags).CreateSetter<Activity, string>();
        }

        internal static Activity? CreateActivityForNewOrchestration(ExecutionStartedEvent startEvent)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: startEvent.Name,
                kind: ActivityKind.Internal);

            if (newActivity != null)
            {
                newActivity.SetTag("dtfx.type", "client");
                newActivity.SetTag("dtfx.instance_id", startEvent.OrchestrationInstance.InstanceId);
                newActivity.SetTag("dtfx.execution_id", startEvent.OrchestrationInstance.ExecutionId);
                
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

            string activityName = startEvent.Name;
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

            activity.SetTag("dtfx.type", "orchestrator");
            activity.SetTag("dtfx.instance_id", startEvent.OrchestrationInstance.InstanceId);
            activity.SetTag("dtfx.execution_id", startEvent.OrchestrationInstance.ExecutionId);

            if (startEvent.ParentTraceContext.Id != null && startEvent.ParentTraceContext.SpanId != null)
            {
                s_idSet(activity, startEvent.ParentTraceContext.Id!);
                s_spanIdSet(activity, startEvent.ParentTraceContext.SpanId!);
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
                name: $"{scheduledEvent.Name} (#{scheduledEvent.EventId})",
                kind: ActivityKind.Server,
                parentContext: activityContext,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("dtfx.type", "activity"),
                    new("dtfx.instance_id", instance.InstanceId),
                    new("dtfx.execution_id", instance.ExecutionId),
                    new("dtfx.task_id", scheduledEvent.EventId),
                });
        }

        /// <summary>
        /// Starts a new trace activity for suborchestration execution.
        /// </summary>
        /// <param name="orchestrationInstance">The associated orchestration instance metadata.</param>
        /// <param name="createdEvent">The related sub-orchestration created event.</param>
        /// <param name="failedEvent">The sub-orchestration failed event.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static void EmitTraceActivityForSubOrchestrationFinished(
            OrchestrationInstance? orchestrationInstance,
            SubOrchestrationInstanceCreatedEvent createdEvent,
            SubOrchestrationInstanceFailedEvent? failedEvent = null)
        {
            if (orchestrationInstance == null || createdEvent == null)
            {
                return;
            }

            Activity? activity = ActivityTraceSource.StartActivity(
                name: createdEvent.Name,
                kind: ActivityKind.Client,
                startTime: createdEvent.Timestamp,
                parentContext: Activity.Current?.Context ?? default);

            if (activity == null)
            {
                return;
            }

            activity.SetTag("dtfx.type", "orchestrator");
            activity.SetTag("dtfx.instance_id", orchestrationInstance?.InstanceId);
            activity.SetTag("dtfx.execution_id", orchestrationInstance?.ExecutionId);

            // Adding additional tags for a SubOrchestrationInstanceFailedEvent
            if (failedEvent != null)
            {
                activity.SetTag("otel.status_code", "ERROR");

                FailureDetails? failureDetails = failedEvent.FailureDetails;
                if (failureDetails != null)
                {
                    activity.SetTag("otel.status_description", failureDetails.ErrorMessage);
                }
            }

            activity.Dispose();
        }

        internal static Activity? CreateActivityForNewEventRaised(EventRaisedEvent eventRaised, OrchestrationInstance instance)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: eventRaised.Name,
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("dtfx.type", "externalevent"),
                    new("dtfx.instance_id", instance.InstanceId),
                    new("dtfx.execution_id", instance.ExecutionId),
                });

            return newActivity;
        }

        internal static void EmitTraceActivityForTimer(OrchestrationInstance? instance, DateTime startTime, DateTime fireAt)
        {
            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: "Timer",
                kind: ActivityKind.Internal,
                startTime: startTime,
                parentContext: Activity.Current?.Context ?? default);

            if (newActivity is not null)
            {
                newActivity.AddTag("dtfx.type", "timer");
                newActivity.AddTag("dtfx.instance_id", instance?.InstanceId);
                newActivity.AddTag("dtfx.execution_id", instance?.ExecutionId);
                newActivity.AddTag("dtfx.fire_at", fireAt.ToString("o"));

                newActivity.Dispose();
            }
        }

        internal static void EmitActivityforTaskFinished(OrchestrationInstance? instance, TaskScheduledEvent taskScheduledEvent, TaskFailedEvent? taskFailedEvent = null)
        {
            if (taskScheduledEvent == null)
            {
                return;
            }

            if (!taskScheduledEvent.TryGetParentTraceContext(out ActivityContext activityContext))
            {
                return;
            }

            Activity? newActivity = ActivityTraceSource.StartActivity(
                name: taskScheduledEvent.Name ?? "",
                kind: ActivityKind.Client,
                startTime: taskScheduledEvent.Timestamp,
                parentContext: activityContext);

            if (newActivity == null)
            {
                return;
            }

            newActivity.AddTag("dtfx.type", "activity");
            newActivity.AddTag("dtfx.instance_id", instance?.InstanceId);
            newActivity.AddTag("dtfx.execution_id", instance?.ExecutionId);

            if (taskFailedEvent != null)
            {
                newActivity.SetTag("otel.status_code", "ERROR");

                FailureDetails? failureDetails = taskFailedEvent.FailureDetails;
                if (failureDetails != null)
                {
                    newActivity.SetTag("otel.status_description", failureDetails.ErrorMessage);
                }
            }

            newActivity.Dispose();
        }

        /// <summary>
        /// Starts a new trace activity for (task) activity execution.
        /// </summary>
        /// <param name="eventRaisedEvent">The associated <see cref="EventRaisedEvent"/>.</param>
        /// <param name="instance">The associated orchestration instance metadata.</param>
        /// <returns>
        /// Returns a newly started <see cref="Activity"/> with (task) activity and orchestration-specific metadata.
        /// </returns>
        internal static Activity? StartTraceActivityForEventRaised(
            EventRaisedEvent eventRaisedEvent,
            OrchestrationInstance? instance)
        {
            return ActivityTraceSource.StartActivity(
                name: eventRaisedEvent.Name,
                kind: ActivityKind.Producer,
                parentContext: Activity.Current?.Context ?? default,
                tags: new KeyValuePair<string, object?>[]
                {
                    new("dtfx.type", "externalevent"),
                    new("dtfx.instance_id", instance?.InstanceId),
                    new("dtfx.execution_id", instance?.ExecutionId)
                });
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
