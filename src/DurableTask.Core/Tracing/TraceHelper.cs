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

namespace DurableTask.Core.Tracing
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using DurableTask.Core.Common;

    /// <summary>
    ///     Helper class for logging/tracing
    /// </summary>
    public class TraceHelper
    {
        const string Source = "DurableTask";

        /// <summary>
        ///     Simple trace with no iid or eid
        /// </summary>
        public static void Trace(TraceEventType eventType, Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, string.Empty, string.Empty, string.Empty, generateMessage()));
        }

        /// <summary>
        ///     Simple trace with no iid or eid
        /// </summary>
        public static void Trace(TraceEventType eventType, string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, string.Empty, string.Empty, string.Empty, FormatString(format, args)));
        }

        /// <summary>
        ///     Trace with iid but no eid
        /// </summary>
        public static void TraceSession(TraceEventType eventType, string sessionId, Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, string.Empty, string.Empty, sessionId, generateMessage()));
        }

        /// <summary>
        ///     Trace with iid but no eid
        /// </summary>
        public static void TraceSession(TraceEventType eventType, string sessionId, string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, string.Empty, string.Empty, sessionId, FormatString(format, args)));
        }

        /// <summary>
        ///     Trace with iid and eid
        /// </summary>
        public static void TraceInstance(TraceEventType eventType, OrchestrationInstance orchestrationInstance,
            string format, params object[] args)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(
                    eventType,
                    Source,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                    string.Empty,
                    FormatString(format, args)));
        }

        /// <summary>
        ///     Trace with iid and eid
        /// </summary>
        public static void TraceInstance(TraceEventType eventType, OrchestrationInstance orchestrationInstance,
            Func<string> generateMessage)
        {
            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(
                    eventType,
                    Source,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                    orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                    string.Empty,
                    generateMessage()));
        }

        /// <summary>
        ///     Trace an exception
        /// </summary>
        public static Exception TraceException(TraceEventType eventType, Exception exception)
        {
            return TraceException(eventType, exception, string.Empty);
        }

        /// <summary>
        ///     Trace an exception and message
        /// </summary>
        public static Exception TraceException(TraceEventType eventType, Exception exception,
            Func<String> generateMessage)
        {
            return TraceExceptionCore(eventType, string.Empty, string.Empty, exception, generateMessage);
        }

        /// <summary>
        ///     Trace an exception and message
        /// </summary>
        public static Exception TraceException(TraceEventType eventType, Exception exception, string format,
            params object[] args)
        {
            return TraceExceptionCore(eventType, string.Empty, string.Empty, exception, format, args);
        }

        /// <summary>
        ///     Trace an instance exception
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, string.Empty);
        }

        /// <summary>
        ///     Trace an instance exception and message
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, Func<string> generateMessage)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, generateMessage);
        }

        /// <summary>
        ///     Trace an instance exception and message
        /// </summary>
        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, string format, params object[] args)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, format, args);
        }

        /// <summary>
        ///     Trace a session exception without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception)
        {
            return TraceExceptionCore(eventType, sessionId, string.Empty, exception, string.Empty);
        }

        /// <summary>
        ///     Trace a session exception and message without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception,
            Func<string> generateMessage)
        {
            return TraceExceptionCore(eventType, sessionId, string.Empty, exception, generateMessage);
        }

        /// <summary>
        ///     Trace a session exception and message without execution id
        /// </summary>
        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception,
            string format, params object[] args)
        {
            return TraceExceptionCore(eventType, sessionId, string.Empty, exception, format, args);
        }

        // helper methods
        static Exception TraceExceptionCore(TraceEventType eventType, string iid, string eid, Exception exception,
            string format, params object[] args)
        {
            string newFormat = format + "\nException: " + exception.GetType() + " : " + exception.Message + "\n\t" +
                               exception.StackTrace + "\nInner Exception: " +
                               exception.InnerException?.ToString();

            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, iid, eid, string.Empty, FormatString(newFormat, args)));

            return exception;
        }

        static Exception TraceExceptionCore(TraceEventType eventType, string iid, string eid, Exception exception,
            Func<string> generateMessage)
        {
            string newFormat = generateMessage() + "\nException: " + exception.GetType() + " : " + exception.Message +
                               "\n\t" + exception.StackTrace + "\nInner Exception: " +
                               exception.InnerException?.ToString();

            ExceptionHandlingWrapper(
                () => DefaultEventSource.Log.TraceEvent(eventType, Source, iid, eid, string.Empty, newFormat));

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
                DefaultEventSource.Log.TraceEvent(TraceEventType.Error, Source, string.Empty, string.Empty, string.Empty, message);

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
                    DefaultEventSource.Log.TraceEvent(TraceEventType.Error, Source, string.Empty, string.Empty, string.Empty, exception);
                }
                catch (Exception anotherException) when (!Utils.IsFatal(anotherException))
                {
                }
            }
        }
    }
}