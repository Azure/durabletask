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

namespace DurableTask.Tracing
{
    using System;
    using System.Diagnostics;

    internal class TraceHelper
    {
        static readonly TraceSource source = new TraceSource("DurableTask");

        // simple tracing, no iid and eid
        public static void Trace(TraceEventType eventType, Func<string> generateMessage)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(null, null, generateMessage())));
            }
        }

        public static void Trace(TraceEventType eventType, string format, params object[] args)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(null, null, format, args)));
            }
        }

        // have iid but no eid
        public static void TraceSession(TraceEventType eventType, string sessionId, Func<string> generateMessage)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(sessionId, null, generateMessage())));
            }
        }

        public static void TraceSession(TraceEventType eventType, string sessionId, string format, params object[] args)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(sessionId, null, format, args)));
            }
        }

        // have both iid and eid
        public static void TraceInstance(TraceEventType eventType, OrchestrationInstance orchestrationInstance,
            string format, params object[] args)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () =>
                        source.TraceEvent(eventType, 0,
                            GetFormattedString(
                                orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                                orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                                format, args)));
            }
        }

        public static void TraceInstance(TraceEventType eventType, OrchestrationInstance orchestrationInstance,
            Func<string> generateMessage)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(
                        orchestrationInstance == null ? string.Empty : orchestrationInstance.InstanceId,
                        orchestrationInstance == null ? string.Empty : orchestrationInstance.ExecutionId,
                        generateMessage())));
            }
        }

        // simple exception tracing methods
        public static Exception TraceException(TraceEventType eventType, Exception exception)
        {
            return TraceException(eventType, exception, string.Empty);
        }

        public static Exception TraceException(TraceEventType eventType, Exception exception,
            Func<String> generateMessage)
        {
            return TraceExceptionCore(eventType, null, null, exception, generateMessage);
        }

        public static Exception TraceException(TraceEventType eventType, Exception exception, string format,
            params object[] args)
        {
            return TraceExceptionCore(eventType, null, null, exception, format, args);
        }

        // instance tracing methods
        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, string.Empty);
        }

        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, Func<string> generateMessage)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, generateMessage);
        }

        public static Exception TraceExceptionInstance(TraceEventType eventType,
            OrchestrationInstance orchestrationInstance, Exception exception, string format, params object[] args)
        {
            return TraceExceptionCore(eventType, orchestrationInstance.InstanceId, orchestrationInstance.ExecutionId,
                exception, format, args);
        }

        // session tracing methods -- execution id not available
        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception)
        {
            return TraceExceptionCore(eventType, sessionId, null, exception, string.Empty);
        }

        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception,
            Func<string> generateMessage)
        {
            return TraceExceptionCore(eventType, sessionId, null, exception, generateMessage);
        }

        public static Exception TraceExceptionSession(TraceEventType eventType, string sessionId, Exception exception,
            string format, params object[] args)
        {
            return TraceExceptionCore(eventType, sessionId, null, exception, format, args);
        }

        // helper methods
        static Exception TraceExceptionCore(TraceEventType eventType, string iid, string eid, Exception exception,
            string format, params object[] args)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                string newFormat = format + "\nException: " + exception.GetType() + " : " + exception.Message + "\n\t" +
                                   exception.StackTrace;
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(iid, eid, newFormat, args)));
            }
            return exception;
        }

        static Exception TraceExceptionCore(TraceEventType eventType, string iid, string eid, Exception exception,
            Func<string> generateMessage)
        {
            if (source.Switch.ShouldTrace(eventType))
            {
                string newFormat = generateMessage() + "\nException: " + exception.GetType() + " : " + exception.Message +
                                   "\n\t" + exception.StackTrace;
                ExceptionHandlingWrapper(
                    () => source.TraceEvent(eventType, 0, GetFormattedString(iid, eid, newFormat)));
            }
            return exception;
        }

        static string GetFormattedString(string iid, string eid, string message, params object[] args)
        {
            string formatted;
            if (!string.IsNullOrEmpty(iid))
            {
                if (!string.IsNullOrEmpty(eid))
                {
                    formatted = "iid=" + iid + ";eid=" + eid + ";msg=" + message;
                }
                else
                {
                    formatted = "iid=" + iid + ";msg=" + message;
                }
            }
            else
            {
                formatted = "msg=" + message;
            }

            if (args == null)
            {
                return formatted;
            }

            try
            {
                return string.Format(formatted, args);
            }
            catch (FormatException ex)
            {
                source.TraceEvent(TraceEventType.Error, 0,
                     "String FormatException for '{0}'. Args count: {1}. Exception: {2}", formatted, args.Length, ex);

                return formatted;
            }
        }

        static void ExceptionHandlingWrapper(Action innerFunc)
        {
            try
            {
                innerFunc();
            }
            catch (Exception exception)
            {
                if (Utils.IsFatal(exception))
                {
                    throw;
                }
                try
                {
                    source.TraceEvent(TraceEventType.Error, 0,
                        $"Failed to log actual trace because one or more trace listeners threw an exception: {exception}");
                }
                catch (Exception anotherException)
                {
                    if (Utils.IsFatal(anotherException))
                    {
                        throw;
                    }
                }
            }
        }
    }
}