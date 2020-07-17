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

namespace DurableTask.Core.Logging
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Extension methods for the <see cref="ILoggerFactory"/> class.
    /// </summary>
    public static class LoggingExtensions
    {
        /// <summary>
        /// Writes an <see cref="ILogEvent"/> to the provider <see cref="ILogger"/> and 
        /// </summary>
        /// <param name="logger">The logger to write to.</param>
        /// <param name="logEvent">The event to be logged.</param>
        /// <param name="exception">Optional exception parameter for logging.</param>
        public static void LogDurableEvent(this ILogger logger, ILogEvent logEvent, Exception exception = null)
        {
            if (logger == null)
            {
                throw new ArgumentNullException(nameof(logger));
            }

            if (logEvent == null)
            {
                throw new ArgumentNullException(nameof(logEvent));
            }

            logger.Log(
                logEvent.Level,
                logEvent.EventId,
                logEvent,
                exception,
                formatter: (s, e) => s.GetLogMessage());

            if (logEvent is IEventSourceEvent eventSourceEvent)
            {
                // TODO: Uncomment this code when distributed tracing is integrated
                ////// If there is an Activity.Current, use it for the EventSource activity ID
                ////if (Activity.Current != null)
                ////{
                ////    Guid activityId = new Guid(Activity.Current.TraceId.ToString());
                ////    StructuredEventSource.SetLogicalTraceActivityId(activityId);
                ////}
                ////else
                ////{
                    // Otherwise, use our own built-in activity ID tracking
                    StructuredEventSource.EnsureLogicalTraceActivityId();
                ////}

                eventSourceEvent.WriteEventSource();
            }
        }
    }
}
