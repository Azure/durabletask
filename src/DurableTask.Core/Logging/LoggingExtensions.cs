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
            if (logEvent == null)
            {
                throw new ArgumentNullException(nameof(logEvent));
            }

            logger?.Log(
                logEvent.Level,
                logEvent.EventId,
                logEvent,
                exception,
                formatter: (s, e) => s.FormattedMessage);

            if (logEvent is IEventSourceEvent eventSourceEvent)
            {
                StructuredEventSource.EnsureLogicalTraceActivityId();
                eventSourceEvent.WriteEventSource();
            }
        }

        /// <summary>
        /// Sets the trace activity ID for the current logical thread.
        /// </summary>
        /// <param name="traceActivityId">The trace activity ID to set.</param>
        public static void SetLogicalTraceActivityId(Guid traceActivityId)
        {
            StructuredEventSource.SetLogicalTraceActivityId(traceActivityId);
        }
    }
}
