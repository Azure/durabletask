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

namespace DurableTask.Tracing
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;

    [EventSource(
        Name = "DurableTask-Default",
        Guid = "7DA4779A-152E-44A2-A6F2-F80D991A5BEE")]
    internal class DefaultEventSource : EventSource
    {
        const int TraceEventId = 1;
        const int DebugEventId = 2;
        const int InfoEventId = 3;
        const int WarningEventId = 4;
        const int ErrorEventId = 5;

        public class Keywords
        {
            public const EventKeywords TraceEventKeyword = (EventKeywords)(1);
            public const EventKeywords DebugEventKeyword = (EventKeywords)(1 << 1);
        }

        public static readonly DefaultEventSource Log = new DefaultEventSource();

        DefaultEventSource()
        {
        }

        public bool IsTraceEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.TraceEventKeyword);

        public bool IsDebugEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.DebugEventKeyword);

        public bool IsInfoEnabled => this.IsEnabled(EventLevel.Informational, EventKeywords.None);

        public bool IsWarningEnabled => this.IsEnabled(EventLevel.Warning, EventKeywords.None);

        public bool IsErrorEnabled => this.IsEnabled(EventLevel.Error, EventKeywords.None);

        [NonEvent]
        public void TraceEvent(TraceEventType eventType, string source, string instanceId, string executionId, string sessionId, string message)
        {
            switch (eventType)
            {
                case TraceEventType.Critical:
                case TraceEventType.Error:
                    this.Error(source, instanceId, executionId, sessionId, message);
                    break;
                case TraceEventType.Warning:
                    this.Warning(source, instanceId, executionId, sessionId, message);
                    break;
                case TraceEventType.Information:
                    this.Info(source, instanceId, executionId, sessionId, message);
                    break;
                default:
                    this.Trace(source, instanceId, executionId, sessionId, message);
                    break;
            }
        }

        [NonEvent]
        public void TraceEvent(TraceEventType eventType, string source, string instanceId, string executionId, string sessionId, Exception exception) =>
            this.TraceEvent(eventType, source, instanceId, executionId, sessionId, exception?.ToString() ?? string.Empty);

        [NonEvent]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Trace(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
             this.Trace(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(TraceEventId, Level = EventLevel.Verbose, Keywords = Keywords.TraceEventKeyword)]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsTraceEnabled)
            {
                this.WriteEvent(TraceEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Debug(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Debug(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(DebugEventId, Level = EventLevel.Verbose, Keywords = Keywords.DebugEventKeyword)]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsDebugEnabled)
            {
                this.WriteEvent(DebugEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Info(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Info(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(InfoEventId, Level = EventLevel.Informational)]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsInfoEnabled)
            {
                this.WriteEvent(InfoEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Warning(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Warning(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(WarningEventId, Level = EventLevel.Warning)]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, string exception)
        {
            if (this.IsWarningEnabled)
            {
                this.WriteEvent(WarningEventId, source, instanceId, executionId, sessionId, message, exception);
            }
        }

        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message) =>
            this.Error(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Error(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(ErrorEventId, Level = EventLevel.Error)]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, string exception)
        {
            if (this.IsErrorEnabled)
            {
                this.WriteEvent(ErrorEventId, instanceId, executionId, sessionId, source, message, exception);
            }
        }
    }
}