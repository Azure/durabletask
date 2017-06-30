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

namespace DurableTask.Core.Tracing
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;

    /// <summary>
    ///     Default event source for all DurableTask tracing
    /// </summary>
    [EventSource(
        Name = "DurableTask-Core",
        Guid = "7DA4779A-152E-44A2-A6F2-F80D991A5BEE")]
    [System.Runtime.InteropServices.ComVisible(false)]
    public class DefaultEventSource : EventSource
    {
        const int TraceEventId = 1;
        const int DebugEventId = 2;
        const int InfoEventId = 3;
        const int WarningEventId = 4;
        const int ErrorEventId = 5;
        const int CriticalEventId = 6;

        /// <summary>
        ///     EventKeywords for the event source
        /// </summary>
        public static class Keywords
        {
            /// <summary>
            /// Diagnostic keyword
            /// </summary>
            public const EventKeywords Diagnostics = (EventKeywords)1L;
        }

        /// <summary>
        /// Gets the static instance of the DefaultEventSource
        /// </summary>
        public static readonly DefaultEventSource Log = new DefaultEventSource();

        readonly string processName;

        /// <summary>
        ///     Creates a new instance of the DefaultEventSource
        /// </summary>
        DefaultEventSource()
        {
            using (Process process = Process.GetCurrentProcess())
            {
                this.processName = process.ProcessName.ToLowerInvariant();
            }
        }

        /// <summary>
        /// Gets whether trace logs are enabled
        /// </summary>
        public bool IsTraceEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.Diagnostics);

        /// <summary>
        /// Gets whether debug logs are enabled
        /// </summary>
        public bool IsDebugEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.Diagnostics);

        /// <summary>
        /// Gets whether informational logs are enabled
        /// </summary>
        public bool IsInfoEnabled => this.IsEnabled(EventLevel.Informational, Keywords.Diagnostics);

        /// <summary>
        /// Gets whether warning logs are enabled
        /// </summary>
        public bool IsWarningEnabled => this.IsEnabled(EventLevel.Warning, Keywords.Diagnostics);

        /// <summary>
        /// Gets whether error logs are enabled
        /// </summary>
        public bool IsErrorEnabled => this.IsEnabled(EventLevel.Error, Keywords.Diagnostics);

        /// <summary>
        /// Gets whether critical logs are enabled
        /// </summary>
        public bool IsCriticalEnabled => this.IsEnabled(EventLevel.Critical, Keywords.Diagnostics);

        /// <summary>
        /// Trace an event for the supplied eventtype and parameters
        /// </summary>
        [NonEvent]
        public void TraceEvent(TraceEventType eventLevel, string source, string instanceId, string executionId, string sessionId, string message, string eventType)
        {
            switch (eventLevel)
            {
                case TraceEventType.Critical:
                    this.Critical(source, instanceId, executionId, sessionId, message, eventType);
                    break;
                case TraceEventType.Error:
                    this.Error(source, instanceId, executionId, sessionId, message, eventType);
                    break;
                case TraceEventType.Warning:
                    this.Warning(source, instanceId, executionId, sessionId, message, eventType);
                    break;
                case TraceEventType.Information:
                    this.Info(source, instanceId, executionId, sessionId, message, eventType);
                    break;
                default:
                    this.Trace(source, instanceId, executionId, sessionId, message, eventType);
                    break;
            }
        }

        /// <summary>
        /// Trace an event for the supplied eventtype, exception and parameters
        /// </summary>
        [NonEvent]
        public void TraceEvent(TraceEventType eventLevel, string source, string instanceId, string executionId, string sessionId, Exception exception, string eventType) =>
            this.TraceEvent(eventLevel, source, instanceId, executionId, sessionId, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Trace an event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, string eventType) => 
            this.Trace(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Trace an event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
             this.Trace(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Trace an event for the supplied parameters
        /// </summary>
        [Event(TraceEventId, Level = EventLevel.Verbose, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, string info, string eventType)
        {
            if (this.IsTraceEnabled)
            {
                this.WriteEventInternal(TraceEventId, source, instanceId, executionId, sessionId, message, info, eventType);
            }
        }

        /// <summary>
        /// Log debug event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, string eventType) => 
            this.Debug(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Log debug event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
            this.Debug(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Log debug event for the supplied parameters
        /// </summary>
        [Event(DebugEventId, Level = EventLevel.Verbose, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, string info, string eventType)
        {
            if (this.IsDebugEnabled)
            {
                this.WriteEventInternal(DebugEventId, source, instanceId, executionId, sessionId, message, info, eventType);
            }
        }

        /// <summary>
        /// Log informational event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, string eventType) => 
            this.Info(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Log informational event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
            this.Info(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Log informational event for the supplied parameters
        /// </summary>
        [Event(InfoEventId, Level = EventLevel.Informational, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, string info, string eventType)
        {
            if (this.IsInfoEnabled)
            {
                this.WriteEventInternal(InfoEventId, source, instanceId, executionId, sessionId, message, info, eventType);
            }
        }

        /// <summary>
        /// Log warning event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, string eventType) => 
            this.Warning(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Log warning event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
            this.Warning(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Log warning event for the supplied parameters
        /// </summary>
        [Event(WarningEventId, Level = EventLevel.Warning, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, string exception, string eventType)
        {
            if (this.IsWarningEnabled)
            {
                this.WriteEventInternal(WarningEventId, source, instanceId, executionId, sessionId, message, exception, eventType);
            }
        }

        /// <summary>
        /// Log error event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, string eventType) =>
            this.Error(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Log error event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
            this.Error(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Log error event for the supplied parameters
        /// </summary>
        [Event(ErrorEventId, Level = EventLevel.Error, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, string exception, string eventType)
        {
            if (this.IsErrorEnabled)
            {
                this.WriteEventInternal(ErrorEventId, source, instanceId, executionId, sessionId, message, exception, eventType);
            }
        }

        /// <summary>
        /// Log critical event for the supplied parameters
        /// </summary>
        [NonEvent]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message, string eventType) =>
            this.Critical(source, instanceId, executionId, sessionId, message, string.Empty, eventType);

        /// <summary>
        /// Log critical event for the supplied exception and parameters
        /// </summary>
        [NonEvent]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message, Exception exception, string eventType) =>
            this.Critical(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty, eventType);

        /// <summary>
        /// Log critical event for the supplied parameters
        /// </summary>
        [Event(CriticalEventId, Level = EventLevel.Critical, Keywords = Keywords.Diagnostics, Version = 3)]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message, string exception, string eventType)
        {
            if (this.IsCriticalEnabled)
            {
                this.WriteEventInternal(CriticalEventId, source, instanceId, executionId, sessionId, message, exception, eventType);
            }
        }

        [NonEvent]
        unsafe void WriteEventInternal(int eventId, string source, string instanceId, string executionId, string sessionId, string message, string info, string eventType)
        {
            source = string.Concat(source, '-', this.processName);

            MakeSafe(ref source);
            MakeSafe(ref instanceId);
            MakeSafe(ref executionId);
            MakeSafe(ref sessionId);
            MakeSafe(ref message);
            MakeSafe(ref info);
            MakeSafe(ref eventType);

            const int EventDataCount = 7;
            fixed (char* chPtrSource = source)
            fixed (char* chPtrInstanceId = instanceId)
            fixed (char* chPtrExecutionId = executionId)
            fixed (char* chPtrSessionId = sessionId)
            fixed (char* chPtrMessage = message)
            fixed (char* chPtrInfo = info)
            fixed (char* chPtrEventType = eventType)
            {
                EventData* data = stackalloc EventData[EventDataCount];
                data[0].DataPointer = (IntPtr)chPtrSource;
                data[0].Size = (source.Length + 1) * 2;
                data[1].DataPointer = (IntPtr)chPtrInstanceId;
                data[1].Size = (instanceId.Length + 1) * 2;
                data[2].DataPointer = (IntPtr)chPtrExecutionId;
                data[2].Size = (executionId.Length + 1) * 2;
                data[3].DataPointer = (IntPtr)chPtrSessionId;
                data[3].Size = (sessionId.Length + 1) * 2;
                data[4].DataPointer = (IntPtr)chPtrMessage;
                data[4].Size = (message.Length + 1) * 2;
                data[5].DataPointer = (IntPtr)chPtrInfo;
                data[5].Size = (info.Length + 1) * 2;
                data[6].DataPointer = (IntPtr)chPtrEventType;
                data[6].Size = (eventType.Length + 1) * 2;

                // todo: use WriteEventWithRelatedActivityIdCore for correlation
                this.WriteEventCore(eventId, EventDataCount, data);
            }
        }

        static void MakeSafe(ref string value)
        {
            const int MaxLength = 0x7C00; // max event size is 64k, truncating to roughly 31k chars
            value = value ?? string.Empty;

            if (value.Length > MaxLength)
            {
                value = value.Remove(MaxLength);
            }
        }
    }
}