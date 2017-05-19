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

        public static class Keywords
        {
            public const EventKeywords Diagnostics = (EventKeywords)1L;
        }

        public static readonly DefaultEventSource Log = new DefaultEventSource();

        readonly string processName;

        DefaultEventSource()
        {
            using (Process process = Process.GetCurrentProcess())
            {
                this.processName = process.ProcessName.ToLowerInvariant();
            }
        }

        public bool IsTraceEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.Diagnostics);

        public bool IsDebugEnabled => this.IsEnabled(EventLevel.Verbose, Keywords.Diagnostics);

        public bool IsInfoEnabled => this.IsEnabled(EventLevel.Informational, EventKeywords.None);

        public bool IsWarningEnabled => this.IsEnabled(EventLevel.Warning, EventKeywords.None);

        public bool IsErrorEnabled => this.IsEnabled(EventLevel.Error, EventKeywords.None);

        public bool IsCriticalEnabled => this.IsEnabled(EventLevel.Critical, EventKeywords.None);

        [NonEvent]
        public void TraceEvent(TraceEventType eventType, string source, string instanceId, string executionId, string sessionId, string message)
        {
            switch (eventType)
            {
                case TraceEventType.Critical:
                    this.Critical(source, instanceId, executionId, sessionId, message);
                    break;
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

        [Event(TraceEventId, Level = EventLevel.Verbose, Keywords = Keywords.Diagnostics, Version = 2)]
        public void Trace(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsTraceEnabled)
            {
                this.WriteEventInternal(TraceEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Debug(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Debug(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(DebugEventId, Level = EventLevel.Verbose, Keywords = Keywords.Diagnostics, Version = 2)]
        public void Debug(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsDebugEnabled)
            {
                this.WriteEventInternal(DebugEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Info(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Info(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(InfoEventId, Level = EventLevel.Informational, Keywords = EventKeywords.None, Version = 2)]
        public void Info(string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            if (this.IsInfoEnabled)
            {
                this.WriteEventInternal(InfoEventId, source, instanceId, executionId, sessionId, message, info);
            }
        }

        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message) => 
            this.Warning(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Warning(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(WarningEventId, Level = EventLevel.Warning, Keywords = EventKeywords.None, Version = 2)]
        public void Warning(string source, string instanceId, string executionId, string sessionId, string message, string exception)
        {
            if (this.IsWarningEnabled)
            {
                this.WriteEventInternal(WarningEventId, source, instanceId, executionId, sessionId, message, exception);
            }
        }

        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message) =>
            this.Error(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Error(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(ErrorEventId, Level = EventLevel.Error, Keywords = EventKeywords.None, Version = 2)]
        public void Error(string source, string instanceId, string executionId, string sessionId, string message, string exception)
        {
            if (this.IsErrorEnabled)
            {
                this.WriteEventInternal(ErrorEventId, source, instanceId, executionId, sessionId, message, exception);
            }
        }

        [NonEvent]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message) =>
            this.Critical(source, instanceId, executionId, sessionId, message, string.Empty);

        [NonEvent]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message, Exception exception) =>
            this.Critical(source, instanceId, executionId, sessionId, message, exception?.ToString() ?? string.Empty);

        [Event(CriticalEventId, Level = EventLevel.Critical, Keywords = EventKeywords.None, Version = 2)]
        public void Critical(string source, string instanceId, string executionId, string sessionId, string message, string exception)
        {
            if (this.IsCriticalEnabled)
            {
                this.WriteEventInternal(CriticalEventId, source, instanceId, executionId, sessionId, message, exception);
            }
        }

        [NonEvent]
        unsafe void WriteEventInternal(int eventId, string source, string instanceId, string executionId, string sessionId, string message, string info)
        {
            source = string.Concat(source, '-', this.processName);

            MakeSafe(ref source);
            MakeSafe(ref instanceId);
            MakeSafe(ref executionId);
            MakeSafe(ref sessionId);
            MakeSafe(ref message);
            MakeSafe(ref info);

            const int EventDataCount = 6;
            fixed (char* chPtrSource = source)
            fixed (char* chPtrInstanceId = instanceId)
            fixed (char* chPtrExecutionId = executionId)
            fixed (char* chPtrSessionId = sessionId)
            fixed (char* chPtrMessage = message)
            fixed (char* chPtrInfo = info)
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