//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.ServiceBus.Task.Tracing
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.Eventing;
    using System.Globalization;
    using System.Text;
    using System.Xml;

    public class OrchestrationEtwListener : TraceListener
    {
        const string OrchestrationEtwProvider = "8e2865db-5e97-4d93-8a56-a1af758bfe3b";
        private EventProvider m_eventProvider;

        public OrchestrationEtwListener()
            : this(OrchestrationEtwProvider)
        {
        }

        public OrchestrationEtwListener(string guid)
        {
            Guid providerGuid = new Guid(guid);
            this.m_eventProvider = new EventProvider(providerGuid);
        }

        private EtwEventLevel ConvertTraceEventTypeToEventLevel(TraceEventType eventType)
        {
            EtwEventLevel verbose = EtwEventLevel.Verbose;
            switch (eventType)
            {
                case TraceEventType.Critical:
                    return EtwEventLevel.Critical;

                case TraceEventType.Error:
                    return EtwEventLevel.Error;

                case (TraceEventType.Error | TraceEventType.Critical):
                    return verbose;

                case TraceEventType.Warning:
                    return EtwEventLevel.Warning;

                case TraceEventType.Information:
                    return EtwEventLevel.Information;

                case TraceEventType.Verbose:
                    return EtwEventLevel.Verbose;
            }
            return verbose;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && (this.m_eventProvider != null))
            {
                this.m_eventProvider.Dispose();
                this.m_eventProvider = null;
            }
            base.Dispose(disposing);
        }

        private void LogToEtw(TraceEventType eventType, int id, string eventMessage)
        {
            EtwEventLevel level = this.ConvertTraceEventTypeToEventLevel(eventType);
            EventDescriptor eventDescriptor = new EventDescriptor(0xf01c, 0, 0, (byte)level, 0, 0, 0L); // From DiagnosticMonitorTraceListener
            try
            {
                this.m_eventProvider.WriteEvent(ref eventDescriptor, new object[] { (ulong)id, eventMessage });
            }
            catch (ArgumentException)
            {
            }
            catch (System.ComponentModel.Win32Exception)
            {
            }
        }

        public override void TraceData(TraceEventCache eventCache, string source, TraceEventType eventType, int id, object data)
        {
        }

        public override void TraceData(TraceEventCache eventCache, string source, TraceEventType eventType, int id, params object[] data)
        {
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id)
        {
            string message = "TraceSource '" + source + "' event";
            this.WriteStringEtw(eventType, id, message);
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string message)
        {
            string str = string.Format(CultureInfo.InvariantCulture, "{0}; TraceSource '{1}' event", new object[] { message, source });
            this.WriteStringEtw(eventType, id, str);
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string format, params object[] args)
        {
            if ((args == null) || (args.Length == 0))
            {
                this.TraceEvent(eventCache, source, eventType, id, format);
            }
            else
            {
                string message = string.Format(CultureInfo.InvariantCulture, format, args);
                this.TraceEvent(eventCache, source, eventType, id, message);
            }
        }

        public override void Write(string message)
        {
            this.WriteStringEtw(TraceEventType.Verbose, 0, message);
        }

        public override void WriteLine(string message)
        {
            this.Write(message);
        }

        private void WriteStringEtw(TraceEventType level, int id, string message)
        {
            this.LogToEtw(level, id, message);
        }

        public override bool IsThreadSafe
        {
            get
            {
                return true;
            }
        }

        private enum EtwEventLevel
        {
            Critical = 1,
            Error = 2,
            Information = 4,
            Verbose = 5,
            Warning = 3
        }
    }
}
