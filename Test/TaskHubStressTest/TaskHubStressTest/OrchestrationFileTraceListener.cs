namespace TaskHubStressTest
{
    using System;
    using System.Linq;
    using System.Diagnostics;

    class OrchestrationFileTraceListener : TextWriterTraceListener
    {
        public OrchestrationFileTraceListener(string file)
            : base(file)
        {
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id)
        {
            base.TraceEvent(eventCache, source, eventType, id);
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string format, params object[] args)
        {
            string message = format;
            try
            {
                if (args != null && args.Length > 0)
                {
                    message = string.Format(format, args);
                }
            }
            catch (Exception)
            {
                message = "msg=Cannot format message";
            }

            this.TraceEvent(eventCache, source, eventType, id, message);
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string message)
        {
            try
            {
                var dict = message.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                   .Select(part => part.Split('='))
                   .ToDictionary(split => split[0], split => split[1]);
                string iid;
                string msg;
                if (dict.TryGetValue("iid", out iid) && dict.TryGetValue("msg", out msg))
                {
                    string toWrite = $"[{DateTime.Now} {iid}] {msg}";
                    base.WriteLine(toWrite);
                }
                else if (dict.TryGetValue("msg", out msg))
                {
                    string toWrite = $"[{DateTime.Now}] {msg}";
                    base.WriteLine(toWrite);
                }
                else
                {
                    string toWrite = $"[{DateTime.Now}] {message}";
                    base.WriteLine(toWrite);
                }
            }
            catch (Exception exception)
            {
                string toWrite = $"Exception while parsing trace:  {exception.Message}\n\t{exception.StackTrace}";
                base.WriteLine(toWrite);
            }
        }
    }
}
