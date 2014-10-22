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
            catch (Exception ex)
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
                if (dict.TryGetValue("iid", out iid))
                {
                    string toWrite = string.Format("[{0} {1}] {2}", DateTime.Now, iid, dict["msg"]);
                    base.WriteLine(toWrite);
                }
                else
                {
                    string toWrite = string.Format("[{0}] {1}", DateTime.Now, dict["msg"]);
                    base.WriteLine(toWrite);
                }
            }
            catch (Exception exception)
            {
                string toWrite = string.Format("Exception while parsing trace:  {0}\n\t", exception.Message, exception.StackTrace);
                base.WriteLine(toWrite);
            }
        }
    }
}
