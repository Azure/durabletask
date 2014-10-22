namespace TaskHubStressTest
{
    using System;
    using System.Linq;
    using System.Diagnostics;

    class OrchestrationConsoleTraceListener : ConsoleTraceListener
    {
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
                    Console.WriteLine(toWrite);
                    Debug.WriteLine(toWrite);
                }
                else
                {
                    string toWrite = string.Format("[{0}] {1}", DateTime.Now, dict["msg"]);
                    Console.WriteLine(toWrite);
                    Debug.WriteLine(toWrite);
                }
            }
            catch (Exception exception)
            {
                string toWrite = string.Format("Exception while parsing trace:  {0}\n\t", exception.Message, exception.StackTrace);
                Console.WriteLine(toWrite);
                Debug.WriteLine(toWrite);
            }
        }
    }
}
