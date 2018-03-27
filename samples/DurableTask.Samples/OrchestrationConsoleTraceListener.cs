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

namespace DurableTask.Samples
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
                string msg;
                if (dict.TryGetValue("iid", out iid) && dict.TryGetValue("msg", out msg))
                {
                    string toWrite = $"[{DateTime.Now} {iid}] {msg}";
                    Console.WriteLine(toWrite);
                    Debug.WriteLine(toWrite);
                }
                else if (dict.TryGetValue("msg", out msg))
                {
                    string toWrite = $"[{DateTime.Now}] {msg}";
                    Console.WriteLine(toWrite);
                    Debug.WriteLine(toWrite);
                }
                else
                {
                    string toWrite = $"[{DateTime.Now}] {message}";
                    Console.WriteLine(toWrite);
                    Debug.WriteLine(toWrite);
                }
            }
            catch (Exception exception)
            {
                string toWrite = $"Exception while parsing trace:  {exception.Message}\n\t{exception.StackTrace}";
                Console.WriteLine(toWrite);
                Debug.WriteLine(toWrite);
            }
        }
    }
}
