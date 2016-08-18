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

namespace DurableTask.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using DurableTask.Common;

    internal class OrchestrationConsoleTraceListener : ConsoleTraceListener
    {
        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id,
            string message)
        {
            try
            {
                Dictionary<string, string> dict = message.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
                    .Select(part => part.Split('='))
                    .ToDictionary(split => split[0], split => split.Length > 1 ? split[1] : string.Empty);
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
            catch (Exception exception) when (!Utils.IsFatal(exception))
            {
                string toWrite = $"Exception while parsing trace: {message} : {exception.Message}\n\t{exception.StackTrace}";
                Console.WriteLine(toWrite);
                Debug.WriteLine(toWrite);
            }
        }
    }
}