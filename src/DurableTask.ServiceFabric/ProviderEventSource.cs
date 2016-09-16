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

namespace DurableTask.ServiceFabric
{
    using System.Diagnostics.Tracing;
    using System.Threading.Tasks;

    [EventSource(Name = "DurableTask-ServiceFabric-Provider")]
    internal sealed class ProviderEventSource : EventSource
    {
        public static readonly ProviderEventSource Instance = new ProviderEventSource();

        static ProviderEventSource()
        {
            // A workaround for the problem where ETW activities do not get tracked until Tasks infrastructure is initialized.
            // This problem will be fixed in .NET Framework 4.6.2.
            Task.Run(() => { });
        }

        private ProviderEventSource() : base()
        {
        }

        public static class Keywords
        {
            public const EventKeywords Orchestration = (EventKeywords) 0x1L;
            public const EventKeywords Activity = (EventKeywords) 0x2L;
        }

        [Event(1, Level = EventLevel.Informational, Message = "Orchestration {0} Created.")]
        public void LogOrchestrationCreated(string instanceId)
        {
            if (IsEnabled(EventLevel.Informational, Keywords.Orchestration))
            {
                WriteEvent(1, instanceId);
            }
        }

        [Event(2, Level = EventLevel.Informational, Message = "Orchestration {0} Finished with the status {1} and result {3} in {2} seconds.")]
        public void LogOrchestrationFinished(string instanceId, string terminalStatus, double runningTimeInSeconds, string result)
        {
            if (IsEnabled(EventLevel.Informational, Keywords.Orchestration))
            {
                WriteEvent(2, instanceId, terminalStatus, runningTimeInSeconds, result);
            }
        }
    }
}
