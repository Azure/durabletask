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
    using Microsoft.Diagnostics.Tracing;
    using System.Threading.Tasks;

    [EventSource(Name = "DurableTask-ServiceFabricProvider")]
    internal sealed class ProviderEventSource : EventSource
    {
        public static readonly ProviderEventSource Log = new ProviderEventSource();

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
            public const EventKeywords Common = (EventKeywords)0x4L;
        }

        [Event(1, Level = EventLevel.Informational, Message = "Orchestration with instanceId : '{0}' and executionId : '{1}' is Created.", Channel = EventChannel.Operational)]
        public void OrchestrationCreated(string instanceId, string executionId)
        {
            if (IsEnabled(EventLevel.Informational, Keywords.Orchestration))
            {
                WriteEvent(1, instanceId, executionId);
            }
        }

        [Event(2, Level = EventLevel.Informational, Message = "Orchestration {0} Finished with the status {1} and result {3} in {2} seconds.", Channel = EventChannel.Operational)]
        public void OrchestrationFinished(string instanceId, string terminalStatus, double runningTimeInSeconds, string result)
        {
            if (IsEnabled(EventLevel.Informational, Keywords.Orchestration))
            {
                WriteEvent(2, instanceId, terminalStatus, runningTimeInSeconds, result);
            }
        }

        [Event(3, Level = EventLevel.Error, Message = "Exception : {0} With Stack Trace: {1}", Channel = EventChannel.Operational)]
        public void LogException(string message, string stackTrace)
        {
            if (IsEnabled(EventLevel.Error, Keywords.Common))
            {
                WriteEvent(3, message, stackTrace);
            }
        }

        [Event(4, Level = EventLevel.Informational, Message = "Current number of entries in store {0} : {1}", Channel = EventChannel.Operational)]
        public void LogStoreCount(string storeName, long count)
        {
            if (IsEnabled(EventLevel.Informational, Keywords.Common))
            {
                WriteEvent(4, storeName, count);
            }
        }

        [Event(5, Level = EventLevel.Error, Message = "We are seeing something that we don't expect to see : {0}", Channel = EventChannel.Operational)]
        public void UnexpectedCodeCondition(string uniqueMessage)
        {
            if (IsEnabled(EventLevel.Error, Keywords.Common))
            {
                WriteEvent(5, uniqueMessage);
            }
        }

        [Event(6, Level = EventLevel.Error, Message = "{0} : Successive Failed Attempt Number : '{1}', Ignored Exception : '{2}' With Stack Trace: '{3}'", Channel = EventChannel.Operational)]
        public void ExceptionWhileProcessingScheduledMessages(string methodName, int attemptNumber, string message, string stackTrace)
        {
            if (IsEnabled(EventLevel.Error, Keywords.Common))
            {
                WriteEvent(6, methodName, attemptNumber, message, stackTrace);
            }
        }

        [Event(7, Level = EventLevel.Error, Message = "Hint : {0}, Exception: {1}", Channel = EventChannel.Operational)]
        public void ExceptionWhileProcessingReliableCollectionTransaction(string uniqueIdentifier, string exception)
        {
            if (IsEnabled(EventLevel.Error, Keywords.Common))
            {
                WriteEvent(7, uniqueIdentifier, exception);
            }
        }

        [Event(101, Level = EventLevel.Verbose, Message = "Trace Message for Session {0} : {1}", Channel = EventChannel.Debug)]
        public void TraceMessage(string instanceId, string message)
        {
#if DEBUG
            if (IsEnabled(EventLevel.Verbose, Keywords.Common))
            {
                WriteEvent(101, instanceId, message);
            }
#endif
        }

        [Event(102, Level = EventLevel.Verbose, Message = "Time taken for {0} : {1} milli seconds.", Channel = EventChannel.Analytic)]
        public void LogMeasurement(string uniqueActionIdentifier, long elapsedMilliseconds)
        {
#if DEBUG
            if (IsEnabled(EventLevel.Verbose, Keywords.Common))
            {
                WriteEvent(102, uniqueActionIdentifier, elapsedMilliseconds);
            }
#endif
        }
    }
}
