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

namespace DurableTask.AzureServiceFabric.Tracing
{
    using System;
    using System.Diagnostics.Tracing;
    using System.Threading.Tasks;

    /// <summary>
    /// The event source which emits ETW events for service fabric based provider functionality.
    /// </summary>
    [EventSource(
        Guid = "2B44FC3F-F4FD-4B1C-8EE4-67F4F2BFEDED",
        Name = "DurableTask-AzureServiceFabricProvider-Core")]
    internal sealed class ServiceFabricProviderEventSource : EventSource
    {
        /// <summary>
        /// Singleton instance of the event source.
        /// </summary>
        public static readonly ServiceFabricProviderEventSource Tracing = new ServiceFabricProviderEventSource();

        static ServiceFabricProviderEventSource()
        {
            // A workaround for the problem where ETW activities do not get tracked until Tasks infrastructure is initialized.
            // This problem will be fixed in .NET Framework 4.6.2.
            Task.Run(() => { });
        }

        private ServiceFabricProviderEventSource() : base()
        {
        }

        /// <summary>
        /// Event keywords.
        /// </summary>
        public class Keywords
        {
            /// <summary>
            /// Orchestration Keyword
            /// </summary>
            public const EventKeywords Orchestration = (EventKeywords)0x1L;

            /// <summary>
            /// Activity Keyword
            /// </summary>
            public const EventKeywords Activity = (EventKeywords)0x2L;

            /// <summary>
            /// Common Keyword
            /// </summary>
            public const EventKeywords Common = (EventKeywords)0x4L;

            /// <summary>
            /// Warning Keyword
            /// </summary>
            public const EventKeywords Warning = (EventKeywords)0x8L;

            /// <summary>
            /// Error Keyword
            /// </summary>
            public const EventKeywords Error = (EventKeywords)0x10L;
        }

        #region Verbose Events 001-500

        [Event(1,
            Keywords = Keywords.Common,
            Level = EventLevel.Verbose,
            Message = "Trace Message for Session {0} : {1}")]
        internal void TraceMessage(string instanceId, string message)
        {
#if DEBUG
            if (this.IsEnabled(EventLevel.Verbose, Keywords.Common))
            {
                this.WriteEvent(1, instanceId, message);
            }
#endif
        }

        [Event(2,
            Keywords = Keywords.Common,
            Level = EventLevel.Verbose,
            Message = "Time taken for {0} : {1} milli seconds.")]
        internal void LogMeasurement(string uniqueActionIdentifier, long elapsedMilliseconds)
        {
#if DEBUG
            if (this.IsEnabled(EventLevel.Verbose, Keywords.Common))
            {
                this.WriteEvent(2, uniqueActionIdentifier, elapsedMilliseconds);
            }
#endif
        }
        #endregion

        #region Informational 501-1000

        [Event(501,
            Keywords = Keywords.Orchestration,
            Level = EventLevel.Informational)]
        internal void LogOrchestrationInformation(string instanceId, string executionId, string message)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.Orchestration))
            {
                this.WriteEvent(501, instanceId, executionId, message);
            }
        }

        [Event(503,
            Keywords = Keywords.Common,
            Level = EventLevel.Verbose,
            Message = "Current number of entries in store {0} : {1}")]
        internal void LogStoreCount(string storeName, long count)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.Common))
            {
                this.WriteEvent(503, storeName, count);
            }
        }

        [Event(504,
            Keywords = Keywords.Common,
            Level = EventLevel.Informational,
            Message = "Time taken for {0} : {1} milli seconds.")]
        internal void LogTimeTaken(string uniqueActionIdentifier, double elapsedMilliseconds)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.Common))
            {
                this.WriteEvent(504, uniqueActionIdentifier, elapsedMilliseconds);
            }
        }

        [Event(505,
            Keywords = Keywords.Common,
            Level = EventLevel.Informational,
            Message = "{0} : {1}")]
        internal void ReliableStateManagement(string operationIdentifier, string operationData)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.Common))
            {
                this.WriteEvent(505, operationIdentifier, operationData);
            }
        }

        #endregion

        #region Warnings 1001-1500
        [Event(1001,
            Keywords = Keywords.Warning,
            Level = EventLevel.Warning,
            Message = "Exception in the background job {0} : {1}")]
        internal void ExceptionWhileRunningBackgroundJob(string operationIdentifier, string exception)
        {
            if (this.IsEnabled(EventLevel.Warning, Keywords.Common | Keywords.Warning))
            {
                this.WriteEvent(1001, operationIdentifier, exception);
            }
        }

        [Event(1002,
            Level = EventLevel.Warning,
            Message = "Hint : {0}, AttemptNumber : {1}, Exception: {2}")]
        internal void RetryableFabricException(string uniqueIdentifier, int attemptNumber, string exception)
        {
            if (this.IsEnabled(EventLevel.Warning, Keywords.Common | Keywords.Warning))
            {
                this.WriteEvent(1002, uniqueIdentifier, attemptNumber, exception);
            }
        }
        #endregion

        #region Errors 1501-2000

        [Event(1501,
            Keywords = Keywords.Common | Keywords.Error,
            Level = EventLevel.Error,
            Message = "We are seeing something that we don't expect to see : {0}")]
        internal void UnexpectedCodeCondition(string uniqueMessage)
        {
            if (this.IsEnabled(EventLevel.Error, Keywords.Common | Keywords.Error))
            {
                this.WriteEvent(1501, uniqueMessage);
            }

#if DEBUG
            // This is so that tests fail when this event happens.
            throw new Exception(uniqueMessage);
#endif
        }

        [Event(1502,
            Keywords = Keywords.Common | Keywords.Error,
            Level = EventLevel.Error,
            Message = "Hint : {0}, Exception: {1}")]
        internal void ExceptionInReliableCollectionOperations(string uniqueIdentifier, string exception)
        {
            if (this.IsEnabled(EventLevel.Error, Keywords.Common | Keywords.Error))
            {
                this.WriteEvent(1502, uniqueIdentifier, exception);
            }
        }

        [Event(1503,
            Keywords = Keywords.Common | Keywords.Error,
            Level = EventLevel.Error)]
        public void LogError(string message)
        {
            if (this.IsEnabled(EventLevel.Error, Keywords.Common | Keywords.Error))
            {
                this.WriteEvent(1503, message);
            }
        }

        #endregion
    }
}
