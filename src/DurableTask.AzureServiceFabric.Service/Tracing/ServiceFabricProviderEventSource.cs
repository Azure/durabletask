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
        Guid = "EA75D62C-21B9-4EB6-B281-F2403FEBC00E",
        Name = "DurableTask-AzureServiceFabricProvider-Service")]
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

            /// <summary>
            /// ProxyService Keyword
            /// </summary>
            public const EventKeywords ProxyService = (EventKeywords)0x20L;

            /// <summary>
            /// Requests Keyword
            /// </summary>
            public const EventKeywords FabricService = (EventKeywords)0x40L;
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

        #endregion

        #region Informational 501-1000

        [Event(506,
            Keywords = Keywords.ProxyService,
            Level = EventLevel.Informational)]
        internal void LogProxyServiceRequestInformation(string message)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.ProxyService))
            {
                this.WriteEvent(506, message);
            }
        }

        [Event(510,
            Keywords = Keywords.FabricService,
            Level = EventLevel.Informational,
            Message = "{7}")]
        internal void LogFabricServiceInformation(
            string serviceName,
            string serviceTypeName,
            long replicaOrInstanceId,
            Guid partitionId,
            string applicationName,
            string applicationTypeName,
            string nodeName,
            string message)
        {
            if (this.IsEnabled(EventLevel.Informational, Keywords.FabricService))
            {
                this.WriteEvent(510, serviceName, serviceTypeName, replicaOrInstanceId, partitionId, applicationName, applicationTypeName, nodeName, message);
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

        [Event(1503,
            Keywords = Keywords.ProxyService,
            Level = EventLevel.Error)]
        internal void LogProxyServiceError(string message)
        {
            if (this.IsEnabled(EventLevel.Error, Keywords.ProxyService))
            {
                this.WriteEvent(1503, message);
            }
        }

        [Event(1504,
            Keywords = Keywords.FabricService,
            Level = EventLevel.Error,
            Message = "Service request '{0}' failed")]
        internal void ServiceRequestFailed(string requestTypeName, string exception)
        {
            if (this.IsEnabled(EventLevel.Error, Keywords.FabricService))
            {
                this.WriteEvent(1504, exception);
            }
        }
        #endregion
    }
}
