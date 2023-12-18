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

namespace DurableTask.ServiceBus.Settings
{
#if NETSTANDARD2_0
    using Azure.Core;
#endif
    using System;

    /// <summary>
    /// Service Bus connection settings
    /// </summary>
    public class ServiceBusConnectionSettings
    {
        /// <summary>
        /// Creates an instance of <see cref="ServiceBusConnectionSettings"/>
        /// </summary>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <returns></returns>
        public static ServiceBusConnectionSettings Create(string connectionString)
        {
            return new ServiceBusConnectionSettings
            {
                ConnectionString = connectionString
            };
        }

#if NETSTANDARD2_0

        /// <summary>
        /// Creates an instance of <see cref="ServiceBusConnectionSettings"/>
        /// </summary>
        /// <param name="namespaceHostName">Service Bus namespace host name</param>
        /// <param name="tokenCredential">Service Bus authentication token credential</param>
        /// <param name="transportType">Service Bus messaging protocol</param>
        /// <returns></returns>
        public static ServiceBusConnectionSettings Create(string namespaceHostName, TokenCredential tokenCredential, Azure.Messaging.ServiceBus.ServiceBusTransportType transportType = Azure.Messaging.ServiceBus.ServiceBusTransportType.AmqpTcp)
        {
            return new ServiceBusConnectionSettings
            {
                Endpoint = new Uri($"sb://{namespaceHostName}/"),
                TokenCredential = tokenCredential,
                TransportType = transportType
            };
        }

        /// <summary>
        /// Creates an instance of <see cref="ServiceBusConnectionSettings"/>
        /// </summary>
        /// <param name="serviceBusEndpoint">Service Bus endpoint</param>
        /// <param name="tokenCredential">Service Bus authentication token credential</param>
        /// <param name="transportType">Service Bus messaging protocol</param>
        /// <returns></returns>
        public static ServiceBusConnectionSettings Create(Uri serviceBusEndpoint, TokenCredential tokenCredential, Azure.Messaging.ServiceBus.ServiceBusTransportType transportType = Azure.Messaging.ServiceBus.ServiceBusTransportType.AmqpTcp)
        {
            return new ServiceBusConnectionSettings
            {
                Endpoint = serviceBusEndpoint,
                TokenCredential = tokenCredential,
                TransportType = transportType
            };
        }

#endif

        private ServiceBusConnectionSettings()
        {
        }

        /// <summary>
        /// Service Bus connection string
        /// </summary>
        public string ConnectionString { get; private set; }

#if NETSTANDARD2_0

        /// <summary>
        /// Service Bus endpoint
        /// </summary>
        public Uri Endpoint { get; private set; }

        /// <summary>
        /// Service Bus messaging protocol
        /// </summary>
        public Azure.Messaging.ServiceBus.ServiceBusTransportType TransportType { get; private set; }

        /// <summary>
        /// Azure.Identity TokenCredential used to authenticate with the service bus
        /// </summary>
        public TokenCredential TokenCredential { get; private set; }
#endif

    }
}
