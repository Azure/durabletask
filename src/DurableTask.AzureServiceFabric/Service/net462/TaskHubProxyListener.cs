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

namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Fabric;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric;
    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <inheritdoc />
    public sealed class TaskHubProxyListener : TaskHubProxyListenerBase
    {
        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="context">stateful service context</param>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations">Delegate invoked before starting the worker.</param>
        [Obsolete]
        public TaskHubProxyListener(StatefulServiceContext context,
                FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations registerOrchestrations) : base(fabricOrchestrationProviderSettings, registerOrchestrations)
        {
        }

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations">Delegate invoked before starting the worker.</param>
        /// <param name="enableHttps">Whether to enable https or http</param>
        public TaskHubProxyListener(FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations registerOrchestrations,
                bool enableHttps = true): base(fabricOrchestrationProviderSettings, registerOrchestrations, enableHttps)
        {
        }

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <remarks>
        /// Use this constructor when there is a need to access <see cref="TaskHubClient"/>
        /// when registering orchestration artifacts with <see cref="TaskHubWorker"/>
        /// </remarks>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations2">Delegate invoked before starting the worker.</param>
        /// <param name="enableHttps">Whether to enable https or http</param>
        public TaskHubProxyListener(FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations2 registerOrchestrations2,
                bool enableHttps = true) : base(fabricOrchestrationProviderSettings, registerOrchestrations2, enableHttps)
        {
        }

        /// <inheritdoc />
        public override ServiceReplicaListener CreateServiceReplicaListener()
        {
            return new ServiceReplicaListener(context =>
            {
                var serviceEndpoint = context.CodePackageActivationContext.GetEndpoint(Constants.TaskHubProxyListenerEndpointName);
                string ipAddress = context.NodeContext.IPAddressOrFQDN;
#if DEBUG
                IPHostEntry entry = Dns.GetHostEntry(ipAddress);
                IPAddress ipv4Address = entry.AddressList.FirstOrDefault(
                    address => (address.AddressFamily == AddressFamily.InterNetwork) && (!IPAddress.IsLoopback(address)));
                ipAddress = ipv4Address.ToString();
#endif

                EnsureFabricOrchestrationProviderIsInitialized();
                string protocol = this.enableHttps ? "https" : "http";
                string listeningAddress = string.Format(CultureInfo.InvariantCulture, "{0}://{1}:{2}/{3}/dtfx/", protocol, ipAddress, serviceEndpoint.Port, context.PartitionId);

                return new OwinCommunicationListener(new Startup(listeningAddress, this.fabricOrchestrationProvider));
            }, Constants.TaskHubProxyServiceName);
        }
    }
}
