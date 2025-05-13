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
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using DurableTask.AzureServiceFabric;
    using DurableTask.Core;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Server.Kestrel.Core;
    using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    /// <inheritdoc />
    public sealed class TaskHubProxyListener : TaskHubProxyListenerBase
    {
        readonly Action<KestrelServerOptions> options;

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations">Delegate invoked before starting the worker.</param>
        /// <param name="options">Kestrel Server Options.</param>
        /// <param name="enableHttps">Whether to enable https or http</param>
        public TaskHubProxyListener(FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations registerOrchestrations, Action<KestrelServerOptions> options,
                bool enableHttps = true) : base(fabricOrchestrationProviderSettings, registerOrchestrations, enableHttps)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
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
        /// <param name="options"></param>
        /// <param name="enableHttps">Whether to enable https or http</param>
        public TaskHubProxyListener(FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations2 registerOrchestrations2, Action<KestrelServerOptions> options,
                bool enableHttps = true) : base(fabricOrchestrationProviderSettings, registerOrchestrations2, enableHttps)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc />
        public override ServiceReplicaListener CreateServiceReplicaListener()
        {
            return new ServiceReplicaListener(context =>
                new KestrelCommunicationListener(context, (url, listener) =>
                {
                    string ipAddress = context.NodeContext.IPAddressOrFQDN;
                    var serviceEndpoint = context.CodePackageActivationContext.GetEndpoint(Constants.TaskHubProxyListenerEndpointName);
#if DEBUG
                    IPHostEntry entry = Dns.GetHostEntry(ipAddress);
                    IPAddress ipv4Address = entry.AddressList.FirstOrDefault(
                        address => (address.AddressFamily == AddressFamily.InterNetwork) && (!IPAddress.IsLoopback(address)));
                    ipAddress = ipv4Address.ToString();
#endif

                    EnsureFabricOrchestrationProviderIsInitialized();
                    string protocol = this.enableHttps ? "https" : "http";
                    // Port 0 is used for requesting system allocated dynamic port
                    string listeningAddress = string.Format(CultureInfo.InvariantCulture, "{0}://{1}:{2}/", protocol, ipAddress, serviceEndpoint.Port);

                    return new WebHostBuilder().UseKestrel(options)
                    .UseContentRoot(Directory.GetCurrentDirectory())
                    // The UseUniqueServiceUrl option injects "/partitionId/instanceId"
                    // to the end of the service fabric service endpoint as a unique identifier
                    // Example Endpoint: https://{MachineName}:{port}/{partitionId}/{instanceId}
                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.None)
                    .UseStartup(x => new Startup(this.fabricOrchestrationProvider))
                    .UseUrls(listeningAddress)
                    .Build();
                }),
                Constants.TaskHubProxyServiceName);
        }


    }
}
