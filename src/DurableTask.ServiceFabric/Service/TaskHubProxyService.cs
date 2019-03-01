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

namespace DurableTask.ServiceFabric.Service
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.ServiceFabric;
    using DurableTask.ServiceFabric.Tracing;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class TaskHubProxyService : StatefulService
    {
        readonly FabricOrchestrationProviderFactory fabricProviderFactory;
        readonly IOrchestrationsProvider orchestrationsProvider;
        FabricOrchestrationProvider fabricOrchestrationProvider;
        TaskHubWorker worker;
        ReplicaRole currentRole;

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyService"/>
        /// </summary>
        /// <param name="context">stateful service context</param>
        /// <param name="orchestrationsProvider">Orchestrations provider</param>
        public TaskHubProxyService(StatefulServiceContext context, IOrchestrationsProvider orchestrationsProvider) : base(context)
        {
            this.orchestrationsProvider = orchestrationsProvider ?? throw new ArgumentNullException(nameof(orchestrationsProvider));

            var providerSettings = this.orchestrationsProvider.GetFabricOrchestrationProviderSettings();
            this.fabricProviderFactory = new FabricOrchestrationProviderFactory(this.StateManager, providerSettings);
            this.fabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
        }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see http://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            yield return new ServiceReplicaListener(context =>
            {
                var serviceEndpoint = context.CodePackageActivationContext.GetEndpoint("ServiceEndpoint");
                int port = serviceEndpoint.Port;

                string ipAddress = context.NodeContext.IPAddressOrFQDN;
#if DEBUG
                IPHostEntry entry = Dns.GetHostEntry(ipAddress);
                IPAddress ipv4Address = entry.AddressList.FirstOrDefault(
                    address => (address.AddressFamily == AddressFamily.InterNetwork) && (!IPAddress.IsLoopback(address)));
                ipAddress = ipv4Address.ToString();
#endif

                string listeningAddress = String.Format(CultureInfo.InvariantCulture, "http://{0}:{1}/", ipAddress, port);
                return new OwinCommunicationListener(new Startup(listeningAddress, this.fabricOrchestrationProvider));
            });
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            if (this.fabricOrchestrationProvider == null)
            {
                this.fabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
            }

            this.worker = new TaskHubWorker(this.fabricOrchestrationProvider.OrchestrationService);

            await this.orchestrationsProvider.RegisterOrchestrationArtifactsAsync(this.worker);

            await this.worker.StartAsync();

            this.worker.TaskActivityDispatcher.IncludeDetails = true;
        }

        /// <summary>
        /// Handles node's role change.
        /// </summary>
        /// <param name="newRole">New <see cref="ReplicaRole" /> for this service replica.</param>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns>
        /// A <see cref="Task" /> that represents outstanding operation.
        /// </returns>
        protected override async Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            ProviderEventSource.Tracing.FabricServiceRequestStart($"Fabric On Change Role Async, current role = {this.currentRole}, new role = {newRole}");
            if (newRole != ReplicaRole.Primary && this.currentRole == ReplicaRole.Primary)
            {
                await ShutdownAsync();
            }
            this.currentRole = newRole;
            ProviderEventSource.Tracing.FabricServiceRequestStop($"Fabric On Change Role Async, current role = {this.currentRole}");
        }

        /// <summary>
        /// Handles OnCloseAsync event, shuts down the service.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> A <see cref="Task">Task</see> that represents outstanding operation. </returns>
        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ProviderEventSource.Tracing.ServiceMessage(this, "OnCloseAsync - will shutdown primary if not already done");
            await ShutdownAsync();
        }

        async Task ShutdownAsync()
        {
            try
            {
                if (this.worker != null)
                {
                    ProviderEventSource.Tracing.ServiceMessage(this, "Stopping Taskhub Worker");
                    await this.worker.StopAsync(isForced: true);
                    this.worker.Dispose();
                    this.fabricOrchestrationProvider.Dispose();
                    this.fabricOrchestrationProvider = null;
                    this.worker = null;
                    ProviderEventSource.Tracing.ServiceMessage(this, "Stopped Taskhub Worker");
                }
            }
            catch (Exception e)
            {
                ProviderEventSource.Tracing.ServiceRequestFailed("Exception when Stopping Worker On Primary Stop", e.ToString());
                throw;
            }
        }
    }
}
