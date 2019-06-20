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
    public sealed class TaskHubProxyListener : IServiceListener
    {
        readonly IOrchestrationsProvider orchestrationsProvider;

        FabricOrchestrationProviderFactory fabricProviderFactory;
        FabricOrchestrationProvider fabricOrchestrationProvider;
        TaskHubWorker worker;
        ReplicaRole currentRole;
        StatefulService statefulService;

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="context">stateful service context</param>
        /// <param name="orchestrationsProvider">Orchestrations provider</param>
        public TaskHubProxyListener(StatefulServiceContext context, IOrchestrationsProvider orchestrationsProvider)
        {
            this.orchestrationsProvider = orchestrationsProvider ?? throw new ArgumentNullException(nameof(orchestrationsProvider));
        }

        private void EnsureFabricOrchestrationProvider()
        {
            if (this.fabricOrchestrationProvider == null)
            {
                this.fabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
            }
        }

        /// <summary>
        /// Handles node's role change.
        /// </summary>
        /// <param name="newRole">New <see cref="ReplicaRole" /> for this service replica.</param>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns>
        /// A <see cref="Task" /> that represents outstanding operation.
        /// </returns>
        public async Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, $"Fabric On Change Role Async, current role = {this.currentRole}, new role = {newRole}");
            if (newRole != ReplicaRole.Primary && this.currentRole == ReplicaRole.Primary)
            {
                await ShutdownAsync();
            }
            this.currentRole = newRole;
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, $"Fabric On Change Role Async, current role = {this.currentRole}");
        }

        /// <summary>
        /// Handles OnCloseAsync event, shuts down the service.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> A <see cref="Task">Task</see> that represents outstanding operation. </returns>
        public async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, "OnCloseAsync - will shutdown primary if not already done");
            await ShutdownAsync();
        }

        async Task ShutdownAsync()
        {
            try
            {
                if (this.worker != null)
                {
                    ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, "Stopping Taskhub Worker");
                    await this.worker.StopAsync(isForced: true);
                    this.worker.Dispose();
                    this.worker = null;
                    this.fabricOrchestrationProvider.Dispose();
                    this.fabricOrchestrationProvider = null;
                    ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, "Stopped Taskhub Worker");
                }
            }
            catch (Exception e)
            {
                ServiceFabricProviderEventSource.Tracing.ServiceRequestFailed("Exception when Stopping Worker On Primary Stop", e.ToString());
                throw;
            }
        }

        /// <inheritdoc />
        public ServiceReplicaListener CreateServiceReplicaListener()
        {
            var providerSettings = this.orchestrationsProvider.GetFabricOrchestrationProviderSettings();
            this.fabricProviderFactory = new FabricOrchestrationProviderFactory(statefulService.StateManager, providerSettings);
            this.fabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();

            return new ServiceReplicaListener(context =>
            {
                var serviceEndpoint = context.CodePackageActivationContext.GetEndpoint(Constants.TaskHubProxyListenerEndpointName);
                int port = serviceEndpoint.Port;

                string ipAddress = context.NodeContext.IPAddressOrFQDN;
#if DEBUG
                IPHostEntry entry = Dns.GetHostEntry(ipAddress);
                IPAddress ipv4Address = entry.AddressList.FirstOrDefault(
                    address => (address.AddressFamily == AddressFamily.InterNetwork) && (!IPAddress.IsLoopback(address)));
                ipAddress = ipv4Address.ToString();
#endif

                EnsureFabricOrchestrationProvider();
                string listeningAddress = String.Format(CultureInfo.InvariantCulture, "http://{0}:{1}/{2}/dtfx/", ipAddress, port, context.PartitionId);
                return new OwinCommunicationListener(new Startup(listeningAddress, this.fabricOrchestrationProvider));
            }, Constants.TaskHubProxyServiceName);
        }

        /// <inheritdoc />
        public async Task OnRunAsync(CancellationToken cancellationToken)
        {
            try
            {
                EnsureFabricOrchestrationProvider();

                this.worker = new TaskHubWorker(this.fabricOrchestrationProvider.OrchestrationService);

                await this.orchestrationsProvider.RegisterOrchestrationArtifactsAsync(this.worker);

                await this.worker.StartAsync();

                this.worker.TaskActivityDispatcher.IncludeDetails = true;
            }
            catch (Exception exception)
            {
                ServiceFabricProviderEventSource.Tracing.ServiceRequestFailed("RunAsync failed", $"Exception Details Type: {exception.GetType()}, Message: {exception.Message}, StackTrace: {exception.StackTrace}");
            }
        }

        /// <inheritdoc />
        public void Initialize(StatefulService statefulService)
        {
            this.statefulService = statefulService;
        }
    }
}
