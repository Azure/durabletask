﻿//  ----------------------------------------------------------------------------------
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

    /// <summary>
    /// Delegate invoked before starting the worker to register orchestrations.
    /// </summary>
    /// <param name="taskHubWorker">Instance of <see cref="TaskHubWorker"/></param>
    public delegate void RegisterOrchestrations(TaskHubWorker taskHubWorker);

    /// <summary>
    /// Delegate invoked before starting the worker to register artifiacts with TaskHubWorker.
    /// Provides an instance to <see cref="TaskHubClient"/> which can be optionally used
    /// when registering delegates
    /// </summary>
    /// <remarks>
    /// The provided <see cref="TaskHubClient"/> is only connected to the local instance of
    /// <see cref="TaskHubWorker"/>. It cannot be used to query orchestrations running in other
    /// worker instances.
    /// </remarks>
    /// <param name="taskHubWorker">Instance of <see cref="TaskHubWorker"/></param>
    /// <param name="taskHubLocalClient">Instance of <see cref="TaskHubClient"/> connected to local instance of <see cref="TaskHubWorker"/></param>
    public delegate void RegisterOrchestrations2(TaskHubWorker taskHubWorker, TaskHubClient taskHubLocalClient);

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// Listening on HTTP port will expose security risk, use this listener at your own discretation.
    /// </summary>
    public sealed class TaskHubProxyListener : IServiceListener
    {
        readonly RegisterOrchestrations registerOrchestrations;
        readonly RegisterOrchestrations2 registerOrchestrations2;
        readonly FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings;
        FabricOrchestrationProviderFactory fabricProviderFactory;
        FabricOrchestrationProvider fabricOrchestrationProvider;
        TaskHubWorker worker;
        TaskHubClient localClient;
        ReplicaRole currentRole;
        StatefulService statefulService;
        bool enableHttps = true;

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="context">stateful service context</param>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations">Delegate invoked before starting the worker.</param>
        [Obsolete]
        public TaskHubProxyListener(StatefulServiceContext context,
                FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations registerOrchestrations)
        {
            this.fabricOrchestrationProviderSettings = fabricOrchestrationProviderSettings ?? throw new ArgumentNullException(nameof(fabricOrchestrationProviderSettings));
            this.registerOrchestrations = registerOrchestrations ?? throw new ArgumentNullException(nameof(registerOrchestrations));
        }

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="fabricOrchestrationProviderSettings">instance of <see cref="FabricOrchestrationProviderSettings"/></param>
        /// <param name="registerOrchestrations">Delegate invoked before starting the worker.</param>
        /// <param name="enableHttps">Whether to enable https or http</param>
        public TaskHubProxyListener(FabricOrchestrationProviderSettings fabricOrchestrationProviderSettings,
                RegisterOrchestrations registerOrchestrations,
                bool enableHttps = true)
        {
            this.fabricOrchestrationProviderSettings = fabricOrchestrationProviderSettings ?? throw new ArgumentNullException(nameof(fabricOrchestrationProviderSettings));
            this.registerOrchestrations = registerOrchestrations ?? throw new ArgumentNullException(nameof(registerOrchestrations));
            this.enableHttps = enableHttps;
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
                bool enableHttps = true)
        {
            this.fabricOrchestrationProviderSettings = fabricOrchestrationProviderSettings ?? throw new ArgumentNullException(nameof(fabricOrchestrationProviderSettings));
            this.registerOrchestrations2 = registerOrchestrations2 ?? throw new ArgumentNullException(nameof(registerOrchestrations2));
            this.enableHttps = enableHttps;
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
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, $"TaskHubProxyListener OnChangeRoleAsync, current role = {this.currentRole}, new role = {newRole}");
            if (newRole != ReplicaRole.Primary && this.currentRole == ReplicaRole.Primary)
            {
                await StopAsync();
            }
            this.currentRole = newRole;
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, $"TaskHubProxyListener OnChangeRoleAsync, current role = {this.currentRole}");
        }

        /// <summary>
        /// Handles OnCloseAsync event, shuts down the service.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> A <see cref="Task">Task</see> that represents outstanding operation. </returns>
        public async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, "OnCloseAsync - will shutdown primary if not already done");
            await StopAsync();
        }

        /// <inheritdoc />
        public ServiceReplicaListener CreateServiceReplicaListener()
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

        /// <inheritdoc />
        public async Task OnRunAsync(CancellationToken cancellationToken)
        {
            await StartAsync();
        }

        /// <inheritdoc />
        public void Initialize(StatefulService statefulService)
        {
            this.statefulService = statefulService;
        }

        /// <inheritdoc />
        public Task OnOpenAsync(ReplicaOpenMode openMode, CancellationToken cancellationToken)
        {
            this.fabricProviderFactory = new FabricOrchestrationProviderFactory(statefulService.StateManager, this.fabricOrchestrationProviderSettings);
            return Task.CompletedTask;
        }

        async Task StartAsync()
        {
            try
            {
                EnsureFabricOrchestrationProviderIsInitialized();

                this.worker = new TaskHubWorker(this.fabricOrchestrationProvider.OrchestrationService, this.fabricOrchestrationProviderSettings.LoggerFactory);

                if (this.registerOrchestrations2 != null)
                {
                    this.localClient = new TaskHubClient(this.fabricOrchestrationProvider.OrchestrationServiceClient, loggerFactory: this.fabricOrchestrationProviderSettings.LoggerFactory);
                    this.registerOrchestrations2(this.worker, this.localClient);
                }
                else
                {
                    this.registerOrchestrations(this.worker);
                }

                await this.worker.StartAsync();

                this.worker.TaskActivityDispatcher.IncludeDetails = true;
            }
            catch (Exception exception)
            {
                ServiceFabricProviderEventSource.Tracing.ServiceRequestFailed("RunAsync failed", $"Exception Details Type: {exception.GetType()}, Message: {exception.Message}, StackTrace: {exception.StackTrace}");
                throw;
            }
        }

        async Task StopAsync()
        {
            try
            {
                if (this.worker != null)
                {
                    ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this.statefulService, "Stopping Taskhub Worker");
                    await this.worker.StopAsync(isForced: true);
                    this.worker.Dispose();
                    this.worker = null;
                    this.localClient = null;
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

        private void EnsureFabricOrchestrationProviderIsInitialized()
        {
            if (this.fabricOrchestrationProvider == null)
            {
                this.fabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
            }
        }
    }
}
