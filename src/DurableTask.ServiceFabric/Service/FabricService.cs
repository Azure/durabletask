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
    using System.Linq;
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
    public sealed class FabricService : StatefulService
    {
        readonly FabricOrchestrationProviderFactory fabricProviderFactory;
        readonly Dictionary<string, Type> knownOrchestrationTypeNames
            = new Dictionary<string, Type>(StringComparer.InvariantCultureIgnoreCase);
        readonly Dictionary<string, TaskOrchestration> knownOrchestrationInstances
            = new Dictionary<string, TaskOrchestration>(StringComparer.InvariantCultureIgnoreCase);

        /// <summary>
        /// 
        /// </summary>
        public FabricOrchestrationProvider FabricOrchestrationProvider { get; set; }

        IFabricServiceSettings serviceSettings;

        TaskHubWorker worker;
        TaskHubClient client;
        ReplicaRole currentRole;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="serviceSettings"></param>
        public FabricService(StatefulServiceContext context, IFabricServiceSettings serviceSettings) : base(context)
        {
            this.serviceSettings = serviceSettings ?? throw new ArgumentNullException(nameof(serviceSettings));

            var providerSettings = new FabricOrchestrationProviderSettings();
            providerSettings.TaskOrchestrationDispatcherSettings.DispatcherCount = this.serviceSettings.GetOrchestrationDispatcherCount();
            providerSettings.TaskActivityDispatcherSettings.DispatcherCount = this.serviceSettings.GetOrchestrationDispatcherCount();

            this.fabricProviderFactory = new FabricOrchestrationProviderFactory(this.StateManager, providerSettings);
            this.FabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
            this.client = new TaskHubClient(this.FabricOrchestrationProvider.OrchestrationServiceClient);
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
            return this.serviceSettings.GetServiceReplicaListeners();
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            if (this.FabricOrchestrationProvider == null)
            {
                this.FabricOrchestrationProvider = this.fabricProviderFactory.CreateProvider();
                this.client = new TaskHubClient(this.FabricOrchestrationProvider.OrchestrationServiceClient);
            }

            foreach (var type in this.serviceSettings.GetOrchestrationTypes())
            {
                this.knownOrchestrationTypeNames.Add(type.FullName, type);
            }

            foreach (var keyValuePair in this.serviceSettings.GetTaskOrchestrations())
            {
                this.knownOrchestrationInstances.Add(keyValuePair.Key, keyValuePair.Value);
            }

            this.worker = new TaskHubWorker(this.FabricOrchestrationProvider.OrchestrationService);

            await this.worker
                .AddTaskOrchestrations(this.knownOrchestrationInstances.Values.Select(instance => new DefaultObjectCreator<TaskOrchestration>(instance)).ToArray())
                .AddTaskOrchestrations(this.knownOrchestrationTypeNames.Values.ToArray())
                .AddTaskActivities(this.serviceSettings.GetActivityTypes().ToArray())
                .StartAsync();

            this.worker.TaskActivityDispatcher.IncludeDetails = true;
        }

        /// <summary>
        /// TODO: Add documentation.
        /// </summary>
        /// <param name="newRole"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected override async Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            ServiceEventSource.Tracing.ServiceRequestStart($"Fabric On Change Role Async, current role = {this.currentRole}, new role = {newRole}");
            if (newRole != ReplicaRole.Primary && this.currentRole == ReplicaRole.Primary)
            {
                await ShutdownAsync();
            }
            this.currentRole = newRole;
            ServiceEventSource.Tracing.ServiceRequestStop($"Fabric On Change Role Async, current role = {this.currentRole}");
        }

        /// <summary>
        /// TODO: Add documentaiton.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ServiceEventSource.Tracing.ServiceMessage(this, "OnCloseAsync - will shutdown primary if not already done");
            await ShutdownAsync();
        }

        async Task ShutdownAsync()
        {
            try
            {
                if (this.worker != null)
                {
                    ServiceEventSource.Tracing.ServiceMessage(this, "Stopping Taskhub Worker");
                    await this.worker.StopAsync(isForced: true);
                    this.worker.Dispose();
                    this.FabricOrchestrationProvider.Dispose();
                    this.FabricOrchestrationProvider = null;
                    this.worker = null;
                    ServiceEventSource.Tracing.ServiceMessage(this, "Stopped Taskhub Worker");
                }
            }
            catch (Exception e)
            {
                ServiceEventSource.Tracing.ServiceRequestFailed("Exception when Stopping Worker On Primary Stop", e.ToString());
                throw;
            }
        }

        // TODO: Delete?
        Type GetOrchestrationType(string typeName)
        {
            if (this.knownOrchestrationInstances.ContainsKey(typeName))
            {
                return this.knownOrchestrationInstances[typeName].GetType();
            }

            if (this.knownOrchestrationTypeNames.ContainsKey(typeName))
            {
                return this.knownOrchestrationTypeNames[typeName];
                
            }

            throw new Exception($"Unknown Orchestration Type Name : {typeName}");
        }
    }
}
