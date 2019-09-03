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
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class TaskHubStatefulService : StatefulService
    {
        readonly IList<IServiceListener> serviceListeners;
        ReplicaRole currentRole;

        /// <summary>
        /// Creates instance of <see cref="TaskHubProxyListener"/>
        /// </summary>
        /// <param name="context">stateful service context</param>
        /// <param name="serviceListeners">List of <see cref="IServiceListener"/></param>
        public TaskHubStatefulService(StatefulServiceContext context, IList<IServiceListener> serviceListeners) : base(context)
        {
            this.serviceListeners = serviceListeners ?? throw new ArgumentNullException(nameof(serviceListeners));
            if (this.serviceListeners.Count < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(serviceListeners), "At least one service should be present");
            }

            foreach (var listener in this.serviceListeners)
            {
                listener.Initialize(this);
            }
        }

        /// <summary>
        /// This method is called when the replica is being opened and it is the final step of opening the service.
        /// Override this method to be notified that Open has completed for this replica's internal components.
        /// </summary>
        /// <param name="openMode"><see cref="T:System.Fabric.ReplicaOpenMode" /> for this service replica.</param>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> </returns>
        protected override async Task OnOpenAsync(ReplicaOpenMode openMode, CancellationToken cancellationToken)
        {
            foreach (var listener in this.serviceListeners)
            {
                await listener.OnOpenAsync(openMode, cancellationToken);
            }
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
            return serviceListeners.Select(listener => listener.CreateServiceReplicaListener()).Where(listener => listener != null);
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                foreach(var listener in this.serviceListeners)
                {
                    await listener.OnRunAsync(cancellationToken);
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(1000);
                }
            }
            catch (Exception exception)
            {
                ServiceFabricProviderEventSource.Tracing.ServiceRequestFailed("RunAsync failed", $"Exception Details Type: {exception.GetType()}, Message: {exception.Message}, StackTrace: {exception.StackTrace}");
                throw new FabricException("Exception in RunAsync", exception);
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
        protected override async Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this, $"Fabric On Change Role Async, current role = {this.currentRole}, new role = {newRole}");
            foreach (var listener in this.serviceListeners)
            {
                await listener.OnChangeRoleAsync(newRole, cancellationToken);
            }
            this.currentRole = newRole;
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this, $"Fabric On Change Role Async, current role = {this.currentRole}");
        }

        /// <summary>
        /// Handles OnCloseAsync event, shuts down the service.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> A <see cref="Task">Task</see> that represents outstanding operation. </returns>
        protected override async Task OnCloseAsync(CancellationToken cancellationToken)
        {
            ServiceFabricProviderEventSource.Tracing.LogFabricServiceInformation(this, "OnCloseAsync - will shutdown primary if not already done");
            foreach (var listener in this.serviceListeners)
            {
                await listener.OnCloseAsync(cancellationToken);
            }
        }
    }
}
