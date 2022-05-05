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
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <summary>
    /// Represents a single listener for <see cref="StatefulService"/>
    /// </summary>
    public interface IServiceListener
    {
        /// <summary>
        /// Initialize required resources for service listener.
        /// </summary>
        /// <param name="statefulService">An instance of <see cref="StatefulService"/> which is creating this listener.</param>
        void Initialize(StatefulService statefulService);

        /// <summary>
        /// This method is called when the replica is being opened and it is the final step of opening the service.
        /// Override this method to be notified that Open has completed for this replica's internal components.
        /// </summary>
        /// <param name="openMode"><see cref="T:System.Fabric.ReplicaOpenMode" /> for this service replica.</param>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns> </returns>
        Task OnOpenAsync(ReplicaOpenMode openMode, CancellationToken cancellationToken);

        /// <summary>
        /// Creates an instance of <see cref="ServiceReplicaListener"/>.
        /// </summary>
        /// <returns>instance of <see cref="ServiceReplicaListener"/> </returns>
        ServiceReplicaListener CreateServiceReplicaListener();

        /// <summary>
        /// This method is invoked when the service is about to start.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns></returns>
        Task OnRunAsync(CancellationToken cancellationToken);

        /// <summary>
        /// This method is called when role of the replica is changing.
        /// </summary>
        /// <param name="newRole">New <see cref="T:System.Fabric.ReplicaRole" /> for this service replica.</param>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns></returns>
        Task OnChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken);

        /// <summary>
        /// This method is called as the final step of closing the service gracefully.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to monitor for cancellation requests.</param>
        /// <returns></returns>
        Task OnCloseAsync(CancellationToken cancellationToken);
    }
}
