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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Threading;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// Manages instances of a service fabric based store provider implementations
    /// for <see cref="DurableTask.IOrchestrationService"/> and <see cref="DurableTask.IOrchestrationServiceClient"/>
    /// to be used in constructing <see cref="DurableTask.TaskHubWorker"/> and <see cref="DurableTask.TaskHubClient"/>.
    /// </summary>
    /// <remarks>
    /// Use <see cref="FabricOrchestrationProviderFactory"/> to create an instance of <see cref="FabricOrchestrationProvider"/>.
    /// Note that this provider object should not be used once <see cref="DurableTask.TaskHubWorker.StopAsync()"/> method is called
    /// on the worker object created using this provider. A new provider object should be created after that point.
    /// </remarks>
    public sealed class FabricOrchestrationProvider : IDisposable
    {
        readonly FabricOrchestrationService orchestrationService;
        readonly FabricOrchestrationServiceClient orchestrationClient;
        readonly CancellationTokenSource cancellationTokenSource;

        internal FabricOrchestrationProvider(IReliableStateManager stateManager, FabricOrchestrationProviderSettings settings)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            var sessionsProvider = new SessionsProvider(stateManager, cancellationTokenSource.Token);
            var instanceStore = new FabricOrchestrationInstanceStore(stateManager, cancellationTokenSource.Token);
            this.orchestrationService = new FabricOrchestrationService(stateManager, sessionsProvider, instanceStore, settings, cancellationTokenSource);
            this.orchestrationClient = new FabricOrchestrationServiceClient(stateManager, sessionsProvider, instanceStore);
        }

        /// <summary>
        /// <see cref="DurableTask.IOrchestrationService"/> instance that can be used for constructing <see cref="DurableTask.TaskHubWorker"/>.
        /// </summary>
        public IOrchestrationService OrchestrationService
        {
            get
            {
                EnsureValidInstance();
                return this.orchestrationService;
            }
        }

        /// <summary>
        /// <see cref="DurableTask.IOrchestrationServiceClient"/> instance that can be used for constructing <see cref="DurableTask.TaskHubClient"/>.
        /// </summary>
        public IOrchestrationServiceClient OrchestrationServiceClient
        {
            get
            {
                EnsureValidInstance();
                return this.orchestrationClient;
            }
        }

        /// <summary>
        /// Disposes the object. The object should be disposed after <see cref="DurableTask.TaskHubWorker.StopAsync()"/>
        /// is invoked on the <see cref="DurableTask.TaskHubWorker" /> created with the <see cref="OrchestrationService"/>.
        /// </summary>
        public void Dispose()
        {
            this.cancellationTokenSource.Dispose();
        }

        void EnsureValidInstance()
        {
            if (this.cancellationTokenSource.IsCancellationRequested)
            {
                throw new InvalidOperationException($"{nameof(IOrchestrationService)} instance has been stopped. Discard this provider instance and create a new provider object.");
            }
        }
    }
}
