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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Threading;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric.Stores;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// Manages instances of a service fabric based store provider implementations
    /// for <see cref="IOrchestrationService"/> and <see cref="IOrchestrationServiceClient"/>
    /// to be used in constructing <see cref="TaskHubWorker"/> and <see cref="TaskHubClient"/>.
    /// </summary>
    /// <remarks>
    /// Use <see cref="FabricOrchestrationProviderFactory"/> to create an instance of <see cref="FabricOrchestrationProvider"/>.
    /// Note that this provider object should not be used once <see cref="TaskHubWorker.StopAsync()"/> method is called
    /// on the worker object created using this provider. A new provider object should be created after that point.
    /// </remarks>
    public sealed class FabricOrchestrationProvider : IDisposable
    {
        readonly FabricOrchestrationService orchestrationService;
        readonly FabricOrchestrationServiceClient orchestrationClient;
        readonly FabricProviderClient fabricProviderClient;
        readonly CancellationTokenSource cancellationTokenSource;

        internal FabricOrchestrationProvider(IReliableStateManager stateManager, FabricOrchestrationProviderSettings settings)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            var sessionProvider = new SessionProvider(stateManager, cancellationTokenSource.Token);
            var instanceStore = new FabricOrchestrationInstanceStore(stateManager, cancellationTokenSource.Token);
            this.orchestrationService = new FabricOrchestrationService(stateManager, sessionProvider, instanceStore, settings, cancellationTokenSource);
            this.orchestrationClient = new FabricOrchestrationServiceClient(stateManager, sessionProvider, instanceStore);
            this.fabricProviderClient = new FabricProviderClient(stateManager, sessionProvider);
        }

        /// <summary>
        /// <see cref="IOrchestrationService"/> instance that can be used for constructing <see cref="TaskHubWorker"/>.
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
        /// <see cref="IOrchestrationServiceClient"/> instance that can be used for constructing <see cref="TaskHubClient"/>.
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
        /// Disposes the object. The object should be disposed after <see cref="TaskHubWorker.StopAsync()"/>
        /// is invoked on the <see cref="TaskHubWorker" /> created with the <see cref="OrchestrationService"/>.
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
