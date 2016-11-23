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
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// Factory to create instances of a service fabric based store provider implementations
    /// for <see cref="DurableTask.IOrchestrationService"/> and <see cref="DurableTask.IOrchestrationServiceClient"/>
    /// to be used in constructing <see cref="DurableTask.TaskHubWorker"/> and <see cref="DurableTask.TaskHubClient"/>.
    /// </summary>
    public class FabricOrchestrationProviderFactory
    {
        readonly IReliableStateManager stateManager;
        readonly object @lock;
        readonly FabricOrchestrationProviderSettings settings;

        SessionsProvider sessionsProvider;
        FabricOrchestrationService orchestrationService;
        FabricOrchestrationServiceClient orchestrationClient;
        FabricOrchestrationInstanceStore instanceStore;

        /// <summary>
        /// Constructor that uses default <see cref="FabricOrchestrationProviderSettings"/>.
        /// </summary>
        /// <param name="stateManager">Reliable state manager instance. Comes from service fabric stateful service implementation.</param>
        public FabricOrchestrationProviderFactory(IReliableStateManager stateManager)
            : this(stateManager, new FabricOrchestrationProviderSettings())
        {
        }

        /// <summary>
        /// Constructor that takes the custom <see cref="FabricOrchestrationProviderSettings"/>.
        /// </summary>
        /// <param name="stateManager">Reliable state manager instance. Comes from service fabric stateful service implementation.</param>
        /// <param name="settings">Settings to be used for the provider. Refer to <see cref="FabricOrchestrationProviderSettings"/> for documentation about specific settings.</param>
        public FabricOrchestrationProviderFactory(IReliableStateManager stateManager, FabricOrchestrationProviderSettings settings)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
            @lock = new object();
            this.settings = settings ?? new FabricOrchestrationProviderSettings();
        }

        /// <summary>
        /// <see cref="DurableTask.IOrchestrationService"/> instance that can be used for constructing <see cref="DurableTask.TaskHubWorker"/>.
        /// </summary>
        public IOrchestrationService OrchestrationService
        {
            get
            {
                this.Initialize();
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
                this.Initialize();
                return this.orchestrationClient;
            }
        }

        void Initialize()
        {
            lock (@lock)
            {
                if (this.orchestrationService == null)
                {
                    this.sessionsProvider = new SessionsProvider(this.stateManager);
                    this.instanceStore = new FabricOrchestrationInstanceStore(this.stateManager);
                    this.orchestrationService = new FabricOrchestrationService(this.stateManager, this.sessionsProvider, this.instanceStore, this.settings);
                    this.orchestrationClient = new FabricOrchestrationServiceClient(this.stateManager, this.sessionsProvider, this.instanceStore);
                }
            }
        }
    }
}
