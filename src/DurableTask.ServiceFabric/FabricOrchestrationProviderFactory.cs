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
    using Microsoft.ServiceFabric.Data;

    public class FabricOrchestrationProviderFactory
    {
        readonly IReliableStateManager stateManager;
        readonly object @lock;

        SessionsProvider sessionsProvider;
        FabricOrchestrationService orchestrationService;
        FabricOrchestrationServiceClient orchestrationClient;
        FabricOrchestrationInstanceStore instanceStore;

        public FabricOrchestrationProviderFactory(IReliableStateManager stateManager)
        {
            this.stateManager = stateManager;
            @lock = new object();
        }

        public IOrchestrationService OrchestrationService
        {
            get
            {
                this.Initialize();
                return this.orchestrationService;
            }
        }

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
                    this.orchestrationService = new FabricOrchestrationService(this.stateManager, this.sessionsProvider, this.instanceStore);
                    this.orchestrationClient = new FabricOrchestrationServiceClient(this.stateManager, this.sessionsProvider, this.instanceStore);
                }
            }
        }
    }
}
