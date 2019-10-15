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
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// Factory to create instances of <see cref="FabricOrchestrationProvider"/>.
    /// </summary>
    public class FabricOrchestrationProviderFactory
    {
        readonly IReliableStateManager stateManager;
        readonly FabricOrchestrationProviderSettings settings;

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
            this.settings = settings ?? new FabricOrchestrationProviderSettings();
        }

        /// <summary>
        /// Creates a new <see cref="FabricOrchestrationProvider"/> object using the factory's state manager and settings.
        /// </summary>
        public FabricOrchestrationProvider CreateProvider()
        {
            return new FabricOrchestrationProvider(this.stateManager, this.settings);
        }
    }
}
