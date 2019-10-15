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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.Core.Serializing;
    using DurableTask.AzureServiceFabric.Stores;
    using Microsoft.ServiceFabric.Data;
    using Newtonsoft.Json;

    internal class FabricProviderClient : IFabricProviderClient
    {
        readonly IReliableStateManager stateManager;
        readonly SessionProvider orchestrationProvider;
        readonly JsonDataConverter FormattingConverter = new JsonDataConverter(new JsonSerializerSettings() { Formatting = Formatting.Indented });

        public FabricProviderClient(IReliableStateManager stateManager, SessionProvider orchestrationProvider)
        {
            this.stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
            this.orchestrationProvider = orchestrationProvider ?? throw new ArgumentNullException(nameof(orchestrationProvider));
        }

        public async Task<IEnumerable<OrchestrationInstance>> GetRunningOrchestrationsAsync()
        {
            var sessions = await this.orchestrationProvider.GetSessions();
            return sessions.Select(s => s.SessionId);
        }

        public async Task<string> GetOrchestrationRuntimeStateAsync(string instanceId)
        {
            var session = await this.orchestrationProvider.GetSession(instanceId);
            if (session == null)
            {
                throw new ArgumentException($"There is no running or pending Orchestration with the instanceId {instanceId}");
            }
            return FormattingConverter.Serialize(session.SessionState);
        }
    }
}
