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

namespace DurableTask.DocumentDb
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Tracking;

    public class DocumentOrchestrationServiceInstanceStore : IOrchestrationServiceInstanceStore
    {
        public int MaxHistoryEntryLength
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public Task<object> DeleteEntitesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            throw new NotImplementedException();
        }

        public Task<object> DeleteJumpStartEntitesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            throw new NotImplementedException();
        }

        public Task DeleteStoreAsync()
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitesAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitesAsync(int top)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            throw new NotImplementedException();
        }

        public Task InitializeStoreAsync(bool recreate)
        {
            throw new NotImplementedException();
        }

        public Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public Task<object> WriteEntitesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            throw new NotImplementedException();
        }

        public Task<object> WriteJumpStartEntitesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities)
        {
            throw new NotImplementedException();
        }
    }
}
