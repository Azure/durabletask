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

namespace DurableTask
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Tracking;

    public interface IOrchestrationServiceHistoryProvider
    {
        int MaxHistoryEntryLength();

        Task<object> WriteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities);

        Task<object> DeleteEntitesAsync(IEnumerable<OrchestrationHistoryEvent> entities);

        Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(string instanceId, string executionId);

        Task<IEnumerable<OrchestrationHistoryEvent>> ReadOrchestrationHistoryEventsAsync(string instanceId, bool allInstances);

        Task PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }
}
