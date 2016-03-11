﻿//  ----------------------------------------------------------------------------------
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
    using System.Threading;
    using System.Threading.Tasks;

    public interface IOrchestrationServiceClient
    {
        Task CreateTaskOrchestrationAsync(TaskMessage creationMessage);

        Task SendTaskOrchestrationMessage(TaskMessage message);

        Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId, 
            string executionId,
            TimeSpan timeout, 
            CancellationToken cancellationToken);

        Task ForceTerminateTaskOrchestrationAsync(string instanceId);

        Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions);

        Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId);

        Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId);

        Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType);
    }
}