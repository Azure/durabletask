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

using System;
using System.Threading.Tasks;
using DurableTask;
using DurableTask.Test.Orchestrations.Perf;
using Microsoft.ServiceFabric.Services.Remoting;

namespace TestApplication.Common
{
    public interface IRemoteClient : IService
    {
        Task<OrchestrationState> RunOrchestrationAsync(string orchestrationTypeName, object input, TimeSpan waitTimeout);

        Task<OrchestrationState> RunDriverOrchestrationAsync(DriverOrchestrationData input, TimeSpan waitTimeout);

        Task<OrchestrationInstance> StartTestOrchestrationAsync(TestOrchestrationData input);

        Task<OrchestrationState> GetOrchestrationState(OrchestrationInstance instance);

        Task<OrchestrationState> WaitForOrchestration(OrchestrationInstance instance, TimeSpan waitTimeout);

        Task PurgeOrchestrationHistoryEventsAsync();
    }
}
