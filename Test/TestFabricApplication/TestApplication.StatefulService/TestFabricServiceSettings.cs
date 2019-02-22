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

namespace TestApplication.StatefulService
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Globalization;

    using DurableTask.Core;
    using DurableTask.ServiceFabric;
    using DurableTask.ServiceFabric.Service;
    using DurableTask.Test.Orchestrations.Performance;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using TestApplication.Common.Orchestrations;


    /// <inheritdoc/>
    public class TestFabricServiceSettings : IFabricServiceSettings
    {

        public FabricOrchestrationProvider FabricOrchestrationProvider { get; set; }

        /// <inheritdoc/>
        public int GetActivityDispatcherCount()
        {
            return 5;
        }

        /// <inheritdoc/>
        public IEnumerable<Type> GetActivityTypes()
        {
            return new Type[]
            {
                typeof(GetUserTask),
                typeof(GreetUserTask),
                typeof(GenerationBasicTask),
                typeof(RandomTimeWaitingTask),
                typeof(ExceptionThrowingTask),
                typeof(ExecutionCountingActivity)
            };
        }

        /// <inheritdoc/>
        public int GetOrchestrationDispatcherCount()
        {
            return 5;
        }

        /// <inheritdoc/>
        public IEnumerable<Type> GetOrchestrationTypes()
        {
            return new Type[]
            {
                typeof(SimpleOrchestrationWithTasks),
                typeof(SimpleOrchestrationWithTimer),
                typeof(GenerationBasicOrchestration),
                typeof(SimpleOrchestrationWithSubOrchestration),
                typeof(DriverOrchestration),
                typeof(TestOrchestration),
                typeof(ExecutionCountingOrchestration)
            };
        }

        /// <inheritdoc/>
        public IEnumerable<ServiceReplicaListener> GetServiceReplicaListeners()
        {
            yield return new ServiceReplicaListener(context =>
            {
                var serviceEndpoint = context.CodePackageActivationContext.GetEndpoint("ServiceEndpoint");
                int port = serviceEndpoint.Port;

                string listeningAddress = String.Format(CultureInfo.InvariantCulture, "http://+:{0}/", port)
                                            .Replace("+", FabricRuntime.GetNodeContext().IPAddressOrFQDN);
                return new OwinCommunicationListener(new Startup(listeningAddress, this.FabricOrchestrationProvider));
            });
        }

        /// <inheritdoc/>
        public IEnumerable<KeyValuePair<string, TaskOrchestration>> GetTaskOrchestrations()
        {
            yield return new KeyValuePair<string, TaskOrchestration>(typeof(OrchestrationRunningIntoRetry).Name, new OrchestrationRunningIntoRetry());
        }
    }
}
