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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;

    internal sealed class TestOrchestrationHost : IDisposable
    {
        readonly TaskHubWorker worker;
        readonly TaskHubClient client;
        readonly HashSet<Type> addedOrchestrationTypes;
        readonly HashSet<Type> addedActivityTypes;

        public TestOrchestrationHost(AzureStorageOrchestrationService service)
        {
            this.worker = new TaskHubWorker(service);
            this.client = new TaskHubClient(service);
            this.addedOrchestrationTypes = new HashSet<Type>();
            this.addedActivityTypes = new HashSet<Type>();
        }

        public void Dispose()
        {
            this.worker.Dispose();
        }

        public Task StartAsync()
        {
            return this.worker.StartAsync();
        }

        public Task StopAsync()
        {
            return this.worker.StopAsync(isForced: true);
        }

        public async Task<TestOrchestrationClient> StartOrchestrationAsync(Type orchestrationType, object input)
        {
            if (!this.addedOrchestrationTypes.Contains(orchestrationType))
            {
                this.worker.AddTaskOrchestrations(orchestrationType);
                this.addedOrchestrationTypes.Add(orchestrationType);
            }

            // Allow orchestration types to declare which activity types they depend on.
            // CONSIDER: Make this a supported pattern in DTFx?
            KnownTypeAttribute[] knownActivityTypes =
                (KnownTypeAttribute[])orchestrationType.GetCustomAttributes(typeof(KnownTypeAttribute), false);

            foreach (KnownTypeAttribute referencedActivity in knownActivityTypes)
            {
                if (!this.addedActivityTypes.Contains(referencedActivity.Type))
                {
                    this.worker.AddTaskActivities(referencedActivity.Type);
                    this.addedActivityTypes.Add(referencedActivity.Type);
                }
            }

            DateTime creationTime = DateTime.UtcNow;
            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                orchestrationType,
                input);

            Trace.TraceInformation($"Started {orchestrationType.Name}, Instance ID = {instance.InstanceId}");
            return new TestOrchestrationClient(this.client, orchestrationType, instance.InstanceId, creationTime);
        }
    }
}
