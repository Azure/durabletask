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

namespace Correlation.Samples
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage;
    using DurableTask.Core;

    internal sealed class TestOrchestrationHost : IDisposable
    {
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly TaskHubWorker worker;
        readonly TaskHubClient client;
        readonly HashSet<Type> addedOrchestrationTypes;
        readonly HashSet<Type> addedActivityTypes;

        public TestOrchestrationHost(AzureStorageOrchestrationServiceSettings settings)
        {
            try
            {
                var service = new AzureStorageOrchestrationService(settings);
                service.CreateAsync().GetAwaiter().GetResult(); // I change Create to CreateIfNotExistsAsync for enabling execute without fail once per twice.

                this.settings = settings;

                this.worker = new TaskHubWorker(service);
                this.client = new TaskHubClient(service);
                this.addedOrchestrationTypes = new HashSet<Type>();
                this.addedActivityTypes = new HashSet<Type>();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public string TaskHub => this.settings.TaskHubName;

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

        public async Task<TestOrchestrationClient> StartOrchestrationAsync(
            Type orchestrationType,
            object input,
            string instanceId = null)
        {
            if (!this.addedOrchestrationTypes.Contains(orchestrationType))
            {
                this.worker.AddTaskOrchestrations(orchestrationType);
                this.addedOrchestrationTypes.Add(orchestrationType);
            }

            // Allow orchestration types to declare which activity types they depend on.
            // CONSIDER: Make this a supported pattern in DTFx?
            KnownTypeAttribute[] knownTypes =
                (KnownTypeAttribute[])orchestrationType.GetCustomAttributes(typeof(KnownTypeAttribute), false);

            foreach (KnownTypeAttribute referencedKnownType in knownTypes)
            {
                bool orch = referencedKnownType.Type.IsSubclassOf(typeof(TaskOrchestration));
                bool activ = referencedKnownType.Type.IsSubclassOf(typeof(TaskActivity));
                if (orch && !this.addedOrchestrationTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskOrchestrations(referencedKnownType.Type);
                    this.addedOrchestrationTypes.Add(referencedKnownType.Type);
                }

                else if (activ && !this.addedActivityTypes.Contains(referencedKnownType.Type))
                {
                    this.worker.AddTaskActivities(referencedKnownType.Type);
                    this.addedActivityTypes.Add(referencedKnownType.Type);
                }
            }

            DateTime creationTime = DateTime.UtcNow;
            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(
                orchestrationType,
                instanceId,
                input);

            Trace.TraceInformation($"Started {orchestrationType.Name}, Instance ID = {instance.InstanceId}");
            return new TestOrchestrationClient(this.client, orchestrationType, instance.InstanceId, creationTime);
        }

        public async Task<IList<OrchestrationState>> GetAllOrchestrationInstancesAsync()
        {
            // This API currently only exists in the service object and is not yet exposed on the TaskHubClient
            var service = (AzureStorageOrchestrationService)this.client.ServiceClient;
            IList<OrchestrationState> instances = await service.GetOrchestrationStateAsync();
            Trace.TraceInformation($"Found {instances.Count} in the task hub instance store.");
            return instances;
        }
    }
}
