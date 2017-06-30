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
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    class TestOrchestrationClient
    {
        readonly TaskHubClient client;
        readonly Type orchestrationType;
        readonly string instanceId;
        readonly DateTime instanceCreationTime;

        public TestOrchestrationClient(
            TaskHubClient client,
            Type orchestrationType,
            string instanceId,
            DateTime instanceCreationTime)
        {
            this.client = client;
            this.orchestrationType = orchestrationType;
            this.instanceId = instanceId;
            this.instanceCreationTime = instanceCreationTime;
        }

        public async Task<OrchestrationState> WaitForCompletionAsync(TimeSpan timeout)
        {
            var latestGeneration = new OrchestrationInstance { InstanceId = this.instanceId };
            Stopwatch sw = Stopwatch.StartNew();
            OrchestrationState state = await this.client.WaitForOrchestrationAsync(latestGeneration, timeout);
            if (state != null)
            {
                Trace.TraceInformation(
                    "{0} (ID = {1}) completed after ~{2}ms. Status = {3}. Output = {4}.",
                    this.orchestrationType.Name,
                    state.OrchestrationInstance.InstanceId,
                    sw.ElapsedMilliseconds,
                    state.OrchestrationStatus,
                    state.Output);
            }
            else
            {
                Trace.TraceWarning(
                    "{0} (ID = {1}) failed to complete after {2}ms.",
                    this.orchestrationType.Name,
                    this.instanceId,
                    timeout.TotalMilliseconds);
            }

            return state;
        }

        internal async Task<OrchestrationState> WaitForStartupAsync(TimeSpan timeout)
        {
            Stopwatch sw = Stopwatch.StartNew();
            do
            {
                OrchestrationState state = await this.GetStatusAsync();
                if (state != null)
                {
                    Trace.TraceInformation($"{state.Name} (ID = {state.OrchestrationInstance.InstanceId}) started successfully after ~{sw.ElapsedMilliseconds}ms. Status = {state.OrchestrationStatus}.");
                    return state;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));

            } while (sw.Elapsed < timeout);

            throw new TimeoutException($"Orchestration '{this.orchestrationType.Name}' with instance ID '{this.instanceId}' failed to start.");
        }

        public async Task<OrchestrationState> GetStatusAsync()
        {
            OrchestrationState state = await this.client.GetOrchestrationStateAsync(this.instanceId);

            if (state != null)
            {
                // Validate the status before returning
                Assert.AreEqual(this.orchestrationType.FullName, state.Name);
                Assert.AreEqual(this.instanceId, state.OrchestrationInstance.InstanceId);
                Assert.IsTrue(state.CreatedTime >= this.instanceCreationTime);
                Assert.IsTrue(state.CreatedTime <= DateTime.UtcNow);
                Assert.IsTrue(state.LastUpdatedTime >= state.CreatedTime);
                Assert.IsTrue(state.LastUpdatedTime <= DateTime.UtcNow);
            }

            return state;
        }

        public async Task RaiseEventAsync(string eventName, object eventData)
        {
            var instance = new OrchestrationInstance { InstanceId = this.instanceId };
            await this.client.RaiseEventAsync(instance, eventName, eventData);
        }

        public async Task TerminateAsync(string reason)
        {
            var instance = new OrchestrationInstance { InstanceId = this.instanceId };
            await this.client.TerminateInstanceAsync(instance, reason);
        }
    }
}
