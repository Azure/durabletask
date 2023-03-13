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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;
    using DurableTask.Core.Entities;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    class TestEntityClient
    {
        readonly EntityId entityId;

        public TestEntityClient(
            TaskHubEntityClient client,
            EntityId entityId)
        {
            this.InnerClient = client;
            this.entityId = entityId;
        }

        public TaskHubEntityClient InnerClient { get; }

        public async Task SignalEntity(
            string operationName,
            object operationContent = null)
        {
            Trace.TraceInformation($"Signaling entity {this.entityId} with operation named {operationName}.");
            await this.InnerClient.SignalEntityAsync(this.entityId, operationName, operationContent);
        }

        public async Task SignalEntity(
            DateTime startTimeUtc,
            string operationName,
            object operationContent = null)
        {
            Trace.TraceInformation($"Signaling entity {this.entityId} with operation named {operationName}.");
            await this.InnerClient.SignalEntityAsync(this.entityId, operationName, operationContent, startTimeUtc);
        }

        public async Task<T> WaitForEntityState<T>(
            TimeSpan? timeout = null,
            Func<T, string> describeWhatWeAreWaitingFor = null)
        {
            if (timeout == null)
            {
                timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(30);
            }

            Stopwatch sw = Stopwatch.StartNew();

            TaskHubEntityClient.StateResponse<T> response;

            do
            {
                response = await this.InnerClient.ReadEntityStateAsync<T>(this.entityId);
                if (response.EntityExists)
                {
                    if (describeWhatWeAreWaitingFor == null)
                    {
                        break;
                    }
                    else
                    {
                        var waitForResult = describeWhatWeAreWaitingFor(response.EntityState);

                        if (string.IsNullOrEmpty(waitForResult))
                        {
                            break;
                        }
                        else
                        {
                            Trace.TraceInformation($"Waiting for {this.entityId} : {waitForResult}");
                        }
                    }
                }
                else
                {
                    Trace.TraceInformation($"Waiting for {this.entityId} to have state.");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }
            while (sw.Elapsed < timeout);

            if (response.EntityExists)
            {
                string serializedState = JsonConvert.SerializeObject(response.EntityState);
                Trace.TraceInformation($"Found state: {serializedState}");
                return response.EntityState;
            }
            else
            {
                throw new TimeoutException($"Durable entity '{this.entityId}' still doesn't have any state!");
            }
        }
    }
}
