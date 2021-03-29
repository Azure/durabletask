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
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class TestInstance<T>
    {
        readonly TaskHubClient client;
        readonly OrchestrationInstance instance;
        readonly DateTime startTime;
        readonly T input;

        public TestInstance(
            TaskHubClient client,
            OrchestrationInstance instance,
            DateTime startTime,
            T input)
        {
            this.client = client;
            this.instance = instance;
            this.startTime = startTime;
            this.input = input;
        }

        public string InstanceId => this.instance?.InstanceId;

        public string ExecutionId => this.instance?.ExecutionId;

        OrchestrationInstance GetInstanceForAnyExecution() => new OrchestrationInstance
        {
            InstanceId = this.instance.InstanceId,
        };

        public async Task<OrchestrationState> WaitForStart(TimeSpan timeout = default)
        {
            AdjustTimeout(ref timeout);

            Stopwatch sw = Stopwatch.StartNew();
            do
            {
                OrchestrationState state = await this.GetStateAsync();
                if (state != null && state.OrchestrationStatus != OrchestrationStatus.Pending)
                {
                    return state;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));

            } while (sw.Elapsed < timeout);

            throw new TimeoutException($"Orchestration with instance ID '{this.instance.InstanceId}' failed to start.");
        }

        public async Task<OrchestrationState> WaitForCompletion(
            TimeSpan timeout = default,
            OrchestrationStatus? expectedStatus = OrchestrationStatus.Completed,
            object expectedOutput = null,
            string expectedOutputRegex = null,
            bool continuedAsNew = false)
        {
            AdjustTimeout(ref timeout);

            OrchestrationState state = await this.client.WaitForOrchestrationAsync(this.GetInstanceForAnyExecution(), timeout);
            Assert.IsNotNull(state);
            if (expectedStatus != null)
            {
                Assert.AreEqual(expectedStatus, state.OrchestrationStatus);
            }

            if (!continuedAsNew)
            {
                if (this.input != null)
                {
                    Assert.AreEqual(JToken.FromObject(this.input).ToString(), JToken.Parse(state.Input).ToString());
                }
                else
                {
                    Assert.IsNull(state.Input);
                }
            }

            // For created time, account for potential clock skew
            Assert.IsTrue(state.CreatedTime >= this.startTime.AddMinutes(-5));
            Assert.IsTrue(state.LastUpdatedTime > state.CreatedTime);
            Assert.IsTrue(state.CompletedTime > state.CreatedTime);
            Assert.IsNotNull(state.OrchestrationInstance);
            Assert.AreEqual(this.instance.InstanceId, state.OrchestrationInstance.InstanceId);

            // Make sure there is an ExecutionId, but don't require it to match any particular value
            Assert.IsNotNull(state.OrchestrationInstance.ExecutionId);

            if (expectedOutput != null)
            {
                Assert.IsNotNull(state.Output);
                try
                {
                    // DTFx usually encodes outputs as JSON values. The exception is error messages.
                    // If this is an error message, we'll throw here and try the logic in the catch block.
                    JToken.Parse(state.Output);
                    Assert.AreEqual(JToken.FromObject(expectedOutput).ToString(Formatting.None), state.Output);
                }
                catch (JsonReaderException)
                {
                    Assert.AreEqual(expectedOutput, state?.Output);
                }
            }

            if (expectedOutputRegex != null)
            {
                Assert.IsTrue(
                    Regex.IsMatch(state.Output, expectedOutputRegex),
                    $"The output '{state.Output}' doesn't match the regex pattern '{expectedOutputRegex}'.");
            }

            return state;
        }

        internal Task<OrchestrationState> GetStateAsync()
        {
            return this.client.GetOrchestrationStateAsync(this.instance);
        }

        internal Task RaiseEventAsync(string name, object value)
        {
            return this.client.RaiseEventAsync(this.instance, name, value);
        }

        internal Task TerminateAsync(string reason)
        {
            return this.client.TerminateInstanceAsync(this.instance, reason);
        }

        static void AdjustTimeout(ref TimeSpan timeout)
        {
            if (timeout == default)
            {
                timeout = TimeSpan.FromSeconds(10);
            }

            if (Debugger.IsAttached)
            {
                TimeSpan debuggingTimeout = TimeSpan.FromMinutes(5);
                if (debuggingTimeout > timeout)
                {
                    timeout = debuggingTimeout;
                }
            }
        }
    }
}
