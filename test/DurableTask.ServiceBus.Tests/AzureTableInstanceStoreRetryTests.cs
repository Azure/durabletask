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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using global::Azure;
    using DurableTask.Core;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Verifies that the execution-id lookup on <see cref="AzureTableInstanceStore"/> retries transient
    /// table failures, matching the instance-id overload. Polling by execution id would otherwise fault
    /// on the first transient error that escapes the storage SDK's own retries.
    /// </summary>
    [TestClass]
    public class AzureTableInstanceStoreRetryTests
    {
        const string InstanceId = "instance-1";
        const string ExecutionId = "generation-1";
        const string HubName = "testhub";

        // Parsed only; constructing a TableServiceClient performs no I/O.
        const string FakeStorageConnectionString = "UseDevelopmentStorage=true";

        static readonly DateTime BaseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static AzureTableOrchestrationStateEntity CreateEntity()
        {
            return new AzureTableOrchestrationStateEntity(new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = InstanceId,
                    ExecutionId = ExecutionId
                },
                OrchestrationStatus = OrchestrationStatus.Completed,
                CreatedTime = BaseTime,
                LastUpdatedTime = BaseTime,
                Output = "final output"
            });
        }

        /// <summary>
        /// A transient failure on the state table must be retried rather than surfaced to the caller.
        /// </summary>
        [TestMethod]
        public async Task GetOrchestrationStateAsync_ByExecutionId_RetriesTransientStateTableFailure()
        {
            var tableClient = new FailOnceTableClient { FailuresBeforeSuccess = 1 };
            var store = new AzureTableInstanceStore(tableClient);

            OrchestrationStateInstanceEntity state = await store.GetOrchestrationStateAsync(InstanceId, ExecutionId);

            Assert.IsNotNull(state, "A single transient failure must not fault the execution-id lookup.");
            Assert.AreEqual(ExecutionId, state.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual("final output", state.State.Output);
            Assert.AreEqual(2, tableClient.StateQueryCount, "Expected one failed attempt followed by a successful retry.");
        }

        /// <summary>
        /// The JumpStart fallback is a separate query and needs the same protection: without it a
        /// transient failure there faults the lookup even though the state table responded fine.
        /// </summary>
        [TestMethod]
        public async Task GetOrchestrationStateAsync_ByExecutionId_RetriesTransientJumpStartTableFailure()
        {
            var tableClient = new FailOnceTableClient
            {
                // Force the JumpStart fallback by leaving the state table empty.
                StateTableIsEmpty = true,
                JumpStartFailuresBeforeSuccess = 1
            };

            var store = new AzureTableInstanceStore(tableClient);

            OrchestrationStateInstanceEntity state = await store.GetOrchestrationStateAsync(InstanceId, ExecutionId);

            Assert.IsNotNull(state, "A single transient failure must not fault the JumpStart fallback.");
            Assert.AreEqual(ExecutionId, state.State.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(2, tableClient.JumpStartQueryCount, "Expected one failed attempt followed by a successful retry.");
        }

        /// <summary>
        /// Table client that throws a retryable storage error a fixed number of times before returning
        /// a row, so a lookup that lacks retries fails and one that retries succeeds.
        /// </summary>
        sealed class FailOnceTableClient : AzureTableClient
        {
            public FailOnceTableClient()
                : base(HubName, FakeStorageConnectionString)
            {
            }

            public int FailuresBeforeSuccess { get; set; }

            public int JumpStartFailuresBeforeSuccess { get; set; }

            public bool StateTableIsEmpty { get; set; }

            public int StateQueryCount { get; private set; }

            public int JumpStartQueryCount { get; private set; }

            public override Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryOrchestrationStatesAsync(
                OrchestrationStateQuery stateQuery)
            {
                this.StateQueryCount++;

                if (this.StateQueryCount <= this.FailuresBeforeSuccess)
                {
                    throw new RequestFailedException(503, "The server is busy.");
                }

                IEnumerable<AzureTableOrchestrationStateEntity> result = this.StateTableIsEmpty
                    ? Enumerable.Empty<AzureTableOrchestrationStateEntity>()
                    : new[] { CreateEntity() };

                return Task.FromResult(result);
            }

            public override Task<IEnumerable<AzureTableOrchestrationStateEntity>> QueryJumpStartOrchestrationsAsync(
                OrchestrationStateQuery stateQuery)
            {
                this.JumpStartQueryCount++;

                if (this.JumpStartQueryCount <= this.JumpStartFailuresBeforeSuccess)
                {
                    throw new RequestFailedException(503, "The server is busy.");
                }

                return Task.FromResult<IEnumerable<AzureTableOrchestrationStateEntity>>(new[] { CreateEntity() });
            }
        }
    }
}
