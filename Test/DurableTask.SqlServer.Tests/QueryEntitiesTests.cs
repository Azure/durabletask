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

namespace DurableTask.SqlServer.Tests
{
    using DurableTask.Core;
    using DurableTask.Core.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class QueryEntitiesTests : BaseTestClass
    {
        [TestMethod]
        public async Task VerifyOrchestrationStateQueryTest()
        {
            var expectedOrchestrationState = Utils.InfiniteOrchestrationTestData().First();

            //additional data to ensure query doesn't return back more data than it should
            var extraOrchestrationState = Utils.InfiniteOrchestrationTestData().First();


            await InstanceStore.WriteEntitiesAsync(new InstanceEntityBase[] { expectedOrchestrationState, extraOrchestrationState });

            var actual = await InstanceStore.GetOrchestrationStateAsync(expectedOrchestrationState.State.OrchestrationInstance.InstanceId, expectedOrchestrationState.State.OrchestrationInstance.ExecutionId);

            Assert.AreEqual(expectedOrchestrationState.State.OrchestrationInstance.InstanceId, actual.State.OrchestrationInstance.InstanceId);
            Assert.AreEqual(expectedOrchestrationState.State.OrchestrationInstance.ExecutionId, actual.State.OrchestrationInstance.ExecutionId);
        }

        public async Task VerifyOrchestrationStateQueryEntitiesTest()
        {
            var expectedOrchestrationState = Utils.InfiniteOrchestrationTestData().First();

            //additional data to ensure query doesn't return back more data than it should
            var extraOrchestrationState = Utils.InfiniteOrchestrationTestData().First();


            await InstanceStore.WriteEntitiesAsync(new InstanceEntityBase[] { expectedOrchestrationState, extraOrchestrationState });

            var actual = (await InstanceStore.GetEntitiesAsync(expectedOrchestrationState.State.OrchestrationInstance.InstanceId, expectedOrchestrationState.State.OrchestrationInstance.ExecutionId)).ToList();

            Assert.AreEqual(1, actual.Count);

            var actualOrchestration = actual.First();
            Assert.AreEqual(expectedOrchestrationState.State.OrchestrationInstance.InstanceId, actualOrchestration.State.OrchestrationInstance.InstanceId);
            Assert.AreEqual(expectedOrchestrationState.State.OrchestrationInstance.ExecutionId, actualOrchestration.State.OrchestrationInstance.ExecutionId);
        }

        [TestMethod]
        public async Task VerifyWorkItemQueryTest()
        {
            var expectedInstanceId = Guid.NewGuid().ToString("N");
            var expectedExecutionId = Guid.NewGuid().ToString("N");

            var expectedWorkItemState = Utils.InfiniteWorkItemTestData(expectedInstanceId, expectedExecutionId).First();

            //additional data to ensure query doesn't return back more data than it should
            var extraWorkItemState = Utils.InfiniteWorkItemTestData(Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N")).First();

            await InstanceStore.WriteEntitiesAsync(new InstanceEntityBase[] { expectedWorkItemState, extraWorkItemState });

            var actual = (await InstanceStore.GetOrchestrationHistoryEventsAsync(expectedInstanceId, expectedExecutionId)).ToList();

            Assert.AreEqual(1, actual.Count);

            var actualWorkItem = actual.First();

            Assert.AreEqual(expectedInstanceId, actualWorkItem.InstanceId);
            Assert.AreEqual(expectedExecutionId, actualWorkItem.ExecutionId);
        }

        [TestMethod]
        public async Task VerifyOrchestrationStateQueryByInstanceIdAllInstancesTest()
        {
            var instanceId = Guid.NewGuid().ToString("N");

            var values = Enum.GetValues(typeof(OrchestrationStatus)).Cast<OrchestrationStatus>().ToArray();

            var entities = new List<OrchestrationStateInstanceEntity>();
            entities.AddRange(Utils.InfiniteOrchestrationTestData().Take(values.Length));

            //ensure each status exists in the collection and they all have the same InstanceId
            entities.Select((e, i) => { e.State.OrchestrationStatus = values[i]; e.State.OrchestrationInstance.InstanceId = instanceId; return e; }).ToList();

            await InstanceStore.WriteEntitiesAsync(entities);

            var actual = (await InstanceStore.GetOrchestrationStateAsync(instanceId, true)).ToList();

            Assert.AreEqual(entities.Count, actual.Count);
        }

        [TestMethod]
        public async Task VerifyOrchestrationStateQueryByInstanceIdTest()
        {
            var instanceId = Guid.NewGuid().ToString("N");

            var values = Enum.GetValues(typeof(OrchestrationStatus)).Cast<OrchestrationStatus>().ToArray();

            var entities = new List<OrchestrationStateInstanceEntity>();
            entities.AddRange(Utils.InfiniteOrchestrationTestData().Take(values.Length));

            //ensure each status exists in the collection and they all have the same InstanceId
            entities.Select((e, i) => { e.State.OrchestrationStatus = values[i]; e.State.OrchestrationInstance.InstanceId = instanceId; return e; }).ToList();

            await InstanceStore.WriteEntitiesAsync(entities);

            var actual = (await InstanceStore.GetOrchestrationStateAsync(instanceId, false)).ToList();

            Assert.AreEqual(1, actual.Count);

            var expectedState = entities
                .Where(e => e.State.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                .OrderBy(e => e.State.LastUpdatedTime)
                .First();

            var actualState = actual.First();

            Assert.AreEqual(expectedState.State.OrchestrationInstance.ExecutionId, actualState.State.OrchestrationInstance.ExecutionId);
        }
    }
}
