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
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class PurgeHistoryTests : BaseTestClass
    {
        [TestMethod]
        public async Task PurgeByCreatedTimeTest()
        {
            var orchestrations = Utils.InfiniteOrchestrationTestData().Take(3).ToArray();

            var histories = orchestrations
                .SelectMany(r => Utils.InfiniteWorkItemTestData(r.State.OrchestrationInstance.InstanceId, r.State.OrchestrationInstance.ExecutionId).Take(5))
                .ToArray();

            int secondsToAdd = 0;
            foreach (var item in orchestrations)
            {
                item.State.CreatedTime = DateTime.UtcNow.AddSeconds(secondsToAdd++);
                item.State.LastUpdatedTime = item.State.CompletedTime = DateTime.MaxValue;
            }

            await InstanceStore.WriteEntitiesAsync(orchestrations.Cast<InstanceEntityBase>().Concat(histories));

            var historyEntriesDeleted = await InstanceStore.PurgeOrchestrationHistoryEventsAsync(orchestrations.ElementAt(1).State.CreatedTime, OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter);

            Assert.AreEqual(10, historyEntriesDeleted);

            var instance = orchestrations.Last().State.OrchestrationInstance;
            var count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
            Assert.AreEqual(5, count);

            foreach (var item in orchestrations.Take(2))
            {
                instance = item.State.OrchestrationInstance;
                count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
                Assert.AreEqual(0, count);
            }
        }

        [TestMethod]
        public async Task PurgeByLastUpdatedTimeTest()
        {
            var orchestrations = Utils.InfiniteOrchestrationTestData().Take(3).ToArray();

            var histories = orchestrations
                .SelectMany(r => Utils.InfiniteWorkItemTestData(r.State.OrchestrationInstance.InstanceId, r.State.OrchestrationInstance.ExecutionId).Take(5))
                .ToArray();

            int secondsToAdd = 0;
            foreach (var item in orchestrations)
            {
                item.State.LastUpdatedTime = DateTime.UtcNow.AddSeconds(secondsToAdd++);
                item.State.CreatedTime = item.State.CompletedTime = DateTime.MaxValue;
            }

            await InstanceStore.WriteEntitiesAsync(orchestrations.Cast<InstanceEntityBase>().Concat(histories));

            var historyEntriesDeleted = await InstanceStore.PurgeOrchestrationHistoryEventsAsync(orchestrations.ElementAt(1).State.LastUpdatedTime, OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter);

            Assert.AreEqual(10, historyEntriesDeleted);

            var instance = orchestrations.Last().State.OrchestrationInstance;
            var count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
            Assert.AreEqual(5, count);

            foreach (var item in orchestrations.Take(2))
            {
                instance = item.State.OrchestrationInstance;
                count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
                Assert.AreEqual(0, count);
            }
        }

        [TestMethod]
        public async Task PurgeByCompletedTimeTest()
        {
            var orchestrations = Utils.InfiniteOrchestrationTestData().Take(3).ToArray();

            var histories = orchestrations
                .SelectMany(r => Utils.InfiniteWorkItemTestData(r.State.OrchestrationInstance.InstanceId, r.State.OrchestrationInstance.ExecutionId).Take(5))
                .ToArray();

            int secondsToAdd = 0;
            foreach (var item in orchestrations)
            {
                item.State.CompletedTime = DateTime.UtcNow.AddSeconds(secondsToAdd++);
                item.State.CreatedTime = item.State.LastUpdatedTime = DateTime.MaxValue;
            }

            await InstanceStore.WriteEntitiesAsync(orchestrations.Cast<InstanceEntityBase>().Concat(histories));

            var historyEntriesDeleted = await InstanceStore.PurgeOrchestrationHistoryEventsAsync(orchestrations.ElementAt(1).State.CompletedTime, OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);

            Assert.AreEqual(10, historyEntriesDeleted);

            var instance = orchestrations.Last().State.OrchestrationInstance;
            var count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
            Assert.AreEqual(5, count);

            foreach (var item in orchestrations.Take(2))
            {
                instance = item.State.OrchestrationInstance;
                count = (await InstanceStore.GetOrchestrationHistoryEventsAsync(instance.InstanceId, instance.ExecutionId)).Count();
                Assert.AreEqual(0, count);
            }
        }
    }
}
