﻿//  ----------------------------------------------------------------------------------
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

namespace FrameworkUnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class InstanceStoreQueryTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            client = TestHelpers.CreateTaskHubClient();

            taskHub = TestHelpers.CreateTaskHub();

            taskHub.DeleteHub();
            taskHub.CreateHubIfNotExists();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            taskHub.Stop(true);
            taskHub.DeleteHub();
        }

        [TestMethod]
        public void QueryByInstanceIdTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instanceId1 = "apiservice1_activate";
            string instanceId2 = "apiservice1_terminate";
            string instanceId3 = "system_gc";
            string instanceId4 = "system_upgrade";
            string instanceId5 = "apiservice2_upgrade";

            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId1, "DONTTHROW");
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId2, "DONTTHROW");
            OrchestrationInstance id3 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId3, "DONTTHROW");
            OrchestrationInstance id4 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId4, "DONTTHROW");
            OrchestrationInstance id5 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId5, "DONTTHROW");

            TestHelpers.WaitForInstance(client, id1, 60);
            TestHelpers.WaitForInstance(client, id2, 60);
            TestHelpers.WaitForInstance(client, id3, 60);
            TestHelpers.WaitForInstance(client, id4, 60);
            TestHelpers.WaitForInstance(client, id5, 60);

            OrchestrationStateQuery apiservice1ExactQuery = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1_activate", id1.ExecutionId);
            OrchestrationStateQuery apiservice1ExecutionIdExactQuery = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1_activate", id1.ExecutionId);

            OrchestrationStateQuery apiservice1AllQuery = new OrchestrationStateQuery().AddInstanceFilter(
                "apiservice1", true);
            OrchestrationStateQuery systemAllQuery = new OrchestrationStateQuery().AddInstanceFilter("system", true);
            OrchestrationStateQuery emptyExactQuery = new OrchestrationStateQuery().AddInstanceFilter("apiservice10");
            OrchestrationStateQuery emptyAllQuery = new OrchestrationStateQuery().AddInstanceFilter("apiservice10", true);
            var allQuery = new OrchestrationStateQuery();


            IEnumerable<OrchestrationState> allResponse = client.QueryOrchestrationStates(allQuery);
            IEnumerable<OrchestrationState> apiservice1ExactResponse =
                client.QueryOrchestrationStates(apiservice1ExactQuery);
            IEnumerable<OrchestrationState> apiservice1ExecutionIdExactResponse =
                client.QueryOrchestrationStates(apiservice1ExecutionIdExactQuery);
            IEnumerable<OrchestrationState> apiservice1AllResponse = client.QueryOrchestrationStates(apiservice1AllQuery);
            IEnumerable<OrchestrationState> systemAllResponse = client.QueryOrchestrationStates(systemAllQuery);
            IEnumerable<OrchestrationState> emptyAllResponse = client.QueryOrchestrationStates(emptyExactQuery);
            IEnumerable<OrchestrationState> emptyExactResponse = client.QueryOrchestrationStates(emptyAllQuery);

            Assert.IsTrue(allResponse.Count() == 5);
            Assert.IsTrue(apiservice1ExactResponse.Count() == 1);
            Assert.IsTrue(apiservice1ExecutionIdExactResponse.Count() == 1);
            Assert.IsTrue(apiservice1AllResponse.Count() == 2);
            Assert.IsTrue(systemAllResponse.Count() == 2);
            Assert.IsTrue(emptyAllResponse.Count() == 0);
            Assert.IsTrue(emptyExactResponse.Count() == 0);

            Assert.AreEqual(id1.InstanceId, apiservice1ExactResponse.First().OrchestrationInstance.InstanceId);
            Assert.AreEqual(id1.InstanceId, apiservice1AllResponse.First().OrchestrationInstance.InstanceId);
            Assert.AreEqual(id2.InstanceId, apiservice1AllResponse.ElementAt(1).OrchestrationInstance.InstanceId);

            Assert.AreEqual(id2.InstanceId, apiservice1AllResponse.ElementAt(1).OrchestrationInstance.InstanceId);
            Assert.AreEqual(id3.InstanceId, systemAllResponse.ElementAt(0).OrchestrationInstance.InstanceId);
            Assert.AreEqual(id4.InstanceId, systemAllResponse.ElementAt(1).OrchestrationInstance.InstanceId);
        }

        [TestMethod]
        public void SegmentedQueryUnequalCountsTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration),
                typeof (InstanceStoreTestOrchestration2))
                .AddTaskActivities(new Activity1())
                .Start();

            for (int i = 0; i < 15; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(
                    i%2 == 0 ? typeof (InstanceStoreTestOrchestration) : typeof (InstanceStoreTestOrchestration2),
                    instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(30));

            var query = new OrchestrationStateQuery();

            OrchestrationStateQuerySegment seg = null;

            var results = new List<OrchestrationState>();

            seg = client.QueryOrchestrationStatesSegmentedAsync(query, null, 2).Result;
            results.AddRange(seg.Results);
            Assert.AreEqual(2, results.Count);

            seg = client.QueryOrchestrationStatesSegmentedAsync(query, seg.ContinuationToken, 5).Result;
            results.AddRange(seg.Results);
            Assert.AreEqual(7, results.Count);

            seg = client.QueryOrchestrationStatesSegmentedAsync(query, seg.ContinuationToken, 10).Result;
            results.AddRange(seg.Results);
            Assert.AreEqual(15, results.Count);
            Assert.IsNull(seg.ContinuationToken);
        }

        [TestMethod]
        public void PurgeOrchestrationHistoryTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration),
                typeof (InstanceStoreTestOrchestration2))
                .AddTaskActivities(new Activity1())
                .Start();

            for (int i = 0; i < 25; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(
                    i%2 == 0 ? typeof (InstanceStoreTestOrchestration) : typeof (InstanceStoreTestOrchestration2),
                    instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(30));

            var query = new OrchestrationStateQuery();

            IEnumerable<OrchestrationState> states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(25, states.Count());

            client.PurgeOrchestrationInstanceHistoryAsync
                (DateTime.UtcNow, OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter).Wait();

            states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(0, states.Count());

            for (int i = 0; i < 10; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration), instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));
            DateTime cutoff = DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(5));

            for (int i = 10; i < 20; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration), instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(30));

            states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(20, states.Count());

            client.PurgeOrchestrationInstanceHistoryAsync
                (cutoff, OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter).Wait();

            states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(10, states.Count());

            foreach (OrchestrationState s in states)
            {
                Assert.IsTrue(s.CreatedTime > cutoff);
            }
        }

        [TestMethod]
        public void PurgeManyOrchestrationHistoryTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration),
                typeof (InstanceStoreTestOrchestration2))
                .AddTaskActivities(new Activity1())
                .Start();

            for (int i = 0; i < 110; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration), instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(50));

            var query = new OrchestrationStateQuery();

            IEnumerable<OrchestrationState> states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(110, states.Count());

            client.PurgeOrchestrationInstanceHistoryAsync
                (DateTime.UtcNow, OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter).Wait();

            states = client.QueryOrchestrationStates(query);
            Assert.AreEqual(0, states.Count());
        }

        [TestMethod]
        public void SegmentedQueryTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration),
                typeof (InstanceStoreTestOrchestration2))
                .AddTaskActivities(new Activity1())
                .Start();

            for (int i = 0; i < 15; i++)
            {
                string instanceId = "apiservice" + i;
                client.CreateOrchestrationInstance(
                    i%2 == 0 ? typeof (InstanceStoreTestOrchestration) : typeof (InstanceStoreTestOrchestration2),
                    instanceId, "DONTTHROW");
            }

            Thread.Sleep(TimeSpan.FromSeconds(30));

            var query = new OrchestrationStateQuery();

            OrchestrationStateQuerySegment seg = null;

            var results = new List<OrchestrationState>();
            do
            {
                seg =
                    client.QueryOrchestrationStatesSegmentedAsync(query, seg == null ? null : seg.ContinuationToken, 2)
                        .Result;
                results.AddRange(seg.Results);
            } while (seg.ContinuationToken != null);

            Assert.AreEqual(15, results.Count);

            query = new OrchestrationStateQuery()
                .AddInstanceFilter("apiservice", true)
                .AddNameVersionFilter("FrameworkUnitTests.InstanceStoreQueryTests+InstanceStoreTestOrchestration");

            results = new List<OrchestrationState>();
            do
            {
                seg =
                    client.QueryOrchestrationStatesSegmentedAsync(query, seg == null ? null : seg.ContinuationToken, 2)
                        .Result;
                results.AddRange(seg.Results);
            } while (seg.ContinuationToken != null);
            Assert.AreEqual(8, results.Count);

            query = new OrchestrationStateQuery()
                .AddInstanceFilter("apiservice", true)
                .AddNameVersionFilter("FrameworkUnitTests.InstanceStoreQueryTests+InstanceStoreTestOrchestration2");

            results = new List<OrchestrationState>();
            do
            {
                seg =
                    client.QueryOrchestrationStatesSegmentedAsync(query, seg == null ? null : seg.ContinuationToken, 2)
                        .Result;
                results.AddRange(seg.Results);
            } while (seg.ContinuationToken != null);
            Assert.AreEqual(7, results.Count);

            query = new OrchestrationStateQuery()
                .AddInstanceFilter("apiservice", true)
                .AddNameVersionFilter("FrameworkUnitTests.InstanceStoreQueryTests+InstanceStoreTestOrchestration2");

            seg =
                client.QueryOrchestrationStatesSegmentedAsync(query, seg == null ? null : seg.ContinuationToken).Result;

            Assert.IsTrue(seg.ContinuationToken == null);
            Assert.AreEqual(7, seg.Results.Count());
        }

        [TestMethod]
        public void QueryByTimeTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instanceId1 = "first";
            string instanceId2 = "second";
            string instanceId3 = "third";

            DateTime firstBatchStart = DateTime.UtcNow;
            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId1, "WAIT_DONTTHROW");
            TestHelpers.WaitForInstance(client, id1, 60);
            DateTime firstBatchEnd = DateTime.UtcNow;


            DateTime secondBatchStart = DateTime.UtcNow;
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId2, "WAIT_DONTTHROW");
            OrchestrationInstance id3 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId3, "WAIT_DONTTHROW");

            TestHelpers.WaitForInstance(client, id2, 60);
            TestHelpers.WaitForInstance(client, id3, 60);
            DateTime secondBatchEnd = DateTime.UtcNow;

            // timespan during which only first batch was created
            OrchestrationStateQuery query = new OrchestrationStateQuery().AddTimeRangeFilter(firstBatchStart,
                firstBatchStart.AddSeconds(5),
                OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter);

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(instanceId1, response.First().OrchestrationInstance.InstanceId);

            // timespan during which first batch finished
            query = new OrchestrationStateQuery().AddTimeRangeFilter(firstBatchEnd.Subtract(TimeSpan.FromSeconds(5)),
                firstBatchEnd.AddSeconds(5),
                OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(instanceId1, response.First().OrchestrationInstance.InstanceId);

            // timespan during which second batch was created
            query = new OrchestrationStateQuery().AddTimeRangeFilter(firstBatchEnd.Subtract(TimeSpan.FromSeconds(5)),
                firstBatchEnd.AddSeconds(5),
                OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 2);
            Assert.AreEqual(instanceId2, response.First().OrchestrationInstance.InstanceId);
            Assert.AreEqual(instanceId3, response.ElementAt(1).OrchestrationInstance.InstanceId);
        }

        [TestMethod]
        public void QueryByTimeForRunningOrchestrationsTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instanceId1 = "first";

            DateTime firstBatchStart = DateTime.UtcNow;
            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId1, "WAIT_DONTTHROW");
            TestHelpers.WaitForInstance(client, id1, 60, false);

            // running orchestrations never get reported in any CompletedTimeFilter query
            OrchestrationStateQuery query = new OrchestrationStateQuery().AddTimeRangeFilter(DateTime.MinValue,
                DateTime.MaxValue,
                OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 0);

            TestHelpers.WaitForInstance(client, id1, 60);

            // now we should get a result
            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
        }

        [TestMethod]
        public void QueryByLastUpdatedTimeTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instanceId1 = "first";

            DateTime firstBatchStart = DateTime.UtcNow;
            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instanceId1, "WAIT_AND_WAIT_DONTTHROW");
            TestHelpers.WaitForInstance(client, id1, 60, false);

            // running orchestrations never get reported in any CompletedTimeFilter query
            OrchestrationStateQuery query = new OrchestrationStateQuery().AddTimeRangeFilter(firstBatchStart,
                firstBatchStart.AddSeconds(5),
                OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter);

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);

            TestHelpers.WaitForInstance(client, id1, 60);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 0);

            query = new OrchestrationStateQuery().AddTimeRangeFilter(firstBatchStart.AddSeconds(15), DateTime.MaxValue,
                OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter);

            // now we should get a result
            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
        }

        [TestMethod]
        public void QueryDuplicateFiltersTest()
        {
            OrchestrationStateQuery query = new OrchestrationStateQuery().AddInstanceFilter(null, false);
            AssertException<ArgumentException>(() => query.AddInstanceFilter(null, false));

            query = new OrchestrationStateQuery().AddNameVersionFilter(null, null);
            AssertException<ArgumentException>(() => query.AddNameVersionFilter(null, null));

            query = new OrchestrationStateQuery().AddTimeRangeFilter(DateTime.MaxValue, DateTime.MaxValue,
                OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter);
            AssertException<ArgumentException>(() => query.AddTimeRangeFilter(DateTime.MaxValue, DateTime.MaxValue,
                OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter));

            query = new OrchestrationStateQuery().AddStatusFilter(OrchestrationStatus.Completed);
            AssertException<ArgumentException>(() => query.AddStatusFilter(OrchestrationStatus.Completed));
        }

        static void AssertException<T>(Action action)
        {
            try
            {
                action();
                Assert.IsTrue(false);
            }
            catch (Exception ex)
            {
                Assert.IsTrue(ex is T);
            }
        }

        [TestMethod]
        public void QueryByStatusTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT_THROW");
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                "WAIT_DONTTHROW");

            OrchestrationStateQuery completedQuery =
                new OrchestrationStateQuery().AddStatusFilter(OrchestrationStatus.Completed);
            OrchestrationStateQuery runningQuery =
                new OrchestrationStateQuery().AddStatusFilter(OrchestrationStatus.Running);
            OrchestrationStateQuery failedQuery =
                new OrchestrationStateQuery().AddStatusFilter(OrchestrationStatus.Failed);

            TestHelpers.WaitForInstance(client, id1, 60, false);
            TestHelpers.WaitForInstance(client, id2, 60, false);

            IEnumerable<OrchestrationState> runningStates = client.QueryOrchestrationStates(runningQuery);
            IEnumerable<OrchestrationState> completedStates = client.QueryOrchestrationStates(completedQuery);
            IEnumerable<OrchestrationState> failedStates = client.QueryOrchestrationStates(failedQuery);

            Assert.IsTrue(runningStates.Count() == 2);
            Assert.IsTrue(completedStates.Count() == 0);
            Assert.IsTrue(failedStates.Count() == 0);

            TestHelpers.WaitForInstance(client, id1, 60);
            TestHelpers.WaitForInstance(client, id2, 60);

            runningStates = client.QueryOrchestrationStates(runningQuery);
            completedStates = client.QueryOrchestrationStates(completedQuery);
            failedStates = client.QueryOrchestrationStates(failedQuery);

            Assert.IsTrue(runningStates.Count() == 0);
            Assert.IsTrue(completedStates.Count() == 1);
            Assert.IsTrue(failedStates.Count() == 1);

            Assert.AreEqual(id1.InstanceId, failedStates.First().OrchestrationInstance.InstanceId);
            Assert.AreEqual(id2.InstanceId, completedStates.First().OrchestrationInstance.InstanceId);
        }

        [TestMethod]
        public void QueryWithMultipleFiltersTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instance1 = "apiservice1_upgrade1";
            string instance2 = "apiservice1_upgrade2";
            string instance3 = "system_gc";

            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instance1, "WAIT_THROW");
            OrchestrationInstance id2 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instance2, "WAIT_DONTTHROW");
            OrchestrationInstance id3 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instance3, "WAIT_DONTTHROW");

            TestHelpers.WaitForInstance(client, id1, 60);
            TestHelpers.WaitForInstance(client, id2, 60);
            TestHelpers.WaitForInstance(client, id3, 60);

            // completed apiservice1 --> 1 result
            OrchestrationStateQuery query = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1", true).
                AddStatusFilter(OrchestrationStatus.Completed);

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(id2.InstanceId, response.First().OrchestrationInstance.InstanceId);

            // failed apiservice1 -> 1 result
            query = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1", true).
                AddStatusFilter(OrchestrationStatus.Failed);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(id1.InstanceId, response.First().OrchestrationInstance.InstanceId);

            // failed gc -> 0 results
            query = new OrchestrationStateQuery().
                AddInstanceFilter("system", true).
                AddStatusFilter(OrchestrationStatus.Failed);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 0);
        }

        [TestMethod]
        public void QueryMultiGenerationalTest()
        {
            taskHub.AddTaskOrchestrations(typeof (InstanceStoreTestOrchestration))
                .AddTaskActivities(new Activity1())
                .Start();

            string instance1 = "apiservice1_upgrade1";

            OrchestrationInstance id1 = client.CreateOrchestrationInstance(typeof (InstanceStoreTestOrchestration),
                instance1, "WAIT_NEWGEN");

            TestHelpers.WaitForInstance(client, id1, 60);

            // completed apiservice1 --> 1 result
            OrchestrationStateQuery query = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1", true).
                AddStatusFilter(OrchestrationStatus.Completed);

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(id1.InstanceId, response.First().OrchestrationInstance.InstanceId);

            // continuedasnew apiservice1 --> 2 results
            query = new OrchestrationStateQuery().
                AddInstanceFilter("apiservice1", true).
                AddStatusFilter(OrchestrationStatus.ContinuedAsNew);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 2);
        }

        [TestMethod]
        public void QueryByNameVersionTest()
        {
            ObjectCreator<TaskOrchestration> c1 = new NameValueObjectCreator<TaskOrchestration>(
                "orch1", "1.0", typeof (InstanceStoreTestOrchestration));

            ObjectCreator<TaskOrchestration> c2 = new NameValueObjectCreator<TaskOrchestration>(
                "orch1", "2.0", typeof (InstanceStoreTestOrchestration));

            ObjectCreator<TaskOrchestration> c3 = new NameValueObjectCreator<TaskOrchestration>(
                "orch2", string.Empty, typeof (InstanceStoreTestOrchestration));

            taskHub.AddTaskOrchestrations(c1, c2, c3)
                .AddTaskActivities(new Activity1())
                .Start();

            OrchestrationInstance id1 = client.CreateOrchestrationInstance("orch1", "1.0", "DONTTHROW");

            OrchestrationInstance id2 = client.CreateOrchestrationInstance("orch1", "2.0", "DONTTHROW");

            OrchestrationInstance id3 = client.CreateOrchestrationInstance("orch2", string.Empty, "DONTTHROW");

            TestHelpers.WaitForInstance(client, id1, 60);
            TestHelpers.WaitForInstance(client, id2, 60);
            TestHelpers.WaitForInstance(client, id3, 60);

            OrchestrationStateQuery query = new OrchestrationStateQuery().AddNameVersionFilter("orch1");

            IEnumerable<OrchestrationState> response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 2);

            // TODO : for some reason sometimes the order gets inverted
            //Assert.AreEqual(id1.InstanceId, response.First().OrchestrationInstance.InstanceId);
            //Assert.AreEqual(id2.InstanceId, response.ElementAt(1).OrchestrationInstance.InstanceId);

            query = new OrchestrationStateQuery().AddNameVersionFilter("orch1", "2.0");

            response = client.QueryOrchestrationStates(query);
            Assert.AreEqual(1, response.Count());
            Assert.AreEqual(id2.InstanceId, response.First().OrchestrationInstance.InstanceId);

            query = new OrchestrationStateQuery().AddNameVersionFilter("orch1", string.Empty);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 0);

            query = new OrchestrationStateQuery().AddNameVersionFilter("orch2", string.Empty);

            response = client.QueryOrchestrationStates(query);
            Assert.IsTrue(response.Count() == 1);
            Assert.AreEqual(id3.InstanceId, response.First().OrchestrationInstance.InstanceId);
        }

        public sealed class Activity1 : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return "Spartacus";
            }
        }

        public class InstanceStoreTestOrchestration : TaskOrchestration<string, string>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = await context.ScheduleTask<string>(typeof (Activity1));
                if (string.Equals(input, "THROW", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("BADFOOD");
                }
                if (string.Equals(input, "DONTTHROW", StringComparison.OrdinalIgnoreCase))
                {
                    // nothing
                }
                else if (string.Equals(input, "WAIT_THROW", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                    throw new InvalidOperationException("BADFOOD");
                }
                else if (string.Equals(input, "WAIT_DONTTHROW", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                }
                else if (string.Equals(input, "WAIT_AND_WAIT_DONTTHROW", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(15), null);
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(10), null);
                }
                else if (string.Equals(input, "WAIT_NEWGEN", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(5), null);
                    context.ContinueAsNew("WAIT_NEWGEN_STOP");
                }
                else if (string.Equals(input, "WAIT_NEWGEN_STOP", StringComparison.OrdinalIgnoreCase))
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(5), null);
                    context.ContinueAsNew("DONTTHROW");
                }
                return result;
            }
        }

        public class InstanceStoreTestOrchestration2 : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult("SOME_RESULT");
            }
        }

        public class TestCreator : ObjectCreator<InstanceStoreTestOrchestration>
        {
            public override InstanceStoreTestOrchestration Create()
            {
                return new InstanceStoreTestOrchestration();
            }
        }
    }
}