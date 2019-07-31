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

namespace DurableTask.EventHubs.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    //using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;


    // These tests are copied from AzureStorageScenarioTests

    public partial class PortedAzureScenarioTests
    {
 
        /// <summary>
        /// End-to-end test which validates function chaining by implementing a naive factorial function orchestration.
        /// </summary>
        //[TestMethod]
        //public async Task SequentialOrchestrationNoReplay()
        //{
        //    // Enable extended sesisons to ensure that the orchestration never gets replayed
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        var client = await host.StartOrchestrationAsync(typeof(Orchestrations.FactorialNoReplay), 10);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        Assert.AreEqual(10, JToken.Parse(status?.Input));
        //        Assert.AreEqual(3628800, JToken.Parse(status?.Output));

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task GetAllOrchestrationStatuses()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
        //    {
        //        // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
        //        await host.StartAsync();
        //        var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world one");
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), "world two");
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        var results = await host.GetAllOrchestrationInstancesAsync();
        //        Assert.AreEqual(2, results.Count);
        //        Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world one!\""));
        //        Assert.IsNotNull(results.SingleOrDefault(r => r.Output == "\"Hello, world two!\""));

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task PurgeInstanceHistoryForSingleInstanceWithoutLargeMessageBlobs()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
        //    {
        //        string instanceId = Guid.NewGuid().ToString();
        //        await host.StartAsync();
        //        TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Factorial), 110, instanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

        //        List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
        //        Assert.IsTrue(historyEvents.Count > 0);

        //        IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
        //        Assert.AreEqual(1, orchestrationStateList.Count);
        //        Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        await client.PurgeInstanceHistory();

        //        List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
        //        Assert.AreEqual(0, historyEventsAfterPurging.Count);

        //        orchestrationStateList = await client.GetStateAsync(instanceId);
        //        Assert.AreEqual(1, orchestrationStateList.Count);
        //        Assert.IsNull(orchestrationStateList.First());

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task PurgeInstanceHistoryForSingleInstanceWithLargeMessageBlobs()
        //{
        //using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
        //{
        //    await host.StartAsync();

        //    string instanceId = Guid.NewGuid().ToString();
        //    string message = this.GenerateMediumRandomStringPayload().ToString();
        //    TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, instanceId);
        //    OrchestrationState status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        //    Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

        //    List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
        //    Assert.IsTrue(historyEvents.Count > 0);

        //    IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
        //    Assert.AreEqual(1, orchestrationStateList.Count);
        //    Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

        //    int blobCount = await this.GetBlobCount("test-largemessages", instanceId);
        //    Assert.IsTrue(blobCount > 0);

        //    IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
        //    Assert.AreEqual(1, results.Count);

        //    await ValidateBlobUrlAsync(
        //        host.TaskHub,
        //        instanceId,
        //        results.First(x => x.OrchestrationInstance.InstanceId == instanceId).Output,
        //        Encoding.UTF8.GetByteCount(message));

        //    await client.PurgeInstanceHistory();


        //    List<HistoryStateEvent> historyEventsAfterPurging = await client.GetOrchestrationHistoryAsync(instanceId);
        //    Assert.AreEqual(0, historyEventsAfterPurging.Count);

        //    orchestrationStateList = await client.GetStateAsync(instanceId);
        //    Assert.AreEqual(1, orchestrationStateList.Count);
        //    Assert.IsNull(orchestrationStateList.First());

        //    blobCount = await this.GetBlobCount("test-largemessages", instanceId);
        //    Assert.AreEqual(0, blobCount);

        //    await host.StopAsync();
        //}
        //}

        //[TestMethod]
        //public async Task PurgeInstanceHistoryForTimePeriodDeleteAll()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
        //    {
        //        await host.StartAsync();
        //        DateTime startDateTime = DateTime.Now;
        //        string firstInstanceId = "instance1";
        //        TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        //        string secondInstanceId = "instance2";
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        //        string thirdInstanceId = "instance3";
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        string fourthInstanceId = "instance4";
        //        string message = this.GenerateMediumRandomStringPayload().ToString();
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message, fourthInstanceId);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

        //        IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
        //        Assert.AreEqual(4, results.Count);
        //        Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == firstInstanceId).Output);
        //        Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == secondInstanceId).Output);
        //        Assert.AreEqual("\"Done\"", results.First(x => x.OrchestrationInstance.InstanceId == thirdInstanceId).Output);

        //        await ValidateBlobUrlAsync(
        //            host.TaskHub,
        //            fourthInstanceId,
        //            results.First(x => x.OrchestrationInstance.InstanceId == fourthInstanceId).Output,
        //            Encoding.UTF8.GetByteCount(message));

        //        List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
        //        Assert.IsTrue(firstHistoryEvents.Count > 0);

        //        List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
        //        Assert.IsTrue(secondHistoryEvents.Count > 0);

        //        List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
        //        Assert.IsTrue(thirdHistoryEvents.Count > 0);

        //        List<HistoryStateEvent> fourthHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
        //        Assert.IsTrue(fourthHistoryEvents.Count > 0);

        //        IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
        //        Assert.AreEqual(1, firstOrchestrationStateList.Count);
        //        Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
        //        Assert.AreEqual(1, secondOrchestrationStateList.Count);
        //        Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
        //        Assert.AreEqual(1, thirdOrchestrationStateList.Count);
        //        Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        IList<OrchestrationState> fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
        //        Assert.AreEqual(1, fourthOrchestrationStateList.Count);
        //        Assert.AreEqual(fourthInstanceId, fourthOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        int blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
        //        Assert.AreEqual(6, blobCount);

        //        await client.PurgeInstanceHistoryByTimePeriod(
        //            startDateTime,
        //            DateTime.UtcNow,
        //            new List<OrchestrationStatus>
        //            {
        //                OrchestrationStatus.Completed,
        //                OrchestrationStatus.Terminated,
        //                OrchestrationStatus.Failed,
        //                OrchestrationStatus.Running
        //            });

        //        List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
        //        Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

        //        List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
        //        Assert.AreEqual(0, secondHistoryEventsAfterPurging.Count);

        //        List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
        //        Assert.AreEqual(0, thirdHistoryEventsAfterPurging.Count);

        //        List<HistoryStateEvent> fourthHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(fourthInstanceId);
        //        Assert.AreEqual(0, fourthHistoryEventsAfterPurging.Count);

        //        firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
        //        Assert.AreEqual(1, firstOrchestrationStateList.Count);
        //        Assert.IsNull(firstOrchestrationStateList.First());

        //        secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
        //        Assert.AreEqual(1, secondOrchestrationStateList.Count);
        //        Assert.IsNull(secondOrchestrationStateList.First());

        //        thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
        //        Assert.AreEqual(1, thirdOrchestrationStateList.Count);
        //        Assert.IsNull(thirdOrchestrationStateList.First());

        //        fourthOrchestrationStateList = await client.GetStateAsync(fourthInstanceId);
        //        Assert.AreEqual(1, fourthOrchestrationStateList.Count);
        //        Assert.IsNull(fourthOrchestrationStateList.First());

        //        blobCount = await this.GetBlobCount("test-largemessages", fourthInstanceId);
        //        Assert.AreEqual(0, blobCount);

        //        await host.StopAsync();
        //    }
        //}

        //private async Task<int> GetBlobCount(string containerName, string directoryName)
        //{
        //    string storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];
        //    CloudStorageAccount storageAccount;
        //    if (!CloudStorageAccount.TryParse(storageConnectionString, out storageAccount))
        //    {
        //        return 0;
        //    }

        //    CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();

        //    CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
        //    await cloudBlobContainer.CreateIfNotExistsAsync();
        //    CloudBlobDirectory instanceDirectory = cloudBlobContainer.GetDirectoryReference(directoryName);
        //    int blobCount = 0;
        //    BlobContinuationToken blobContinuationToken = null;
        //    do
        //    {
        //        BlobResultSegment results = await instanceDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
        //        blobContinuationToken = results.ContinuationToken;
        //        blobCount += results.Results.Count();
        //    } while (blobContinuationToken != null);

        //    return blobCount;
        //}


        //[TestMethod]
        //public async Task PurgeInstanceHistoryForTimePeriodDeletePartially()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: false))
        //    {
        //        // Execute the orchestrator twice. Orchestrator will be replied. However instances might be two.
        //        await host.StartAsync();
        //        DateTime startDateTime = DateTime.Now;
        //        string firstInstanceId = Guid.NewGuid().ToString();
        //        TestOrchestrationClient client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, firstInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        //        DateTime endDateTime = DateTime.Now;
        //        await Task.Delay(5000);
        //        string secondInstanceId = Guid.NewGuid().ToString();
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, secondInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
        //        string thirdInstanceId = Guid.NewGuid().ToString();
        //        client = await host.StartOrchestrationAsync(typeof(Orchestrations.FanOutFanIn), 50, thirdInstanceId);
        //        await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        IList<OrchestrationState> results = await host.GetAllOrchestrationInstancesAsync();
        //        Assert.AreEqual(3, results.Count);
        //        Assert.IsNotNull(results[0].Output.Equals("\"Done\""));
        //        Assert.IsNotNull(results[1].Output.Equals("\"Done\""));
        //        Assert.IsNotNull(results[2].Output.Equals("\"Done\""));


        //        List<HistoryStateEvent> firstHistoryEvents = await client.GetOrchestrationHistoryAsync(firstInstanceId);
        //        Assert.IsTrue(firstHistoryEvents.Count > 0);

        //        List<HistoryStateEvent> secondHistoryEvents = await client.GetOrchestrationHistoryAsync(secondInstanceId);
        //        Assert.IsTrue(secondHistoryEvents.Count > 0);

        //        List<HistoryStateEvent> thirdHistoryEvents = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
        //        Assert.IsTrue(secondHistoryEvents.Count > 0);

        //        IList<OrchestrationState> firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
        //        Assert.AreEqual(1, firstOrchestrationStateList.Count);
        //        Assert.AreEqual(firstInstanceId, firstOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        IList<OrchestrationState> secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
        //        Assert.AreEqual(1, secondOrchestrationStateList.Count);
        //        Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        IList<OrchestrationState> thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
        //        Assert.AreEqual(1, thirdOrchestrationStateList.Count);
        //        Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        await client.PurgeInstanceHistoryByTimePeriod(startDateTime, endDateTime, new List<OrchestrationStatus> { OrchestrationStatus.Completed, OrchestrationStatus.Terminated, OrchestrationStatus.Failed, OrchestrationStatus.Running });

        //        List<HistoryStateEvent> firstHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(firstInstanceId);
        //        Assert.AreEqual(0, firstHistoryEventsAfterPurging.Count);

        //        List<HistoryStateEvent> secondHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(secondInstanceId);
        //        Assert.IsTrue(secondHistoryEventsAfterPurging.Count > 0);

        //        List<HistoryStateEvent> thirdHistoryEventsAfterPurging = await client.GetOrchestrationHistoryAsync(thirdInstanceId);
        //        Assert.IsTrue(thirdHistoryEventsAfterPurging.Count > 0);

        //        firstOrchestrationStateList = await client.GetStateAsync(firstInstanceId);
        //        Assert.AreEqual(1, firstOrchestrationStateList.Count);
        //        Assert.IsNull(firstOrchestrationStateList.First());

        //        secondOrchestrationStateList = await client.GetStateAsync(secondInstanceId);
        //        Assert.AreEqual(1, secondOrchestrationStateList.Count);
        //        Assert.AreEqual(secondInstanceId, secondOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        thirdOrchestrationStateList = await client.GetStateAsync(thirdInstanceId);
        //        Assert.AreEqual(1, thirdOrchestrationStateList.Count);
        //        Assert.AreEqual(thirdInstanceId, thirdOrchestrationStateList.First().OrchestrationInstance.InstanceId);

        //        await host.StopAsync();
        //    }
        //}


 

         /// <summary>
        /// End-to-end test which validates the ContinueAsNew functionality by implementing character counter actor pattern.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task ActorOrchestrationForLargeInput(bool enableExtendedSessions)
        //{
        //    await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
        //}

        /// <summary>
        /// End-to-end test which validates the deletion of all data generated by the ContinueAsNew functionality in the character counter actor pattern.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task ActorOrchestrationDeleteAllLargeMessageBlobs(bool enableExtendedSessions)
        //{
        //    DateTime startDateTime = DateTime.UtcNow;

        //    Tuple<string, TestOrchestrationClient> resultTuple = await this.ValidateCharacterCounterIntegrationTest(enableExtendedSessions);
        //    string instanceId = resultTuple.Item1;
        //    TestOrchestrationClient client = resultTuple.Item2;

        //    List<HistoryStateEvent> historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
        //    Assert.IsTrue(historyEvents.Count > 0);

        //    IList<OrchestrationState> orchestrationStateList = await client.GetStateAsync(instanceId);
        //    Assert.AreEqual(1, orchestrationStateList.Count);
        //    Assert.AreEqual(instanceId, orchestrationStateList.First().OrchestrationInstance.InstanceId);

        //    int blobCount = await this.GetBlobCount("test-largemessages", instanceId);

        //    Assert.AreEqual(3, blobCount);

        //    await client.PurgeInstanceHistoryByTimePeriod(
        //        startDateTime,
        //        DateTime.UtcNow,
        //        new List<OrchestrationStatus>
        //        {
        //            OrchestrationStatus.Completed,
        //            OrchestrationStatus.Terminated,
        //            OrchestrationStatus.Failed,
        //            OrchestrationStatus.Running
        //        });

        //    historyEvents = await client.GetOrchestrationHistoryAsync(instanceId);
        //    Assert.AreEqual(0, historyEvents.Count);

        //    orchestrationStateList = await client.GetStateAsync(instanceId);
        //    Assert.AreEqual(1, orchestrationStateList.Count);
        //    Assert.IsNull(orchestrationStateList.First());

        //    blobCount = await this.GetBlobCount("test-largemessages", instanceId);
        //    Assert.AreEqual(0, blobCount);
        //}

        //private async Task<Tuple<string, TestOrchestrationClient>> ValidateCharacterCounterIntegrationTest(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        string initialMessage = this.GenerateMediumRandomStringPayload().ToString();
        //        int counter = initialMessage.Length;
        //        var initialValue = new Tuple<string, int>(initialMessage, counter);
        //        TestOrchestrationClient client =
        //            await host.StartOrchestrationAsync(typeof(Orchestrations.CharacterCounter), initialValue);

        //        // Need to wait for the instance to start before sending events to it.
        //        // TODO: This requirement may not be ideal and should be revisited.
        //        OrchestrationState orchestrationState =
        //            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //        // Perform some operations
        //        await client.RaiseEventAsync("operation", "double");
        //        counter *= 2;

        //        // TODO: Sleeping to avoid a race condition where multiple ContinueAsNew messages
        //        //       are processed by the same instance at the same time, resulting in a corrupt
        //        //       storage failure in DTFx.
        //        await Task.Delay(10000);
        //        await client.RaiseEventAsync("operation", "double");
        //        counter *= 2;
        //        await Task.Delay(10000);
        //        await client.RaiseEventAsync("operation", "double");
        //        counter *= 2;
        //        await Task.Delay(10000);
        //        await client.RaiseEventAsync("operation", "double");
        //        counter *= 2;
        //        await Task.Delay(10000);
        //        await client.RaiseEventAsync("operation", "double");
        //        counter *= 2;
        //        await Task.Delay(10000);

        //        // Make sure it's still running and didn't complete early (or fail).
        //        var status = await client.GetStatusAsync();
        //        Assert.IsTrue(
        //            status?.OrchestrationStatus == OrchestrationStatus.Running ||
        //            status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

        //        // The end message will cause the actor to complete itself.
        //        await client.RaiseEventAsync("operation", "end");

        //        status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        var result = status?.Output;
        //        Assert.IsNotNull(result);

        //        await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, result);
        //        await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Input);

        //        await host.StopAsync();

        //        return new Tuple<string, TestOrchestrationClient>(
        //            orchestrationState.OrchestrationInstance.InstanceId,
        //            client);
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates the Rewind functionality on more than one orchestration.
        /// </summary>
        //[TestMethod]
        //public async Task RewindOrchestrationsFail()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        Orchestrations.FactorialOrchestratorFail.ShouldFail = true;
        //        await host.StartAsync();

        //        string singletonInstanceId1 = $"1_Test_{Guid.NewGuid():N}";
        //        string singletonInstanceId2 = $"2_Test_{Guid.NewGuid():N}";

        //        var client1 = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.FactorialOrchestratorFail),
        //            input: 3,
        //            instanceId: singletonInstanceId1);

        //        var statusFail = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Orchestrations.FactorialOrchestratorFail.ShouldFail = false;

        //        var client2 = await host.StartOrchestrationAsync(
        //        typeof(Orchestrations.SayHelloWithActivity),
        //        input: "Catherine",
        //        instanceId: singletonInstanceId2);

        //        await client1.RewindAsync("Rewind failed orchestration only");

        //        var statusRewind = await client1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
        //        Assert.AreEqual("6", statusRewind?.Output);

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates the Rewind functionality with fan in fan out pattern.
        /// </summary>
        //[TestMethod]
        //public async Task RewindActivityFailFanOut()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        Activities.HelloFailFanOut.ShouldFail1 = false;
        //        await host.StartAsync();

        //        string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.FanOutFanInRewind),
        //            input: 3,
        //            instanceId: singletonInstanceId);

        //        var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailFanOut.ShouldFail2 = false;

        //        await client.RewindAsync("Rewind orchestrator with failed parallel activity.");

        //        var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
        //        Assert.AreEqual("\"Done\"", statusRewind?.Output);

        //        await host.StopAsync();
        //    }
        //}


        /// <summary>
        /// End-to-end test which validates the Rewind functionality on an activity function failure 
        /// with modified (to fail initially) SayHelloWithActivity orchestrator.
        /// </summary>
        //[TestMethod]
        //public async Task RewindActivityFail()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"{Guid.NewGuid():N}";

        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.SayHelloWithActivityFail),
        //            input: "World",
        //            instanceId: singletonInstanceId);

        //        var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailActivity.ShouldFail = false;

        //        await client.RewindAsync("Activity failure test.");

        //        var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
        //        Assert.AreEqual("\"Hello, World!\"", statusRewind?.Output);

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task RewindMultipleActivityFail()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"Test_{Guid.NewGuid():N}";

        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.FactorialMultipleActivityFail),
        //            input: 4,
        //            instanceId: singletonInstanceId);

        //        var statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.MultiplyMultipleActivityFail.ShouldFail1 = false;

        //        await client.RewindAsync("Rewind for activity failure 1.");

        //        statusFail = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.MultiplyMultipleActivityFail.ShouldFail2 = false;

        //        await client.RewindAsync("Rewind for activity failure 2.");

        //        var statusRewind = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
        //        Assert.AreEqual("24", statusRewind?.Output);

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task RewindSubOrchestrationsTest()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
        //        string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

        //        var clientParent = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.ParentWorkflowSubOrchestrationFail),
        //            input: true,
        //            instanceId: ParentInstanceId);

        //        var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail1 = false;

        //        await clientParent.RewindAsync("Rewind first suborchestration failure.");

        //        statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Orchestrations.ChildWorkflowSubOrchestrationFail.ShouldFail2 = false;

        //        await clientParent.RewindAsync("Rewind second suborchestration failure.");

        //        var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task RewindSubOrchestrationActivityTest()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        string ParentInstanceId = $"Parent_{Guid.NewGuid():N}";
        //        string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

        //        var clientParent = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.ParentWorkflowSubOrchestrationActivityFail),
        //            input: true,
        //            instanceId: ParentInstanceId);

        //        var statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailSubOrchestrationActivity.ShouldFail1 = false;

        //        await clientParent.RewindAsync("Rewinding 1: child should still fail.");

        //        statusFail = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailSubOrchestrationActivity.ShouldFail2 = false;

        //        await clientParent.RewindAsync("Rewinding 2: child should complete.");

        //        var statusRewind = await clientParent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);

        //        await host.StopAsync();
        //    }
        //}

        //[TestMethod]
        //public async Task RewindNestedSubOrchestrationTest()
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions: true))
        //    {
        //        await host.StartAsync();

        //        string GrandparentInstanceId = $"Grandparent_{Guid.NewGuid():N}";
        //        string ChildInstanceId = $"Child_{Guid.NewGuid():N}";

        //        var clientGrandparent = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.GrandparentWorkflowNestedActivityFail),
        //            input: true,
        //            instanceId: GrandparentInstanceId);

        //        var statusFail = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailNestedSuborchestration.ShouldFail1 = false;

        //        await clientGrandparent.RewindAsync("Rewind 1: Nested child activity still fails.");

        //        Assert.AreEqual(OrchestrationStatus.Failed, statusFail?.OrchestrationStatus);

        //        Activities.HelloFailNestedSuborchestration.ShouldFail2 = false;

        //        await clientGrandparent.RewindAsync("Rewind 2: Nested child activity completes.");

        //        var statusRewind = await clientGrandparent.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, statusRewind?.OrchestrationStatus);
        //        //Assert.AreEqual("\"Hello, Catherine!\"", statusRewind?.Output);

        //        await host.StopAsync();
        //    }
        //}

  
        /// <summary>
        /// Test which validates the ETW event source.
        /// </summary>
        //[TestMethod]
        //public void ValidateEventSource()
        //{
        //    EventSourceAnalyzer.InspectAll(AnalyticsEventSource.Log);
        //}

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB text message sizes can run successfully.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task LargeTextMessagePayloads(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        string message = this.GenerateMediumRandomStringPayload().ToString();
        //        var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        await ValidateBlobUrlAsync(
        //            host.TaskHub,
        //            client.InstanceId,
        //            status?.Output,
        //            Encoding.UTF8.GetByteCount(message));

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary bytes message sizes can run successfully.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task LargeBinaryByteMessagePayloads(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        // Construct byte array from large binary file of size 826KB
        //        string originalFileName = "large.jpeg";
        //        string currentDirectory = Directory.GetCurrentDirectory();
        //        string originalFilePath = Path.Combine(currentDirectory, originalFileName);
        //        byte[] readBytes = File.ReadAllBytes(originalFilePath);

        //        var client = await host.StartOrchestrationAsync(typeof(Orchestrations.EchoBytes), readBytes);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

        //        // Large message payloads may actually get bigger when stored in blob storage.
        //        await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Output, (int)(readBytes.Length * 1.3));

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that orchestrations with > 60KB binary string message sizes can run successfully.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task LargeBinaryStringMessagePayloads(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        // Construct string message from large binary file of size 826KB
        //        string originalFileName = "large.jpeg";
        //        string currentDirectory = Directory.GetCurrentDirectory();
        //        string originalFilePath = Path.Combine(currentDirectory, originalFileName);
        //        byte[] readBytes = File.ReadAllBytes(originalFilePath);
        //        string message = Convert.ToBase64String(readBytes);

        //        var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Echo), message);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);

        //        // Large message payloads may actually get bigger when stored in blob storage.
        //        await ValidateBlobUrlAsync(host.TaskHub, client.InstanceId, status?.Output, (int)(readBytes.Length * 1.3));

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that a completed singleton instance can be recreated.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task RecreateCompletedInstance(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.SayHelloWithActivity),
        //            input: "One",
        //            instanceId: singletonInstanceId);
        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        Assert.AreEqual("One", JToken.Parse(status?.Input));
        //        Assert.AreEqual("Hello, One!", JToken.Parse(status?.Output));

        //        client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.SayHelloWithActivity),
        //            input: "Two",
        //            instanceId: singletonInstanceId);
        //        status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        Assert.AreEqual("Two", JToken.Parse(status?.Input));
        //        Assert.AreEqual("Hello, Two!", JToken.Parse(status?.Output));

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that a failed singleton instance can be recreated.
        /// </summary>
        //        [DataTestMethod]
        //        [DataRow(true)]
        //        [DataRow(false)]
        //=        public async Task RecreateFailedInstance(bool enableExtendedSessions)
        //        {
        //            using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //            {
        //                await host.StartAsync();

        //                string singletonInstanceId = $"HelloSingleton_{Guid.NewGuid():N}";

        //                var client = await host.StartOrchestrationAsync(
        //                    typeof(Orchestrations.SayHelloWithActivity),
        //                    input: null, // this will cause the orchestration to fail
        //                    instanceId: singletonInstanceId);
        //                var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //                Assert.AreEqual(OrchestrationStatus.Failed, status?.OrchestrationStatus);

        //                client = await host.StartOrchestrationAsync(
        //                    typeof(Orchestrations.SayHelloWithActivity),
        //                    input: "NotNull",
        //                    instanceId: singletonInstanceId);
        //                status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

        //                Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //                Assert.AreEqual("Hello, NotNull!", JToken.Parse(status?.Output));

        //                await host.StopAsync();
        //            }
        //        }

        /// <summary>
        /// End-to-end test which validates that a terminated orchestration can be recreated.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task RecreateTerminatedInstance(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(enableExtendedSessions))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"SingletonCounter_{Guid.NewGuid():N}";

        //        // Using the counter orchestration because it will wait indefinitely for input.
        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.Counter),
        //            input: -1,
        //            instanceId: singletonInstanceId);

        //        // Need to wait for the instance to start before we can terminate it.
        //        await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //        await client.TerminateAsync("sayōnara");

        //        var status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Terminated, status?.OrchestrationStatus);
        //        Assert.AreEqual("-1", status?.Input);
        //        Assert.AreEqual("sayōnara", status?.Output);

        //        client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.Counter),
        //            input: 0,
        //            instanceId: singletonInstanceId);
        //        status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
        //        Assert.AreEqual("0", status?.Input);

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that a running orchestration can be recreated.
        /// </summary>
        //[DataTestMethod]
        //[DataRow(true)]
        //[DataRow(false)]
        //public async Task RecreateRunningInstance(bool enableExtendedSessions)
        //{
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
        //        enableExtendedSessions,
        //        extendedSessionTimeoutInSeconds: 15))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

        //        // Using the counter orchestration because it will wait indefinitely for input.
        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.Counter),
        //            input: 0,
        //            instanceId: singletonInstanceId);

        //        var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
        //        Assert.AreEqual("0", status?.Input);
        //        Assert.AreEqual(null, status?.Output);

        //        client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.Counter),
        //            input: 99,
        //            instanceId: singletonInstanceId);

        //        // Note that with extended sessions, the startup time may take longer because the dispatcher
        //        // will wait for the current extended session to expire before the new create message is accepted.
        //        status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(20));

        //        Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
        //        Assert.AreEqual("99", status?.Input);

        //        await host.StopAsync();
        //    }
        //}

        /// <summary>
        /// End-to-end test which validates that an orchestration can continue processing
        /// even after its extended session has expired.
        /// </summary>
        //[TestMethod]
        //public async Task ExtendedSessions_SessionTimeout()
        //{
        //    const int SessionTimeoutInseconds = 5;
        //    using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(
        //        enableExtendedSessions: true,
        //        extendedSessionTimeoutInSeconds: SessionTimeoutInseconds))
        //    {
        //        await host.StartAsync();

        //        string singletonInstanceId = $"SingletonCounter_{DateTime.Now:o}";

        //        // Using the counter orchestration because it will wait indefinitely for input.
        //        var client = await host.StartOrchestrationAsync(
        //            typeof(Orchestrations.Counter),
        //            input: 0,
        //            instanceId: singletonInstanceId);

        //        var status = await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Running, status?.OrchestrationStatus);
        //        Assert.AreEqual("0", status?.Input);
        //        Assert.AreEqual(null, status?.Output);

        //        // Delay long enough for the session to expire
        //        await Task.Delay(TimeSpan.FromSeconds(SessionTimeoutInseconds + 1));

        //        await client.RaiseEventAsync("operation", "incr");
        //        await Task.Delay(TimeSpan.FromSeconds(2));

        //        // Make sure it's still running and didn't complete early (or fail).
        //        status = await client.GetStatusAsync();
        //        Assert.IsTrue(
        //            status?.OrchestrationStatus == OrchestrationStatus.Running ||
        //            status?.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew);

        //        // The end message will cause the actor to complete itself.
        //        await client.RaiseEventAsync("operation", "end");

        //        status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

        //        Assert.AreEqual(OrchestrationStatus.Completed, status?.OrchestrationStatus);
        //        Assert.AreEqual(1, JToken.Parse(status?.Output));

        //        await host.StopAsync();
        //    }
        //}

        //private static async Task ValidateBlobUrlAsync(string taskHubName, string instanceId, string value, int originalPayloadSize = 0)
        //{
        //    CloudStorageAccount account = CloudStorageAccount.Parse(TestHelpers.GetTestStorageAccountConnectionString());
        //    Assert.IsTrue(value.StartsWith(account.BlobStorageUri.PrimaryUri.OriginalString));
        //    Assert.IsTrue(value.Contains("/" + instanceId + "/"));
        //    Assert.IsTrue(value.EndsWith(".json.gz"));

        //    string containerName = $"{taskHubName.ToLowerInvariant()}-largemessages";
        //    CloudBlobClient client = account.CreateCloudBlobClient();
        //    CloudBlobContainer container = client.GetContainerReference(containerName);
        //    Assert.IsTrue(await container.ExistsAsync(), $"Blob container {containerName} is expected to exist.");

        //    await client.GetBlobReferenceFromServerAsync(new Uri(value));
        //    CloudBlobDirectory instanceDirectory = container.GetDirectoryReference(instanceId);

        //    string blobName = value.Split('/').Last();
        //    CloudBlob blob = instanceDirectory.GetBlobReference(blobName);
        //    Assert.IsTrue(await blob.ExistsAsync(), $"Blob named {blob.Uri} is expected to exist.");

        //    if (originalPayloadSize > 0)
        //    {
        //        await blob.FetchAttributesAsync();
        //        Assert.IsTrue(blob.Properties.Length < originalPayloadSize, "Blob is expected to be compressed");
        //    }
        //}
    }
}
