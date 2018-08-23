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
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.ServiceBus.Settings;
    using DurableTask.ServiceBus.Tracking;
    using DurableTask.Core.Tests;

    [TestClass]
    public class RuntimeStateStreamConverterTest
    {
        const int sessionOverflowThresholdInBytes = 2 * 1024;
        const int sessionMaxSizeInBytes = 10 * 1024;
        const string sessionId = "session123";
        ServiceBusSessionSettings serviceBusSessionSettings = new ServiceBusSessionSettings(sessionOverflowThresholdInBytes, sessionMaxSizeInBytes);

        AzureTableInstanceStore azureTableInstanceStore;
        AzureStorageBlobStore azureStorageBlobStore;

        [TestInitialize]
        public void TestInitialize()
        {
            azureTableInstanceStore = TestHelpers.CreateAzureTableInstanceStore();
            azureStorageBlobStore = TestHelpers.CreateAzureStorageBlobStore();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            azureTableInstanceStore.DeleteStoreAsync().Wait();
            azureStorageBlobStore.DeleteStoreAsync().Wait();
        }

        [TestMethod]
        public async Task SmallRuntimeStateConverterTest()
        {       
            string smallInput = "abc";

            OrchestrationRuntimeState newOrchestrationRuntimeStateSmall = generateOrchestrationRuntimeState(smallInput);
            OrchestrationRuntimeState runtimeState = new OrchestrationRuntimeState();
            DataConverter dataConverter = new JsonDataConverter();

            // a small runtime state doesn't need external storage.
            Stream rawStreamSmall = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                newOrchestrationRuntimeStateSmall,
                runtimeState,
                dataConverter,
                true,
                serviceBusSessionSettings,
                azureStorageBlobStore,
                sessionId);
            OrchestrationRuntimeState convertedRuntimeStateSmall = await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawStreamSmall, "sessionId", azureStorageBlobStore, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall);

            // test for un-compress case
            Stream rawStreamSmall2 = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                newOrchestrationRuntimeStateSmall,
                runtimeState,
                dataConverter,
                false,
                serviceBusSessionSettings,
                azureStorageBlobStore,
                sessionId);
            OrchestrationRuntimeState convertedRuntimeStateSmall2 = await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawStreamSmall2, "sessionId", azureStorageBlobStore, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall2);

            // test for backward comp: ok for an un-implemented (or null) IBlobStorage for small runtime states
            Stream rawStreamSmall3 = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                newOrchestrationRuntimeStateSmall,
                runtimeState,
                dataConverter,
                true,
                serviceBusSessionSettings,
                null,
                sessionId);
            OrchestrationRuntimeState convertedRuntimeStateSmall3 = await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawStreamSmall3, "sessionId", null, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall3);
        }

        [TestMethod]
        public async Task LargeRuntimeStateConverterTest()
        {
            string largeInput = TestUtils.GenerateRandomString(5 * 1024);
            OrchestrationRuntimeState newOrchestrationRuntimeStateLarge = generateOrchestrationRuntimeState(largeInput);
            OrchestrationRuntimeState runtimeState = new OrchestrationRuntimeState();
            DataConverter dataConverter = new JsonDataConverter();

            // a large runtime state that needs external storage.
            Stream rawStreamLarge = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                newOrchestrationRuntimeStateLarge,
                runtimeState,
                dataConverter,
                true,
                serviceBusSessionSettings,
                azureStorageBlobStore,
                sessionId);
            OrchestrationRuntimeState convertedRuntimeStateLarge = await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawStreamLarge, "sessionId", azureStorageBlobStore, dataConverter);
            verifyEventInput(largeInput, convertedRuntimeStateLarge);

            // test for un-compress case
            string largeInput2 = TestUtils.GenerateRandomString(3 * 1024);
            OrchestrationRuntimeState newOrchestrationRuntimeStateLarge2 = generateOrchestrationRuntimeState(largeInput2);
            Stream rawStreamLarge2 = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                newOrchestrationRuntimeStateLarge2,
                runtimeState,
                dataConverter,
                false,
                serviceBusSessionSettings,
                azureStorageBlobStore,
                sessionId);
            OrchestrationRuntimeState convertedRuntimeStateLarge2 = await RuntimeStateStreamConverter.RawStreamToRuntimeState(rawStreamLarge2, "sessionId", azureStorageBlobStore, dataConverter);
            verifyEventInput(largeInput2, convertedRuntimeStateLarge2);

            // test for an un-implemented (or null) IBlobStorage for large runtime states: should throw exception
            try
            {
                await
                    RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                        newOrchestrationRuntimeStateLarge,
                        runtimeState,
                        dataConverter,
                        true,
                        serviceBusSessionSettings,
                        null,
                        sessionId);
                Assert.Fail("ArgumentException must be thrown");
            }
            catch (OrchestrationException e)
            {
                // expected
                Assert.IsTrue(e.Message.Contains("IOrchestrationServiceBlobStore"), "Exception must contain IOrchestrationServiceBlobStore.");
            }
        }

        [TestMethod]
        public async Task VeryLargeRuntimeStateConverterTest()
        {
            string veryLargeInput = TestUtils.GenerateRandomString(20 * 1024);
            OrchestrationRuntimeState newOrchestrationRuntimeStateLarge = generateOrchestrationRuntimeState(veryLargeInput);
            OrchestrationRuntimeState runtimeState = new OrchestrationRuntimeState();
            DataConverter dataConverter = new JsonDataConverter();

            // test for very large size rumtime state that cannot be saved externally: should throw exception
            try
            {
                Stream rawStreamVeryLarge = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                    newOrchestrationRuntimeStateLarge,
                    runtimeState,
                    dataConverter,
                    true,
                    serviceBusSessionSettings,
                    azureStorageBlobStore,
                    sessionId);
                Assert.Fail("ArgumentException must be thrown");
            }
            catch (OrchestrationException e)
            {
                // expected
                Assert.IsTrue(e.Message.Contains("exceeded"), "Exception must contain exceeded.");
            }
        }

        [TestMethod]
        public async Task ConverterCompatabilityTest()
        {
            string smallInput = "abc";
            OrchestrationRuntimeState newOrchestrationRuntimeStateSmall = generateOrchestrationRuntimeState(smallInput);
            OrchestrationRuntimeState runtimeState = new OrchestrationRuntimeState();
            DataConverter dataConverter = new JsonDataConverter();

            // deserialize a OrchestrationRuntimeState object, with both compression and not compression
            Stream stream = serializeToStream(dataConverter, newOrchestrationRuntimeStateSmall, true);
            OrchestrationRuntimeState convertedRuntimeStateSmall = await RuntimeStateStreamConverter.RawStreamToRuntimeState(stream, "sessionId", null, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall);

            stream = serializeToStream(dataConverter, newOrchestrationRuntimeStateSmall, false);
            convertedRuntimeStateSmall = await RuntimeStateStreamConverter.RawStreamToRuntimeState(stream, "sessionId", null, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall);

            // deserialize a IList<HistoryEvent> object, with both compression and not compression
            Stream stream2 = serializeToStream(dataConverter, newOrchestrationRuntimeStateSmall.Events, true);
            OrchestrationRuntimeState convertedRuntimeStateSmall2 = await RuntimeStateStreamConverter.RawStreamToRuntimeState(stream2, "sessionId", null, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall2);

            stream2 = serializeToStream(dataConverter, newOrchestrationRuntimeStateSmall.Events, false);
            convertedRuntimeStateSmall2 = await RuntimeStateStreamConverter.RawStreamToRuntimeState(stream2, "sessionId", null, dataConverter);
            verifyEventInput(smallInput, convertedRuntimeStateSmall2);
        }

        private Stream serializeToStream(DataConverter dataConverter, OrchestrationRuntimeState orchestrationRuntimeState, bool shouldCompress)
        {
            string serializedState = dataConverter.Serialize(orchestrationRuntimeState);
            long originalStreamSize = 0;
            return Utils.WriteStringToStream(
                serializedState,
                shouldCompress,
                out originalStreamSize);
        }

        private Stream serializeToStream(DataConverter dataConverter, IList<HistoryEvent> events, bool shouldCompress)
        {
            string serializedState = dataConverter.Serialize(events);
            long originalStreamSize = 0;
            return Utils.WriteStringToStream(
                serializedState,
                shouldCompress,
                out originalStreamSize);
        }

        OrchestrationRuntimeState generateOrchestrationRuntimeState(string input)
        {
            IList<HistoryEvent> historyEvents = new List<HistoryEvent>();
            ExecutionStartedEvent historyEvent = new ExecutionStartedEvent(1, input);
            historyEvents.Add(historyEvent);
            OrchestrationRuntimeState newOrchestrationRuntimeState = new OrchestrationRuntimeState(historyEvents);

            return newOrchestrationRuntimeState;
        }

        void verifyEventInput(string expectedHistoryEventInput, OrchestrationRuntimeState runtimeState)
        {
            ExecutionStartedEvent executionStartedEvent = runtimeState.Events[0] as ExecutionStartedEvent;
            Assert.AreEqual(expectedHistoryEventInput, executionStartedEvent.Input);
        }
    }
}
