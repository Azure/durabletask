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

namespace DurableTask.AzureServiceFabric.Tests
{
    using System;
    using System.Collections.Generic;
    using DurableTask.AzureServiceFabric.Remote;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class RemoteOrchestrationTypeBinderTests
    {
        // Mirrors the production formatter settings configured in RemoteOrchestrationServiceClient.PutJsonAsync.
        static readonly JsonSerializerSettings RoundTripSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto,
            SerializationBinder = new RemoteOrchestrationTypeBinder()
        };

        [TestMethod]
        public void RoundTripsTaskMessageWithPolymorphicHistoryEvent()
        {
            var original = new TaskMessage
            {
                SequenceNumber = 7,
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance-1",
                    ExecutionId = "execution-1"
                },
                Event = new ExecutionStartedEvent(eventId: -1, input: "input")
                {
                    Name = "Orchestration",
                    Version = "1.0",
                    Tags = new Dictionary<string, string> { ["tag1"] = "value1" }
                }
            };

            string json = JsonConvert.SerializeObject(original, RoundTripSettings);
            var deserialized = JsonConvert.DeserializeObject<TaskMessage>(json, RoundTripSettings);

            Assert.IsInstanceOfType(deserialized.Event, typeof(ExecutionStartedEvent));
            var startedEvent = (ExecutionStartedEvent)deserialized.Event;
            Assert.AreEqual("Orchestration", startedEvent.Name);
            Assert.AreEqual("input", startedEvent.Input);
            Assert.AreEqual("value1", startedEvent.Tags["tag1"]);
        }

        [TestMethod]
        public void AllowsTypesFromDurableTaskCoreAssembly()
        {
            var binder = new RemoteOrchestrationTypeBinder();
            Type bound = binder.BindToType(
                typeof(TaskMessage).Assembly.GetName().Name,
                typeof(TaskMessage).FullName);
            Assert.AreEqual(typeof(TaskMessage), bound);
        }

        [TestMethod]
        public void AllowsTypesFromDurableTaskAzureServiceFabricAssembly()
        {
            var binder = new RemoteOrchestrationTypeBinder();
            Type bound = binder.BindToType(
                typeof(TaskMessageItem).Assembly.GetName().Name,
                typeof(TaskMessageItem).FullName);
            Assert.AreEqual(typeof(TaskMessageItem), bound);
        }

        [TestMethod]
        public void RejectsNonAllowlistedRootType()
        {
            string json = "{\"$type\":\"System.IO.FileInfo, System.Private.CoreLib\",\"OriginalPath\":\"c:\\\\evil\"}";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<object>(json, RoundTripSettings));
        }

        [TestMethod]
        public void RejectsNonAllowlistedNestedType()
        {
            string json = "{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent, DurableTask.Core\","
                + "\"Tags\":{\"$type\":\"System.Collections.Generic.SortedDictionary`2[[System.String, System.Private.CoreLib],[System.String, System.Private.CoreLib]], System.Collections\"}}";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<HistoryEvent>(json, RoundTripSettings));
        }
    }
}
