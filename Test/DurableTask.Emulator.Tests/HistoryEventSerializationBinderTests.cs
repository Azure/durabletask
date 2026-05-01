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

namespace DurableTask.Emulator.Tests
{
    using System;
    using System.Collections.Generic;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class HistoryEventSerializationBinderTests
    {
        // Mirrors the production state settings used by LocalOrchestrationService.
        static readonly JsonSerializerSettings StateJsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto,
            SerializationBinder = new HistoryEventSerializationBinder()
        };

        [TestMethod]
        public void RoundTripsHistoryEventListWithPolymorphicEventsAndTags()
        {
            IList<HistoryEvent> original = new List<HistoryEvent>
            {
                new ExecutionStartedEvent(eventId: -1, input: "input")
                {
                    Name = "Orchestration",
                    Version = "1.0",
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = "instance-1",
                        ExecutionId = "execution-1"
                    },
                    Tags = new Dictionary<string, string> { ["tag1"] = "value1" }
                },
                new TaskScheduledEvent(eventId: 1)
                {
                    Name = "Activity",
                    Version = "1.0",
                    Input = "activity-input"
                }
            };

            string json = JsonConvert.SerializeObject(original, StateJsonSettings);
            var deserialized = JsonConvert.DeserializeObject<IList<HistoryEvent>>(json, StateJsonSettings);

            Assert.AreEqual(2, deserialized.Count);
            Assert.IsInstanceOfType(deserialized[0], typeof(ExecutionStartedEvent));
            var startedEvent = (ExecutionStartedEvent)deserialized[0];
            Assert.AreEqual("Orchestration", startedEvent.Name);
            Assert.AreEqual("input", startedEvent.Input);
            Assert.AreEqual("value1", startedEvent.Tags["tag1"]);
            Assert.IsInstanceOfType(deserialized[1], typeof(TaskScheduledEvent));
            Assert.AreEqual("Activity", ((TaskScheduledEvent)deserialized[1]).Name);
        }

        [TestMethod]
        public void RejectsNonAllowlistedRootType()
        {
            string json = "{\"$type\":\"System.IO.FileInfo, System.Private.CoreLib\",\"OriginalPath\":\"c:\\\\evil\"}";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<HistoryEvent>(json, StateJsonSettings));
        }

        [TestMethod]
        public void RejectsNonAllowlistedNestedType()
        {
            string json = "[{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent, DurableTask.Core\","
                + "\"Tags\":{\"$type\":\"System.Collections.Generic.SortedDictionary`2[[System.String, System.Private.CoreLib],[System.String, System.Private.CoreLib]], System.Collections\"}}]";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<IList<HistoryEvent>>(json, StateJsonSettings));
        }
    }
}
