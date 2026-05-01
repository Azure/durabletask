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
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class HistoryEventSerializationBinderTests
    {
        // Mirrors the production read settings on AzureTableOrchestrationHistoryEventEntity.
        static readonly JsonSerializerSettings ReadJsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects,
            SerializationBinder = new HistoryEventSerializationBinder()
        };

        // Settings used to *produce* the JSON exactly as the production code does.
        static readonly JsonSerializerSettings WriteJsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects
        };

        [TestMethod]
        public void RoundTripsExecutionStartedEventWithTags()
        {
            var original = new ExecutionStartedEvent(eventId: -1, input: "input")
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = "instance-1",
                    ExecutionId = "execution-1",
                },
                Name = "OrchestrationName",
                Version = "1.0",
                Tags = new Dictionary<string, string> { ["tag1"] = "value1" },
            };

            string json = JsonConvert.SerializeObject(original, WriteJsonSettings);
            var deserialized = JsonConvert.DeserializeObject<HistoryEvent>(json, ReadJsonSettings);

            Assert.IsInstanceOfType(deserialized, typeof(ExecutionStartedEvent));
            var deserializedStarted = (ExecutionStartedEvent)deserialized;
            Assert.AreEqual("OrchestrationName", deserializedStarted.Name);
            Assert.AreEqual("input", deserializedStarted.Input);
            Assert.AreEqual("value1", deserializedStarted.Tags["tag1"]);
        }

        [TestMethod]
        public void AllowsAllConcreteHistoryEventTypes()
        {
            // Sanity check that every concrete HistoryEvent subclass declared in DurableTask.Core
            // is accepted by the binder's BindToType.
            var binder = new HistoryEventSerializationBinder();
            foreach (Type concreteType in HistoryEvent.KnownTypes())
            {
                Type bound = binder.BindToType(concreteType.Assembly.GetName().Name, concreteType.FullName);
                Assert.AreEqual(concreteType, bound);
            }
        }

        [TestMethod]
        public void RejectsNonAllowlistedRootType()
        {
            // System.IO.FileInfo is a classic gadget-chain probe. The binder must reject it
            // even though the BCL would happily resolve it via Type.GetType. FileInfo lives
            // in the BCL (which is on the assembly-name allowlist so that Dictionary<string,
            // string> can round-trip), so this test specifically exercises the post-resolution
            // IsAllowed filter rather than the assembly-name pre-filter.
            // The BCL assembly name differs between TFMs (System.Private.CoreLib on .NET,
            // mscorlib on .NET Framework), so build the $type string from the actual runtime
            // type to avoid passing for the wrong reason (type-resolution failure).
            string bclAssemblyName = typeof(FileInfo).Assembly.GetName().Name;
            string json = $"{{\"$type\":\"System.IO.FileInfo, {bclAssemblyName}\",\"OriginalPath\":\"c:\\\\evil\"}}";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<HistoryEvent>(json, ReadJsonSettings));
        }

        [TestMethod]
        public void RejectsNonAllowlistedNestedType()
        {
            // Embed a malicious $type inside an otherwise valid ExecutionStartedEvent's Tags
            // member. The Tags member is IDictionary<string,string>, so the $type token is
            // honored by Newtonsoft.Json when reading. Anything other than
            // Dictionary<string,string> must be rejected. SortedDictionary lives in a non-BCL
            // assembly (System.Collections on .NET, System on .NET Framework) so this test
            // exercises the assembly-name pre-filter.
            // Build $type strings dynamically so they resolve under both target frameworks.
            string bclAssemblyName = typeof(string).Assembly.GetName().Name;
            string sortedDictAssemblyName = typeof(SortedDictionary<string, string>).Assembly.GetName().Name;
            string json =
                "{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent, DurableTask.Core\","
                + $"\"Tags\":{{\"$type\":\"System.Collections.Generic.SortedDictionary`2[[System.String, {bclAssemblyName}],[System.String, {bclAssemblyName}]], {sortedDictAssemblyName}\"}}}}";
            Assert.ThrowsException<JsonSerializationException>(
                () => JsonConvert.DeserializeObject<HistoryEvent>(json, ReadJsonSettings));
        }

        [TestMethod]
        public void AllowsDictionaryStringStringForTags()
        {
            // The exact concrete type Newtonsoft.Json emits for IDictionary<string,string> Tags.
            string bclAssemblyName = typeof(string).Assembly.GetName().Name;
            string dictAssemblyName = typeof(Dictionary<string, string>).Assembly.GetName().Name;
            string json =
                "{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent, DurableTask.Core\","
                + $"\"Tags\":{{\"$type\":\"System.Collections.Generic.Dictionary`2[[System.String, {bclAssemblyName}],[System.String, {bclAssemblyName}]], {dictAssemblyName}\","
                + "\"tag1\":\"v1\"}}";

            var deserialized = (ExecutionStartedEvent)JsonConvert.DeserializeObject<HistoryEvent>(json, ReadJsonSettings);
            Assert.AreEqual("v1", deserialized.Tags["tag1"]);
        }

        [TestMethod]
        public void AllowsLegacyDurableTaskAssemblyNameRewrite()
        {
            // Pre-v2 DTFx payloads were written with assembly name 'DurableTask' (or
            // 'DurableTaskFx') and a 'DurableTask.<X>' type name. PackageUpgradeSerializationBinder
            // (the base class) rewrites these to 'DurableTask.Core.<X>' for upgrade compatibility.
            // This test guards that path: HistoryEventSerializationBinder must keep the legacy
            // assembly name on its allowlist and must accept the rewritten type after resolution.
            var binder = new HistoryEventSerializationBinder();
            Type bound = binder.BindToType(
                assemblyName: "DurableTask",
                typeName: "DurableTask.History.ExecutionStartedEvent");
            Assert.AreEqual(typeof(ExecutionStartedEvent), bound);
        }
    }
}
