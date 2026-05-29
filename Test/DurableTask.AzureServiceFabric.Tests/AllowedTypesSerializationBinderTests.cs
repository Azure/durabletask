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
    using System.Linq;
    using System.Reflection;
    using DurableTask.AzureServiceFabric.Models;
    using DurableTask.AzureServiceFabric.Service;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class AllowedTypesSerializationBinderTests
    {
        readonly AllowedTypesSerializationBinder binder = new AllowedTypesSerializationBinder();

        [TestMethod]
        public void BindToType_AllowsDurableTaskCoreTypes()
        {
            var type = typeof(TaskMessage);
            var result = this.binder.BindToType(type.Assembly.GetName().Name, type.FullName);
            Assert.AreEqual(type, result);
        }

        [TestMethod]
        public void BindToType_AllowsHistoryEventSubclasses()
        {
            var type = typeof(ExecutionStartedEvent);
            var result = this.binder.BindToType(type.Assembly.GetName().Name, type.FullName);
            Assert.AreEqual(type, result);
        }

        [TestMethod]
        public void BindToType_AllowsServiceFabricProxyTypes()
        {
            var type = typeof(CreateTaskOrchestrationParameters);
            var result = this.binder.BindToType(type.Assembly.GetName().Name, type.FullName);
            Assert.AreEqual(type, result);
        }

        [TestMethod]
        public void BindToType_AllowsMscorlibTypes()
        {
            var type = typeof(Dictionary<string, string>);
            var result = this.binder.BindToType("mscorlib", type.FullName);
            Assert.AreEqual(type, result);
        }

        [TestMethod]
        public void BindToType_AllowsQualifiedAssemblyName()
        {
            var type = typeof(TaskMessage);
            string qualifiedName = type.Assembly.FullName; // e.g. "DurableTask.Core, Version=..."
            var result = this.binder.BindToType(qualifiedName, type.FullName);
            Assert.AreEqual(type, result);
        }

        [TestMethod]
        public void BindToType_AllowsNullAssemblyName()
        {
            // Null/empty assembly name should pass through to the default binder
            var result = this.binder.BindToType(null, typeof(string).FullName);
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void BindToType_RejectsArbitraryAssembly()
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
                this.binder.BindToType("Evil.Assembly", "Evil.PwnedDescriptor"));
        }

        [TestMethod]
        public void BindToType_RejectsQualifiedArbitraryAssembly()
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
                this.binder.BindToType("Evil.Assembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", "Evil.PwnedDescriptor"));
        }

        [TestMethod]
        public void BindToType_RejectsSystemDiagnosticsProcess()
        {
            // A common gadget type — must be rejected
            var type = typeof(System.Diagnostics.Process);
            Assert.ThrowsException<InvalidOperationException>(() =>
                this.binder.BindToType(type.Assembly.GetName().Name, type.FullName));
        }

        [TestMethod]
        public void BindToType_RejectsNonAllowlistedMscorlibType()
        {
            // System.Type is from mscorlib but not in the type allowlist
            Assert.ThrowsException<InvalidOperationException>(() =>
                this.binder.BindToType("mscorlib", typeof(Type).FullName));
        }

        [TestMethod]
        public void BindToType_RejectsNonAllowlistedDurableTaskCoreType()
        {
            // TaskOrchestration is a DurableTask.Core type but not in the proxy endpoint allowlist
            var type = typeof(TaskOrchestration);
            Assert.ThrowsException<InvalidOperationException>(() =>
                this.binder.BindToType(type.Assembly.GetName().Name, type.FullName));
        }

        [TestMethod]
        public void BindToType_AllowsAllHistoryEventKnownTypes()
        {
            IEnumerable<Type> knownTypes;
            try
            {
                knownTypes = HistoryEvent.KnownTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                // In test environments, not all types may be loadable
                knownTypes = ex.Types.Where(t => t != null && !t.IsAbstract && typeof(HistoryEvent).IsAssignableFrom(t));
            }

            foreach (Type knownType in knownTypes)
            {
                var result = this.binder.BindToType(knownType.Assembly.GetName().Name, knownType.FullName);
                Assert.AreEqual(knownType, result, $"HistoryEvent subclass {knownType.Name} should be allowed");
            }
        }

        [TestMethod]
        public void BindToName_DelegatesToDefaultBinder()
        {
            this.binder.BindToName(typeof(TaskMessage), out string assemblyName, out string typeName);
            // DefaultSerializationBinder delegates to the runtime; just verify it doesn't throw
            // and returns consistent results for a known type.
            this.binder.BindToName(typeof(TaskMessage), out string assemblyName2, out string typeName2);
            Assert.AreEqual(assemblyName, assemblyName2);
            Assert.AreEqual(typeName, typeName2);
        }

        [TestMethod]
        public void RoundTrip_TaskMessageWithHistoryEvent_Succeeds()
        {
            var message = new TaskMessage
            {
                SequenceNumber = 42,
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "test-1", ExecutionId = "exec-1" },
                Event = new ExecutionStartedEvent(-1, "input-data")
                {
                    Tags = new Dictionary<string, string> { { "key", "value" } },
                    Name = "TestOrchestration",
                    Version = "1.0"
                }
            };

            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All,
                SerializationBinder = this.binder
            };

            string json = JsonConvert.SerializeObject(message, settings);
            var deserialized = JsonConvert.DeserializeObject<TaskMessage>(json, settings);

            Assert.IsNotNull(deserialized);
            Assert.AreEqual(42, deserialized.SequenceNumber);
            Assert.AreEqual("test-1", deserialized.OrchestrationInstance.InstanceId);
            Assert.IsInstanceOfType(deserialized.Event, typeof(ExecutionStartedEvent));

            var startedEvent = (ExecutionStartedEvent)deserialized.Event;
            Assert.AreEqual("TestOrchestration", startedEvent.Name);
            Assert.AreEqual("value", startedEvent.Tags["key"]);
        }

        [TestMethod]
        public void Deserialize_MaliciousPayload_IsRejected()
        {
            string maliciousJson = @"{
                ""$type"": ""System.Diagnostics.Process, System"",
                ""StartInfo"": { ""FileName"": ""cmd.exe"" }
            }";

            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All,
                SerializationBinder = this.binder
            };

            // Newtonsoft wraps the binder's InvalidOperationException in a JsonSerializationException
            var ex = Assert.ThrowsException<JsonSerializationException>(() =>
                JsonConvert.DeserializeObject<object>(maliciousJson, settings));
            Assert.IsInstanceOfType(ex.InnerException, typeof(InvalidOperationException));
            StringAssert.Contains(ex.InnerException.Message, "is not allowed");
        }

        [TestMethod]
        public void Settings_DefaultBinderIsAllowedTypes()
        {
            var providerSettings = new FabricOrchestrationProviderSettings();
            Assert.IsNotNull(providerSettings.JsonSerializationBinder);
            Assert.IsInstanceOfType(providerSettings.JsonSerializationBinder, typeof(AllowedTypesSerializationBinder));
        }

        [TestMethod]
        public void Settings_BinderCanBeSetToNull()
        {
            var providerSettings = new FabricOrchestrationProviderSettings();
            providerSettings.JsonSerializationBinder = null;
            Assert.IsNull(providerSettings.JsonSerializationBinder);
        }

        [TestMethod]
        public void Settings_BinderCanBeOverridden()
        {
            var customBinder = new Newtonsoft.Json.Serialization.DefaultSerializationBinder();
            var providerSettings = new FabricOrchestrationProviderSettings();
            providerSettings.JsonSerializationBinder = customBinder;
            Assert.AreSame(customBinder, providerSettings.JsonSerializationBinder);
        }
    }
}
