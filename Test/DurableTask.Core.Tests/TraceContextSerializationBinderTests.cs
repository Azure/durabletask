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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using DurableTask.Core.Serializing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class TraceContextSerializationBinderTests
    {
        [TestMethod]
        public void RejectsNonTraceContextRootType()
        {
            // Root $type resolves to System.String, which is not a TraceContextBase subclass.
            // Restore's pre-binder check rejects with a generic Exception.
            string json = "{\"$type\":\"System.String\",\"Value\":\"evil\"}";
            Assert.ThrowsException<Exception>(() => TraceContextBase.Restore(json));
        }

        [TestMethod]
        public void RejectsNonAllowlistedNestedType()
        {
            // Root is a legitimate W3CTraceContext, but a nested $type points outside the allowlist.
            string json = "{\"$type\":\"DurableTask.Core.W3CTraceContext, DurableTask.Core\","
                + "\"OrchestrationTraceContexts\":[{\"$type\":\"System.String\",\"Value\":\"evil\"}]}";
            Assert.ThrowsException<JsonSerializationException>(
                () => TraceContextBase.Restore(json));
        }

        [TestMethod]
        public void AllowsLegitimateW3CTraceContextRoundTrip()
        {
            string json = "{\"$id\":\"1\",\"$type\":\"DurableTask.Core.W3CTraceContext, DurableTask.Core\","
                + "\"TraceParent\":\"00-a422532de19d3e4f8f67af06f8f880c7-81354b086ec6fb41-02\","
                + "\"TraceState\":null,\"ParentSpanId\":\"b69bc0f95af84240\","
                + "\"StartTime\":\"2019-05-03T23:43:27.6728211+00:00\","
                + "\"OrchestrationTraceContexts\":[{\"$id\":\"2\",\"$type\":\"DurableTask.Core.W3CTraceContext, DurableTask.Core\","
                + "\"TraceParent\":\"00-a422532de19d3e4f8f67af06f8f880c7-f86a8711d7226d42-02\","
                + "\"TraceState\":null,\"ParentSpanId\":\"2ec2a64f22dbb143\","
                + "\"StartTime\":\"2019-05-03T23:43:12.7553182+00:00\","
                + "\"OrchestrationTraceContexts\":[{\"$ref\":\"2\"}]}]}";

            TraceContextBase context = TraceContextBase.Restore(json);

            Assert.IsInstanceOfType(context, typeof(W3CTraceContext));
            Assert.AreEqual(DateTimeOffset.Parse("2019-05-03T23:43:27.6728211+00:00"), context.StartTime);
        }

        [TestMethod]
        public void AllowsActualSerializeDeserializeRoundTrip()
        {
            // End-to-end check: SerializableTraceContext produces a payload that Restore
            // accepts. This guards against a binder allowlist that is too tight to round-trip
            // a legitimately serialized TraceContext.
            var original = new W3CTraceContext
            {
                TraceParent = "00-a422532de19d3e4f8f67af06f8f880c7-81354b086ec6fb41-02",
                ParentSpanId = "b69bc0f95af84240",
                StartTime = DateTimeOffset.Parse("2019-05-03T23:43:27.6728211+00:00"),
                OperationName = "TestOp",
                TelemetryType = TelemetryType.Request,
            };

            string json = original.SerializableTraceContext;
            TraceContextBase restored = TraceContextBase.Restore(json);

            Assert.IsInstanceOfType(restored, typeof(W3CTraceContext));
            Assert.AreEqual(original.StartTime, restored.StartTime);
            Assert.AreEqual(original.OperationName, restored.OperationName);
        }

        [TestMethod]
        public void RejectsNullAssemblyName()
        {
            // Json.NET can invoke BindToType with a null assemblyName when an incoming $type
            // token omits the assembly portion. The binder must fail deterministically with a
            // JsonSerializationException rather than letting a NullReferenceException leak out
            // of PackageUpgradeSerializationBinder.
            var binder = new TraceContextSerializationBinder();
            Assert.ThrowsException<JsonSerializationException>(
                () => binder.BindToType(assemblyName: null, typeName: "DurableTask.Core.W3CTraceContext"));
        }

        [TestMethod]
        public void RejectsNonAllowlistedAssembly()
        {
            // Even a type *named* like a TraceContext must be rejected if it claims to live in
            // a non-allowlisted assembly. The pre-filter rejects without loading the assembly.
            var binder = new TraceContextSerializationBinder();
            Assert.ThrowsException<JsonSerializationException>(
                () => binder.BindToType(
                    assemblyName: "Some.Evil.Assembly",
                    typeName: "DurableTask.Core.W3CTraceContext"));
        }

        [TestMethod]
        public void RejectsBclGadgetTypeInAllowlistedAssembly()
        {
            // System.IO.FileInfo is a classic gadget-chain probe. It lives in the BCL (which
            // is on the assembly-name allowlist so that Stack<TraceContextBase> can be bound),
            // so this test specifically exercises the post-resolution IsAllowed filter rather
            // than the assembly-name pre-filter. The BCL assembly name differs between TFMs
            // (System.Private.CoreLib on .NET, mscorlib on .NET Framework), so build the
            // assembly name from the runtime type to avoid hard-coding it.
            string bclAssemblyName = typeof(FileInfo).Assembly.GetName().Name;
            var binder = new TraceContextSerializationBinder();
            Assert.ThrowsException<JsonSerializationException>(
                () => binder.BindToType(assemblyName: bclAssemblyName, typeName: "System.IO.FileInfo"));
        }

        [TestMethod]
        public void AllowsAllConcreteTraceContextTypes()
        {
            // Sanity check that every concrete TraceContextBase subclass declared in
            // DurableTask.Core is accepted by the binder.
            var binder = new TraceContextSerializationBinder();
            string assemblyName = typeof(TraceContextBase).Assembly.GetName().Name;
            foreach (Type concreteType in new[]
            {
                typeof(W3CTraceContext),
                typeof(HttpCorrelationProtocolTraceContext),
                typeof(NullObjectTraceContext),
            })
            {
                Type bound = binder.BindToType(assemblyName, concreteType.FullName);
                Assert.AreEqual(concreteType, bound);
            }
        }

        [TestMethod]
        public void AllowsStackOfTraceContextBase()
        {
            // The Stack<TraceContextBase> generic closed type is on the allowlist so that the
            // OrchestrationTraceContexts member can be bound. Confirm that direct lookup works.
            var binder = new TraceContextSerializationBinder();
            Type stackType = typeof(Stack<TraceContextBase>);
            string typeName = stackType.FullName;
            string assemblyName = stackType.Assembly.GetName().Name;
            Type bound = binder.BindToType(assemblyName, typeName);
            Assert.AreEqual(stackType, bound);
        }

        [TestMethod]
        public void AllowsLegacyDurableTaskAssemblyNameRewrite()
        {
            // Pre-v2 DTFx payloads were written with assembly name 'DurableTask' (or
            // 'DurableTaskFx') and a 'DurableTask.<X>' type name. PackageUpgradeSerializationBinder
            // (the base class) rewrites these to 'DurableTask.Core.<X>' for upgrade compatibility.
            // This test guards that path: TraceContextSerializationBinder must keep the legacy
            // assembly name on its allowlist and must accept the rewritten type after resolution.
            var binder = new TraceContextSerializationBinder();
            Type bound = binder.BindToType(
                assemblyName: "DurableTask",
                typeName: "DurableTask.W3CTraceContext");
            Assert.AreEqual(typeof(W3CTraceContext), bound);
        }
    }
}
