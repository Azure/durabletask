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
    using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            Assert.ThrowsException<Newtonsoft.Json.JsonSerializationException>(
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
    }
}
