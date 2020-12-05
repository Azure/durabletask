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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Xml.Serialization;

    [TestClass]
    public class W3CTraceContextTest
    {
        [TestMethod]
        public void SetParentNormalCase()
        {
            var ExpectedTraceState = "congo=t61rcWkgMzE";
            var parentContext = new W3CTraceContext()
            {
                OperationName = "Foo",
                TraceState = ExpectedTraceState
            };
            parentContext.StartAsNew();
            Assert.AreEqual(ExpectedTraceState, parentContext.TraceState);
            Assert.AreEqual(parentContext.CurrentActivity.Id, parentContext.TraceParent);
            Assert.AreEqual(parentContext.CurrentActivity.SpanId.ToHexString(), parentContext.TelemetryId);
            Assert.AreEqual(parentContext.CurrentActivity.RootId, parentContext.TelemetryContextOperationId);
            Assert.AreEqual(parentContext.CurrentActivity.ParentSpanId.ToHexString(), parentContext.TelemetryContextOperationParentId);

            var childContext = new W3CTraceContext()
            {
                OperationName = "Bar"
            };
            childContext.SetParentAndStart(parentContext);
            Assert.AreEqual(ExpectedTraceState, childContext.TraceState);
            Assert.AreEqual(childContext.CurrentActivity.Id, childContext.TraceParent);
            Assert.AreEqual(childContext.CurrentActivity.SpanId.ToHexString(),childContext.TelemetryId);
            Assert.AreEqual(childContext.CurrentActivity.RootId, childContext.TelemetryContextOperationId);
            Assert.AreEqual(parentContext.CurrentActivity.SpanId.ToHexString(), childContext.ParentSpanId);
            Assert.AreEqual(parentContext.CurrentActivity.SpanId.ToHexString(), childContext.TelemetryContextOperationParentId);
        }

        [TestMethod]
        public void RestoredSoThatNoCurrentActivity()
        {
            var ExpectedTraceState = "congo=t61rcWkgMzE";
            var ExpectedSpanId = "b7ad6b7169203331";
            var ExpectedRootId = "0af7651916cd43dd8448eb211c80319c";
            var ExpectedTraceParent = $"00-{ExpectedRootId}-{ExpectedSpanId}-01";
            var ExpectedParentSpanId = "00f067aa0ba902b7";
            var context = new W3CTraceContext()
            {
                OperationName = "Foo",
                TraceState = ExpectedTraceState,
                TraceParent = ExpectedTraceParent,
                ParentSpanId = ExpectedParentSpanId,
                TelemetryType = TelemetryType.Request
            };
            Assert.AreEqual(ExpectedTraceState, context.TraceState);
            Assert.AreEqual(ExpectedTraceParent, context.TraceParent);
            Assert.AreEqual(ExpectedSpanId, context.TelemetryId);
            Assert.AreEqual(ExpectedRootId, context.TelemetryContextOperationId);
            Assert.AreEqual(ExpectedParentSpanId, context.TelemetryContextOperationParentId);
        }

        // SetParent sometimes accept NullObject 
        [TestMethod]
        public void SetParentWithNullObject()
        {
            var traceContext = new W3CTraceContext();
            var parentTraceContext = new NullObjectTraceContext();
            traceContext.SetParentAndStart(parentTraceContext);
            Assert.AreEqual(traceContext.StartTime, traceContext.CurrentActivity.StartTimeUtc);
        }
    }
}
