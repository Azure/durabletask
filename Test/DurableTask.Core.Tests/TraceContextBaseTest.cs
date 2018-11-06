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
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TraceContextBaseTest
    {
        [TestMethod]
        public void SerializeTraceContextBase()
        {
            Foo context = new Foo();
            context.StartAsNew();
            var expectedStartTime = context.StartTime;
            context.Comment = "hello";
            var json = context.SerializableTraceContext;
            var result = TraceContextBase.Restore(json);
            Assert.AreEqual(expectedStartTime, result.StartTime);
            Assert.AreEqual(context.Comment, ((Foo)result).Comment);
        }

        [TestMethod]
        public void SerializeAndDeserializeTraceContextWithParent()
        {
            TraceContextBase context = new Foo();
            context.StartAsNew();
            var expectedStartTime = context.StartTime;
            context.OrchestrationTraceContexts.Push(context); // Adding Orchestration Context it might include $type 
            var json = context.SerializableTraceContext;
            var result = TraceContextBase.Restore(json);
            Assert.AreEqual(expectedStartTime, result.StartTime);
        }

        [TestMethod]
        public void SerializeAndDeserializeTraceContextWithMultipleOrchestrationTraceContexts()
        {
            TraceContextBase one = new Foo() { Comment = "one" };
            TraceContextBase two = new Foo() { Comment = "two" };
            TraceContextBase three = new Foo() { Comment = "three" };
            one.OrchestrationTraceContexts.Push(one);
            one.OrchestrationTraceContexts.Push(two);
            one.OrchestrationTraceContexts.Push(three);
            var json = one.SerializableTraceContext;
            var restored = TraceContextBase.Restore(json);
            Assert.AreEqual("three", ((Foo)restored.OrchestrationTraceContexts.Pop()).Comment);
            Assert.AreEqual("two", ((Foo)restored.OrchestrationTraceContexts.Pop()).Comment);
            Assert.AreEqual("one", ((Foo)restored.OrchestrationTraceContexts.Pop()).Comment);
        }

        [TestMethod]
        public void DeserializeScenario()
        {
            var json  = "{ \"$id\":\"1\",\"$type\":\"DurableTask.Core.W3CTraceContext, DurableTask.Core\",\"Traceparent\":\"00-a422532de19d3e4f8f67af06f8f880c7-81354b086ec6fb41-02\",\"Tracestate\":null,\"ParentSpanId\":\"b69bc0f95af84240\",\"StartTime\":\"2019-05-03T23:43:27.6728211+00:00\",\"OrchestrationTraceContexts\":[{\"$id\":\"2\",\"$type\":\"DurableTask.Core.W3CTraceContext, DurableTask.Core\",\"Traceparent\":\"00-a422532de19d3e4f8f67af06f8f880c7-f86a8711d7226d42-02\",\"Tracestate\":null,\"ParentSpanId\":\"2ec2a64f22dbb143\",\"StartTime\":\"2019-05-03T23:43:12.7553182+00:00\",\"OrchestrationTraceContexts\":[{\"$ref\":\"2\"}]}]}";
            TraceContextBase context = TraceContextBase.Restore(json);
            Assert.AreEqual(DateTimeOffset.Parse("2019-05-03T23:43:27.6728211+00:00"), context.StartTime);
        }

        [TestMethod]
        public void DeserializeNullScenario()
        {
            TraceContextBase context = TraceContextBase.Restore(null);
            Assert.AreEqual(typeof(NullObjectTraceContext), context.GetType());
        }

        [TestMethod]
        public void DeserializeEmptyScenario()
        {
            TraceContextBase context = TraceContextBase.Restore("");
            Assert.AreEqual(typeof(NullObjectTraceContext), context.GetType());
        }

        [TestMethod]
        public void GetCurrentOrchestrationRequestTraceContextScenario()
        {
            TraceContextBase currentContext = new Foo();
            
            currentContext.OrchestrationTraceContexts.Push(GetNewRequestContext("foo"));
            currentContext.OrchestrationTraceContexts.Push(GetNewDependencyContext("bar"));

            var currentRequestContext = currentContext.GetCurrentOrchestrationRequestTraceContext();

            Assert.AreEqual(TelemetryType.Request, currentRequestContext.TelemetryType);
            Assert.AreEqual("foo", ((Foo)currentRequestContext).Comment);
        }

        [TestMethod]
        public void GetCurrentOrchestrationRequestTraceContextMultiOrchestratorScenario()
        {
            TraceContextBase currentContext = new Foo();

            currentContext.OrchestrationTraceContexts.Push(GetNewRequestContext("foo"));
            currentContext.OrchestrationTraceContexts.Push(GetNewDependencyContext("bar"));
            currentContext.OrchestrationTraceContexts.Push(GetNewRequestContext("baz"));
            currentContext.OrchestrationTraceContexts.Push(GetNewDependencyContext("qux"));

            var currentRequestContext = currentContext.GetCurrentOrchestrationRequestTraceContext();

            Assert.AreEqual(TelemetryType.Request, currentRequestContext.TelemetryType);
            Assert.AreEqual("baz", ((Foo)currentRequestContext).Comment);
        }

        [TestMethod]
        public void GetCurrentOrchestrationRequestTraceContextWithNoRequestTraceContextScenario()
        {
            TraceContextBase currentContext = new Foo();
            Assert.ThrowsException<InvalidOperationException>(() => currentContext.GetCurrentOrchestrationRequestTraceContext());
        }

        private Foo GetNewRequestContext(string comment)
        {
            var requestContext = new Foo() { Comment = comment };
            requestContext.TelemetryType = TelemetryType.Request;
            return requestContext;
        }

        private Foo GetNewDependencyContext(string comment)
        {
            var dependencyContext = new Foo() { Comment = comment };
            dependencyContext.TelemetryType = TelemetryType.Dependency;
            return dependencyContext;
        }

        class Foo : TraceContextBase
        {
            public Foo() : base() { }

            public string Comment { get; set; }

            public override TimeSpan Duration => TimeSpan.FromMilliseconds(10);

            public override string TelemetryId => "foo";

            public override string TelemetryContextOperationId { get; }

            public override string TelemetryContextOperationParentId { get; }

            public override void SetParentAndStart(TraceContextBase parentTraceContext)
            {
                throw new NotImplementedException();
            }

            public override void StartAsNew()
            {
                CurrentActivity = new Activity(this.OperationName);
                CurrentActivity.Start();
                StartTime = CurrentActivity.StartTimeUtc;
            }
        }
    }
}
