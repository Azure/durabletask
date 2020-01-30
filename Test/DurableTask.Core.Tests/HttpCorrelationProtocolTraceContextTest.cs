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
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class HttpCorrelationProtocolTraceContextTest
    {
        [TestMethod]
        public void GetRootIdNormalCase()
        {
            var id = "|ea55fd0a-45699198bc3873c3.ea55fd0b_";
            var childId = "|ea55fd0a-45699198bc3873c3.ea55fd0b_ea55fd0c_";
            var expected = "ea55fd0a-45699198bc3873c3";

            var traceContext = new HttpCorrelationProtocolTraceContext();
            Assert.AreEqual(expected, traceContext.GetRootId(id));
            Assert.AreEqual(expected,traceContext.GetRootId(childId));
        }

        [TestMethod]
        public void GetRootIdWithNull()
        {
            string id = null;
            var traceContext = new HttpCorrelationProtocolTraceContext();
            Assert.IsNull(traceContext.GetRootId(id));
        }

        [TestMethod]
        public void GetRootIdWithMalformed()
        {
            // Currently it doesn't fail and doesn't throw exception.
            string id = "ea55fd0a-45699198bc3873c3";
            var traceContext = new HttpCorrelationProtocolTraceContext();
            Assert.AreEqual("ea55fd0a-45699198bc3873c3", traceContext.GetRootId(id));
        }

        [TestMethod]
        public void SetParentAndStartWithNullObject()
        {
            var traceContext = new HttpCorrelationProtocolTraceContext();
            var parentTraceContext = new NullObjectTraceContext();
            traceContext.SetParentAndStart(parentTraceContext);
            Assert.AreEqual(traceContext.StartTime, traceContext.CurrentActivity.StartTimeUtc);
        }
    }
}
