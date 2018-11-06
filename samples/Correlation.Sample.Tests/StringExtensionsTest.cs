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

namespace Correlation.Sample.Tests
{
    using System;
    using Correlation.Samples;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StringExtensionsTest
    {
        [TestMethod]
        public void TestParseTraceParent()
        {
            var traceparentString = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
            var traceparent = traceparentString.ToTraceParent();
            Assert.AreEqual("00", traceparent.Version);
            Assert.AreEqual("4bf92f3577b34da6a3ce929d0e0e4736", traceparent.TraceId);
            Assert.AreEqual("00f067aa0ba902b7", traceparent.SpanId);
            Assert.AreEqual("01", traceparent.TraceFlags);
        }

        [TestMethod]
        public void TestParseTraceParentThrowsException()
        {
            var wrongTraceparentString = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7";
            Assert.ThrowsException<ArgumentException>(
                () => { wrongTraceparentString.ToTraceParent(); });
        }

        [TestMethod]
        public void TestParseTraceParenWithNull()
        {
            string someString = null;
            var result = someString?.ToTraceParent();
            Assert.IsNull(result);
        }
    }
}
