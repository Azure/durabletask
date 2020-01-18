﻿//  ----------------------------------------------------------------------------------
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
    using Microsoft.ApplicationInsights.W3C;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    [TestClass]
    public class W3CTraceContextTest
    {
        // SetParent sometimes accept NullObject 
        [TestMethod]
        public void SetParentWithNullObject()
        {
            var traceContext = new W3CTraceContext();
            var parentTraceContext = new NullObjectTraceContext();
            traceContext.SetParentAndStart(parentTraceContext);
#pragma warning disable 618 // IsW3CActviity() is deprecated. However, it is required for W3C for this version.
            Assert.IsTrue(traceContext.CurrentActivity.IsW3CActivity());
#pragma warning restore 618
            Assert.AreEqual(traceContext.StartTime, traceContext.CurrentActivity.StartTimeUtc);
        }
    }
}
