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
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask.Core.Common;
    using DurableTask.Core.Tracing;

    [TestClass]
    public class CommonTests
    {
        [TestMethod]
        public void DateTimeExtensionsIsSetTest()
        {
            Assert.IsTrue(DateTime.Now.IsSet());
            Assert.IsTrue(DateTime.MaxValue.IsSet());
            Assert.IsFalse(DateTime.MinValue.IsSet());
            Assert.IsFalse(DateTimeUtils.MinDateTime.IsSet());

            if (DateTimeUtils.MinDateTime == DateTime.FromFileTimeUtc(0))
            {
                Assert.IsFalse(DateTime.FromFileTimeUtc(0).IsSet());
            }
        }

        [TestMethod]
        public void ShouldValidateEventSource()
        {
            EventSourceAnalyzer.InspectAll(DefaultEventSource.Log);
        }
    }
}