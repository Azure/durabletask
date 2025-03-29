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
    using DurableTask.Core.Common;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Threading.Tasks;
    using System.Threading;

    [TestClass]
    public class TypeExtensionsTests
    {
        [TestMethod]
        [DataRow(typeof(Task<TestClass1>), true)]
        [DataRow(typeof(CancellationToken), true)]
        [DataRow(typeof(Semaphore), true)]
        [DataRow(typeof(Task<(bool, string)>), true)]
        [DataRow(typeof(TestClass1), false)]
        [DataRow(typeof(TestClass2), false)]
        [DataRow(typeof(string), false)]
        public void IsEqualOrContainsCancellationTokenType(Type typeContaining, bool isTrue)
        {
            if (isTrue)
            {
                Assert.IsTrue(typeContaining.IsEqualOrContainsNativeType());
            }
            else
            {
                Assert.IsFalse(typeContaining.IsEqualOrContainsNativeType());
            }
        }

        [TestMethod]
        [DataRow(typeof(Task<TestClass1>), typeof(CancellationToken), true)]
        [DataRow(typeof(CancellationToken), typeof(CancellationToken), true)]
        [DataRow(typeof(Task<TestClass1>), typeof(TestClass1), true)]
        [DataRow(typeof(Task<(bool, string)>), typeof(string), true)]
        [DataRow(typeof(Task<(bool, string)>), typeof(bool), true)]
        [DataRow(typeof(TestClass1), typeof(TestClass1), true)]
        [DataRow(typeof(TestClass1), typeof(string), true)]
        [DataRow(typeof(TestClass1), typeof(double), true)]
        [DataRow(typeof(TestClass1), typeof(TestClass2), true)]
        [DataRow(typeof(TestClass2), typeof(double), true)]
        [DataRow(typeof(TestClass2), typeof(string), false)]
        public void Test_ContainsType(Type typeContaining, Type typeContained, bool isTrue)
        {
            if (isTrue)
            {
                Assert.IsTrue(typeContaining.IsEqualOrContainsType(typeContained));
            }
            else
            {
                Assert.IsFalse(typeContaining.IsEqualOrContainsType(typeContained));
            }
        }

        internal class TestClass1
        {
            public string abc;

            public TestClass2 t2;

            public TestClass2 NewT2 { get; set; }

            public TestClass1()
            {
                abc = string.Empty;
                t2 = new TestClass2();
            }
        }

        internal class TestClass2
        {
            public double def;

            public TestClass2()
            {
                def = 1.0;
            }
        }
    }
}
