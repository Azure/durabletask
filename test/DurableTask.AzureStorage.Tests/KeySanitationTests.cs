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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Linq;
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Extension methods for String Test
    /// </summary>
    [TestClass]
    public class KeySanitationTests
    {
        [DataTestMethod]
        [DataRow("\r")]
        [DataRow("")]
        [DataRow("hello")]
        [DataRow("\uFFFF")]
        [DataRow("\u0000")]
        [DataRow("#")]
        [DataRow("%")]
        [DataRow("/")]
        [DataRow("\\")]
        [DataRow("?")]
        [DataRow("^")]
        [DataRow("^^")]
        [DataRow("^^^")]
        [DataRow("!@#$%^&*()_+=-0987654321d")]
        [DataRow("\'\"\\\r\n\t")]
        [DataRow("\u001F\u007F\u009F")]
        [DataRow(null)]

        public void TestRoundTrip(string original)
        {
            string sanitized = KeySanitation.EscapePartitionKey(original);
            string roundtrip = KeySanitation.UnescapePartitionKey(sanitized);

            Assert.AreEqual(original, roundtrip);

            if (sanitized != null)
            {
                Assert.IsTrue(sanitized.All(c => IsValid(c)));
            }

            bool IsValid(char c)
            {
                if (c == '\\' || c == '?' || c == '#' || c == '/')
                {
                    return false;
                }
                uint val = (uint)c;
                if (val <= 0x1F || (val >= 0x7F && val <= 0x9F))
                {
                    return false;
                }
                return true;
            }
        }
    }
}
