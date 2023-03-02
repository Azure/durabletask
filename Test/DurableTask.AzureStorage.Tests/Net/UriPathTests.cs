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
#nullable enable
namespace DurableTask.AzureStorage.Net
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class UriPathTests
    {
        [DataTestMethod]
        [DataRow("", "", "")]
        [DataRow("", "bar/baz", "bar/baz")]
        [DataRow("foo", "", "foo")]
        [DataRow("foo", "/", "foo/")]
        [DataRow("foo", "bar", "foo/bar")]
        [DataRow("foo", "/bar", "foo/bar")]
        [DataRow("foo/", "", "foo/")]
        [DataRow("foo/", "/", "foo/")]
        [DataRow("foo/", "bar", "foo/bar")]
        [DataRow("foo/", "/bar", "foo/bar")]
        [DataRow("/foo//", "//bar/baz", "/foo///bar/baz")]
        public void Combine(string path1, string path2, string expected)
        {
            Assert.AreEqual(expected, UriPath.Combine(path1, path2));
        }
    }
}
