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

namespace DurableTask.AzureStorage.Tests.Storage
{
    using System;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;

    [TestClass]
    public class StorageUriExtensionsTests
    {
        [TestMethod]
        [DataRow("https://foo.blob.core.windows.net", "foo")]
        [DataRow("https://bar.queue.unit.test", "bar")]
        [DataRow("https://baz.table", "baz")]
        [DataRow("https://dev.file.core.windows.net", "dev")]
        [DataRow("https://dev.unknown.core.windows.net", "dev.unknown.core.windows.net")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1", "devstoreaccount1")]
        [DataRow("http://[::1]/devstoreaccount1", "devstoreaccount1")]
        [DataRow("http://unit.test/custom/uri", "unit.test/custom/uri")]
        public void GetAccountName(string uri, string accountName)
        {
            Assert.AreEqual(accountName, new StorageUri(new Uri(uri, UriKind.Absolute)).GetAccountName());
        }
    }
}
